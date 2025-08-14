import logging
import traceback
import os
import json
import re
import unicodedata
import pandas as pd
import azure.functions as func
from io import BytesIO
from azure.storage.blob import BlobServiceClient

# ===================== Utils =====================


def load_excel_from_blob(file_name: str) -> pd.DataFrame:
    conn_str = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
    container = os.getenv("BLOB_CONTAINER_NAME")
    if not conn_str:
        raise RuntimeError("BLOB_STORAGE_CONNECTION_STRING manquant")
    if not container:
        raise RuntimeError("BLOB_CONTAINER_NAME manquant")
    bsc = BlobServiceClient.from_connection_string(conn_str)
    cc = bsc.get_container_client(container)
    bc = cc.get_blob_client(file_name)
    if not bc.exists():
        existing = [b.name for b in cc.list_blobs()]
        raise RuntimeError(
            f"Blob introuvable: {file_name} dans {container}. Exemples: {existing[:10]}")
    data = bc.download_blob().readall()
    return pd.read_excel(BytesIO(data))


def parse_payload(payload):
    """Robuste: payload dict ou str JSON; gère qty/quantity."""
    try:
        if isinstance(payload, (dict, list)):
            data = payload
        elif isinstance(payload, str) and payload.strip():
            data = json.loads(payload)
        else:
            return pd.Series([None, None, 0])

        email = (data.get("email") or "").strip().lower()
        items = data.get("items") or []
        sku, qty = None, 0
        if items and isinstance(items, list):
            first = items[0] or {}
            sku = (first.get("sku") or first.get("itemCode") or "").strip()
            q = first.get("qty")
            if q is None:
                q = first.get("quantity")
            try:
                qty = int(q)
            except Exception:
                qty = 0
        return pd.Series([email, sku, qty])
    except Exception as e:
        logging.warning(f"Erreur parsing payload : {e}")
        return pd.Series([None, None, 0])


# ---------- Normalisation SKU ----------
SKU_KEEP = re.compile(r"[A-Z0-9]+")


def normalize_sku(x: str) -> str | None:
    if x is None:
        return None
    s = str(x)
    s = unicodedata.normalize("NFKC", s).replace("\u00A0", " ")
    s = s.strip().upper()
    s = "".join(SKU_KEEP.findall(s))
    return s or None


# ---------- Nettoyage numérique ----------
NUM_RE = re.compile(r"[^\d,.\-]")  # garde chiffres, point, virgule, signe -


def _first_notna(row):
    for v in row:
        if pd.notna(v):
            return v
    return pd.NA


def _pick_series(df: pd.DataFrame, name: str) -> pd.Series | None:
    """Retourne une Series pour 'name' même si colonnes dupliquées."""
    if name not in df.columns:
        return None
    obj = df[name]
    if isinstance(obj, pd.Series):
        return obj
    return obj.apply(_first_notna, axis=1)


def _to_number(obj) -> pd.Series:
    """Accepte Series OU DataFrame (colonnes en double)."""
    if isinstance(obj, pd.DataFrame):
        obj = obj.apply(_first_notna, axis=1)
    s = obj.astype(str)
    s = s.apply(lambda x: NUM_RE.sub("", x))
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _drop_dupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Supprime les colonnes dupliquées (garde la 1re)."""
    return df.loc[:, ~df.columns.duplicated(keep="first")]


def clean_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.astype(
        str).str.strip().str.lower().str.replace(" ", "_")

    if "sku" in df.columns:
        s = _pick_series(df, "sku")
        df["sku"] = s.apply(normalize_sku)

    if "email" in df.columns:
        s = _pick_series(df, "email")
        df["email"] = s.astype(str).str.lower().str.strip()

    if "quantity" in df.columns:
        s = _pick_series(df, "quantity")
        df["quantity"] = pd.to_numeric(
            s, errors="coerce").fillna(0).astype(int)

    for col in ["stock_qty", "purchase_value", "retail_value", "retail_value_ttc",
                "net_wo_tax", "net_w_tax"]:
        if col in df.columns:
            s = _pick_series(df, col)
            df[col] = _to_number(s)

    # Dédup finale
    df = _drop_dupe_columns(df)
    return df


def apply_business_rules(df: pd.DataFrame, stock_df: pd.DataFrame) -> pd.DataFrame:
    if "is_valid_sku" not in df.columns:
        df["is_valid_sku"] = df["sku"].isin(stock_df["sku"])
    df["is_valid_qty"] = df["quantity"] > 0
    df["is_valid"] = df["is_valid_sku"] & df["is_valid_qty"]
    df["error_reason"] = df.apply(
        lambda r: "Invalid SKU" if not r["is_valid_sku"]
        else ("Invalid Quantity" if not r["is_valid_qty"] else None),
        axis=1
    )
    return df


def calculate_financials(df: pd.DataFrame) -> pd.DataFrame:
    qty = pd.to_numeric(df.get("quantity"), errors="coerce").fillna(0)

    # Si pas de HT, essayer de dériver depuis TTC ventes
    ht = df.get("retail_value")
    if ht is None or ht.isna().all() or (ht.fillna(0) == 0).all():
        ttc_unit = df.get("retail_value_ttc")
        if ttc_unit is not None:
            df["total_ttc"] = qty * ttc_unit.fillna(0)
            df["tva"] = df["total_ttc"] * (20/120)
            df["total_ht"] = df["total_ttc"] - df["tva"]
        else:
            df["total_ht"] = 0.0
            df["tva"] = 0.0
            df["total_ttc"] = 0.0
        return df

    # Cas général: HT dispo
    df["total_ht"] = qty * df["retail_value"].fillna(0)
    df["tva"] = df["total_ht"] * 0.20
    df["total_ttc"] = df["total_ht"] + df["tva"]
    return df


def extract_retry_table(conn) -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM retry_table", conn)
    df[["email", "sku", "quantity"]] = df["payload"].apply(parse_payload)
    df["source"] = "middleware"
    return df


def extract_excel_from_blob(file_name, source_name) -> pd.DataFrame:
    df = load_excel_from_blob(file_name)
    logging.info(f"📄 Colonnes lues ({file_name}): {list(df.columns)}")
    df["source"] = source_name
    return df


def load_to_postgres(df: pd.DataFrame, conn):
    # Remplir 0 pour TOUTES les colonnes numériques -> pas de NULL en BDD
    numeric_cols = ["quantity", "stock_qty", "purchase_value", "retail_value",
                    "total_ht", "tva", "total_ttc"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    if "is_valid" in df.columns:
        df["is_valid"] = df["is_valid"].fillna(False)

    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS unified_data (
            email TEXT,
            sku TEXT,
            quantity INT,
            source TEXT,
            stock_qty NUMERIC,
            purchase_value NUMERIC,
            retail_value NUMERIC,
            total_ht NUMERIC,
            tva NUMERIC,
            total_ttc NUMERIC,
            is_valid BOOLEAN,
            error_reason TEXT
        )
    """)
    conn.commit()
    cur.execute("TRUNCATE unified_data")

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO unified_data (
                email, sku, quantity, source, stock_qty,
                purchase_value, retail_value, total_ht, tva, total_ttc,
                is_valid, error_reason
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row.get("email") or "",
            row.get("sku") or "",
            int(row.get("quantity") or 0),
            row.get("source") or "",
            float(row.get("stock_qty") or 0),
            float(row.get("purchase_value") or 0),
            float(row.get("retail_value") or 0),
            float(row.get("total_ht") or 0),
            float(row.get("tva") or 0),
            float(row.get("total_ttc") or 0),
            bool(row.get("is_valid")) if row.get(
                "is_valid") is not None else False,
            row.get("error_reason")
        ))
    conn.commit()
    cur.close()

# ---------- DIAG: staging des SKU ----------


def upsert_staging(conn, table_name: str, skus: list[str]):
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} (sku TEXT PRIMARY KEY)")
    conn.commit()
    cur.execute(f"TRUNCATE {table_name}")
    args = [(s,) for s in skus if s]
    if args:
        cur.executemany(
            f"INSERT INTO {table_name} (sku) VALUES (%s) ON CONFLICT (sku) DO NOTHING",
            args
        )
    conn.commit()
    cur.close()


def _sample(seq, n=10):
    return [x for x in list(seq) if x][:n]

# ---------- Canonicalisation & détection colonnes ----------


def _canon(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-zA-Z0-9]+", " ", s).strip().lower()
    return s


def guess_stock_columns(df: pd.DataFrame):
    """Devine Item (sku), Qté de l'image (stock_qty), Valo. PA (purchase), Valo. PR (retail)."""
    canon = {c: _canon(c) for c in df.columns}
    sku_col = qty_col = pa_col = pr_col = None
    for col, c in canon.items():
        if sku_col is None and ("item" in c or "code" in c or c == "sku"):
            sku_col = col
        if qty_col is None and (("qte" in c or "quantite" in c or "stock" in c) and ("image" in c or "img" in c or "qty" in c)):
            qty_col = col
        if pa_col is None and (("valo" in c or "value" in c or "price" in c) and ("pa" in c or "purchase" in c or "buy" in c or "cost" in c)):
            pa_col = col
        if pr_col is None and (("valo" in c or "value" in c or "price" in c) and ("pr" in c or "retail" in c or "sell" in c or "sale" in c)):
            pr_col = col
    mapping = {}
    if sku_col:
        mapping[sku_col] = "sku"
    if qty_col:
        mapping[qty_col] = "stock_qty"
    if pa_col:
        mapping[pa_col] = "purchase_value"
    if pr_col:
        mapping[pr_col] = "retail_value"
    return mapping


def guess_sales_columns(df: pd.DataFrame):
    """Ventes: Item Code, Qty, Net w/o Tax, Net w/Tax."""
    canon = {c: _canon(c) for c in df.columns}
    mapping = {}
    for col, c in canon.items():
        if "item" in c and "code" in c:
            mapping[col] = "sku"
        elif c in ("qty", "quantity", "qte"):
            mapping[col] = "quantity"
        elif ("net" in c and "tax" in c and ("wo" in c or "without" in c or "sans" in c or "w_o" in c)):
            mapping[col] = "net_wo_tax"
        elif ("net" in c and "tax" in c):
            mapping[col] = "net_w_tax"
    return mapping


def coalesce(*vals):
    for v in vals:
        if pd.notna(v):
            return v
    return None

# ======================= MAIN =======================


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.getLogger().setLevel(logging.INFO)
    try:
        logging.info("🚀 Démarrage integrate_data")

        # import tardif
        try:
            from shared import get_connection
        except Exception:
            logging.exception("❌ Import get_connection échoué")
            return func.HttpResponse("Erreur import shared.get_connection:\n"+traceback.format_exc(), status_code=500)

        try:
            conn = get_connection()
        except Exception:
            logging.exception("❌ Connexion PostgreSQL échouée")
            return func.HttpResponse("Erreur connexion PostgreSQL:\n"+traceback.format_exc(), status_code=500)

        # 1) EXTRACTIONS
        retry_df = extract_retry_table(conn)
        ventes_df = extract_excel_from_blob("Ventes 2025 UAE.XLSX", "sales")
        stock_df = extract_excel_from_blob("Image de stock UAE.XLSX", "stock")
        logging.info(f"✅ retry_table: {len(retry_df)} lignes")

        # Retirer 'source' pour éviter source_x/source_y
        for df in (stock_df, ventes_df):
            if "source" in df.columns:
                df.drop(columns=["source"], inplace=True)

        # 2) RENAMES robustes
        stock_map = guess_stock_columns(stock_df)
        if stock_map:
            logging.info("🧭 Mapping stock détecté: %s", stock_map)
            stock_df.rename(columns=stock_map, inplace=True)
        else:
            logging.warning(
                "⚠️ Mapping stock non détecté. Colonnes: %s", list(stock_df.columns))

        sales_map = guess_sales_columns(ventes_df)
        if sales_map:
            logging.info("🧭 Mapping ventes détecté: %s", sales_map)
            ventes_df.rename(columns=sales_map, inplace=True)
        else:
            logging.warning(
                "⚠️ Mapping ventes non détecté. Colonnes: %s", list(ventes_df.columns))

        # 1er token avant normalisation
        if "sku" in stock_df.columns:
            stock_df["sku"] = stock_df["sku"].astype(
                str).str.strip().str.split().str[0]
        if "sku" in ventes_df.columns:
            ventes_df["sku"] = ventes_df["sku"].astype(
                str).str.strip().str.split().str[0]

        # 3) CLEAN
        stock_df = clean_and_normalize(stock_df)
        retry_df = clean_and_normalize(retry_df)
        ventes_df = clean_and_normalize(ventes_df)

        # (sécurité) dédupliquer
        stock_df = _drop_dupe_columns(stock_df)
        ventes_df = _drop_dupe_columns(ventes_df)

        # ---- PRIX depuis Ventes (médiane par SKU), vectorisé
        price_cols = [c for c in ["net_wo_tax", "net_w_tax"]
                      if c in ventes_df.columns]
        price_cols = list(dict.fromkeys(price_cols))
        if price_cols and "sku" in ventes_df.columns:
            sp = ventes_df[["sku"] + price_cols].copy()
            sales_price = sp.groupby(
                "sku", as_index=False).median(numeric_only=True)
            if "net_wo_tax" not in sales_price.columns:
                sales_price["net_wo_tax"] = pd.NA
            if "net_w_tax" not in sales_price.columns:
                sales_price["net_w_tax"] = pd.NA
            sales_price["retail_from_sales"] = sales_price["net_wo_tax"].fillna(
                sales_price["net_w_tax"] / 1.20)
        else:
            sales_price = pd.DataFrame(columns=["sku", "retail_from_sales"])

        # Diagnostic
        logging.info(
            "📈 stock non-null -> stock_qty:%d purchase:%d retail:%d",
            int(stock_df["stock_qty"].notna().sum()
                ) if "stock_qty" in stock_df else -1,
            int(stock_df["purchase_value"].notna().sum()
                ) if "purchase_value" in stock_df else -1,
            int(stock_df["retail_value"].notna().sum()
                ) if "retail_value" in stock_df else -1
        )

        # Colonnes utiles pour les merges
        keep_cols = [c for c in ["sku", "stock_qty",
                                 "purchase_value", "retail_value"] if c in stock_df.columns]
        stock_df = stock_df[keep_cols]
        ventes_qty = (ventes_df[["sku", "quantity"]].rename(columns={"quantity": "quantity_sales"})
                      if "sku" in ventes_df.columns and "quantity" in ventes_df.columns
                      else pd.DataFrame(columns=["sku", "quantity_sales"]))

        # 4) FUSIONS
        merged_df = retry_df.merge(
            stock_df, on="sku", how="left", indicator=True)
        merged_df.rename(columns={"_merge": "_merge_stock"}, inplace=True)

        if not sales_price.empty:
            merged_df = merged_df.merge(
                sales_price[["sku", "retail_from_sales"]], on="sku", how="left")
        if not ventes_qty.empty:
            merged_df = merged_df.merge(ventes_qty, on="sku", how="left")

        # retail_value final = stock.retail_value OU ventes (HT calculé) -> vectorisé (pas d'apply)
        if "retail_value" in merged_df.columns:
            merged_df["retail_value"] = merged_df["retail_value"].fillna(
                merged_df.get("retail_from_sales"))
        else:
            merged_df["retail_value"] = merged_df.get("retail_from_sales")

        # 5) RÈGLES
        merged_df["is_valid_sku"] = (merged_df["_merge_stock"] == "both")
        merged_df.drop(
            columns=["_merge_stock", "retail_from_sales"], inplace=True, errors="ignore")
        merged_df = apply_business_rules(merged_df, stock_df)

        # 6) CALCULS
        merged_df = calculate_financials(merged_df)
        logging.info(
            "💰 lignes avec retail_value(HT)>0: %d ; total_ttc>0: %d",
            int((merged_df.get("retail_value", 0).fillna(0) > 0).sum()),
            int((merged_df.get("total_ttc", 0).fillna(0) > 0).sum())
        )

        # 7) CHARGEMENT (sans NULL numériques)
        try:
            load_to_postgres(merged_df, conn)
        finally:
            try:
                conn.close()
            except Exception:
                logging.warning(
                    "⚠️ Fermeture connexion PG a échoué", exc_info=True)

        summary = (
            f"retry_rows={len(retry_df)}, stock_rows={len(stock_df)}, ventes_rows={len(ventes_df)} | "
            f"retry_sample={_sample(set(retry_df['sku'].dropna().unique()))} | "
            f"stock_sample={_sample(set(stock_df['sku'].dropna().unique()))}"
        )
        logging.info("📤 SUMMARY: %s", summary)
        return func.HttpResponse("Pipeline exécuté et données insérées dans unified_data\n" + summary, status_code=200)

    except Exception:
        logging.exception("🔥 Exception non gérée")
        return func.HttpResponse("Erreur interne:\n"+traceback.format_exc(), status_code=500)
