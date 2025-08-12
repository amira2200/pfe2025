import logging
import re
import json
import pandas as pd
import psycopg2
from azure.functions import HttpRequest, HttpResponse
from shared import get_connection  # Connexion PostgreSQL

# 📌 Ajouts pour lecture depuis Azure Blob Storage
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import os

# =======================
# Fonction pour lire un Excel depuis Blob
# =======================


def load_excel_from_blob(file_name):
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("BLOB_STORAGE_CONNECTION_STRING")
    )
    container_client = blob_service_client.get_container_client(
        os.getenv("BLOB_CONTAINER_NAME")
    )
    blob_data = container_client.download_blob(file_name).readall()
    return pd.read_excel(BytesIO(blob_data))

# =======================
# 1. EXTRACTION
# =======================


def extract_retry_table(conn):
    df = pd.read_sql("SELECT * FROM retry_table", conn)
    df[["email", "sku", "quantity"]] = df["payload"].apply(parse_payload)
    df["source"] = "middleware"
    return df


def extract_excel_from_blob(file_name, source_name):
    df = load_excel_from_blob(file_name)
    df["source"] = source_name
    return df


def parse_payload(payload):
    try:
        data = json.loads(payload)
        email = data.get("email", "").strip()
        items = data.get("items", [])
        sku, qty = None, None
        if items:
            sku = items[0].get("sku", "").strip()
            qty = items[0].get("qty", 0)
        return pd.Series([email, sku, qty])
    except Exception as e:
        logging.warning(f"Erreur parsing payload : {e}")
        return pd.Series([None, None, None])

# =======================
# 2. CLEANING & NORMALISATION
# =======================


def clean_and_normalize(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    if "sku" in df.columns:
        df["sku"] = df["sku"].astype(str).str.upper().str.strip()
        df["sku"] = df["sku"].apply(lambda x: re.sub(r"[^A-Z0-9]", "", x))
    if "email" in df.columns:
        df["email"] = df["email"].astype(str).str.lower().str.strip()
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(
            df["quantity"], errors="coerce").fillna(0).astype(int)
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    return df

# =======================
# 3. RÈGLES MÉTIERS
# =======================


def apply_business_rules(df, stock_df):
    df["is_valid_sku"] = df["sku"].isin(stock_df["sku"])
    df["is_valid_qty"] = df["quantity"] > 0
    df["is_valid"] = df["is_valid_sku"] & df["is_valid_qty"]
    df["error_reason"] = df.apply(lambda row: "Invalid SKU" if not row["is_valid_sku"]
                                  else ("Invalid Quantity" if not row["is_valid_qty"] else None), axis=1)
    return df

# =======================
# 4. CALCULS FINANCIERS
# =======================


def calculate_financials(df):
    if "retail_value" in df.columns:
        df["total_ht"] = df["quantity"] * df["retail_value"].fillna(0)
        df["tva"] = df["total_ht"] * 0.20
        df["total_ttc"] = df["total_ht"] + df["tva"]
    else:
        df["total_ht"] = df["tva"] = df["total_ttc"] = 0
    return df

# =======================
# 5. CHARGEMENT
# =======================


def load_to_postgres(df, conn):
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
            row.get("email"),
            row.get("sku"),
            row.get("quantity"),
            row.get("source"),
            row.get("stock_qty"),
            row.get("purchase_value"),
            row.get("retail_value"),
            row.get("total_ht"),
            row.get("tva"),
            row.get("total_ttc"),
            row.get("is_valid"),
            row.get("error_reason")
        ))
    conn.commit()
    cur.close()

# =======================
# MAIN FUNCTION
# =======================


def main(req: HttpRequest) -> HttpResponse:
    logging.info("🚀 Démarrage du pipeline d'intégration et normalisation")

    try:
        conn = get_connection()

        # Extraction depuis PostgreSQL
        retry_df = extract_retry_table(conn)

        # Extraction depuis Azure Blob
        ventes_df = extract_excel_from_blob("Ventes_2025_UAE.xlsx", "sales")
        stock_df = extract_excel_from_blob("Image_de_stock_UAE.xlsx", "stock")

        # Normalisation du stock
        stock_df.rename(columns={
            "item": "sku",
            "qté_de_l'image": "stock_qty",
            "valo._pa": "purchase_value",
            "valo._pr": "retail_value"
        }, inplace=True)
        stock_df = clean_and_normalize(stock_df)

        # Cleaning & normalisation
        retry_df = clean_and_normalize(retry_df)
        ventes_df = clean_and_normalize(ventes_df)

        # Fusion des données
        merged_df = retry_df.merge(stock_df, on="sku", how="left")
        if "sku" in ventes_df.columns:
            merged_df = merged_df.merge(
                ventes_df, on="sku", how="left", suffixes=("", "_sales"))

        # Application des règles métiers
        merged_df = apply_business_rules(merged_df, stock_df)

        # Calculs financiers
        merged_df = calculate_financials(merged_df)

        # Chargement en base
        load_to_postgres(merged_df, conn)
        conn.close()

        logging.info("✅ Pipeline terminé avec succès")
        return HttpResponse("Pipeline exécuté et données insérées dans unified_data", status_code=200)

    except Exception as e:
        logging.error(f"❌ Erreur dans le pipeline : {e}")
        return HttpResponse(f"Erreur : {e}", status_code=500)
