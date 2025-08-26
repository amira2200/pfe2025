import logging
import os
import io
from datetime import datetime

from azure.functions import HttpRequest, HttpResponse
import pandas as pd

# ReportLab – text only (no tables)
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet

try:
    from azure.storage.blob import BlobServiceClient
except Exception:
    BlobServiceClient = None


# ---------- Helpers ----------
def _p(text, style):
    return Paragraph(text.replace("\n", "<br/>"), style)


def _upload_to_blob(binary_bytes: bytes, blob_name: str):
    conn_str = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
    container = os.getenv("BLOB_CONTAINER_NAME")
    if not conn_str or not container:
        raise RuntimeError("Missing Blob env vars")
    if not BlobServiceClient:
        raise RuntimeError("azure-storage-blob not available")

    bsc = BlobServiceClient.from_connection_string(conn_str)
    cc = bsc.get_container_client(container)
    try:
        cc.create_container()  # ok if it already exists
    except Exception:
        pass

    bc = cc.get_blob_client(blob_name)
    bc.upload_blob(binary_bytes, overwrite=True,
                   content_type="application/pdf")
    logging.info("Uploaded %s/%s", container, blob_name)
    return f"blob://{container}/{blob_name}"


# ---------- Finance (text) ----------
def build_finance_text_pdf(conn, title="Finance Summary"):
    """
    Reads:
      - sales_agg(sku, qty_sold, revenue_ht, price_ht_from_sales)
      - stock_snapshot(sku, stock_qty, stock_value_ht, price_ht_priority)
      - unified_data (optional, just for info)
    Renders a narrative PDF with paragraphs only.
    """
    def q(sql, cols):
        try:
            return pd.read_sql(sql, conn)
        except Exception:
            return pd.DataFrame(columns=cols)

    sales = q("SELECT * FROM sales_agg",
              ["sku", "qty_sold", "revenue_ht", "price_ht_from_sales"])
    stock = q("SELECT * FROM stock_snapshot",
              ["sku", "stock_qty", "stock_value_ht", "price_ht_priority"])
    uni = q("SELECT email, sku, quantity, total_ttc, is_valid FROM unified_data",
            ["email", "sku", "quantity", "total_ttc", "is_valid"])

    revenue_total = float(sales["revenue_ht"].sum()
                          ) if not sales.empty else 0.0
    qty_total = float(sales["qty_sold"].sum()) if not sales.empty else 0.0
    price_median = float(
        sales["price_ht_from_sales"].median()) if not sales.empty else 0.0

    inv_total = float(stock["stock_value_ht"].sum()
                      ) if not stock.empty else 0.0
    inv_items = int((stock["stock_qty"].fillna(
        0) > 0).sum()) if not stock.empty else 0
    inv_qty_total = float(stock["stock_qty"].sum()
                          ) if "stock_qty" in stock.columns else 0.0

    # Build the PDF
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4, leftMargin=16*mm, rightMargin=16*mm)
    styles = getSampleStyleSheet()
    h1, h2, txt = styles["Title"], styles["Heading2"], styles["Normal"]

    story = []
    story += [
        _p(title, h1),
        _p(f"Generated: {datetime.now():%Y-%m-%d %H:%M}", txt),
        Spacer(1, 6*mm),

        _p("1) Sales (HT)", h2),
        _p(f"• Total revenue (HT): {revenue_total:,.2f}", txt),
        _p(f"• Quantity sold (Σ): {qty_total:,.0f}", txt),
        _p(f"• Median unit price (HT) from sales: {price_median:,.2f}", txt),
        Spacer(1, 4*mm),

        _p("2) Inventory (HT)", h2),
        _p(f"• Inventory value (HT): {inv_total:,.2f}", txt),
        _p(f"• SKUs with stock > 0: {inv_items}", txt),
        _p(f"• Total stock quantity: {inv_qty_total:,.0f}", txt),
        Spacer(1, 4*mm),
    ]

    # Optional: small contextual note about unified_data presence
    if not uni.empty:
        valid_count = int(uni["is_valid"].fillna(
            False).sum()) if "is_valid" in uni.columns else 0
        story += [
            _p("3) Import context", h2),
            _p(f"• Unified rows in scope: {len(uni)}", txt),
            _p(f"• Valid rows: {valid_count}", txt),
        ]

    doc.build(story)
    buf.seek(0)
    return buf.getvalue()


# ---------- Errors (text) ----------
def build_errors_text_pdf(conn, title="Order Errors"):
    """
    Reads:
      - order_errors (email, sku, quantity, error_reason)
    Renders a list of errors as plain text paragraphs.
    """
    try:
        errs = pd.read_sql(
            "SELECT email, sku, quantity, error_reason FROM order_errors ORDER BY email, sku",
            conn
        )
    except Exception:
        errs = pd.DataFrame(columns=["email", "sku, quantity", "error_reason"])

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4, leftMargin=16*mm, rightMargin=16*mm)
    styles = getSampleStyleSheet()
    h1, h2, txt = styles["Title"], styles["Heading2"], styles["Normal"]

    story = []
    story += [
        _p(title, h1),
        _p(f"Generated: {datetime.now():%Y-%m-%d %H:%M}", txt),
        Spacer(1, 6*mm),
    ]

    if errs.empty:
        story.append(_p("No invalid orders.", txt))
    else:
        story += [
            _p(f"Invalid orders: {len(errs)}", h2),
            Spacer(1, 2*mm),
        ]
        # One paragraph per error row (flows across pages automatically)
        for _, r in errs.iterrows():
            line = (
                f"• Email: {r.get('email','')}  |  SKU: {r.get('sku','')}  |  "
                f"Qty: {int(r.get('quantity') or 0)}  |  Reason: {r.get('error_reason','')}"
            )
            story.append(_p(line, txt))

    doc.build(story)
    buf.seek(0)
    return buf.getvalue()


# ---------- MAIN (no params; always create 2 files) ----------
def main(req: HttpRequest) -> HttpResponse:
    logging.info("Generate text PDFs (finance + errors)")

    # Filenames in your container
    finance_blob = "finance_report_text.pdf"
    errors_blob = "errors_report_text.pdf"

    # DB
    try:
        from shared import get_connection
        conn = get_connection()
    except Exception as e:
        logging.exception("DB connection failed")
        return HttpResponse("DB error: " + str(e), status_code=500)

    try:
        # Build binaries
        pdf_fin = build_finance_text_pdf(conn, title="Finance Summary")
        pdf_err = build_errors_text_pdf(conn,  title="Order Errors")

        # Upload both
        uri_fin = _upload_to_blob(pdf_fin, finance_blob)
        uri_err = _upload_to_blob(pdf_err, errors_blob)

        msg = f"Uploaded:\n - {uri_fin}\n - {uri_err}\n"
        return HttpResponse(msg, status_code=200)

    finally:
        try:
            conn.close()
        except Exception:
            pass
