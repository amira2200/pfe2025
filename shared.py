import zeep
import psycopg2
import os
import json
import logging
import requests
from zeep import Client, Settings
from zeep.transports import Transport
from requests.auth import HTTPBasicAuth
import traceback
from zeep.helpers import serialize_object
from reportlab.pdfgen import canvas
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
import base64
from io import BytesIO
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.platypus import Table, TableStyle

order_schema = {
    "type": "object",
    "required": ["orderType", "orderNumber", "orderDate", "firstName", "lastName", "email", "totalAmount", "items"],
    "properties": {
        "orderType": {"type": "string", "enum": ["sale", "return", "replacement"]},
        "orderNumber": {"type": "string"},
        "orderDate": {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$"},
        "email": {"type": "string", "format": "email"},
        "firstName": {"type": "string"},
        "lastName": {"type": "string"},
        "totalAmount": {"type": "number"},
        "paymentType": {"type": ["number", "null"]},
        "items": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["sku", "quantity", "originalPrice", "finalPrice"],
                "properties": {
                    "sku": {"type": "string"},
                    "quantity": {"type": "number"},
                    "originalPrice": {"type": "number"},
                    "finalPrice": {"type": "number"},
                    "promotionId": {"type": ["string", "null"]}
                }
            }
        }
    }
}


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )


def enregistrer_commande(data, status, error_message=None):
    try:
        logging.info(
            f"💾 Tentative d'insertion dans retry_table : Status={status}, OrderNumber={data.get('orderNumber', 'UNKNOWN')}")
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO retry_table (external_id, payload, status, error_message)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (external_id) DO NOTHING
        """, (
            data.get("orderNumber", "UNKNOWN"),
            json.dumps(data),
            status,
            error_message
        ))
        conn.commit()
        cur.close()
        conn.close()
        logging.info("✅ Insertion réussie dans retry_table.")
    except Exception as db_error:
        logging.error(
            f"❌ Erreur lors de l'insertion dans retry_table : {str(db_error)}")
        # 💡 Affiche la stack trace complète pour débogage
        logging.error(traceback.format_exc())


def get_pending_orders():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, external_id, payload FROM retry_table WHERE status = 'Pending'")
    orders = cur.fetchall()
    cur.close()
    conn.close()
    return orders


def update_order_status(order_id, status, message=None):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE retry_table SET status=%s, error_message=%s, retries=retries + 1 WHERE id=%s",
        (status, message, order_id)
    )
    conn.commit()
    cursor.close()
    conn.close()


def build_zeep_order(payload):
    return {
        "clientContext": {
            "DatabaseId": os.getenv("CEGID_DATABASE_ID")
        },
        "createRequest": {
            "DeliveryAddress": {
                "FirstName": payload.get("firstName", "Client")
            },
            "Header": {
                "Active": True,
                "Comment": f"https://pdfblobstorageuae.blob.core.windows.net/pdf-generation/tmp/{payload['orderNumber']}.pdf",
                "CustomerId": "SC00004000",
                "Date": payload["orderDate"],
                "ExternalReference": payload["orderNumber"],
                "InternalReference": "RET-04-" + payload["orderNumber"].replace("S-", ""),
                "OmniChannel": {
                    "BillingStatus": "Totally",
                    "DeliveryType": "ShipByCentral",
                    "FollowUpStatus": "Validated",
                    "PaymentStatus": "Totally",
                    "ReturnStatus": "NotReturned",
                    "ShippingStatus": "Totally"
                },
                "Origin": "ECommerce",
                "StoreId": "IQST01",
                "Type": "Receipt",
                "UserDefinedTables": {
                    "UserDefinedTable": [{
                        "Id": 1,
                        "Value": "SAL05"
                    }]
                },
                "WarehouseId": "IQWH01"
            },
            "Lines": {
                "Create_Line": [{
                    "ExternalReference": "0",
                    "ItemIdentifier": {"Reference": item["sku"]},
                    "NetUnitPrice": item["finalPrice"],
                    "Origin": "ECommerce",
                    "Quantity": -abs(item["quantity"]),
                    "UnitPrice": item["originalPrice"]
                } for item in payload["items"]]
            },
            "Payments": {
                "Create_Payment": [{
                    "Amount": -abs(payload["totalAmount"]),
                    "CurrencyId": "AED",
                    "DueDate": payload["orderDate"],
                    "Id": 1,
                    "IsReceivedPayment": 0,
                    "MethodId": payload.get("paymentType", 1)
                }]
            }
        }
    }


def send_order(order):
    wsdl = os.getenv("CEGID_WSDL_URL")
    user = os.getenv("cegid_soap_username")
    pwd = os.getenv("cegid_soap_password")

    session = requests.Session()
    session.auth = HTTPBasicAuth(user, pwd)

    client = Client(
        wsdl=wsdl,
        settings=Settings(strict=False, xml_huge_tree=True),
        transport=Transport(session=session)
    )

    try:
        response = client.service.Create(**order)
        logging.info(
            f"📨 Réponse SOAP complète (Zeep) : {serialize_object(response)}")
        return response
    except Exception as e:
        logging.error(f"💥 Exception Zeep : {str(e)}")
        raise


def process_pending_orders():
    orders = get_pending_orders()
    total = len(orders)

    logging.info(
        f"📦 Nombre total de commandes récupérées avec statut 'Pending' : {total}")

    if total == 0:
        logging.info("📭 Aucune commande à traiter.")
        return

    for index, (order_id, _, payload) in enumerate(orders, start=1):
        logging.info(
            f"🔄 Traitement de la commande {index}/{total} (ID: {order_id})...")

        try:
            payload_dict = payload if isinstance(
                payload, dict) else json.loads(payload)
            logging.info(
                f"🔍 Payload parsé : {payload_dict.get('orderNumber')}")

            order = build_zeep_order(payload_dict)
            logging.info(
                f"📦 Order Cegid généré pour : {payload_dict.get('orderNumber')}")

            response = send_order(order)
            logging.info(
                f"📡 Réponse SOAP reçue pour : {payload_dict.get('orderNumber')}")

            if response:
                update_order_status(order_id, "Success", None)
                logging.info(
                    f"✅ Succès : commande envoyée {payload_dict.get('orderNumber')}")

                # ✅ Génération du PDF
                pdf_bytes = generate_pdf_by_type(payload_dict)
                with open(f"/tmp/invoice_{payload_dict['orderNumber']}.pdf", "wb") as f:
                    f.write(pdf_bytes)
                logging.info(
                    f"🧾 PDF généré pour la commande {payload_dict['orderNumber']}")

                # ✅ Envoi du PDF par e-mail
                send_invoice_email(payload_dict, pdf_bytes)

                # ✅ Upload dans Azure Blob Storage
                upload_pdf_to_blob(payload_dict['orderNumber'], pdf_bytes)

            else:
                update_order_status(order_id, "Failed",
                                    "No response from SOAP")
                logging.warning(
                    f"⚠️ Aucune réponse SOAP pour : {payload_dict.get('orderNumber')}")

        except Exception as e:
            update_order_status(order_id, "Failed", str(e)[:255])
            logging.error(
                f"❌ Erreur lors du traitement de la commande ID {order_id} : {str(e)}")


# ========= Helpers de mise en page =========
def _header_block(c, title_en, doc_info):
    W, H = A4
    M = 12*mm
    y = H - M

    # En‑tête bilingue société (reprend ton exemple)
    c.setFont("Helvetica-Bold", 13)
    c.drawCentredString(W/2, y, "راشيدين آر آر بي لتجارة التبغ ذ.م.م")
    y -= 6*mm
    c.setFont("Helvetica", 12)
    c.drawCentredString(W/2, y, "RASHIDEEN R R P TOBACCO TRADING LLC")
    y -= 8*mm

    # Service client + TVA
    c.setFont("Helvetica", 9)
    c.drawString(M, y, "Customer care N.800-8500017 خدمة العملاء")
    c.drawRightString(
        W-M, y, "VAT Registration.: 100304102500003 الرقم الضريبي")
    y -= 10*mm

    # Titre
    c.setFont("Helvetica-Bold", 18)
    c.drawCentredString(W/2, y, title_en)
    y -= 10*mm

    # Lignes info
    c.setFont("Helvetica", 11)
    c.drawString(M, y, f"Customer : {doc_info.get('customer','')}   العميل")
    right_label = doc_info.get('label_no', 'Invoice No.')
    c.drawRightString(
        W-M, y, f"{right_label}: {doc_info.get('number','')}  رقم الفاتورة")
    y -= 6*mm
    c.drawString(M, y, f"Date: {doc_info.get('date','')} التاريخ")
    c.drawRightString(W-M, y, f"Time:  {doc_info.get('time','')} الوقت")
    return y - 8*mm  # position Y pour la suite


def _info_footer(c):
    W, H = A4
    M = 12*mm
    # Texte court (comme sur tes factures)
    c.setFont("Helvetica", 8)
    c.drawCentredString(W/2, 25*mm,
                        "This is a computer generated document and does not require a signature. "
                        "Consideration charged includes prices of all items   Terms & Conditions apply")

    c.setFont("Helvetica", 7.2)
    block_en = ("Information on Returns and Warranty • Please retain the original invoice. "
                "Return allowed within 14 days in unopened packaging. "
                "Consumables (incl. tobacco) are not eligible. "
                "Warranty for electronics is 12 months from purchase date.")
    block_ar = ("معلومات عن المرتجعات والضمان • يرجى الاحتفاظ بالفاتورة الأصلية. "
                "يسمح بالإرجاع خلال 14 يوماً مع عبوة أصلية غير مفتوحة. "
                "المنتجات الاستهلاكية (بما فيها التبغ) غير قابلة للإرجاع. "
                "مدة الضمان 12 شهراً من تاريخ الشراء.")
    c.drawString(M, 13*mm, block_en)
    c.drawString(M, 8*mm, block_ar)


def _make_table(data, col_widths):
    tbl = Table(data, colWidths=col_widths, hAlign='LEFT')
    tbl.setStyle(TableStyle([
        ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONT', (0, 0), (-1, -1), 'Helvetica', 9),
        ('BACKGROUND', (0, 0), (-1, 0), colors.whitesmoke),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ]))
    return tbl


def _totals_block(c, y, totals, voucher=False):
    W, H = A4
    M = 12*mm
    c.setFont("Helvetica", 10)
    y -= 2*mm
    c.drawString(M, y, "Method of Payment طريقة الدفع")
    c.drawRightString(W-M, y, f"Amount المبلغ {totals.get('paid','0.00')}    "
                      f"Qty of items sold مجموع الكميات {totals.get('qty','0')}")
    y -= 10*mm

    lines = [
        ("Total Amount excl. VAT AED", totals.get('subtotal', '0.00')),
        ("VAT AED", totals.get('vat', '0.00')),
        ("Total after VAT AED", totals.get('total', '0.00')),
    ]
    if voucher:
        lines.append(("Voucher AED", totals.get('voucher', '0')))
        lines.append(("Total after VAT and voucher AED",
                     totals.get('after_voucher', '0.00')))
    for label, val in lines:
        c.drawString(M, y, f"{label}  المجموع")
        c.drawRightString(W-M, y, str(val))
        y -= 6*mm
    return y

# ========= Moteur choisi selon orderType =========


def generate_pdf_by_type(order):
    t = (order or {}).get("orderType", "sale")
    if t == "sale":
        return _generate_sale_pdf(order)
    if t == "return":
        return _generate_return_pdf(order)
    if t == "replacement":
        return _generate_replacement_pdf(order)
    raise ValueError("Type de commande invalide")

# ========= Templates =========


def _now_hhmm():
    return datetime.now().strftime("%I:%M %p")


def _name(order):
    return f"{order.get('firstName','').strip()} {order.get('lastName','').strip()}".strip() or "Customer"


def _generate_sale_pdf(order):
    """
    Colonnes : Reference | Description | Qty | U.Price | Value (Tax Excl) | 5% VAT | Total
    Hypothèse : 'finalPrice' = prix unitaire TTC. On calcule HT = TTC / 1.05 et TVA = TTC - HT.
    """
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)

    y = _header_block(c, "TAX INVOICE", {
        "customer": _name(order),
        "label_no": "Invoice No.",
        "number": str(order.get("orderNumber", "")),
        "date": str(order.get("orderDate", "")),
        "time": _now_hhmm(),
    })

    # En-têtes bilingues
    data = [[
        "الرقم المرجعي\nReference",
        "الوصف\nDescription",
        "الكمية\nQty",
        "سعر الوحدة\nU.Price",
        "سعر الوحدة بدون ضريبة\nValue (Tax Excl)",
        "نسبة ضريبة 5%\n5% VAT",
        "المبلغ الإجمالي\nTotal"
    ]]

    subtotal_ht = 0.0
    total_vat = 0.0
    total_ttc = 0.0
    total_qty = 0

    for it in order.get("items", []):
        ref = str(it.get("sku", ""))
        qty = int(it.get("quantity", 0))
        u_ttc = float(it.get("finalPrice", 0))   # prix unitaire TTC
        u_ht = round(u_ttc / 1.05, 2)
        u_vat = round(u_ttc - u_ht, 2)

        line_ht = round(u_ht * qty, 2)
        line_vat = round(u_vat * qty, 2)
        line_ttc = round(u_ttc * qty, 2)

        subtotal_ht += line_ht
        total_vat += line_vat
        total_ttc += line_ttc
        total_qty += qty

        desc = it.get("description", "") or ref
        data.append([ref, desc, str(
            qty), f"{u_ttc:.2f}", f"{u_ht:.2f}", f"{u_vat:.2f}", f"{line_ttc:.2f}"])

    # (Exemple) Ajouter une ligne “Shipment fees” si présente
    if order.get("shippingFee"):
        fee = float(order["shippingFee"])
        data.append(["", "Shipment fees", "1",
                    f"{fee:.2f}", f"{(fee/1.05):.2f}", f"{(fee-fee/1.05):.2f}", f"{fee:.2f}"])
        subtotal_ht += round(fee/1.05, 2)
        total_vat += round(fee - fee/1.05, 2)
        total_ttc += fee
        total_qty += 1

    table = _make_table(
        data, [32*mm, 60*mm, 16*mm, 22*mm, 28*mm, 20*mm, 22*mm])
    table.wrapOn(c, 12*mm, y)
    table.drawOn(c, 12*mm, y - 36*mm)
    y = y - 38*mm

    # Totaux (avec “voucher” si tu utilises un bon)
    totals = {
        "subtotal": f"{subtotal_ht:.2f}",
        "vat": f"{total_vat:.2f}",
        "total": f"{total_ttc:.2f}",
        "paid": f"{total_ttc:.2f}",
        "qty": str(total_qty),
        "voucher": str(order.get("voucherAmount", 0)),
        "after_voucher": f"{(total_ttc - float(order.get('voucherAmount', 0))):.2f}"
    }
    _totals_block(c, y, totals, voucher=True)

    _info_footer(c)
    c.showPage()
    c.save()
    buf.seek(0)
    return buf.getvalue()


def _generate_return_pdf(order):
    """
    Même tableau que sale mais montants négatifs selon les quantités (retour).
    """
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)

    y = _header_block(c, "TAX CREDIT NOTE", {
        "customer": _name(order),
        "label_no": "Credit Note No.",
        "number": str(order.get("orderNumber", "")),
        "date": str(order.get("orderDate", "")),
        "time": _now_hhmm(),
    })

    data = [[
        "الرقم المرجعي\nReference",
        "الوصف\nDescription",
        "الكمية\nQty",
        "سعر الوحدة\nU.Price",
        "سعر الوحدة بدون ضريبة\nValue (Tax Excl)",
        "نسبة ضريبة 5%\n5% VAT",
        "المبلغ الإجمالي\nTotal"
    ]]

    subtotal_ht = 0.0
    total_vat = 0.0
    total_ttc = 0.0
    total_qty = 0

    for it in order.get("items", []):
        ref = str(it.get("sku", ""))
        qty = int(it.get("quantity", 0))  # mets -1 pour un retour
        u_ttc = float(it.get("finalPrice", 0))
        u_ht = round(u_ttc / 1.05, 2)
        u_vat = round(u_ttc - u_ht, 2)

        line_ht = round(u_ht * qty, 2)
        line_vat = round(u_vat * qty, 2)
        line_ttc = round(u_ttc * qty, 2)

        subtotal_ht += line_ht
        total_vat += line_vat
        total_ttc += line_ttc
        total_qty += qty

        desc = it.get("description", "") or ref
        data.append([ref, desc, str(
            qty), f"{u_ttc:.2f}", f"{u_ht:.2f}", f"{u_vat:.2f}", f"{line_ttc:.2f}"])

    table = _make_table(
        data, [32*mm, 60*mm, 16*mm, 22*mm, 28*mm, 20*mm, 22*mm])
    table.wrapOn(c, 12*mm, y)
    table.drawOn(c, 12*mm, y - 28*mm)
    y = y - 30*mm

    totals = {
        "subtotal": f"{subtotal_ht:.2f}",
        "vat": f"{total_vat:.2f}",
        "total": f"{total_ttc:.2f}",
        "paid": f"{total_ttc:.2f}",
        "qty": str(total_qty),
    }
    _totals_block(c, y, totals, voucher=False)

    _info_footer(c)
    c.showPage()
    c.save()
    buf.seek(0)
    return buf.getvalue()


def _generate_replacement_pdf(order):
    """
    Remplacement : tableau simple Reference | Description | Qty (comme ton exemple).
    """
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)

    y = _header_block(c, "WARRANTY PRODUCT REPLACEMENT NOTE", {
        "customer": _name(order),
        "label_no": "Transaction No.",
        "number": str(order.get("orderNumber", "")),
        "date": str(order.get("orderDate", "")),
        "time": _now_hhmm(),
    })

    data = [[
        "الرقم المرجعي\nReference",
        "الوصف\nDescription",
        "الكمية\nQty"
    ]]

    total_qty = 0
    for it in order.get("items", []):
        ref = str(it.get("sku", ""))
        qty = int(it.get("quantity", 0))
        desc = it.get("description", "") or ref
        data.append([ref, desc, str(qty)])
        total_qty += qty

    table = _make_table(data, [50*mm, 90*mm, 20*mm])
    table.wrapOn(c, 12*mm, y)
    table.drawOn(c, 12*mm, y - 24*mm)

    _info_footer(c)
    c.showPage()
    c.save()
    buf.seek(0)
    return buf.getvalue()


def send_invoice_email(order, pdf_bytes):
    message = Mail(
        from_email="amira11soua@gmail.com",  # 🔁 Modifie si nécessaire
        to_emails=order["email"],
        subject=f"Invoice for your order {order['orderNumber']}",
        html_content=f"""
        <p>Bonjour {order.get('firstName', '')},</p>
        <p>Merci pour votre commande. Vous trouverez ci-joint votre facture.</p>
        <p>Cordialement,<br>L’équipe</p>
        """
    )

    encoded_pdf = base64.b64encode(pdf_bytes).decode()
    attachment = Attachment(
        FileContent(encoded_pdf),
        FileName(f"invoice_{order['orderNumber']}.pdf"),
        FileType("application/pdf"),
        Disposition("attachment")
    )
    message.attachment = attachment

    try:
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)
        logging.info(
            f"📧 Email envoyé à {order['email']}, statut : {response.status_code}")
    except Exception as e:
        logging.error(f"❌ Échec de l’envoi de l’email : {str(e)}")


def upload_pdf_to_blob(order_number, pdf_bytes):
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("BLOB_STORAGE_CONNECTION_STRING"))
    container_name = "invoices"
    blob_name = f"invoice_{order_number}.pdf"
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name)

    blob_client.upload_blob(pdf_bytes, overwrite=True)
    logging.info(f"☁️ PDF uploadé dans Blob Storage : {blob_name}")
