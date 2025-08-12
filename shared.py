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


def generate_pdf_by_type(order_payload):
    order_type = order_payload.get("orderType", "sale")

    if order_type == "sale":
        return generate_sale_pdf(order_payload)
    elif order_type == "return":
        return generate_return_pdf(order_payload)
    elif order_type == "replacement":
        return generate_replacement_pdf(order_payload)
    else:
        raise ValueError("Type de commande invalide")


def generate_sale_pdf(order):
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer)
    pdf.setTitle("TAX INVOICE")

    pdf.drawString(100, 800, "TAX INVOICE")
    pdf.drawString(100, 780, f"Order Number: {order.get('orderNumber')}")
    pdf.drawString(
        100, 760, f"Customer: {order.get('firstName', '')} {order.get('lastName', '')}")
    pdf.drawString(100, 740, f"Email: {order.get('email', '')}")
    pdf.drawString(100, 720, f"Date: {order.get('orderDate', '')}")
    pdf.drawString(100, 700, f"Total: {order.get('totalAmount', 0)} AED")

    y = 660
    for item in order.get("items", []):
        pdf.drawString(
            100, y, f"- {item.get('sku')} x{item.get('quantity')} = {item.get('finalPrice')} AED")
        y -= 20

    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


def generate_return_pdf(order):
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer)
    pdf.setTitle("TAX CREDIT NOTE")

    pdf.drawString(100, 800, "TAX CREDIT NOTE")
    pdf.drawString(100, 780, f"Return Number: {order.get('orderNumber')}")
    pdf.drawString(
        100, 760, f"Customer: {order.get('firstName', '')} {order.get('lastName', '')}")
    pdf.drawString(100, 740, f"Email: {order.get('email', '')}")
    pdf.drawString(100, 720, f"Date: {order.get('orderDate', '')}")
    pdf.drawString(
        100, 700, f"Refund Total: {order.get('totalAmount', 0)} AED")

    y = 660
    for item in order.get("items", []):
        pdf.drawString(
            100, y, f"- {item.get('sku')} returned x{item.get('quantity')} = {item.get('finalPrice')} AED")
        y -= 20

    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


def generate_replacement_pdf(order):
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer)
    pdf.setTitle("REPLACEMENT NOTE")

    pdf.drawString(100, 800, "REPLACEMENT ORDER")
    pdf.drawString(100, 780, f"Replacement Order: {order.get('orderNumber')}")
    pdf.drawString(
        100, 760, f"Customer: {order.get('firstName', '')} {order.get('lastName', '')}")
    pdf.drawString(100, 740, f"Email: {order.get('email', '')}")
    pdf.drawString(100, 720, f"Date: {order.get('orderDate', '')}")
    pdf.drawString(
        100, 700, f"Net Adjustment: {order.get('totalAmount', 0)} AED")

    y = 660
    for item in order.get("items", []):
        action = "Added" if item.get("quantity") > 0 else "Removed"
        pdf.drawString(
            100, y, f"- {item.get('sku')} {action} x{abs(item.get('quantity'))} = {item.get('finalPrice')} AED")
        y -= 20

    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


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
