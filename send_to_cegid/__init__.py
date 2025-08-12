import azure.functions as func
import logging
import os
import json
import requests

from ..shared import get_pending_orders, update_order_status, transform_json_to_cegid_format, json_to_soap_xml


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Démarrage de la fonction send_to_cegid...")

    try:
        logging.info("📚 Récupération des commandes Pending...")
        orders = get_pending_orders()
        logging.info(f"📦 Nombre de commandes récupérées : {len(orders)}")
    except Exception as e:
        logging.error(
            f"❌ Erreur lors de la récupération des commandes : {str(e)}")
        return func.HttpResponse("Erreur de récupération des commandes.", status_code=500)

    if not orders:
        logging.info("✅ Aucune commande à envoyer. Fin de la fonction.")
        return func.HttpResponse("✅ Aucune commande à envoyer.", status_code=200)

    apim_url = os.getenv("APIM_URL")
    logging.info(f"🔗 APIM_URL récupérée : {apim_url}")

    if not apim_url:
        logging.error(
            "❌ APIM_URL non configurée dans les variables d'environnement.")
        return func.HttpResponse("❌ APIM_URL non configurée.", status_code=500)

    headers = {
        'Content-Type': 'text/xml',
        'SOAPAction': '"http://www.cegid.fr/Retail/1.0/ISaleDocumentService/Create"',
        'Ocp-Apim-Subscription-Key': 'ec65ab88625f456392b99adcd76d2709'
    }

    results = []

    for order_id, external_id, payload in orders:
        try:
            logging.info(
                f"📄 Traitement de la commande ID: {order_id}, External ID: {external_id}")
            order_data = payload if isinstance(
                payload, dict) else json.loads(payload)

            transformed_payload = transform_json_to_cegid_format(order_data)
            logging.info(
                f"📦 Payload transformé (JSON): {json.dumps(transformed_payload, indent=2)}")

            # ✅ Conversion en XML pour Cegid
            soap_payload = json_to_soap_xml(transformed_payload)
            logging.info(f"📦 Payload transformé (SOAP XML): {soap_payload}")
            # 🔥 Ajouter ce log ici pour voir les headers envoyés
            logging.info(f"📨 Headers envoyés : {headers}")

            logging.info("📤 Envoi du payload XML à l'APIM...")
            response = requests.post(
                apim_url, data=soap_payload.encode('utf-8'), headers=headers
            )

            logging.info(
                f"📨 Réponse APIM: {response.status_code} - {response.text}")

            if response.status_code == 200 and "Success" in response.text:
                update_order_status(order_id, "Sent")
                msg = f"✅ Order {external_id} envoyée avec succès via APIM."
            else:
                update_order_status(order_id, "Failed",
                                    f"Réponse APIM: {response.status_code}")
                msg = f"⚠️ Échec envoi {external_id}. Réponse APIM: {response.status_code}"

            logging.info(msg)
            results.append(msg)

        except Exception as e:
            update_order_status(order_id, "Failed", str(e))
            msg = f"❌ Erreur lors de l'envoi de {external_id} : {str(e)}"
            logging.error(msg)
            results.append(msg)

    logging.info("🏁 Fin de traitement de toutes les commandes.")
    return func.HttpResponse("\n".join(results), status_code=200)
