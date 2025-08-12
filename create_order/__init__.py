import azure.functions as func
import logging
import json
import psycopg2
import os
from jsonschema import validate, ValidationError, FormatChecker

# ⬅️ on va les isoler dans shared.py
from ..shared import get_connection, enregistrer_commande, order_schema


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🧪 Validation de la commande reçue")
    try:
        data = req.get_json()
    except ValueError:
        enregistrer_commande({}, "Invalid", "JSON invalide")
        return func.HttpResponse("❌ JSON invalide", status_code=400)

    try:
        validate(instance=data, schema=order_schema,
                 format_checker=FormatChecker())
    except ValidationError as e:
        enregistrer_commande(
            data, "Invalid", f"Erreur de validation : {e.message}")
        return func.HttpResponse(f"❌ Erreur de validation : {e.message}", status_code=400)

    prefix_map = {
        "sale": "S-",
        "return": "RET",
        "replacement": "REP"
    }
    expected_prefix = prefix_map.get(data["orderType"])
    if not data["orderNumber"].startswith(expected_prefix):
        enregistrer_commande(data, "Invalid", "Préfixe incorrect")
        return func.HttpResponse(
            f"❌ Le numéro de commande doit commencer par '{expected_prefix}' pour une commande de type '{data['orderType']}'",
            status_code=400
        )

    total_calcule = sum(item["finalPrice"] * item["quantity"]
                        for item in data["items"])
    if round(total_calcule, 2) != round(data["totalAmount"], 2):
        enregistrer_commande(data, "Invalid", "totalAmount incorrect")
        return func.HttpResponse("❌ totalAmount ne correspond pas à la somme des lignes", status_code=400)

    for item in data["items"]:
        if item["finalPrice"] < item["originalPrice"] and item.get("promotionId") is None:
            enregistrer_commande(
                data, "Invalid", "promotionId requis pour un voucher")
            return func.HttpResponse(
                "❌ We don't handle discounts, we only handle vouchers.",
                status_code=400
            )

    if data["orderType"] == "return":
        for item in data["items"]:
            if item["quantity"] != 1:
                enregistrer_commande(
                    data, "Invalid", "Quantité ≠ 1 pour return")
                return func.HttpResponse("❌ Les retours doivent avoir des quantités exactement égales à 1", status_code=400)

    if data["orderType"] == "replacement":
        has_positive = any(item["quantity"] == 1 for item in data["items"])
        has_negative = any(item["quantity"] == -1 for item in data["items"])
        if not (has_positive and has_negative):
            enregistrer_commande(
                data, "Invalid", "Remplacement invalide : manque +1 ou -1")
            return func.HttpResponse(
                "❌ Une commande de remplacement doit avoir un article ajouté (1) et un retiré (-1)",
                status_code=400
            )

    enregistrer_commande(data, "Pending")
    return func.HttpResponse(json.dumps({
        "status": "success",
        "message": f"Commande {data['orderNumber']} validée ✅"
    }), mimetype="application/json")
