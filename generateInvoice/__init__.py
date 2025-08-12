import logging
from azure.functions import HttpRequest, HttpResponse
from shared import generate_pdf_by_type  # ✅ Import de la fonction intelligente


def main(req: HttpRequest) -> HttpResponse:
    try:
        order = req.get_json()
    except Exception as e:
        return HttpResponse(f"❌ JSON invalide : {str(e)}", status_code=400)

    try:
        # ✅ Génère le bon type de PDF automatiquement
        pdf_bytes = generate_pdf_by_type(order)
    except Exception as e:
        logging.error(f"❌ Erreur PDF : {str(e)}")
        return HttpResponse(f"❌ Erreur lors de la génération du PDF : {str(e)}", status_code=500)

    return HttpResponse(
        body=pdf_bytes,
        status_code=200,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f"inline; filename=invoice_{order.get('orderNumber', 'unknown')}.pdf"
        }
    )
