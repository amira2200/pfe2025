import logging
import azure.functions as func
from datetime import datetime, timezone
from shared import process_pending_orders  # ✅ importe ta logique métier


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('⏰ The timer is past due!')

    logging.info('⏰ Python timer trigger function ran at %s', utc_timestamp)

    try:
        process_pending_orders()  # ✅ traite et envoie les commandes
    except Exception as e:
        logging.error(
            f"🔥 Une erreur est survenue pendant le traitement : {str(e)}")
