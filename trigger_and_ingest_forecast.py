import logging
from datetime import datetime, timedelta
from modules.data_preparator import DataPreparator
from modules.model_api_client import ModelApiClient
from modules.prediction_db_ingestion import PredictionDbIngestion

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/pipeline.log", encoding='utf-8'), # Ajoute encoding='utf-8'
        logging.StreamHandler()
    ]
)

def run_pipeline():
    logging.info("Démarrage du pipeline d'orchestration dynamique...")
    
    preparator = DataPreparator()
    api_client = ModelApiClient()
    ingestor = PredictionDbIngestion()

    # CALCUL DYNAMIQUE : J-6 par rapport à aujourd'hui
    base_date = datetime.now() - timedelta(days=6)
    t0 = datetime(base_date.year, base_date.month, base_date.day, 12, 0)
    t_minus_6h = datetime(base_date.year, base_date.month, base_date.day, 6, 0)

    logging.info(f"Cible temporelle : T0={t0}, T-6h={t_minus_6h}")

    # 1. Préparation du payload (On passe explicitement 4 steps)
    payload = preparator.prepare_api_payload(t_minus_6h, t0, area="europe")
    if payload:
        payload["prediction_steps"] = 4  # FORCE LE MODE 24H (4 x 6h)
    
    if not payload:
        logging.error("Échec : Impossible de préparer les données (vérifiez la DB).")
        return

    # 2. Appel à l'API GraphCast
    logging.info(f"Step 2: Envoi à l'API pour 4 horizons (T+6h à T+24h)...")
    response = api_client.get_forecast(payload)
    
    if not response or response.get("status") != "success":
        logging.error("Échec : L'API n'a pas renvoyé de prédictions valides.")
        return

    # 3. Ingestion Multi-horizons
    logging.info(f"Step 3: Ingestion de {len(response['predictions'])} points en base...")
    success = ingestor.ingest_predictions(response["predictions"], t0)
    
    if success:
        logging.info("✅ ÉTAPE 13 VALIDÉE : Pipeline complet et multi-horizons terminé.")
    else:
        logging.error("Échec : L'ingestion a échoué malgré des données valides.")

if __name__ == "__main__":
    run_pipeline()