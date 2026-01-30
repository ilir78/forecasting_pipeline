import psycopg2
from psycopg2.extras import execute_values
import logging

# On réutilise la même config que le préparateur
from modules.data_preparator import DB_CONFIG

class PredictionDbIngestion:
    def __init__(self):
        self.conn = None

    def _connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**DB_CONFIG)

    def ingest_predictions(self, predictions_list, reference_time):
        """Insère la liste des prédictions dans la table SQL"""
        query = """
            INSERT INTO predictions 
            (reference_time, forecast_time, variable_name, latitude, longitude, value)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        self._connect()
        try:
            with self.conn.cursor() as cur:
                # Puisque c'est une liste directe de dictionnaires
                data_to_insert = [
                    (
                        reference_time,
                        p['forecast_time'],
                        p['variable_name'],
                        p['latitude'],
                        p['longitude'],
                        p['value']
                    )
                    for p in predictions_list
                ]
                
                cur.executemany(query, data_to_insert)
                self.conn.commit()
                logging.info(f"{len(data_to_insert)} prédictions insérées en base.")
                return True
        except Exception as e:
            logging.error(f"Erreur lors de l'ingestion des prédictions : {e}")
            if self.conn:
                self.conn.rollback()
            return False