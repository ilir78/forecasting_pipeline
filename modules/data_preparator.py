import psycopg2
from psycopg2.extras import RealDictCursor
import logging

DB_CONFIG = {
    "host": "localhost",
    "database": "trading_data",
    "user": "dev_user",
    "password": "Salutmec2!",
    "port": 5432
}

class DataPreparator:
    def __init__(self):
        self.conn = None

    def _connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**DB_CONFIG)

    def get_snapshot(self, target_timestamp):
        """Extrait toutes les variables pour un timestamp donné"""
        self._connect()
        query = """
            SELECT variable_name, value 
            FROM raw_weather_data 
            WHERE timestamp = %s
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (target_timestamp,))
                rows = cur.fetchall()
                
                if not rows:
                    logging.warning(f"Aucun snapshot trouvé pour {target_timestamp}")
                    return None
                
                # Organisation des données : {"t2m": [...], "msl": [...]}
                formatted_data = {}
                for var_name, value in rows:
                    if var_name not in formatted_data:
                        formatted_data[var_name] = []
                    formatted_data[var_name].append(float(value))
                
                return {
                    "timestamp": target_timestamp.isoformat() if hasattr(target_timestamp, 'isoformat') else str(target_timestamp),
                    "data": formatted_data
                }
        except Exception as e:
            logging.error(f"Erreur SQL lors de l'extraction : {e}")
            return None

    def prepare_api_payload(self, t_minus_6h, t_0, area="europe", prediction_steps=4):
        """Assemble les deux snapshots pour l'API GraphCast"""
        logging.info(f"Préparation du payload pour {area} ({prediction_steps} étapes)...")
        
        snap_6h = self.get_snapshot(t_minus_6h)
        snap_0 = self.get_snapshot(t_0)
        
        if not snap_6h or not snap_0:
            return None
            
        return {
            "area": area,
            "snapshot_minus_6h": snap_6h,
            "snapshot_t0": snap_0,
            "prediction_steps": prediction_steps  # Utilise maintenant l'argument
        }