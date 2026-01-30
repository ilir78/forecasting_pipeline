import cdsapi
import psycopg2
from psycopg2.extras import execute_values
import xarray as xr
import os
import logging
import time
from datetime import datetime, timedelta
from config.settings import DB_CONFIG, AREA, ERA5_VARIABLES

# --- CONFIGURATION DES LOGS ---
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/acquisition.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Dans ton script d'étape 11 :
def fetch_era5(target_file, retries=5):
    c = cdsapi.Client()
    
    # CALCUL DYNAMIQUE : J-6
    target_date = datetime.now() - timedelta(days=6)
    
    logging.info(f"Tentative de récupération ERA5 pour le {target_date.strftime('%Y-%m-%d')}")

    for i in range(retries):
        try:
            c.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'format': 'grib',
                    'variable': ERA5_VARIABLES,
                    'year': target_date.strftime('%Y'),
                    'month': target_date.strftime('%m'),
                    'day': target_date.strftime('%d'),
                    'time': ['06:00', '12:00'], # On télécharge bien les deux snapshots
                    'area': AREA, 
                },
                target_file)
            return True
        # ... reste du code identique
        except Exception as e:
            logging.warning(f"Échec tentative {i+1}/{retries}: {e}")
            if i < retries - 1:
                # Attente de 5 minutes avant la prochaine tentative pour laisser le serveur se libérer
                logging.info("Attente de 5 minutes avant le prochain essai...")
                time.sleep(300) 
            else:
                logging.error("Toutes les tentatives de téléchargement ont échoué.")
                raise

def store_in_db(file_path):
    """Ingestion optimisée en Bulk (Point 2)"""
    logging.info("Ingestion Bulk en base de données...")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Le fichier GRIB {file_path} est introuvable.")

    # Lecture du dataset GRIB
    ds = xr.open_dataset(file_path, engine="cfgrib")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    total_inserted = 0
    try:
        for var_name in ds.data_vars:
            # Transformation de la variable en DataFrame
            df = ds[var_name].to_dataframe().reset_index()
            
            # Préparation des données en liste de tuples pour execute_values (Bulk)
            data_values = [
                (row['time'], var_name, 'surface', 0, row['latitude'], row['longitude'], float(row[var_name]), 'ERA5')
                for _, row in df.iterrows()
            ]

            # Requête d'insertion groupée avec gestion des conflits
            query = """
                INSERT INTO raw_weather_data 
                (timestamp, variable_name, level_type, level_value, latitude, longitude, value, data_source)
                VALUES %s
                ON CONFLICT (timestamp, variable_name, level_type, level_value, latitude, longitude) 
                DO NOTHING
            """
            
            execute_values(cur, query, data_values)
            total_inserted += cur.rowcount
        
        conn.commit()
        logging.info(f"Succès : {total_inserted} nouvelles lignes insérées en base.")
        print(f"Ingestion terminée : {total_inserted} points ajoutés.")
        
    except Exception as e:
        logging.error(f"Erreur SQL durant l'ingestion : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
        ds.close()

if __name__ == "__main__":
    TMP_FILE = "raw_data_tmp.grib"
    start_time = time.time()
    
    try:
        fetch_era5(TMP_FILE)
        store_in_db(TMP_FILE)
        
        # Nettoyage du fichier temporaire
        if os.path.exists(TMP_FILE):
            os.remove(TMP_FILE)
            
        duration = (time.time() - start_time) / 60
        logging.info(f"Fin du pipeline. Durée totale : {duration:.2f} minutes.")
        
    except Exception as e:
        print(f"Échec critique : {e}. Consultez logs/acquisition.log")