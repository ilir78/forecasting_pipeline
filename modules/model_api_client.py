import httpx
import logging

class ModelApiClient:
    def __init__(self, api_url="http://127.0.0.1:8001"):
        self.api_url = api_url

    def get_forecast(self, payload: dict):
        """Envoie les données brutes à l'API et récupère la prédiction"""
        try:
            # On met un timeout long (ex: 5 min) car GraphCast est lourd
            with httpx.Client(timeout=300.0) as client:
                response = client.post(f"{self.api_url}/predict", json=payload)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logging.error(f"Erreur lors de l'appel à l'API Modèle : {e}")
            return None