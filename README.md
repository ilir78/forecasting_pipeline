🌦️ Pipeline d'Orchestration GraphCast - Étape 13
Ce module constitue le "cerveau" du projet. Il orchestre l'extraction des données d'initialisation depuis PostgreSQL, l'inférence via l'API GraphCast et l'ingestion des résultats finaux.

🚀 Fonctionnement du Pipeline
Le script principal trigger_and_ingest_forecast.py coordonne les étapes suivantes :

Préparation : Calcul de la date cible (J-6) et extraction des snapshots T-6h et T0.

Inférence : Envoi d'un payload JSON à l'API FastAPI (port 8001).

Ingestion : Stockage de 80 points de prévision (4 horizons : T+6h, T+12h, T+18h, T+24h) dans la base PostgreSQL.

📂 Structure du Projet
Plaintext

/Etape13Forecasting_Orchestration
├── trigger_and_ingest_forecast.py  # Orchestrateur principal
├── modules/
│   ├── data_preparator.py          # Extraction SQL des données ERA5
│   ├── model_api_client.py         # Client HTTP pour l'API GraphCast
│   └── prediction_db_ingestion.py  # Ingestion des prévisions en base
├── config/
│   └── settings.py                 # Configuration DB et API
└── logs/                           # Fichiers de suivi du pipeline
🛠️ Installation & Configuration
1. Prérequis
Docker (avec le conteneur trading_pg_db actif).

API GraphCast (Etape 12) lancée sur le port 8001.

Bibliothèques Python : psycopg2, requests.

2. Lancer le pipeline manuellement
PowerShell

python trigger_and_ingest_forecast.py
📅 Planification (Automation)
Sur Windows (Planificateur de tâches)
Créer une nouvelle tâche nommée GraphCast_Pipeline.

Déclencheur : Quotidien, répéter toutes les 6 heures indéfiniment.

Action : Démarrer le programme python.exe.

Argument : C:\Chemin\Vers\Etape13\trigger_and_ingest_forecast.py.

Sur Linux (Cron)
Ajouter la ligne suivante via crontab -e :

Bash

0 */6 * * * /usr/bin/python3 /chemin/ton_projet/trigger_and_ingest_forecast.py >> /chemin/ton_projet/logs/pipeline.log 2>&1
🔍 Vérification des données
Pour confirmer que les prévisions sont bien stockées en base :


docker exec -it trading_pg_db psql -U dev_user -d trading_data -c "SELECT forecast_time, COUNT(*), AVG(value) FROM predictions GROUP BY forecast_time ORDER BY forecast_time;"