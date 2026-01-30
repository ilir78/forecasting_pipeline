@echo off
:: On se déplace dans le dossier du projet
cd /d "C:\Users\pasca\ProjetMeteoIA\StructureProjet\Etape11Data_Acquisition_pipeline"

:: On lance le script avec le Python de ton système
python -m fetch_and_store_era5

:: (Optionnel) Laisse la fenêtre ouverte en cas d'erreur pour voir le message
if %errorlevel% neq 0 pause