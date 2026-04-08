import logging
import json
import requests
from datetime import datetime, date, timedelta
import pendulum
from airflow import DAG
try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.providers.standard.operators.python import PythonOperator

try:
    from airflow.exceptions import AirflowException
except ImportError:
    from airflow.exceptions import AirflowException # Usually doesn't move, but same pattern if needed

# Configuration des paramètres régionaux (Expert RTE Mapping)
# Mapping: Nom -> (Lat, Lon) pour Open-Meteo, Libellé pour ODRE
REGIONS_CONFIG = {
    "Île-de-France": {"coords": (48.85, 2.35), "odre_name": "Île-de-France"},
    "Occitanie": {"coords": (43.60, 1.44), "odre_name": "Occitanie"},
    "Nouvelle-Aquitaine": {"coords": (44.83, -0.57), "odre_name": "Nouvelle-Aquitaine"},
    "Auvergne-Rhône-Alpes": {"coords": (45.75, 4.85), "odre_name": "Auvergne-Rhône-Alpes"},
    "Hauts-de-France": {"coords": (50.63, 3.05), "odre_name": "Hauts-de-France"}
}

# --- LOGIQUE MÉTIER ---

def verifier_apis():
    """Vérifie la disponibilité des APIs critiques."""
    urls = [
        "https://api.open-meteo.com/v1/forecast",
        "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
    ]
    for url in urls:
        logging.info(f"Vérification de l'API : {url}")
        try:
            response = requests.head(url, timeout=10)
            if response.status_code not in [200, 301, 302]:
                raise ValueError(f"L'API {url} a répondu avec le code {response.status_code}")
        except Exception as e:
            logging.error(f"Échec de la vérification pour {url}: {e}")
            raise AirflowException(f"API critique indisponible: {url}")
    logging.info("Toutes les APIs sont opérationnelles.")

def collecter_meteo_regions():
    """Récupère les prévisions météo pour les 5 régions."""
    resultats = {}
    for region, config in REGIONS_CONFIG.items():
        lat, lon = config["coords"]
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=sunshine_duration,wind_speed_10m_max&timezone=Europe/Paris&forecast_days=1"
        
        logging.info(f"Récupération météo pour {region}...")
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            # Conversion secondes -> heures
            sunshine_sec = data['daily']['sunshine_duration'][0] or 0
            sunshine_hrs = round(sunshine_sec / 3600, 2)
            wind_max = data['daily']['wind_speed_10m_max'][0] or 0.0
            
            resultats[region] = {
                "ensoleillement_h": sunshine_hrs,
                "vitesse_vent_kmh": wind_max
            }
            logging.info(f"{region} : {sunshine_hrs}h soleil, {wind_max}km/h vent")
        except Exception as e:
            logging.error(f"Erreur météo {region}: {e}")
            resultats[region] = {"ensoleillement_h": 0.0, "vitesse_vent_kmh": 0.0}
            
    return resultats

def collecter_production_electrique():
    """Récupère la production solaire et éolienne via éCO2mix (ODRE)."""
    resultats = {}
    # Dataset: eco2mix-regional-tr (temps réel)
    # On filtre les 5 régions et on prend les enregistrements les plus récents
    base_url = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-tr/records"
    regions_filter = ",".join([f"'{c['odre_name']}'" for c in REGIONS_CONFIG.values()])
    query = f"limit=20&where=libelle_region in ({regions_filter})&order_by=date_heure DESC"
    
    url = f"{base_url}?{query}"
    logging.info(f"Récupération production électricité : {url}")
    
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        records = response.json().get('results', [])
        
        # On ne garde que l'entrée la plus récente par région unique
        processed_regions = set()
        for record in records:
            libelle = record.get('libelle_region')
            if libelle and libelle not in processed_regions:
                # Gestion des valeurs nulles (remplacer par 0.0)
                solaire = record.get('solaire', 0.0) or 0.0
                eolien = record.get('eolien', 0.0) or 0.0
                
                resultats[libelle] = {
                    "production_solaire_mw": solaire,
                    "production_eolienne_mw": eolien
                }
                processed_regions.add(libelle)
                logging.info(f"Prod {libelle} : Solaire={solaire}MW, Éolien={eolien}MW")
                
    except Exception as e:
        logging.error(f"Erreur lors de la collecte ODRE: {e}")
        # En cas d'erreur API, on initialise avec des zéros pour éviter de bloquer le workflow
        for region in REGIONS_CONFIG.keys():
            resultats[region] = {"production_solaire_mw": 0.0, "production_eolienne_mw": 0.0}

    return resultats

def analyser_correlation(**kwargs):
    """Analyse la corrélation météo/énergie et génère des alertes."""
    ti = kwargs['ti']
    meteo_data = ti.xcom_pull(task_ids='collecter_meteo_regions')
    prod_data = ti.xcom_pull(task_ids='collecter_production_electrique')
    
    alertes = {}
    for region in REGIONS_CONFIG.keys():
        m = meteo_data.get(region, {})
        p = prod_data.get(region, {})
        
        sunshine = m.get('ensoleillement_h', 0)
        wind = m.get('vitesse_vent_kmh', 0)
        solar_prod = p.get('production_solaire_mw', 0)
        wind_prod = p.get('production_eolienne_mw', 0)
        
        # Logique métier des alertes
        # Alerte Solaire : Si Ensoleillement > 5h ET Production Solaire < 500MW
        status_solaire = "ALERTE" if (sunshine > 5 and solar_prod < 500) else "OK"
        
        # Alerte Éolienne : Si Vent > 30km/h ET Production Éolienne < 1000MW
        status_eolien = "ALERTE" if (wind > 30 and wind_prod < 1000) else "OK"
        
        alertes[region] = {
            "status_solaire": status_solaire,
            "status_eolien": status_eolien,
            "meteo_summary": f"Sun: {sunshine}h, Wind: {wind}km/h",
            "prod_summary": f"Solar: {solar_prod}MW, Wind: {wind_prod}MW"
        }
        logging.info(f"Analyse {region} terminée : Solaire={status_solaire}, Éolien={status_eolien}")
        
    return alertes

def generer_rapport_energie(**kwargs):
    """Génère le rapport final au format JSON."""
    ti = kwargs['ti']
    meteo = ti.xcom_pull(task_ids='collecter_meteo_regions')
    prod = ti.xcom_pull(task_ids='collecter_production_electrique')
    alertes = ti.xcom_pull(task_ids='analyser_correlation')
    
    ds = kwargs.get('ds_nodash', datetime.now().strftime('%Y%m%d'))
    filepath = f"/tmp/rapport_energie_{ds}.json"
    
    rapport = {
        "metadata": {
            "date_execution": str(kwargs.get('execution_date', datetime.now())),
            "regions_analysées": list(REGIONS_CONFIG.keys())
        },
        "donnees": {
            region: {
                "meteo": meteo.get(region),
                "production": prod.get(region),
                "alertes": alertes.get(region)
            } for region in REGIONS_CONFIG.keys()
        }
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(rapport, f, ensure_ascii=False, indent=4)
        
    logging.info(f"Rapport généré avec succès : {filepath}")

# --- DEFINITION DU DAG ---

local_tz = pendulum.timezone("Europe/Paris")

default_args = {
    'owner': 'Data Engineering Expert',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1, tzinfo=local_tz),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'energie_meteo_dag',
    default_args=default_args,
    description='Corrélation météo et production énergie RTE',
    schedule_interval='0 6 * * *', # Quotidien à 06h00
    catchup=False,
    tags=['RTE', 'Météo', 'Énergie'],
) as dag:

    t1 = PythonOperator(
        task_id='verifier_apis',
        python_callable=verifier_apis,
    )

    t2 = PythonOperator(
        task_id='collecter_meteo_regions',
        python_callable=collecter_meteo_regions,
    )

    t3 = PythonOperator(
        task_id='collecter_production_electrique',
        python_callable=collecter_production_electrique,
    )

    t4 = PythonOperator(
        task_id='analyser_correlation',
        python_callable=analyser_correlation,
    )

    t5 = PythonOperator(
        task_id='generer_rapport_energie',
        python_callable=generer_rapport_energie,
    )

    # Dépendances
    t1 >> [t2, t3] >> t4 >> t5
