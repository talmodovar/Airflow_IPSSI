# Airflow Energie RTE

Projet d'automatisation de corrélation entre météo et production d'énergie renouvelable

##  Installation & Lancement

1. **Prérequis** : Docker.
2. **Démarrage** :
   ```bash
   docker compose up -d
   ```
3. **Accès UI** : [http://localhost:8080](http://localhost:8080) !! Ne marche que sur Chrome... !!
   - **Login** : `airflow`
   - **Pass** : `airflow`




##  Pipeline : `energie_meteo_dag`

Le pipeline s'exécute **tous les jours à 06:00** (Paris) et suit ces étapes :
1. **Vérification** des API (Open-Meteo & ODRE).
2. **Extraction** : Météo (Vent/Soleil) et Production RTE (Éolien/Solaire).
3. **Analyse** : Détection d'alertes si la production est anormalement basse par rapport à la météo.
4. **Reporting** : Génération d'un JSON dans `/tmp/rapport_energie_{date}.json`.

##  Structure
- `dags/` : Contient les scripts de workflow.
- `docker-compose.yaml` : Configuration de l'infrastructure Airflow.
- `.env` : Variables d'environnement pour les permissions Docker.
