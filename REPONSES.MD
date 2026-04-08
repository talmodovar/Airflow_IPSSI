
1 Interface Airfl ow UI avec le DAG energie_meteo_dag visible et en état success
![alt text](image.png)


2 Vue Graph du DAG montrant les 5 tâches et leurs dépendances

![alt text](image-4.png)

## Réponses aux questions théoriques



3. Logs d’une exécution réussie de generer_rapport_energie avec le tableau affiché

![alt text](image-2.png)

4. Onglet XCom de la tâche analyser_correlation montrant le dictionnaire d’alertes
![alt text](image-3.png)

5. Contenu du fichier JSON généré (via docker compose exec ... cat /tmp/rapport_energie_*.json)

![alt text](image-4.png)


### Q1 — Docker Executor
- **Local** : Tout tourne sur  machine .
- **Celery** : Les tâches sont envoyées à plusieurs serveurs.
- **Kubernetes** : Chaque tâche crée son propre petit conteneur 

### Q2 — Volumes Docker
- **Bind mount** : C'est un lien direct entre le dossier et Docker. 
- **Prod** : Sur plusieurs serveurs, on ne peut pas utiliser de dossiers locaux. On synchronise dans l'image Docker.

### Q3 — Idempotence et catchup
- **Catchup=True** : Airflow va essayer de rattraper tous les jours
- **Idempotence** : C'est pouvoir relancer le même DAG sans créer de doublons ou d'erreurs. 
- **Secret** : Il faut que le code utilise la date passée par Airflow au lieu de faire un `date.today()`.

### Q4 — Timezone
- **RTE** : L'énergie est consommée à l'heure française donc la `Europe/Paris`.
- **Heure d'été** : Un décalage d'une heure.