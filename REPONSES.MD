
1 Interface Airfl ow UI avec le DAG energie_meteo_dag visible et en état success
![alt text](image.png)


2 Vue Graph du DAG montrant les 5 tâches et leurs dépendances

![alt text](image-1.png)


3. Logs d’une exécution réussie de generer_rapport_energie avec le tableau affiché

![alt text](image-2.png)

4. Onglet XCom de la tâche analyser_correlation montrant le dictionnaire d’alertes
![alt text](image-3.png)

5. Contenu du fichier JSON généré (via docker compose exec ... cat /tmp/rapport_energie_*.json)

![alt text](image-4.png)