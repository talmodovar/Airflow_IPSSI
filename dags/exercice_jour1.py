from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, date

# Définition de la fonction pour la tâche Python
def print_today_date():
    today = date.today()
    print(f"La date du jour est : {today}")
    return str(today)

# Configuration des default_args (Objectif Bonus)
default_args = {
    'owner': 'thomas',
    'email_on_failure': False, # Objectif Bonus
}

# Création du DAG
with DAG(
    dag_id='exercice_jour1',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False, # Conseil
    tags=['tp', 'jour1'] # Objectif Bonus
) as dag:

    # Tâche 1 : BashOperator
    t1 = BashOperator(
        task_id='debut_workflow',
        bash_command='echo "Début du workflow"'
    )

    # Tâche 2 : PythonOperator
    t2 = PythonOperator(
        task_id='date_du_jour',
        python_callable=print_today_date
    )

    # Tâche 3 : BashOperator
    t3 = BashOperator(
        task_id='fin_workflow',
        bash_command='echo "Fin du workflow"'
    )

    # Définition des dépendances (Conseil)
    t1 >> t2 >> t3
