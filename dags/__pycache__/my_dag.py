from datetime import datetime, timedelta
from random import randint
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

# # Define default_args
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Define DAG
# dag = DAG(
#     'data_load_dag',
#     default_args=default_args,
#     description='DAG to load data files into the database',
#     schedule_interval='@daily',  # Adjust as needed
# )

# # Define tasks for Prod, Dev, and Staging

# def load_data_prod():
#     # Add production data loading logic here
#     print("Loading data into Prod database")

# def load_data_dev():
#     # Add development data loading logic here
#     print("Loading data into Dev database")

# def load_data_staging():
#     # Add staging data loading logic here
#     print("Loading data into Staging database")

# # Tasks for Prod
# load_data_prod_task = PythonOperator(
#     task_id='load_data_prod',
#     python_callable=load_data_prod,
#     dag=dag,
# )

# # Tasks for Dev
# load_data_dev_task = PythonOperator(
#     task_id='load_data_dev',
#     python_callable=load_data_dev,
#     dag=dag,
# )

# # Tasks for Staging
# load_data_staging_task = PythonOperator(
#     task_id='load_data_staging',
#     python_callable=load_data_staging,
#     dag=dag,
# )

# # Set task dependencies
# load_data_prod_task >> load_data_dev_task >> load_data_staging_task


def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B', 
        'training_model_C'
    ])
    best_accuracy = max(accuracies)
    if accurate >8 :
        return 'accurate'
    return inaccurate

def _training_model():
    return randint(1, 10)


with DAG("my_dag", 
         start_date = datetime(2021, 1, 1),
         schedule_interval = "@daily",
         catchup = False) as dag:
    
    # first task
    training_model_A = PythonOperator(
        task_id = "training_model_A",
        python_callable=_training_model
    )

    training_model_B = PythonOperator(
        task_id = "training_model_B",
        python_callable=_training_model
    )

    training_model_C = PythonOperator(
        task_id = "training_model_C",
        python_callable=_training_model
    )

    choose_best_model = BranchPythonOperator(
        task_id = "choose_best_model",
        python_callable= _choose_best_model
    )

    accurate = BashOperator(
        task_id = "accurate",
        bash_command= "echo 'accurate' "
    )

    inaccurate = BashOperator(
        task_id = "inaccurate",
        bash_command= "echo 'inaccurate' "
    )

    # Dependencies
    [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]