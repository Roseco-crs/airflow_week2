from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd


# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'data_load_dag',
    default_args=default_args,
    description='DAG to load data files into the database',
    schedule_interval='@daily',  # Adjust as needed
    catchup = False, 
)

# First Task: Loading dataframe
# 1. data 1
load_data_1_into_dataframe = PythonOperator(
    task_id = 'Load track dataframe',
    python_callable = load_data_1_into_dataframe,
    dag = dag,
)
# 2. data 2
load_data_2_into_dataframe = PythonOperator( 
    task_id = 'Load trajectory dataframe',
    python_callable = load_data_2_into_dataframe,
    dag = dag,
)

# Second Task: Insert dataframe into postgres

insert_dataframe_into_postgres = PythonOperator(
    task_id = "Insert_dataframe_into_postgres",
    python_callable= Insert_dataframe_into_postgres,
    dag = dag,
)


def insert_dataframe_into_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='load_data_task')

    # PostgreSQL connection details
    connection_params = {'host': 'localhost', 'user': 'airflow',
                         'password': 'airflow', 'port': "9090",
                         'database': postgres }
    pg_conn = create_engine(f"postgresql+psycopg2://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}")
    # Update with your PostgreSQL connection ID
    pg_table_name1 = 'track'  # Update with your PostgreSQL table name
    pg_table_name2 = "trajectory"

    # # Insert data into PostgreSQL
    insert_sql1 = f"INSERT INTO {pg_table_name1} VALUES %s"
    insert_sql2 = f"INSERT INTO {pg_table_name2} VALUES %s"

    return insert_sql1, insert_sql2

    #df = pd.read_csv(insert_sql1, con= pg_conn)
    #hook = PostgresHook(postgres_conn_id=pg_conn_id)
    # hook.insert_rows(table=pg_table_name, rows=df.values.tolist(), commit_every=1000)


def load_data_1_into_dataframe(**kwargs):
    # Load CSV data into Pandas DataFrame
    csv_path = ".\track.csv"
    df1 = pd.read_csv(csv_path)
    return df1

def load_data_2_into_dataframe(**kwargs):
    # Load CSV data into Pandas DataFrame
    csv_path = ".\trajectory.csv"
    df2 = pd.read_csv(csv_path)
    return df2

[load_data_1_into_dataframe, load_data_2_into_dataframe] >> insert_dataframe_into_postgres 

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

# Set task dependencies
#load_data_prod_task >> load_data_dev_task >> load_data_staging_task
