import pandas as pd
from sqlalchemy import create_engine

# load your data
data1 = pd.read_csv('track.csv')
data2 = pd.read_csv('trajectory.csv')

# create the engine
engine = create_engine('postgresql://username:password@localhost:5432/dbname')

# write data to PostgreSQL
data1.to_sql('track', engine, if_exists='replace')
data2.to_sql('trajectory', engine, if_exists='replace')

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator



with DAG('Load data using Airflow', 
         start_date = datetime(2023, 12, 12),
         schedule_interval = "@daily",
         catchup = False,
         ) as dag:
    
    # 1st Task:
    load = PythonOperator(
        task_id = "Load data",
        python_callable = load
    )


def load(**kwargs):
    
# Load your CSV file
df_track = pd.read_csv('./data/track.csv')
df_trajectory = pd.read_csv('./data/trajectory.csv')

# Create the connection string
engine = create_engine('postgresql://airflow:airflowd@localhost:9090/postgre')

# Load data
df_track.to_sql('track_table', engine)
df_trajectory.to_sql('trajectory_table', engine)