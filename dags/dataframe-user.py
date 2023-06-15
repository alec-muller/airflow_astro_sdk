# TODO always develop your DAGs using TaskFlowAPI
"""
Tasks performed by this DAG:
"""

# importa libs
from datetime import datetime, timedelta
from airflow.decorators import dag, task
#from airflow.operators import DummyOperator
from airflow.operators.empty import EmptyOperator

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

import pandas as pd
from pandas import DataFrame

# connections & variaveis
AWS_CONN_ID = "aws_default"
#SNOWFLAKE_CONN_ID = "snowflake_default"

# default args
default_args = {
    "owner": "Alec Müller",
    "retries": 1,
    "retry_delay": 0
}

# dataframe
@aql.dataframe(columns_names_capitalization="original")
def most_active_plans(users: pd.DataFrame):
    print(f"Total de linhas {len(users)}")
    grouped_plans = users.groupby(['gender'])['gender'].count()
    print(f"Platform genders: {grouped_plans}")
    return grouped_plans.to_dict()

# init dag
@dag(
    dag_id="dataframe-user",
    start_date=datetime(2023, 6, 13),
    schedule_interval=timedelta(hours=24),
    max_active_runs=1, # qtd de vezes em que a DAG pode rodar simultaneamente 
    catchup=False, # Se a dag ficou inativa e estiver marcado como true, quando ativar irá executar todas as vezes perdidas
    default_args=default_args, # chama as informações padrões setadas acima
    owner_links={"linkedin": "https://www.linkedin.com/in/alec-muller/"},
    tags=['pandas', 'json', 'users', 'astro-sdk', 'aws']
)

# declaração da main function do airflow
def dataframe_etl():
    
    # init & finish
    init = EmptyOperator(task_id="init")
    finish = EmptyOperator(task_id="finish")

    # ingest from lake to airflow
    users_file = aql.load_file(
        task_id="users_df",
        input_file=File(path="s3://alec-bucket/users/", filetype=FileType.JSON,
                        conn_id=AWS_CONN_ID)
        #if_exists="replace",
        #use_native_support=True,
        #columns_names_capitalization="original"
    )

    plans = most_active_plans(users=users_file)

    # define sequencia
    init >> users_file >> plans >> finish

# init
dag = dataframe_etl()