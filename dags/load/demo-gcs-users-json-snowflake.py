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

# connections & variaveis
GCP_CONN_ID = "google_cloud_default"
SNOWFLAKE_CONN_ID = "snowflake_default"

# default args
default_args = {
    "owner": "Alec Müller",
    "retries": 1,
    "retry_delay": 0
}

# init dag
@dag(
    dag_id="demo-gcs-users-json-snowflake",
    start_date=datetime(2023, 4, 15),
    schedule_interval=timedelta(hours=24),
    max_active_runs=1, # qtd de vezes em que a DAG pode rodar simultaneamente 
    catchup=False, # Se a dag ficou inativa e estiver marcado como true, quando ativar irá executar todas as vezes perdidas
    default_args=default_args, # chama as informações padrões setadas acima
    owner_links={"linkedin": "https://www.linkedin.com/in/alec-muller/"},
    tags=['gcs', 'json', 'users', 'astro-sdk', 'snowflake', 'bigquery']
)

# declaração da main function do airflow
def load_files_warehouse():
    
    # init & finish
    init = EmptyOperator(task_id="init")
    finish = EmptyOperator(task_id="finish")

    # ingest from lake to snowflake
    users_json_files = aql.load_file(
        task_id="users_json_files",
        input_file=File(path="< caminho p/ google cloud storage. Ex: gs://worfj-landing-zone/users >", filetype=FileType.JSON,
                        conn_id=GCP_CONN_ID),
        output_table=Table(name="users", conn_id=SNOWFLAKE_CONN_ID), # metadata=Metadata(schema="NomeSchema")),
        if_exists="replace",    
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # define sequencia
    init >> finish

# init
dag = load_files_warehouse()