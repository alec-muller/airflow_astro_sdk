# import libs
import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

import pandas as pd
from pandas import DataFrame

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections
S3_CONN_ID = "aws_default"
OUPUT_CONN_ID = "snowflake_default"

# default args & init
default_args = {
    "owner": "Alec Müller",
    "retries": 1,
    "retry_delay": 0
}

# transforms
@aql.transform
def join_tables(user: Table, subscription: Table):
    return """
    SELECT  u.user_id,
            u.first_name,
            u.last_name,
            s.user_id AS superior_id,
            s.first_name AS nome_sup,
            s.last_name AS sobrenome_sup
    FROM {{ user }} AS u
    INNER JOIN {{ subscription }} AS s ON u.user_id = s.user_id
    """

# declare dag
@dag(
    dag_id="transform-user-subscription",
    start_date=datetime(2023, 6, 13),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['dev', 'elt', 'astrosdk', 's3', 'transform']
)

# init main function
def transform_etl():

    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")
    
    arquivo = File(path="s3://alec-bucket/users/", filetype=FileType.JSON,
                        conn_id=S3_CONN_ID)

    # users
    df_user = aql.load_file(
        task_id="df_user",
        input_file=arquivo,
        output_table=Table(name="user", conn_id=OUPUT_CONN_ID, metadata=Metadata(schema="public"),),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # subscription
    df_subscription = aql.load_file(
        task_id="df_subscription",
        input_file=arquivo,
        output_table=Table(name="subscription", conn_id=OUPUT_CONN_ID, metadata=Metadata(schema="public"),),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # join datasets using query logic
    # storing output in a table
    join_datasets = join_tables(
        user=df_user,
        subscription=df_subscription,
        output_table=Table(name="subscriptions", conn_id=OUPUT_CONN_ID, metadata=Metadata(schema="public")),
    )

    # define sequence
    init_data_load >> [df_user, df_subscription] >> join_datasets >> finish_data_load

# init dag
dag = transform_etl()