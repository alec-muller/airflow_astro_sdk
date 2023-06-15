# Curso em 4h 45min.

# importação
import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

import pandas as pd
from pandas import DataFrame

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections
S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
CURATED_ZONE = "curated"
OUTPUT_CONN_ID = "postgres_conn"

# default args & init dag
default_args = {
    "owner": "Alec R. Müller",
    "retries": 1,
    "retry_delay": 0
}


##
# START transforms {sql}
@aql.transform
def enrich_user_data(user: Table):
    return  """
            SELECT DISTINCT
                u.user_id,
                CONCAT(u.first_name, ' ', u.last_name) AS name,
                u.email,
                u.gender,
                u.phone_number,
                u.date_of_birth,
                CAST(REPLACE(u.employment, '''', '"') AS JSONB)->>'title' AS title,
                CAST(REPLACE(u.subscrition, '''', '"') AS JSONB)->>'payment_method' AS method
            FROM {{ user }} AS u;
            """

@aql.transform
def enrich_subscription_data(subscrition: Table):
    return  """
            SELECT DISTINCT
                u.user_id,
                CONCAT(u.first_name, ' ', u.last_name) AS name,
                u.email,
                u.gender,
                u.phone_number,
                u.date_of_birth,
                CAST(REPLACE(u.employment, '''', '"') AS JSONB)->>'title' AS title,
                CAST(REPLACE(u.subscrition, '''', '"') AS JSONB)->>'payment_method' AS method
            FROM {{ subscrition }} AS u;
            """
# END transforms {sql}
##


# START dataframes {pandas}
@aql.dataframe()
def plan_importance(subscription: pd.DataFrame):
    subscription["importance"] = subscription["plan"].apply(lambda types: "high" if types in ('Business', 'Diamond', 'Gold', 'Platinum', 'Premium') else "low")
    return subscription
# END dataframes {pandas}


# declare dag
@dag(
    dag_id = "demo-etl-subscriptions-analysis-dt",
    start_date = datetime(2023, 6, 14),
    max_active_runs = 1,
    schedule_interval = timedelta(hours=24),
    default_args = default_args,
    catchup = False,
    tags = ['development', 'elt', 'astrosdk']
)

# init main function
def etl_subscriptions_analysis():
    
    # init & finish task
    init = EmptyOperator(task_id="init")
    finish = EmptyOperator(task_id="finish")
    
    ###
    #   Carregamento de dados usando aql.load_file a partir do S3, levando para o Snowflake.
    ###
    
    # user
    user_file = aql.load_file(
        task_id = "user_file",
        input_file = File(path="s3://", filetype=FileType.JSON, conn_id=S3_CONN_ID),
        output_table = Table(name="user", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="public")),
        if_exists = "replace",
        use_native_support = True,
        columns_names_capitalization = "original"
    )
    
    # subscrition
    subscrition_file = aql.load_file(
        task_id = "subscrition_file",
        input_file = File(path="s3://", filetype=FileType.JSON, conn_id=S3_CONN_ID),
        output_table = Table(name="subscrition", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="public")),
        if_exists = "replace",
        use_native_support = True,
        columns_names_capitalization = "original"
    )
    
    ###
    #   Utilizando dados carregados em transformações SQL.
    ###
    
    # sanitize data = user & subscrition
    # create a function to query database
    user_sanitize = enrich_user_data(
        user = user_file,
        output_table = Table(name="sanitized_user", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="public"))
    )
    
    subscription_sanitize = enrich_subscription_data(
        subscrition = subscrition_file,
        output_table = Table(name="sanitized_subscription", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="public"))
    )
    
    # aql.dataframe = pd.DataFrame
    # apply logic to find out
    # users by importance -> [pandas dataframe]
    verify_plans_subscriptions = plan_importance(
        subscription=subscription_sanitize
    )
    
    #aql.transform = SQL
    # join datasets with new columns
    curated_subscriptions_dataset = join_tables(
        user=user_sanitize,
        subscription=verify_plans_subscriptions,
        output_table = Table(name="subscriptions", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="public"))
    )
    
    
    # TODO add quality rules
    quality_rules_check_table_level = aql.check_table(
        task_id = "quality_rules_check_table_level",
        dataset = final_curated_subscriptions_ds,
        checks={
            "row_count": {"check_statement": "Count(*) > 0"},
            "method": {"check_statement": "method IS NOT NULL"},
            "type": {"check_statement": "method IS NOT NULL"},
            "importance": {"check_statement": "importance IN('low', 'medium', 'high')"}
        }
    )
    
    # export to data lake
    # to be used by the offline services
    export_to_s3_curated_zone = aql.export_to_file(
        task_id = "export_to_s3_curated_zone",
        input_data = curated_subscriptions_dataset,
        output_file = File(path="s3://curated/{{ ds }}/subscriptions.parquet", conn_id=S3_CONN_ID),
        if_exists = "replace"
    )
    
    
    # load into warehouse system
    subscriptions_curated_warehouse = aql.load_file(
        input_file = File(path="s3://curated/{{ ds }}/subscriptions.parquet", filetype=FileType.PARQUET, conn_id=S3_CONN_ID),
        output_table = Table(name="subscriptions", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="public")),
        task_id = "subscriptions_curated_warehouse",
        if_exists = "replace",
        use_native_support = True,
        columns_names_capitalization = "original"
    )
    
    
    # define sequence 
    chain(
        init,
        [user_file, subscrition_file],
        [user_sanitize, subscription_sanitize],
        verify_plans_subscriptions,
        curated_subscriptions_dataset,
        quality_rules_check_table_level,
        [export_to_s3_curated_zone, subscriptions_curated_warehouse],
        finish
    )

# init dag
dag = demo-etl-subscriptions-analysis-dt