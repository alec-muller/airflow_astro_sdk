# TODO always develop your DAGs using TaskFlowAPI
"""
Tasks performed by this DAG:
"""

# importa libs
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_var():
    my_var = 1
    print(my_var)

with DAG(dag_id='exemplo', start_date=datetime(2021, 10, 1)) as dag:
    task_print_var = PythonOperator(
        task_id='imprimir_variavel',
        python_callable=print_var
    )