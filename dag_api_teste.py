from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'depends_on_past': False,
    #'retries': 1,
}

with DAG(
    dag_id='dag_teste_maxinutri',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'maxinutri']
) as dag:

    spark_task = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/opt/airflow/dags/api_teste_maxinutri.py',
        name='arrow-spark',
        verbose=True,
        master='spark://spark-master:7077',
    )

    spark_task
