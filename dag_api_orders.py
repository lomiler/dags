from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='dag_api_orders',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'maxinutri']
) as dag:

    dag_api_orders = SparkSubmitOperator(
        task_id='dag_api_orders',
        application='/opt/airflow/dags/api_orders.py',
        name='arrow-spark',
        verbose=True,
        driver_class_path='/opt/airflow/jars/postgresql-42.7.3.jar',
        jars='/opt/airflow/jars/postgresql-42.7.3.jar',
        conf={
            "spark.driver.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.7.3.jar",
            "spark.executor.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
        },
    )

    dag_orders_silver = SparkSubmitOperator(
        task_id='dag_orders_silver',
        application='/opt/airflow/dags/orders_silver.py',
        name='arrow-spark',
        verbose=True,
        driver_class_path='/opt/airflow/jars/postgresql-42.7.3.jar',
        jars='/opt/airflow/jars/postgresql-42.7.3.jar',
        conf={
            "spark.driver.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.7.3.jar",
            "spark.executor.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
        },
    )

    dag_orders_gold = SparkSubmitOperator(
        task_id='dag_orders_gold',
        application='/opt/airflow/dags/orders_gold.py',
        name='arrow-spark',
        verbose=True,
        driver_class_path='/opt/airflow/jars/postgresql-42.7.3.jar',
        jars='/opt/airflow/jars/postgresql-42.7.3.jar',
        conf={
            "spark.driver.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.7.3.jar",
            "spark.executor.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
        },
    )

    dag_api_orders >> dag_orders_silver >> dag_orders_gold