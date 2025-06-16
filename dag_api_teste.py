import airflow
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
import sys
sys.path.append('/opt/airflow/dags')

default_args = {
    'owner': 'Jonas Lomiler',
    'start_date': pendulum.datetime(2025, 6, 16, tz='America/Sao_Paulo'),
}

with airflow.DAG('dag_teste_maxinutri',
                  default_args=default_args,
                  schedule_interval='20 00 * * *',
                  catchup=False,
                  tags=['API','DESENV']) as dag:    
    # Scripts
    dag_teste_maxinutri = SparkSubmitOperator(
        task_id='dag_teste_maxinutri',  
        application="api_teste_maxinutri.py",
        #Forçar o Spark standalone
        conf={"spark.master": "spark://spark-master:7077"},
    )

    
    # Setting the execution order
    dag_teste_maxinutri 