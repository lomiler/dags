from pyspark.sql import SparkSession, functions as f
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.functions import  col, exists
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, TimestampType, BooleanType, LongType
import json
import credentials
import requests
from datetime import date, timedelta

spark = SparkSession \
    .builder \
    .appName('dag_teste_maxinutri') \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# show configured parameters
print(SparkConf().getAll())

# set log level
spark.sparkContext.setLogLevel("ERROR")


# URL da API e token de autenticação
URL_BASE = credentials.BASE_URL
API_TOKEN = credentials.API_TOKEN

HEADERS = {
    "Authorization": API_TOKEN,
    "Content-Type": "application/json"
}
page = 1

params = {
            'page': page
        }

#Schema
schema = StructType([
    StructField("coluna1", StringType(), True),
    StructField("coluna2", StringType(), True)
])

# Verificando se a requisição foi bem-sucedida
collected_data = []

response = requests.request("POST", URL_BASE, headers=HEADERS, params=params)
page = 1

if response.status_code == 200:
    print(f"Resposta da API: {response.status_code}")
else:
    print(f"Erro com resposta da API: {response.status_code}")
   

