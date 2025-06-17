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
#spark.sparkContext.setLogLevel("ERROR")

# URL da API e token de autenticação
URL_BASE = credentials.BASE_URL
API_TOKEN = credentials.API_TOKEN

# Define o schema manualmente com base no JSON fornecido
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("review_score", DoubleType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True),
    StructField("product_photos_qty", DoubleType(), True),
    StructField("product_weight_g", DoubleType(), True),
    StructField("product_length_cm", DoubleType(), True),
    StructField("product_height_cm", DoubleType(), True),
    StructField("product_width_cm", DoubleType(), True),
    StructField("customer_zip_code_prefix", IntegerType(), True)
])

collected_data = []

page = 1

while True:
    params = {
        'token': API_TOKEN,
        'page': page
    }
    response = requests.get(URL_BASE, params=params)
    
    if response.status_code != 200:
        print(f"Falha na requisição: Status {response.status_code}")
        break
    
    dados = response.json()
    
    if 'dados' not in dados or not dados['dados']:
        print("Sem mais dados para coletar.")
        break
    
    # Adiciona os dados coletados à lista
    collected_data.extend(dados['dados'])
    
    print(f"Página {dados['pagina']} de {dados.get('total_paginas', '?')} - Registros nesta página: {len(dados['dados'])}")
    
    if dados['pagina'] >= dados.get('total_paginas', dados['pagina']):
        # Última página
        break
    
    page += 1

# Cria DataFrame Spark com schema definido
df = spark.createDataFrame(collected_data, schema=schema)

# Exibe as primeiras 5 linhas
df.show(5, truncate=False)

# Conta o número total de linhas do DataFrame
total_linhas = df.count()

print(f"Total de linhas no DataFrame: {total_linhas}")
