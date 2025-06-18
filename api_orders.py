from pyspark.sql import SparkSession, functions as f
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.functions import col, exists, current_timestamp, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, TimestampType, BooleanType, LongType
import json
import credentials
import requests
from datetime import date, timedelta
import math
import time

spark = SparkSession \
    .builder \
    .appName('api_orders') \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.sql.session.timeZone", "America/Sao_Paulo") \
    .getOrCreate()

# Mostra os prametros de configuração
print(SparkConf().getAll())

# seta o log level que será exíbido de mensagens de log
spark.sparkContext.setLogLevel("INFO")

# URL da API e token de autenticação
URL_BASE = credentials.BASE_URL
API_TOKEN = credentials.API_TOKEN

# Define o schema para DataFrame Spark
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

#Lista vazia para incremento no Loop
collected_data = []

#Variável para loop do pagination iniciando na página 1
page = 1

#Variável para controle do total de páginas.
total_paginas = None

#Inicio do LOOP para consumo da API agregando os dados das N páginas na lista collected_data
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
    
    # Calcula total de páginas na primeira iteração
    if total_paginas is None:
        total_linhas = dados.get('total_linhas', 0)
        linhas_por_pagina = dados.get('linhas_por_pagina', 1)
        total_paginas = math.ceil(total_linhas / linhas_por_pagina)
    
    # Adiciona os dados coletados à lista
    collected_data.extend(dados['dados'])
    
    print(f"Página {dados['pagina']} de {total_paginas} - Registros nesta página: {len(dados['dados'])}")
    
    if page >= total_paginas:
        break
    
    page += 1
    # Espera 1 segundo entre as chamadas para evitar flooding
    time.sleep(1)

# Cria DataFrame Spark com schema definido
df = spark.createDataFrame(collected_data, schema=schema)

# Converte as colunas de timestamp e date de string para os tipos corretos
df = df \
    .withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp")) \
    .withColumn("order_approved_at", to_timestamp("order_approved_at")) \
    .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date")) \
    .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date")) \
    .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date")) \
    .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp")) \
    .withColumn("review_creation_date", to_date("review_creation_date")) \
    .withColumn("product_photos_qty", col("product_photos_qty").cast("integer")) \
    .withColumn("product_weight_g", col("product_weight_g").cast("double")) \
    .withColumn("product_length_cm", col("product_length_cm").cast("double")) \
    .withColumn("product_height_cm", col("product_height_cm").cast("double")) \
    .withColumn("product_width_cm", col("product_width_cm").cast("double")) \
    .withColumn("customer_zip_code_prefix", col("customer_zip_code_prefix").cast("integer")) \
    .withColumn("ingestion_timestamp", current_timestamp())

print("Exibindo as 5 primeiras linhas do Dataframe: ")
df.show(5, truncate=False)

print(f"Total de linhas no DataFrame: {df.count()}")

# Parâmetros da conexão JDBC para o Postgres DW
jdbc_url = f"jdbc:postgresql://{credentials.POSTGRES_INSTANCE}:{credentials.POSTGRES_PORT}/{credentials.POSTGRES_DB}"

# Grava o DataFrame no Postgres no schema bronze, tabela orders_raw usando .format("jdbc").option(...)
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "org.postgresql.Driver") \
    .option("user", credentials.POSTGRES_USER) \
    .option("password", credentials.POSTGRES_PASSWORD) \
    .option("dbtable", "bronze.orders_raw") \
    .mode("overwrite") \
    .save()

print("Dados gravados com sucesso na tabela bronze.orders_raw")
spark.stop()