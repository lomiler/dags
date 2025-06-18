from pyspark.sql import SparkSession, functions as f
from pyspark import SparkConf
from pyspark.sql.functions import col, exists, current_timestamp, to_timestamp, to_date, when, lit, coalesce, trim, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, TimestampType, BooleanType, LongType
import credentials

spark = SparkSession \
    .builder \
    .appName('orders_silver') \
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

# Parâmetros da conexão JDBC para o Postgres DW
jdbc_url = f"jdbc:postgresql://{credentials.POSTGRES_INSTANCE}:{credentials.POSTGRES_PORT}/{credentials.POSTGRES_DB}"

jdbc_properties = {
    "user": credentials.POSTGRES_USER,
    "password": credentials.POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Carrega os dados da bronze para um DataframeSPark já filtrando order_status diferente de unavailable
print("Lendo dados da camada Bronze (bronze.orders_raw) para transformação Silver...")
bronze_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "bronze.orders_raw") \
    .load() \
    .filter(col("order_status") != "unavailable")

print(f"Total de linhas lidas da Bronze (após filtro de status): {bronze_df.count()}")


print("Iniciando a transformação de Bronze para Silver...")

# 1. Tabela de Clientes (silver.tb_cliente)
print("Processando silver.tb_cliente...")
tb_cliente_df = bronze_df.select(
    "customer_id",
    "customer_city",
    "customer_state",
    "customer_zip_code_prefix"
).distinct()

tb_cliente_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "silver.tb_cliente") \
    .mode("overwrite") \
    .save()
print("Tabela silver.tb_cliente gravada com sucesso.")


# 2. Tabela de Produtos (silver.tb_produto)
print("Processando silver.tb_produto...")
tb_produto_df = bronze_df.select(
    "product_id",
    "product_category_name",
    col("product_photos_qty").alias("photos_qty"),
    col("product_weight_g").alias("weight_g"),
    col("product_length_cm").alias("length_cm"),
    col("product_height_cm").alias("height_cm"),
    col("product_width_cm").alias("width_cm")
).distinct()

tb_produto_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "silver.tb_produto") \
    .mode("overwrite") \
    .save()
print("Tabela silver.tb_produto gravada com sucesso.")

# --- 3. Tabela de Revisões (silver.tb_revisao) ---
print("Processando silver.tb_revisao...")
tb_revisao_df = bronze_df.select(
    "order_id",
    "review_score",
    when(col("review_comment_title").isNull() | (lower(trim(col("review_comment_title"))) == "nan") | (trim(col("review_comment_title")) == ""), "Não informado").otherwise(col("review_comment_title")).alias("review_comment_title"),
    when(col("review_comment_message").isNull() | (lower(trim(col("review_comment_message"))) == "nan") | (trim(col("review_comment_message")) == ""), "Não informado").otherwise(col("review_comment_message")).alias("review_comment_message"),
    "review_creation_date",
    "review_answer_timestamp"
).distinct() 
#.filter(lower(trim(col("review_score"))) != "nan") \


tb_revisao_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "silver.tb_revisao") \
    .mode("overwrite") \
    .save()
print("Tabela silver.tb_revisao gravada com sucesso.")

# 4. Tabela de Pedidos (silver.tb_pedidos)
print("Processando silver.tb_pedidos...")
tb_pedidos_df = bronze_df.select(
    "order_id",
    "customer_id",
    "product_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "price",
    "freight_value",
    "ingestion_timestamp"
)

tb_pedidos_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "silver.tb_pedidos") \
    .mode("overwrite") \
    .save()
print("Tabela silver.tb_pedidos gravada com sucesso.")

print("Transformação de Bronze para Silver concluída.")

spark.stop()