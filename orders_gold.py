from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

import credentials
import psycopg2

# Inicializa a sessão Spark
spark = SparkSession \
    .builder \
    .appName('silver-to-gold-transformation') \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.sql.session.timeZone", "America/Sao_Paulo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Parâmetros da conexão JDBC para o Postgres DW (usados para leitura/escrita de DataFrames)
jdbc_url = f"jdbc:postgresql://{credentials.POSTGRES_INSTANCE}:{credentials.POSTGRES_PORT}/{credentials.POSTGRES_DB}"
jdbc_properties = {
    "user": credentials.POSTGRES_USER,
    "password": credentials.POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Parâmetros para a conexão com psycopg2 (para DMLs diretas)
db_params_psycopg2 = {
    "host": credentials.POSTGRES_INSTANCE,
    "database": credentials.POSTGRES_DB,
    "user": credentials.POSTGRES_USER,
    "password": credentials.POSTGRES_PASSWORD,
    "port": credentials.POSTGRES_PORT
}


print("Iniciando a transformação de Silver para Gold...")

# --- Função auxiliar para executar comandos SQL usando psycopg2 ---
def execute_sql_query_psycopg2(sql_query, db_params):
    """Executa uma query SQL arbitrária no banco de dados usando psycopg2."""
    print(f"Executando SQL via psycopg2:\n{sql_query}")
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
        cur.close()
        print("SQL executado com sucesso via psycopg2.")
    except Exception as e:
        print(f"ERRO ao executar SQL via psycopg2: {e}")
        if conn:
            conn.rollback() # Garante rollback em caso de erro
        raise e # Levanta o erro para que a DAG falhe
    finally:
        if conn:
            conn.close()


# --- ETAPA 0: TRUNCATE DE TODAS AS TABELAS GOLD NA ORDEM CORRETA ---
# TRUNCATE DA FATO PRIMEIRO, DEPOIS AS DIMENSÕES (para respeitar chaves estrangeiras)
try:
    print("Iniciando TRUNCATE das tabelas Gold (Fato primeiro, depois Dimensões)...")
    
    # TRUNCATE fato_pedidos
    execute_sql_query_psycopg2("TRUNCATE TABLE gold.fato_pedidos RESTART IDENTITY CASCADE;", db_params_psycopg2)
    print("gold.fato_pedidos truncada com sucesso.")

    # TRUNCATE dim_cliente
    execute_sql_query_psycopg2("TRUNCATE TABLE gold.dim_cliente RESTART IDENTITY CASCADE;", db_params_psycopg2)
    print("gold.dim_cliente truncada com sucesso.")

    # TRUNCATE dim_produto
    execute_sql_query_psycopg2("TRUNCATE TABLE gold.dim_produto RESTART IDENTITY CASCADE;", db_params_psycopg2)
    print("gold.dim_produto truncada com sucesso.")

    # TRUNCATE dim_revisao
    execute_sql_query_psycopg2("TRUNCATE TABLE gold.dim_revisao RESTART IDENTITY CASCADE;", db_params_psycopg2)
    print("gold.dim_revisao truncada com sucesso.")

except Exception as e:
    print(f"ERRO: Falha ao executar TRUNCATE em tabelas Gold: {e}")
    raise e # Levanta o erro para que a DAG falhe


# --- ETAPA 1: Carrega os dados das tabelas Silver ---
print("Lendo dados da camada Silver...")
silver_cliente_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "silver.tb_cliente") \
    .load()

silver_produto_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "silver.tb_produto") \
    .load()

silver_revisao_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "silver.tb_revisao") \
    .load()

silver_pedidos_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "silver.tb_pedidos") \
    .load()

print(f"Total de linhas lidas da Silver (tb_cliente): {silver_cliente_df.count()}")
print(f"Total de linhas lidas da Silver (tb_produto): {silver_produto_df.count()}")
print(f"Total de linhas lidas da Silver (tb_revisao): {silver_revisao_df.count()}")
print(f"Total de linhas lidas da Silver (tb_pedidos): {silver_pedidos_df.count()}")


# --- ETAPA 2: Carregar Dimensões (gold.dim_cliente, gold.dim_produto, gold.dim_revisao) com APPEND ---
# Agora que truncamos, podemos usar append para o carregamento inicial.

# 2.1. Dimensão Cliente (gold.dim_cliente)
print("Carregando gold.dim_cliente...")
dim_cliente_gold_df = silver_cliente_df.select(
    col("customer_id"),
    col("customer_city"),
    col("customer_state"),
    col("customer_zip_code_prefix")
).withColumn("ingestion_timestamp", current_timestamp())

dim_cliente_gold_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "gold.dim_cliente") \
    .mode("append") \
    .save()
print("gold.dim_cliente carregada.")


# 2.2. Dimensão Produto (gold.dim_produto)
print("Carregando gold.dim_produto...")
dim_produto_gold_df = silver_produto_df.select(
    col("product_id"),
    col("product_category_name"),
    col("photos_qty"),
    col("weight_g"),
    col("length_cm"),
    col("height_cm"),
    col("width_cm")
).withColumn("ingestion_timestamp", current_timestamp())

dim_produto_gold_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "gold.dim_produto") \
    .mode("append") \
    .save()
print("gold.dim_produto carregada.")


# 2.3. Dimensão Revisão (gold.dim_revisao)
print("Carregando gold.dim_revisao...")
dim_revisao_gold_df = silver_revisao_df.select(
    col("order_id").alias("order_id_nk"), # Renomeando para NK conforme DDL
    col("review_score"),
    col("review_comment_title"),
    col("review_comment_message"),
    col("review_creation_date"),
    col("review_answer_timestamp")
).withColumn("ingestion_timestamp", current_timestamp())

dim_revisao_gold_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "gold.dim_revisao") \
    .mode("append") \
    .save()
print("gold.dim_revisao carregada.")


# --- ETAPA 3: Carregar Tabela de Fato (gold.fato_pedidos) ---

# Para popular a Fato, precisamos das Surrogate Keys das dimensões.
# Lemos as dimensões *recém-carregadas* na Gold para pegar as SKs e mapeá-las para as NKs.
print("Lendo dimensões Gold recém-carregadas para mapeamento de SKs para a FATO...")

dim_cliente_map_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "gold.dim_cliente") \
    .load() \
    .select(col("customer_id").alias("customer_id_dim"), "cliente_sk")

dim_produto_map_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "gold.dim_produto") \
    .load() \
    .select(col("product_id").alias("product_id_dim"), "produto_sk")

dim_revisao_map_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "gold.dim_revisao") \
    .load() \
    .select(col("order_id_nk").alias("order_id_dim"), "revisao_sk")


# Junta o DataFrame de pedidos (Silver) com os DataFrames de mapeamento das SKs
print("Juntando dados da fato com as SKs das dimensões...")
fact_pedidos_gold_df = silver_pedidos_df \
    .join(dim_cliente_map_df, silver_pedidos_df.customer_id == dim_cliente_map_df.customer_id_dim, "left") \
    .join(dim_produto_map_df, silver_pedidos_df.product_id == dim_produto_map_df.product_id_dim, "left") \
    .join(dim_revisao_map_df, silver_pedidos_df.order_id == dim_revisao_map_df.order_id_dim, "left") \
    .select(
        silver_pedidos_df.order_id, # Natural Key do pedido
        col("cliente_sk"),
        col("produto_sk"),
        col("revisao_sk"),
        silver_pedidos_df.order_status,
        silver_pedidos_df.order_purchase_timestamp,
        silver_pedidos_df.order_approved_at,
        silver_pedidos_df.order_delivered_carrier_date,
        silver_pedidos_df.order_delivered_customer_date,
        silver_pedidos_df.order_estimated_delivery_date,
        silver_pedidos_df.price,
        silver_pedidos_df.freight_value
    ).withColumn("ingestion_timestamp", current_timestamp())

# O .mode("append") é usado aqui porque a tabela já foi truncada acima.
fact_pedidos_gold_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_properties) \
    .option("dbtable", "gold.fato_pedidos") \
    .mode("append") \
    .save()
print("gold.fato_pedidos carregada.")

print("Transformação de Silver para Gold concluída.")

spark.stop()