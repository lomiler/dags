# Projeto de Engenharia de Dados com Airflow - Spark Cluster - Pyspark - PostgreSQL

## Visão Geral
Este projeto tem como objetivo consumir dados de uma API, recepcionando o dado JSON, inserindo-o no banco de dados estruturado. 
Usar esses dados para criar um modelo de dados (Star Schema) para subsidiar consultas ou sistemas OLAPs para apoio à decisões.

# Definições do Projeto
1) Usado o conceito de Medalhão (Bronze, Silver e Gold).
  1.1) Bronze - Dado raw gravado em banco de dado estruturado sem nenhum tratamento ou limpeza de dados.
  1.2) Silver - Consumindo da camada Bronze, separando em tabelas cada "Entidade" e seus respectivos atributos. Aplicado também as devidas limpezas e filtros necessários.
  1.3) Gold - Consumindo as tabelas criadas na Silver, criou-se as respectivas dimensões e fato, respeitando as melhores práticas (sorrogate key, SCD (se necessário), chaves e constraints)

## Ferramentas Utilizadas
1) Airflow - Como agendador e orquestrador dos pipelines de dados.
2) Apache Spark - Usado para processamento dos dados - processamento paralelo em cluster de computadores.
3) Python - Linguagem de programação para desenvolvimento, bem como aproveitamento de bibliotecas prontas para soluções de problemas/necessidades.
4) Postgres - Banco de dados relacional
5) Conteiners Docker

# Airflow
- Criado os conteiners:
  1) webserver (para parte visual do Airflow)
  2) scheduler (para agendamentos e orquestração)
 
# Apache Spark
- Criado os containers
  1) spark-master (gerencia e distribui os processamentos nos workers do cluster)
  2) spark-worker1 (executa o processamento enviado pelo master)
 
# Python
- Usado versão 3.11
- Bibliotecas:
  1) datetime
  2) requests
  3) math
  4) time
  5) psycopg2
  6) credentials (este foi um arquivo .py criado para armazenar as credenciais de API e Banco de dados - inserido no gitgnore para não ir ao projeto)
 
# Postgres
- Criado um container:
  1) postgres-1 (esse é para o Airflow gravar os logs)
  2) postgres_dw (este para o projeto de DW)
     2.1) Criado schemas:
       2.1.1) Bronze
       2.1.2) Silver
       2.1.3) Gold
 
# Docker
- Usado docker desktop com engine do Docker.
- Usado conceitos de docker-compose.yml e Dockerfiles especificos


## Detalhes do Projeto (execução)
1) DAG do Airflow (Está no arquivo dag_api_orders.py) - setado algums configurações de jars para conexão JDBC com o POSTGRES)

2) Pipeline BRONZE (Dado RAW): (Está no arquivo api_orders.py)
   Este pipeline tem como objetivo consumir a API disponibilizada, usando biblioteca requests do Python, Dataframe do Spark e gravando no Postgres na respectiva tabela
   
3) Pipeline SILVER (Dados tratado e separado em tabelas diferentes): (Está no arquivo orders_silver.py)
   Este pipeline consome dados da Bronze faz transformações e filtros e grava na SILVER nas respectivas tabelas.
   
4) Pipeline GOLD (Definição das tabelas de Dimensões e Fatos): (Está no arquivo orders_gold.py)
   Este pipeline consome dados da SILVER e cria as devidas SORROGATE KEYS, mantendo também a NATURAL KEY em todas as tabelas, bem como constraints etc

6) DDLs  estão no arquivo "ddls.sql"

## Stack de Dados
Informações sobre a stack de dados estão no repositório abaixo:
https://github.com/lomiler/stack_dados
