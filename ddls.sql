/*---------- BROZEN ----------*/

-- Criar schema bronze se não existir
CREATE SCHEMA IF NOT EXISTS bronze;

/*Tabela orders_raw*/
CREATE TABLE IF NOT EXISTS bronze.orders_raw (
    order_id TEXT,
    customer_id TEXT,
    product_id TEXT,
    review_score NUMERIC,
    product_category_name TEXT,
    order_status TEXT,
    customer_city TEXT,
    customer_state TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    price NUMERIC,
    freight_value NUMERIC,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATE,
    review_answer_timestamp TIMESTAMP,
    product_photos_qty INTEGER,
    product_weight_g NUMERIC,
    product_length_cm NUMERIC,
    product_height_cm NUMERIC,
    product_width_cm NUMERIC,
    customer_zip_code_prefix INTEGER,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



/*---------- SILVER ----------*/

-- Criar schema silver se não existir
CREATE SCHEMA IF NOT EXISTS silver;

--- Tabelas da Camada Silver ---

-- Tabela Cliente 
CREATE TABLE IF NOT EXISTS silver.tb_cliente (
    customer_id TEXT PRIMARY KEY,
    customer_city TEXT,
    customer_state TEXT,
    customer_zip_code_prefix INTEGER
);

-- Tabela Produto
CREATE TABLE IF NOT EXISTS silver.tb_produto (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    photos_qty INTEGER,
    weight_g NUMERIC,
    length_cm NUMERIC,
    height_cm NUMERIC,
    width_cm NUMERIC
);

-- Tabela Revisão
CREATE TABLE IF NOT EXISTS silver.tb_revisao (
    order_id TEXT PRIMARY KEY,
    review_score NUMERIC,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATE,
    review_answer_timestamp TIMESTAMP
);

-- Tabela Pedidos
CREATE TABLE IF NOT EXISTS silver.tb_pedidos (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT, -- Chave Natural para tb_cliente
    product_id TEXT,  -- Chave Natural para tb_produto
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    price NUMERIC,
    freight_value NUMERIC,
    ingestion_timestamp TIMESTAMP
);


/*---------- GOLD ----------*/
-- Criar schema gold se não existir
CREATE SCHEMA IF NOT EXISTS gold;


--- Tabelas da Camada Silver ---

-- Tabela Dimensão Cliente
CREATE TABLE IF NOT EXISTS gold.dim_cliente (
    cliente_sk BIGSERIAL PRIMARY KEY, -- Surrogate Key, auto-incrementável
    customer_id TEXT UNIQUE NOT NULL, -- Natural Key, deve ser única e não nula
    customer_city TEXT,
    customer_state TEXT,
    customer_zip_code_prefix INTEGER,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Para rastrear quando o registro foi inserido/atualizado na Gold
);

-- Tabela Dimensão Produto
CREATE TABLE IF NOT EXISTS gold.dim_produto (
    produto_sk BIGSERIAL PRIMARY KEY, -- Surrogate Key, auto-incrementável
    product_id TEXT UNIQUE NOT NULL, -- Natural Key, deve ser única e não nula
    product_category_name TEXT,
    photos_qty INTEGER,
    weight_g NUMERIC,
    length_cm NUMERIC,
    height_cm NUMERIC,
    width_cm NUMERIC,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela Dimensão Revisão
CREATE TABLE IF NOT EXISTS gold.dim_revisao (
    revisao_sk BIGSERIAL PRIMARY KEY, -- Surrogate Key, auto-incrementável
    order_id_nk TEXT UNIQUE NOT NULL, -- Natural Key (do pedido associado à revisão)
    review_score NUMERIC,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATE,
    review_answer_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela Fato Cliente
CREATE TABLE IF NOT EXISTS  gold.fato_pedidos (
    fato_pedido_sk BIGSERIAL PRIMARY KEY, -- Surrogate Key para a tabela de fato (opcional, mas boa prática para PK)
    order_id TEXT UNIQUE NOT NULL,       -- Natural Key do pedido
    
    -- Foreign Keys (Surrogate Keys das Dimensões)
    cliente_sk BIGINT NOT NULL,
    produto_sk BIGINT NOT NULL,
    revisao_sk BIGINT NOT NULL,
    
    -- Métricas e atributos diretos da fato
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    price NUMERIC,
    freight_value NUMERIC,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Definição das Chaves Estrangeiras (FOREIGN KEYs)
    CONSTRAINT fk_cliente
        FOREIGN KEY (cliente_sk)
        REFERENCES gold.dim_cliente (cliente_sk),
    CONSTRAINT fk_produto
        FOREIGN KEY (produto_sk)
        REFERENCES gold.dim_produto (produto_sk),
    CONSTRAINT fk_revisao
        FOREIGN KEY (revisao_sk)
        REFERENCES gold.dim_revisao (revisao_sk)
);