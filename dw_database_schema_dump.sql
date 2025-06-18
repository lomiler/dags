--
-- PostgreSQL database dump
--

-- Dumped from database version 14.0 (Debian 14.0-1.pgdg110+1)
-- Dumped by pg_dump version 14.0 (Debian 14.0-1.pgdg110+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: bronze; Type: SCHEMA; Schema: -; Owner: dw_user
--

CREATE SCHEMA bronze;


ALTER SCHEMA bronze OWNER TO dw_user;

--
-- Name: gold; Type: SCHEMA; Schema: -; Owner: dw_user
--

CREATE SCHEMA gold;


ALTER SCHEMA gold OWNER TO dw_user;

--
-- Name: silver; Type: SCHEMA; Schema: -; Owner: dw_user
--

CREATE SCHEMA silver;


ALTER SCHEMA silver OWNER TO dw_user;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: orders_raw; Type: TABLE; Schema: bronze; Owner: dw_user
--

CREATE TABLE bronze.orders_raw (
    order_id text,
    customer_id text,
    product_id text,
    review_score double precision,
    product_category_name text,
    order_status text,
    customer_city text,
    customer_state text,
    order_purchase_timestamp timestamp without time zone,
    order_approved_at timestamp without time zone,
    order_delivered_carrier_date timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date timestamp without time zone,
    price double precision,
    freight_value double precision,
    review_comment_title text,
    review_comment_message text,
    review_creation_date date,
    review_answer_timestamp timestamp without time zone,
    product_photos_qty integer,
    product_weight_g double precision,
    product_length_cm double precision,
    product_height_cm double precision,
    product_width_cm double precision,
    customer_zip_code_prefix integer,
    ingestion_timestamp timestamp without time zone NOT NULL
);


ALTER TABLE bronze.orders_raw OWNER TO dw_user;

--
-- Name: dim_cliente; Type: TABLE; Schema: gold; Owner: dw_user
--

CREATE TABLE gold.dim_cliente (
    cliente_sk bigint NOT NULL,
    customer_id text NOT NULL,
    customer_city text,
    customer_state text,
    customer_zip_code_prefix integer,
    ingestion_timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE gold.dim_cliente OWNER TO dw_user;

--
-- Name: dim_cliente_cliente_sk_seq; Type: SEQUENCE; Schema: gold; Owner: dw_user
--

CREATE SEQUENCE gold.dim_cliente_cliente_sk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE gold.dim_cliente_cliente_sk_seq OWNER TO dw_user;

--
-- Name: dim_cliente_cliente_sk_seq; Type: SEQUENCE OWNED BY; Schema: gold; Owner: dw_user
--

ALTER SEQUENCE gold.dim_cliente_cliente_sk_seq OWNED BY gold.dim_cliente.cliente_sk;


--
-- Name: dim_produto; Type: TABLE; Schema: gold; Owner: dw_user
--

CREATE TABLE gold.dim_produto (
    produto_sk bigint NOT NULL,
    product_id text NOT NULL,
    product_category_name text,
    photos_qty integer,
    weight_g numeric,
    length_cm numeric,
    height_cm numeric,
    width_cm numeric,
    ingestion_timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE gold.dim_produto OWNER TO dw_user;

--
-- Name: dim_produto_produto_sk_seq; Type: SEQUENCE; Schema: gold; Owner: dw_user
--

CREATE SEQUENCE gold.dim_produto_produto_sk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE gold.dim_produto_produto_sk_seq OWNER TO dw_user;

--
-- Name: dim_produto_produto_sk_seq; Type: SEQUENCE OWNED BY; Schema: gold; Owner: dw_user
--

ALTER SEQUENCE gold.dim_produto_produto_sk_seq OWNED BY gold.dim_produto.produto_sk;


--
-- Name: dim_revisao; Type: TABLE; Schema: gold; Owner: dw_user
--

CREATE TABLE gold.dim_revisao (
    revisao_sk bigint NOT NULL,
    order_id_nk text NOT NULL,
    review_score numeric,
    review_comment_title text,
    review_comment_message text,
    review_creation_date date,
    review_answer_timestamp timestamp without time zone,
    ingestion_timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE gold.dim_revisao OWNER TO dw_user;

--
-- Name: dim_revisao_revisao_sk_seq; Type: SEQUENCE; Schema: gold; Owner: dw_user
--

CREATE SEQUENCE gold.dim_revisao_revisao_sk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE gold.dim_revisao_revisao_sk_seq OWNER TO dw_user;

--
-- Name: dim_revisao_revisao_sk_seq; Type: SEQUENCE OWNED BY; Schema: gold; Owner: dw_user
--

ALTER SEQUENCE gold.dim_revisao_revisao_sk_seq OWNED BY gold.dim_revisao.revisao_sk;


--
-- Name: fato_pedidos; Type: TABLE; Schema: gold; Owner: dw_user
--

CREATE TABLE gold.fato_pedidos (
    fato_pedido_sk bigint NOT NULL,
    order_id text NOT NULL,
    cliente_sk bigint NOT NULL,
    produto_sk bigint NOT NULL,
    revisao_sk bigint NOT NULL,
    order_status text,
    order_purchase_timestamp timestamp without time zone,
    order_approved_at timestamp without time zone,
    order_delivered_carrier_date timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date timestamp without time zone,
    price numeric,
    freight_value numeric,
    ingestion_timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE gold.fato_pedidos OWNER TO dw_user;

--
-- Name: fato_pedidos_fato_pedido_sk_seq; Type: SEQUENCE; Schema: gold; Owner: dw_user
--

CREATE SEQUENCE gold.fato_pedidos_fato_pedido_sk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE gold.fato_pedidos_fato_pedido_sk_seq OWNER TO dw_user;

--
-- Name: fato_pedidos_fato_pedido_sk_seq; Type: SEQUENCE OWNED BY; Schema: gold; Owner: dw_user
--

ALTER SEQUENCE gold.fato_pedidos_fato_pedido_sk_seq OWNED BY gold.fato_pedidos.fato_pedido_sk;


--
-- Name: temp_gold_dim_cliente_staging; Type: TABLE; Schema: public; Owner: dw_user
--

CREATE TABLE public.temp_gold_dim_cliente_staging (
    customer_id text,
    customer_city text,
    customer_state text,
    customer_zip_code_prefix integer,
    ingestion_timestamp timestamp without time zone NOT NULL
);


ALTER TABLE public.temp_gold_dim_cliente_staging OWNER TO dw_user;

--
-- Name: tb_cliente; Type: TABLE; Schema: silver; Owner: dw_user
--

CREATE TABLE silver.tb_cliente (
    customer_id text,
    customer_city text,
    customer_state text,
    customer_zip_code_prefix integer
);


ALTER TABLE silver.tb_cliente OWNER TO dw_user;

--
-- Name: tb_pedidos; Type: TABLE; Schema: silver; Owner: dw_user
--

CREATE TABLE silver.tb_pedidos (
    order_id text,
    customer_id text,
    product_id text,
    order_status text,
    order_purchase_timestamp timestamp without time zone,
    order_approved_at timestamp without time zone,
    order_delivered_carrier_date timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date timestamp without time zone,
    price double precision,
    freight_value double precision,
    ingestion_timestamp timestamp without time zone
);


ALTER TABLE silver.tb_pedidos OWNER TO dw_user;

--
-- Name: tb_produto; Type: TABLE; Schema: silver; Owner: dw_user
--

CREATE TABLE silver.tb_produto (
    product_id text,
    product_category_name text,
    photos_qty integer,
    weight_g double precision,
    length_cm double precision,
    height_cm double precision,
    width_cm double precision
);


ALTER TABLE silver.tb_produto OWNER TO dw_user;

--
-- Name: tb_revisao; Type: TABLE; Schema: silver; Owner: dw_user
--

CREATE TABLE silver.tb_revisao (
    order_id text,
    review_score double precision,
    review_comment_title text,
    review_comment_message text,
    review_creation_date date,
    review_answer_timestamp timestamp without time zone
);


ALTER TABLE silver.tb_revisao OWNER TO dw_user;

--
-- Name: dim_cliente cliente_sk; Type: DEFAULT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.dim_cliente ALTER COLUMN cliente_sk SET DEFAULT nextval('gold.dim_cliente_cliente_sk_seq'::regclass);


--
-- Name: dim_produto produto_sk; Type: DEFAULT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.dim_produto ALTER COLUMN produto_sk SET DEFAULT nextval('gold.dim_produto_produto_sk_seq'::regclass);


--
-- Name: dim_revisao revisao_sk; Type: DEFAULT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.dim_revisao ALTER COLUMN revisao_sk SET DEFAULT nextval('gold.dim_revisao_revisao_sk_seq'::regclass);


--
-- Name: fato_pedidos fato_pedido_sk; Type: DEFAULT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.fato_pedidos ALTER COLUMN fato_pedido_sk SET DEFAULT nextval('gold.fato_pedidos_fato_pedido_sk_seq'::regclass);


--
-- Name: dim_cliente dim_cliente_customer_id_key; Type: CONSTRAINT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.dim_cliente
    ADD CONSTRAINT dim_cliente_customer_id_key UNIQUE (customer_id);


--
-- Name: dim_cliente dim_cliente_pkey; Type: CONSTRAINT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.dim_cliente
    ADD CONSTRAINT dim_cliente_pkey PRIMARY KEY (cliente_sk);


--
-- Name: dim_produto dim_produto_pkey; Type: CONSTRAINT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.dim_produto
    ADD CONSTRAINT dim_produto_pkey PRIMARY KEY (produto_sk);


--
-- Name: dim_produto dim_produto_product_id_key; Type: CONSTRAINT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.dim_produto
    ADD CONSTRAINT dim_produto_product_id_key UNIQUE (product_id);


--
-- Name: dim_revisao dim_revisao_order_id_nk_key; Type: CONSTRAINT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.dim_revisao
    ADD CONSTRAINT dim_revisao_order_id_nk_key UNIQUE (order_id_nk);


--
-- Name: dim_revisao dim_revisao_pkey; Type: CONSTRAINT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.dim_revisao
    ADD CONSTRAINT dim_revisao_pkey PRIMARY KEY (revisao_sk);


--
-- Name: fato_pedidos fato_pedidos_order_id_key; Type: CONSTRAINT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.fato_pedidos
    ADD CONSTRAINT fato_pedidos_order_id_key UNIQUE (order_id);


--
-- Name: fato_pedidos fato_pedidos_pkey; Type: CONSTRAINT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.fato_pedidos
    ADD CONSTRAINT fato_pedidos_pkey PRIMARY KEY (fato_pedido_sk);


--
-- Name: fato_pedidos fk_cliente; Type: FK CONSTRAINT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.fato_pedidos
    ADD CONSTRAINT fk_cliente FOREIGN KEY (cliente_sk) REFERENCES gold.dim_cliente(cliente_sk);


--
-- Name: fato_pedidos fk_produto; Type: FK CONSTRAINT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.fato_pedidos
    ADD CONSTRAINT fk_produto FOREIGN KEY (produto_sk) REFERENCES gold.dim_produto(produto_sk);


--
-- Name: fato_pedidos fk_revisao; Type: FK CONSTRAINT; Schema: gold; Owner: dw_user
--

ALTER TABLE ONLY gold.fato_pedidos
    ADD CONSTRAINT fk_revisao FOREIGN KEY (revisao_sk) REFERENCES gold.dim_revisao(revisao_sk);


--
-- PostgreSQL database dump complete
--

