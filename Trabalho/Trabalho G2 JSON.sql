-- Databricks notebook source
CREATE TABLE IF NOT EXISTS bestsellers 
USING JSON OPTIONS(
  header = "true",
  path = "dbfs:/FileStore/shared_uploads/guilherme.graebim@hotmail.com/nyt2.json"
);

-- COMMAND ----------

SELECT * FROM bestsellers;

-- COMMAND ----------

1. Consulta para listar os 10 livros mais vendidos na semana atual
2. Consulta para listar os livros de um determinado autor
4. Consulta para listar os livros que estão na lista há mais de 10 semanas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **1. Consulta para listar os 10 livros mais vendidos na semana atual**

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW q1Results AS
  SELECT
    title,
    author,
    REGEXP_EXTRACT(CAST(rank AS STRING), r'^\{(.*)\}$', 1) AS rank,
    FROM_UNIXTIME(CAST(REGEXP_EXTRACT(CAST(published_date AS STRING), r'^\{\{(.*)\}\}$', 1) AS BIGINT) / 1000) AS published_date
  FROM
    bestsellers
  ORDER BY 
    rank ASC, published_date DESC
  LIMIT
    10;
  
SELECT * FROM q1Results

-- COMMAND ----------

CACHE TABLE bestsellers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **2. Consulta para listar os livros de um determinado autor**

-- COMMAND ----------

SELECT
  author,
  title,
  REGEXP_EXTRACT(CAST(rank AS STRING), r'^\{(.*)\}$', 1) AS rank,
  FROM_UNIXTIME(CAST(REGEXP_EXTRACT(CAST(published_date AS STRING), r'^\{\{(.*)\}\}$', 1) AS BIGINT) / 1000) AS published_date,
  description,
  price
FROM
  bestsellers
WHERE 
  author = 'Adriana Trigiani'
ORDER BY 
  published_date ASC;



-- COMMAND ----------

UNCACHE TABLE  IF EXISTS bestsellers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **3. Consulta para listar os livros que estão na lista há mais de 10 semanas**

-- COMMAND ----------

SELECT *
FROM (
  SELECT    
    author,
    title,
    REGEXP_EXTRACT(CAST(rank AS STRING), r'^\{(.*)\}$', 1) AS rank,
    FROM_UNIXTIME(CAST(REGEXP_EXTRACT(CAST(published_date AS STRING), r'^\{\{(.*)\}\}$', 1) AS BIGINT) / 1000) AS published_date,
    description,
    price,
    CAST(REGEXP_EXTRACT(CAST(weeks_on_list AS STRING), r'^\{(.*)\}$', 1) AS INT) AS weeks_on_list
  FROM
    bestsellers
)
WHERE 
  weeks_on_list >= 10
ORDER BY 
  weeks_on_list 
  DESC
