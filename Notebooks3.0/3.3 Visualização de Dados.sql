-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Visualização de dados com Databricks
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Início
-- MAGIC
-- MAGIC Rode a célula a seguir para conectar com as fontes de dados. 

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criar uma tabela
-- MAGIC
-- MAGIC Cria uma tabela especificando um **schema**. O schema descreve a estrutura dos dados. Se o tipo do dado não for informado ele é inferido pelo Spark.
-- MAGIC
-- MAGIC O schema é: 
-- MAGIC
-- MAGIC |Column Name | Type |
-- MAGIC | ---------- | ---- |
-- MAGIC | userId | INT|
-- MAGIC | movieId | INT|
-- MAGIC | rating | FLOAT|
-- MAGIC | timeRecorded | INT|

-- COMMAND ----------

DROP TABLE IF EXISTS movieRatings;
CREATE TABLE movieRatings (
  userId INT,
  movieId INT,
  rating FLOAT,
  timeRecorded INT
) USING csv OPTIONS (
  PATH "/mnt/training/movies/20m/ratings.csv",
  header "true"
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Visualizar dados tabulados
-- MAGIC
-- MAGIC Esta tabela contém 20 milhões de registros. Selecione todos os dados

-- COMMAND ----------

-- Escreva a consulta aqui

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cast como timestamp
-- MAGIC
-- MAGIC A função `CAST()` mostra a data em formato reconhecível. 

-- COMMAND ----------

SELECT
  rating,
  CAST(timeRecorded as timestamp)
FROM
  movieRatings;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criar uma view temporária
-- MAGIC Note o use de funções. O quê esta view contém?

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW ratingsByMonth AS
SELECT
  ROUND(AVG(rating), 3) AS avgRating,
  month(CAST(timeRecorded as timestamp)) AS month
FROM
  movieRatings
GROUP BY
  month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Visualize os dados
-- MAGIC
-- MAGIC Selecione todos os dados da view ordenando pela nota média (avgRating). Use o resultado tabulado para criar uma visualização em barras ou linha (nota média por mês).

-- COMMAND ----------

-- Escreva a query aqui.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup
