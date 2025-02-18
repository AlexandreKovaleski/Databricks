-- Databricks notebook source
-- MAGIC
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- TODO Exercício 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Erro de Catálogo

-- COMMAND ----------

SELECT
  firstNme,
  lastName,
  birthDate
FROM
  People10M
WHERE
  year(birthDate) > 1990
  AND gender = 'F'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Plano de otimização

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW joined AS
SELECT People10m.firstName,
  to_date(birthDate) AS date
FROM People10m
  JOIN ssaNames ON People10m.firstName = ssaNames.firstName;

CREATE OR REPLACE TEMPORARY VIEW filtered AS
SELECT firstName,count(firstName)
FROM joined
WHERE
  date >= "1980-01-01"
GROUP BY
  firstName, date;


-- COMMAND ----------

SELECT * FROM  filtered;

-- COMMAND ----------

CACHE TABLE filtered;

-- COMMAND ----------

SELECT * FROM filtered;

-- COMMAND ----------

SELECT * FROM filtered WHERE firstName = "Latisha";

-- COMMAND ----------

UNCACHE TABLE IF EXISTS filtered;

-- COMMAND ----------

SELECT * FROM filtered WHERE firstName = "Latisha";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configurar particionamento

-- COMMAND ----------

-- TODO Exercício 4

-- COMMAND ----------

-- TODO Exercício 4

-- COMMAND ----------

DROP TABLE IF EXISTS bikeShare_partitioned;
CREATE TABLE bikeShare_partitioned
PARTITIONED BY (p_hr)
  AS
SELECT
  instant,
  dteday,
  season, 
  yr,
  mnth,
  hr as p_hr,
  holiday,
  weekday, 
  workingday,
  weathersit,
  temp
FROM
  bikeShare

-- COMMAND ----------

SELECT * FROM bikeShare_partitioned WHERE p_hr = 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cuidado com arquivos muito pequenos

-- COMMAND ----------

DROP TABLE IF EXISTS bikeShare_parquet;
CREATE TABLE bikeShare
PARTITIONED BY (p_instant)
  AS
SELECT
  instant AS p_instant,
  dteday,
  season, 
  yr,
  mnth,
  hr
  holiday,
  weekday, 
  workingday,
  weathersit,
  temp
FROM
  bikeShare_csv

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup
