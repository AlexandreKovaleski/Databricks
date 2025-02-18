-- Databricks notebook source
-- MAGIC
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Gerenciando dados aninhados com Spark SQL
-- MAGIC
-- MAGIC Neste notebook trabalharemos com dados de data centers. O exemplo usa dados de 4 data centers com sensores que periodicamente coletam temperatura e CO<sub>2</sub>. Os dados são armazenados em arrays.
-- MAGIC
-- MAGIC Rode as queries a seguir para trabalhar com dados aninhados em Spark SQL.<br>
-- MAGIC
-- MAGIC * Trabalhar com dados hierárquicos
-- MAGIC * Usar common table expressions (CTE)
-- MAGIC * Criar tabelas baseadas em CTEs
-- MAGIC * Usar `EXPLODE` para gerenciar os objetos aninhados

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Iniciando
-- MAGIC
-- MAGIC Rodar a célula a seguir **adaptando o caminho**. 

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criar tabela
-- MAGIC
-- MAGIC O [Databricks File System (DBFS)](https://docs.databricks.com/data/databricks-file-system.html) é um sistema de arquivos distribuído montado no workspace da plataforma.
-- MAGIC
-- MAGIC Abaixo criaremos uma tabela com os dados de data centers.

-- COMMAND ----------

DROP TABLE IF EXISTS DCDataRaw;
CREATE TABLE DCDataRaw
USING parquet                           
OPTIONS (
    PATH "/mnt/training/iot-devices/data-centers/2019-q2-q3"
    )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visualizar metadados e informações detalhadas
-- MAGIC
-- MAGIC A palavra-chave `EXTENDED` adiciona informações ao `DESCRIBE`.
-- MAGIC
-- MAGIC Mais detalhes sobre os tipos de Spark SQL nos [docs](https://spark.apache.org/docs/latest/sql-ref-datatypes.html).

-- COMMAND ----------

DESCRIBE EXTENDED DCDataRaw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visualizar amostras
-- MAGIC A função `RAND()` mostra registros aleatórios enquanto `LIMIT` controla a quantidade. 

-- COMMAND ----------

SELECT * FROM DCDataRaw
ORDER BY RAND()
LIMIT 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Explodir objetos aninhados
-- MAGIC A coluna `source` contém objetos `key-value` (chave-valor). A função `EXPLODE` permite visualizar os dados de forma detalhada. 
-- MAGIC

-- COMMAND ----------

SELECT EXPLODE (source)
FROM DCDataRaw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Common Table Expressions
-- MAGIC
-- MAGIC Common Table Expressions (CTE) fornece um resultado temporário que podem ser usado no contexto de um `SELECT`. São diferentes de views temporárias pois podem ser usados apenas no contexto em que são declarados.

-- COMMAND ----------

WITH ExplodeSource  -- nome do resultado
AS                  
(                   -- consulta isolada
  SELECT            -- resultado temporário
    dc_id,
    to_date(date) AS date,
    EXPLODE (source)
  FROM
    DCDataRaw
)
SELECT             -- consulta no resultado temporário
  key,
  dc_id,
  date,
  value.description,  
  value.ip,
  value.temps,
  value.co2_level
FROM               -- esta query vem do CTE nomeado
  ExplodeSource;  
                  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Table as Select (CTAS)
-- MAGIC
-- MAGIC Criar uma tabela usando CTE. 

-- COMMAND ----------

DROP TABLE IF EXISTS DeviceData;
CREATE TABLE DeviceData                 
USING parquet
WITH ExplodeSource                       -- The start of the CTE from the last cell
AS
  (
  SELECT 
  dc_id,
  to_date(date) AS date,
  EXPLODE (source)
  FROM DCDataRaw
  )
SELECT 
  dc_id,
  key device_type,                       
  date,
  value.description,
  value.ip,
  value.temps,
  value.co2_level
  
FROM ExplodeSource;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Consulta na nova tabela.

-- COMMAND ----------

SELECT * FROM DeviceData

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
