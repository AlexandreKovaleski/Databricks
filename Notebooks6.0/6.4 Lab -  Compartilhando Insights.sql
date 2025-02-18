-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Laboratório - Compartilhando Insights
-- MAGIC ## Módulo 6 - Tarefa
-- MAGIC
-- MAGIC Neste laboratório, exploraremos um pequeno conjunto de dados fictícios de um grupo de data centers. Você verá que é semelhante aos dados com os quais você vem trabalhando, mas contém algumas novas colunas e está estruturado ligeiramente diferente para testar suas habilidades de manipulação de dados hierárquicos.
-- MAGIC
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Nesta tarefa, você irá: </br>
-- MAGIC
-- MAGIC * Aplicar funções de alta ordem a dados de matriz
-- MAGIC * Aplicar técnicas avançadas de agregação e resumo para processar dados
-- MAGIC * Apresentar dados em um painel interativo ou arquivo estático
-- MAGIC
-- MAGIC Execute a célula abaixo para preparar este espaço de trabalho para o laboratório.

-- COMMAND ----------

-- MAGIC %run ./Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercício 1: Criar uma tabela
-- MAGIC
-- MAGIC **Resumo:** Criar uma tabela.
-- MAGIC
-- MAGIC Use este caminho para acessar os dados: `/mnt/training/iot-devices/data-centers/energy.json`
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Escreva uma declaração `CREATE TABLE` para os dados localizados no endpoint listado acima.
-- MAGIC * Use json como o formato de arquivo.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DCDataRaw
USING JSON 
OPTIONS (
  path "/mnt/training/iot-devices/data-centers/energy.json"
)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercício 2: Amostrar a tabela
-- MAGIC
-- MAGIC **Resumo:** Amostrar a tabela para dar uma olhada mais próxima em algumas linhas.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Escreva uma consulta que permita ver algumas linhas dos dados.

-- COMMAND ----------

DESCRIBE DCDataRaw;

-- COMMAND ----------

SELECT timestamp
FROM dcdataraw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercício 3: Criar uma visualização
-- MAGIC
-- MAGIC **Resumo:** Crie uma visualização temporária que exiba a coluna de data e hora como um tipo de data e hora.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Crie uma visualização temporária chamada `DCDevices`.
-- MAGIC * Converta a coluna `timestamp` para um tipo de data e hora. Consulte a documentação [Padrões de Data e Hora](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html#) para obter informações sobre formatação.
-- MAGIC * (Opcional) Renomeie as colunas para usar camelCase.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW DCDevices AS
SELECT  to_timestamp(timestamp, "yyyy/MM/dd HH:mm:ss") AS dateFormated
FROM DCDataRaw;

SELECT * FROM DCDevices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercício 4: Sinalizar registros com baterias defeituosas
-- MAGIC
-- MAGIC **Resumo:** Quando uma bateria está com mau funcionamento, ela pode relatar níveis de bateria negativos. Crie uma nova coluna booleana `needService` que mostra se um dispositivo precisa de serviço.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Escreva uma consulta que mostre quais dispositivos têm baterias com mau funcionamento.
-- MAGIC * Inclua as colunas `batteryLevel`, `deviceId` e `needService`.
-- MAGIC * Ordene os resultados por `deviceId` e, em seguida, por `batteryLevel`.

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Exercício 5: Exibir altos níveis de CO<sub>2</sub>
-- MAGIC
-- MAGIC **Resumo:** Crie uma nova coluna para exibir apenas os níveis de CO<sub>2</sub> que excedam 1400 ppm.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Inclua as colunas `deviceId`, `deviceType`, `highCO2` e `time`.
-- MAGIC * A coluna `highCO2` deve conter um array de leituras de CO<sub>2</sub> acima de 1400.
-- MAGIC * Mostre apenas registros que contenham valores de `highCO2`.
-- MAGIC * Ordene por `deviceId` e, em seguida, por `highCO2`.
-- MAGIC
-- MAGIC Você pode precisar usar uma subconsulta para escrever isso em uma única declaração de consulta.

-- COMMAND ----------

SELECT *
FROM (
SELECT  device_id,
        device_type,
        filter(co2_level, co2 -> co2 > 1400 ) AS highCO2
FROM    DCDataRaw
ORDER BY device_id, highCO2

)
WHERE   size(highCO2) > 0



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercício 6: Criar uma tabela particionada
-- MAGIC
-- MAGIC **Resumo:** Crie uma nova tabela particionada por `deviceId`.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Inclua todas as colunas.
-- MAGIC * Crie a tabela usando o formato Parquet.
-- MAGIC * Renomeie a coluna particionada para `p_deviceId`.
-- MAGIC * Execute um `SELECT *` para visualizar sua tabela.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS partitionedDeviceId
USING PARQUET 
PARTITIONED BY (device_id) AS
  SELECT * 
  FROM DCDataRaw;
SELECT device_id AS p_deviceId, * FROM partitionedDeviceId

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercício 7: Visualize as temperaturas médias

-- COMMAND ----------

SELECT 
  device_id,
  ROUND(AVG(temps),4) AS avgTemp,
  ROUND(STD(temps), 2) AS stdTemp
FROM dcdataraw
WHERE device_id = getArgument("3")
GROUP BY device_id

-- COMMAND ----------

SELECT  device_id,
        avg(temps) OVER (PARTITION BY device_id) AS AvgTemps
FROM dcdataraw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercício 8: Crie um widget

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercício 9: Use o widget na query

-- COMMAND ----------

--TODO


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
