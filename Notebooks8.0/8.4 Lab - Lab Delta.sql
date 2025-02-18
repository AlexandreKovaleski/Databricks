-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Laboratório 4 - Laboratório Delta
-- MAGIC ## Tarefa do Módulo
-- MAGIC Neste laboratório, você continuará seu trabalho em nome da Moovio, a empresa de rastreadores de fitness. Você estará trabalhando com um novo conjunto de arquivos que deve ser movido para uma tabela de "nível ouro". Você precisará modificar e reparar registros, criar novas colunas e mesclar dados que chegaram atrasados.

-- COMMAND ----------

-- MAGIC %run ./Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 1: Criar uma tabela
-- MAGIC
-- MAGIC **Resumo:** Crie uma tabela a partir de arquivos `json`.
-- MAGIC
-- MAGIC Use este caminho para acessar os dados: <br>
-- MAGIC `"dbfs:/mnt/training/healthcare/tracker/raw.json/"`
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Crie uma tabela com o nome `health_tracker_data_2020`
-- MAGIC * Use campos opcionais para indicar o caminho de leitura e expressar que o esquema deve ser inferido.

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_data_2020;

CREATE TABLE health_tracker_data_2020
USING JSON
OPTIONS (
  path "dbfs:/mnt/training/healthcare/tracker/raw.json/",
  inferSchema "true"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 2: Visualizar os dados
-- MAGIC
-- MAGIC **Resumo:** Visualize uma amostra dos dados na tabela.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Consulte a tabela com `SELECT *` para ver todas as colunas.
-- MAGIC * Amostra 5 linhas da tabela.

-- COMMAND ----------

SELECT * FROM health_tracker_data_2020 TABLESAMPLE (5 ROWS)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 3: Contar Registros
-- MAGIC
-- MAGIC **Resumo:** Escreva uma consulta para encontrar o número total de registros.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Conte o número de registros na tabela.

-- COMMAND ----------

SELECT  COUNT(*) as totalRaws
FROM    health_tracker_data_2020 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 4: Criar uma Tabela Delta Silver
-- MAGIC
-- MAGIC **Resumo:** Crie uma tabela Delta que transforma e reestrutura sua tabela.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Elimine a coluna existente `month`.
-- MAGIC * Isole cada propriedade do objeto na coluna `value` em sua própria coluna.
-- MAGIC * Converta o tempo para timestamp **e** para data.
-- MAGIC * Particione por `device_id`.
-- MAGIC * Use o Delta para gravar a tabela.

-- COMMAND ----------

CREATE OR REPLACE TABLE health_tracker
USING DELTA
PARTITIONED BY (p_device_id) AS (
SELECT
  value.name,
  value.heartrate,
  CAST(FROM_UNIXTIME(value.time) AS timestamp) AS time,
  CAST(FROM_UNIXTIME(value.time) AS DATE) AS dt,
  value.device_id p_device_id
FROM
  health_tracker_data_2020
)

-- COMMAND ----------

DESCRIBE DETAIL health_tracker

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 5: Registrar a tabela no Metastore
-- MAGIC
-- MAGIC **Resumo:** Registre sua tabela Silver no Metastore.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Certifique-se de que é possível executar a célula mais de uma vez sem gerar um erro.
-- MAGIC * Grave na localização: `/health_tracker/silver`

-- COMMAND ----------

CREATE OR REPLACE TABLE health_tracker_silver 
USING DELTA
PARTITIONED BY (p_device_id)
LOCATION "/health_tracker/silver" AS (
SELECT
  *
FROM
  health_tracker
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 6: Verificar o número de registros
-- MAGIC
-- MAGIC **Resumo:** Verifique se todos os dispositivos estão relatando o mesmo número de registros.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Escreva uma consulta que conte o número de registros para cada dispositivo.
-- MAGIC * Inclua sua coluna de identificação de dispositivo particionada e a contagem desses registros.

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 7: Plotar registros
-- MAGIC
-- MAGIC **Resumo:** Tente avaliar visualmente quais datas podem estar faltando registros.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Escreva uma consulta que retornará registros de um dispositivo que **não** está faltando registros, bem como o dispositivo que parece estar faltando registros.
-- MAGIC * Plote os resultados para inspecionar visualmente os dados.
-- MAGIC * Identifique as datas que estão faltando registros.
-- MAGIC

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 8: Verificar Leituras Defeituosas
-- MAGIC
-- MAGIC **Resumo:** Verifique se seus dados contêm registros que indicariam que um dispositivo relatou dados incorretos.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Crie uma visualização que contenha todos os registros que relatam uma frequência cardíaca negativa.
-- MAGIC * Plote/visualize esses dados para ver quais dias incluem leituras defeituosas.

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 9: Reparar Registros
-- MAGIC
-- MAGIC **Resumo:** Crie uma visualização que contenha valores interpolados para leituras defeituosas.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Crie uma visualização temporária que conterá todos os registros que você deseja atualizar.
-- MAGIC * Transforme os dados de forma que todas as leituras defeituosas (onde a frequência cardíaca é relatada como menor que zero) sejam interpoladas como a média dos pontos de dados imediatamente ao redor da leitura defeituosa.
-- MAGIC * Depois de gravar a visualização, conte o número de registros nela.

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 10: Ler dados que chegaram atrasados
-- MAGIC
-- MAGIC **Resumo:** Leia novos dados que chegaram atrasados.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Crie uma nova tabela que contenha os dados que chegaram atrasados neste caminho: `"dbfs:/mnt/training/healthcare/tracker/raw-late.json"`.
-- MAGIC * Conte os registros.

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 11: Preparar inserções
-- MAGIC
-- MAGIC **Resumo:** Prepare os novos dados que chegaram atrasados para inserção na tabela Silver.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Crie uma visualização temporária que contenha os novos dados que chegaram atrasados.
-- MAGIC * Aplique transformações aos dados para que o esquema corresponda à nossa tabela Silver existente.

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 12: Preparar atualizações
-- MAGIC
-- MAGIC **Resumo:** Prepare uma visualização para atualizar nossa tabela Silver.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Crie uma visualização temporária que seja a `UNIÃO` das visualizações que contêm os dados que você deseja inserir e os dados que deseja atualizar.
-- MAGIC * Conte os registros.

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 13: Realizar atualizações
-- MAGIC
-- MAGIC **Resumo:** Mesclar as atualizações em sua tabela Silver.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Mesclar dados nas colunas de tempo e identificação do dispositivo de sua tabela Silver e sua tabela de atualizações.
-- MAGIC * Use condições `MATCH` para decidir se deve aplicar uma atualização ou uma inserção.

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício 14: Gravar em "Gold"
-- MAGIC
-- MAGIC **Resumo:** Crie uma tabela Delta de nível "Gold" que contenha dados agregados.
-- MAGIC
-- MAGIC Passos para concluir:
-- MAGIC * Crie uma tabela Delta de nível "Gold".
-- MAGIC * Agregue a frequência cardíaca para exibir a média e o desvio padrão para cada dispositivo.
-- MAGIC * Conte o número de registros.

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Limpeza
-- MAGIC Execute a célula a seguir para limpar o ambiente de trabalho.

-- COMMAND ----------

-- %run .Includes/Classroom-Cleanup

