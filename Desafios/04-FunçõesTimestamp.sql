-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Criar Tabelas
-- MAGIC Execute a célula abaixo para criar tabelas para as perguntas neste notebook.

-- COMMAND ----------

-- MAGIC %run ./04-CreateTables

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Questão 1: Extrair Ano e Mês
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Extraia o ano e o mês do campo **`Timestamp`** na tabela **`timetable1`** e armazene os registros apenas do 12º mês na tabela **`q1Results`**.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL que realize o seguinte:
-- MAGIC * Extraia o **`ano`** e o **`mês`** da coluna **`Timestamp`** da tabela **`timetable1`**
-- MAGIC     - **`Timestamp`** é um número inteiro que representa os segundos desde a meia-noite de 1º de janeiro de 1970 (por exemplo, 1519344286). Você deve convertê-lo para um tipo `timestamp` para extrair anos e meses.
-- MAGIC * Filtrar os registros para incluir apenas o mês 12
-- MAGIC * Armazenar os registros resultantes em uma visualização temporária chamada **`q1Results`** com o seguinte esquema. (apresentando os dados resultantes)
-- MAGIC
-- MAGIC | column | type |
-- MAGIC |--------|--------|
-- MAGIC | Date | timestamp |
-- MAGIC | Year | integer |
-- MAGIC | Month | integer |
-- MAGIC
-- MAGIC
-- MAGIC * Uma solução corretamente concluída deve produzir um DataFrame semelhante a este exemplo de saída:
-- MAGIC
-- MAGIC |               Date|Year|Month|
-- MAGIC |-------------------|----|-----|
-- MAGIC |2010-12-15 21:36:55|2010|   12|
-- MAGIC |2002-12-01 11:17:54|2002|   12|
-- MAGIC |2017-12-13 11:28:03|2017|   12|

-- COMMAND ----------

describe timetable1

-- COMMAND ----------

SELECT  name,
        month( CAST(Timestamp AS timestamp) ) AS mes,
        year( CAST(Timestamp AS timestamp) ) AS ano

FROM    timetable1
WHERE   month( CAST(Timestamp AS timestamp) ) = 12

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 2: Extrair ano, mês e dia do ano
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Extraia o **ano**, o **mês** e o **dia do ano** do campo **`Timestamp`** na tabela **`timetable2`** e retorne os registros apenas para o 4º mês.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL que realize o seguinte:
-- MAGIC * Crie as colunas **`Year`**, **`Month`** e **`DayOfYear`** a partir da coluna **`Timestamp`** na tabela **`timetable2`**
-- MAGIC    - **`Timestamp`** é um número inteiro que representa os segundos desde a meia-noite de 1º de janeiro de 1970 (por exemplo, 1519344286). Você deve convertê-lo para um tipo `timestamp` para extrair anos, meses e o dia do ano.
-- MAGIC * Filtrar os registros para incluir apenas o mês 4
-- MAGIC * Armazenar os registros em uma tabela temporária chamada **`q2Results`** com o seguinte esquema. (apresentando os dados resultantes)
-- MAGIC
-- MAGIC | column    | type      |
-- MAGIC |-----------|-----------|
-- MAGIC | Date      | timestamp |
-- MAGIC | Year      | integer   |
-- MAGIC | Month     | integer   |
-- MAGIC | DayOfYear | integer   |
-- MAGIC
-- MAGIC <br>
-- MAGIC * Uma solução corretamente concluída deve produzir um DataFrame semelhante a este exemplo de saída:
-- MAGIC
-- MAGIC |               Date|Year|Month| DayOfYear |
-- MAGIC |-------------------|----|-----|-----------|
-- MAGIC |2002-04-22 06:41:39|2002|    4|      112  |
-- MAGIC |2012-04-01 05:00:06|2012|    4|       92  |
-- MAGIC |2019-04-05 12:38:42|2019|    4|       95  |

-- COMMAND ----------

select * from timetable2

-- COMMAND ----------

SELECT  from_unixtime(Timestamp) AS date,
        year ( CAST( Timestamp AS timestamp ) ) year,
        month( CAST( Timestamp AS timestamp ) ) month,
        date_format( CAST( Timestamp AS timestamp ), "D") day         

FROM    timetable1
WHERE   month( CAST(Timestamp AS timestamp) ) = 4
