-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Criar Tabelas
-- MAGIC Execute a célula abaixo para criar tabelas para as perguntas neste notebook.

-- COMMAND ----------

-- MAGIC %run ../Utilities/02-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Questão 1: Deduplicação e Ordenação
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Deduplicar e ordenar dados de produtos que precisam ser reabastecidos em uma mercearia.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL na tabela **`products`** que realize o seguinte:
-- MAGIC * Remova as linhas duplicadas
-- MAGIC * Ordene as linhas pela coluna **`aisle`** em ordem crescente (com nulos aparecendo por último), e depois pela coluna **`price`** em ordem crescente
-- MAGIC * Armazene os resultados em uma visualização temporária chamada **`q1Results`**
-- MAGIC
-- MAGIC Uma solução corretamente executada deve retornar um DataFrame que se parece com isso: (apresentando os dados resultantes)
-- MAGIC
-- MAGIC |itemId|amount|aisle|price|
-- MAGIC |---|---|---|---|
-- MAGIC |  9958|    64|    2|    2|
-- MAGIC |  1432|    23|    3|   24|
-- MAGIC |  3242|    14|    5|    5|
-- MAGIC |...|...|...|...|
-- MAGIC |  7064|    34| null|   24|
-- MAGIC |  0244|    7| null|   36|

-- COMMAND ----------

SELECT *
FROM products

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW q1Results AS
  SELECT DISTINCT *
  FROM products
  ORDER BY aisle ASC NULLS LAST , price ASC;

SELECT * FROM q1Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 2: Limitar Resultados
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Retorne os cinco melhores resultados para dados que correspondam a um conjunto de critérios.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL na tabela **`raceResults`** que realize o seguinte:
-- MAGIC * Converte a coluna **`lastFinish`** para o tipo de data e renomeia para **`raceDate`**
-- MAGIC * Ordena as linhas pela coluna **`winOdds`** em ordem decrescente
-- MAGIC * Limita os resultados aos 5 melhores **`winOdds`**
-- MAGIC * Armazena os resultados em uma visualização temporária chamada **`q2Results`**
-- MAGIC
-- MAGIC Uma solução corretamente executada deve retornar um DataFrame que se parece com isso: (apresentando os dados resultantes)
-- MAGIC
-- MAGIC |name|winOdds|raceDate|
-- MAGIC |--- |-------|----------|
-- MAGIC | Dolor Incididunt|    .9634252|    2015-07-29| 
-- MAGIC | Excepteur Mollit|    .9524401|    2019-08-15| 
-- MAGIC | Magna Ad        |    .9420442|    2017-05-12| 
-- MAGIC |  Sed In|.9325211| 2014-08-29|
-- MAGIC |  Qui Cupidat| .9242451| 2011-08-23| 

-- COMMAND ----------

DESCRIBE raceResults

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW q2Results AS
  SELECT  to_date(lastFinish, "MM/dd/yy") raceDate
  FROM    raceResults
  ORDER BY winOdds DESC LIMIT 5;

SELECT * FROM q2Results
