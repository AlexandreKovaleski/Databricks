-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Criar Tabelas
-- MAGIC Execute a célula abaixo para criar tabelas para as perguntas neste notebook.

-- COMMAND ----------

-- MAGIC %run ./03-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 1: Junções
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Realize uma junção de duas tabelas, **`purchases`** e **`prices`**.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL que realize o seguinte:
-- MAGIC * Realiza uma junção interna em duas tabelas, **`purchases`** e **`prices`**, na coluna **`itemId`**
-- MAGIC * Inclui **apenas** 3 colunas: **`transactionId`**, **`itemId`** e **`value`**

-- COMMAND ----------

DESCRIBE prices

-- COMMAND ----------

DESCRIBE purchases

-- COMMAND ----------

SELECT a.transactionId, a.itemId, b.value

FROM   purchases AS a
  JOIN prices    AS b ON ( a.itemId = b.itemId )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 2: Combinar Tabelas
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Realize uma junção externa em duas tabelas, **`discounts`** e **`products`**, e armazene os resultados na visualização **`q2Results`**.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL que realize o seguinte:
-- MAGIC * Realiza uma junção externa nas tabelas **`discounts`** e **`products`** na coluna **`itemName`**
-- MAGIC * Garante que a visualização resultante inclua **apenas uma coluna** **`itemName`** que vem da tabela **`products`**.
-- MAGIC
-- MAGIC O esquema final e o DataFrame devem conter as seguintes colunas, embora não necessariamente nesta ordem: (apresentando os dados resultantes)
-- MAGIC
-- MAGIC |column |type |
-- MAGIC |---|---|
-- MAGIC |itemName | string|
-- MAGIC |discountId | integer|
-- MAGIC |discountCode | string|
-- MAGIC |price | double|
-- MAGIC |active | boolean|
-- MAGIC |itemId | integer|
-- MAGIC |amount | integer|

-- COMMAND ----------

DESCRIBE discounts

-- COMMAND ----------

DESCRIBE products

-- COMMAND ----------

SELECT p.itemName

FROM   discounts AS d
  RIGHT OUTER JOIN products AS p ON ( d.itemName = p.itemName)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 3: Realizar uma Junção Cruzada de Tabelas
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Realize uma junção cruzada em duas tabelas, **`stores`** e **`articles`**.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC * Realize uma junção cruzada nas tabelas **`stores`** e **`articles`**.

-- COMMAND ----------

SELECT *

FROM stores
CROSS JOIN articles

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
