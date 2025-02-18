-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Criar Tabelas
-- MAGIC Execute a célula abaixo para criar tabelas para as perguntas neste notebook.

-- COMMAND ----------

-- MAGIC %run ./01-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 1: Modificar uma Tabela
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Modifique as colunas na tabela **`discounts`** para corresponder ao esquema fornecido.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL que realize o seguinte:
-- MAGIC * Selecione as colunas **`discountId`**, **`code`** e **`price`**
-- MAGIC * Converta a coluna **`discountId`** para o tipo **`Long`**
-- MAGIC * Converta a coluna **`price`** para o tipo **`Double`**, multiplique por 100 e, em seguida, converta para o tipo **`Integer`**
-- MAGIC * Salve isso em uma visualização temporária chamada **`q1Results`**

-- COMMAND ----------

SELECT  discountId,
        code,
        price
FROM discounts

-- COMMAND ----------

DESCRIBE discounts

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW  q1Results AS
  SELECT  
          CAST( discountId AS LONG ) AS LongDiscountsId,
          CAST( try_multiply( CAST( price AS DOUBLE ), 100 ) AS INT) AS DoublePrice

  FROM    discounts

-- COMMAND ----------

SELECT * 
FROM q1Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 2: Matemática Básica e Remoção de Colunas
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Modifique as colunas na tabela **`discounts2`** para corresponder ao esquema fornecido.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL na tabela **`discounts2`** que realize o seguinte:
-- MAGIC * Converta a coluna **`active`** para o tipo **`Boolean`**
-- MAGIC * Crie a coluna **`price`** convertendo a coluna **`cents`** para o tipo **`Double`** e dividindo por 100
-- MAGIC * Remova a coluna **`cents`**
-- MAGIC * Salve isso em uma visualização temporária chamada **`q2Results`**

-- COMMAND ----------

SELECT *
FROM discounts2

-- COMMAND ----------

DESCRIBE discounts2

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW  q2Results AS
  SELECT  CAST( active AS BOOLEAN ) AS booleanActive,
          CAST( cents AS DOUBLE) / 100  AS price
          
  FROM  discounts2;

SELECT * FROM q2Results



-- COMMAND ----------

SELECT * FROM q2Results

-- COMMAND ----------


