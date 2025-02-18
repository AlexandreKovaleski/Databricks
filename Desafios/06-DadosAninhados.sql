-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Criar Tabelas
-- MAGIC Execute a célula abaixo para criar tabelas para as perguntas neste notebook.

-- COMMAND ----------

-- MAGIC %run ../Utilities/06-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 1: Array e Explode
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Obtenha uma lista distinta de autores que contribuíram com postagens de blog na categoria "Company Blog".
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL que realize o seguinte:
-- MAGIC - Exploda o campo **`authors`** para criar um campo **`author`** que contenha apenas um autor por linha na tabela **`databricksBlog`**.
-- MAGIC - Limite os registros para conter **apenas** autores únicos
-- MAGIC - Filtra os registros em que as **`categories`** não incluem "Company Blog"
-- MAGIC - Armazene os resultados em uma visualização temporária chamada **`results`**
-- MAGIC
-- MAGIC Uma solução corretamente executada deve produzir uma visualização chamada **`results`** semelhante a este exemplo de saída: (apresentando os dados resultantes)
-- MAGIC
-- MAGIC |              author|        categories|
-- MAGIC |--------------------|----------------|
-- MAGIC |Anthony Joseph      |["Announcements"]| 
-- MAGIC |Vida Ha        |["Product"]| 
-- MAGIC |Nan Zhu (Chief Architect at Faimdata)    |["Product"]| 

-- COMMAND ----------

SELECT * 
FROM (
  SELECT
    EXPLODE(authors) author, filter(categories, cat -> cat NOT IN ("Company Blog")) cats 
  FROM databricksBlog
  )
GROUP BY author, cats
ORDER BY author


-- COMMAND ----------



-- COMMAND ----------

select * from databricksBlog

-- COMMAND ----------

DESCRIBE databricksBlog
