-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Funções de alta ordem
-- MAGIC
-- MAGIC Funções de alta ordem em Spark SQL permitem com que o programador trabalhe diretamente com dados de tipos complexos. Tabelas com colunas que possuem dados aninhados e hierárquicos são normalmente armazenadas como array ou map. Iremos trabalhar com as funções transform, filter, e flag enquanto a estrutura original é preservada. Neste notebook usaremos arrays de strings; no próximo, usaremos funções e dados numéricos. 
-- MAGIC
-- MAGIC Neste notebook: 
-- MAGIC * Aplicar funções de alta ordem (`TRANSFORM`, `FILTER`, `EXISTS`) em arrays de strings

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Trabalhando com dados de texto
-- MAGIC
-- MAGIC Estes exemplos usam dados coletados de blog posts.
-- MAGIC
-- MAGIC No data set, as colunas `authors` e `categories` são arrays.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DatabricksBlog
  USING json
  OPTIONS (
    path "dbfs:/mnt/training/databricks-blog.json",
    inferSchema "true"
  )

-- COMMAND ----------

DESCRIBE DatabricksBlog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Filter
-- MAGIC
-- MAGIC A [Filter](https://spark.apache.org/docs/latest/api/sql/#filter) permite criar uma nova coluna baseada em outra de acordo com determinadas condições. Por exemplo, queremos remover a categoria `"Engineering Blog"` de todos os registros da coluna `categories`. Pode-se usar `FILTER` para criar uma nova coluna. 
-- MAGIC
-- MAGIC Compreendendo a função:
-- MAGIC
-- MAGIC `FILTER (categories, category -> category <> "Engineering Blog") woEngineering`
-- MAGIC
-- MAGIC **`FILTER`** : o nome da função <br>
-- MAGIC **`categories`** : o nome do array de entrada <br>
-- MAGIC **`category`** : o nome da variável iteradora. A variável é usada em uma função lambda.<br>
-- MAGIC **`->`** :  indica o início da função <br>
-- MAGIC **`category <> "Engineering Blog"`** : Esta é a função. Cada valor é verificado para ver se **é diferente** do valor `"Engineering Blog"`. Se for, é filtrado para a nova coluna, `woEnginieering`

-- COMMAND ----------

SELECT
  categories,
  FILTER (categories, category -> category <> "Engineering Blog") woEngineering
FROM DatabricksBlog


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Filter, subqueries, e `WHERE`
-- MAGIC
-- MAGIC Pode acontecer de um filtro produzir vários arrays vazios na nova coluna. Quando isso acontece, pode ser útil usar um `WHERE` para mostrar apenas arrays não-vazios. 
-- MAGIC
-- MAGIC Neste exemplo, conseguimos isso usando uma subconsulta. Uma **subconsulta** em SQL é uma consulta dentro de outra consulta. Elas são úteis para realizar operações em múltiplas etapas. Neste caso, estamos usando-a para criar a coluna nomeada que usaremos com uma cláusula WHERE.

-- COMMAND ----------

SELECT
  *
FROM
  (
    SELECT
      authors, title,
      FILTER(categories, category -> category = "Engineering Blog") AS blogType
    FROM
      DatabricksBlog
  )
WHERE
  size(blogType) > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Exists
-- MAGIC
-- MAGIC [Exists](https://spark.apache.org/docs/latest/api/sql/#exists) testa se uma declaração é verdadeira para um ou mais elementos em uma matriz. Digamos que queremos sinalizar todas as postagens do blog com "Company Blog" no campo de categorias. Posso usar a função EXISTS para marcar quais entradas incluem essa categoria.
-- MAGIC
-- MAGIC A função: 
-- MAGIC
-- MAGIC `EXISTS (categories, c -> c = "Company Blog") companyFlag`
-- MAGIC
-- MAGIC **`EXISTS`** : nome da função <br>
-- MAGIC **`categories`** : array de entrada <br>
-- MAGIC **`c`** : Variável iteradora da função lambda. Ele itera sobre a matriz, passando cada valor para a função um de cada vez. Note que estamos usando o mesmo tipo de referências que na comando anterior, mas nomeamos o iterador com uma única letra.<br>
-- MAGIC **`->`** :  Início da função <br>
-- MAGIC **`c = "Engineering Blog"`** : Esta é a função. Cada valor é verificado para ver se é igual ao valor "Company Blog". Se for, ele é marcado na nova coluna companyFlag.

-- COMMAND ----------

SELECT
  categories,
  EXISTS (categories, c -> c = "Company Blog") companyFlag
FROM DatabricksBlog


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transform
-- MAGIC
-- MAGIC [Transform](https://spark.apache.org/docs/latest/api/sql/#transform) usa a função fornecida para transformar todos os elementos de uma matriz. As funções internas do SQL são projetadas para operar em um único tipo de dado simples dentro de uma célula. Elas não podem processar valores de matriz. A função TRANSFORM pode ser particularmente útil quando você deseja aplicar uma função existente a cada elemento em uma matriz. Neste caso, queremos reescrever todos os nomes na coluna categories em letras minúsculas.
-- MAGIC
-- MAGIC A função:
-- MAGIC
-- MAGIC **TRANSFORM**: o nome da função de ordem superior
-- MAGIC **categories**: o nome de nossa matriz de entrada
-- MAGIC **cat**: o nome da variável iteradora. Você escolhe este nome e depois o usa na função lambda. Ela itera sobre a matriz, passando cada valor para a função um de cada vez. Note que estamos usando o mesmo tipo de referências que na comando anterior, mas nomeamos o iterador com uma nova variável.
-- MAGIC **->**: Indica o início de uma função
-- MAGIC **LOWER(cat)**: Esta é a função. Para cada valor na matriz de entrada, a função interna LOWER() é aplicada para transformar a palavra em minúsculas.

-- COMMAND ----------

SELECT 
  TRANSFORM(categories, cat -> LOWER(cat)) lwrCategories
FROM DatabricksBlog

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
