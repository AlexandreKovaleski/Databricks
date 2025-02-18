-- Databricks notebook source
-- MAGIC
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Manipulando dados
-- MAGIC
-- MAGIC Neste notebook, trabalharemos com dados que contém `NULL` e formatos de data. 
-- MAGIC
-- MAGIC Neste notebook:
-- MAGIC
-- MAGIC * Amostras de tabela
-- MAGIC * Acessar valores individuais de arrays
-- MAGIC * Reformatar valores usando padding
-- MAGIC * Concatenar valores para estar em conformidade com um padrão
-- MAGIC * Acessar partes de um `DateType` como mês, dia e ano

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Início
-- MAGIC
-- MAGIC Célula de setup abaixo. 

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criar a tabela
-- MAGIC
-- MAGIC Armazenada como CSV

-- COMMAND ----------

DROP TABLE IF EXISTS outdoorProductsRaw;
CREATE TABLE outdoorProductsRaw USING csv OPTIONS (
  path "/mnt/training/online_retail/data-001/data.csv",
  header "true"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Describe
-- MAGIC
-- MAGIC Todas as colunas são string.

-- COMMAND ----------

DESCRIBE outdoorProductsRaw

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Amostra da tabela
-- MAGIC A função, `TABLESAMPLE` combina LIMIT com RAND(). 
-- MAGIC
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Também é possível amostrar por percentual

-- COMMAND ----------

SELECT * FROM outdoorProductsRaw TABLESAMPLE (5 ROWS)

-- COMMAND ----------

SELECT * FROM outdoorProductsRaw TABLESAMPLE (2 PERCENT) ORDER BY InvoiceDate 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checar valores nulos
-- MAGIC
-- MAGIC Conta valores nulos. 

-- COMMAND ----------

SELECT count(*) FROM outdoorProductsRaw WHERE Description IS NULL;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criar uma view temporária
-- MAGIC
-- MAGIC
-- MAGIC Note as inconsistências nos formatos de data.
-- MAGIC
-- MAGIC Exemplo: `12/1/11` apresenta um mês com 1 dígito e `1/10/11` apresenta mês com dois dígitos. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Code breakdown 
-- MAGIC
-- MAGIC **`COALESCE`** - Comum em SQL pode ser usado para substituir valores nulos [docs](https://spark.apache.org/docs/latest/api/sql/index.html#coalesce).
-- MAGIC
-- MAGIC **`SPLIT`** - Divide de acordo com um caractere específico e retorna um **array**. Em arrays pode-se acionar itens pelo índice.
-- MAGIC
-- MAGIC A **`Linha 10`** contém um `SPLIT` **aninhado**. 
-- MAGIC
-- MAGIC `SPLIT(InvoiceDate, " ")[0]` --> Remove o horário da string e obtém a data [docs](https://spark.apache.org/docs/latest/api/sql/#split).

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW outdoorProducts AS
SELECT
  InvoiceNo,
  StockCode,
  COALESCE(Description, "Misc") AS Description,
  Quantity,
  SPLIT(InvoiceDate, "/")[0] month,
  SPLIT(InvoiceDate, "/")[1] day,
  SPLIT(SPLIT(InvoiceDate, " ")[0], "/")[2] year,
  UnitPrice,
  Country
FROM
  outdoorProductsRaw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checar valores "Misc"
-- MAGIC
-- MAGIC Verifica se os nulos foram substituídos. 

-- COMMAND ----------

SELECT count(*) FROM outdoorProducts WHERE Description = "Misc" 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criar uma nova tabela
-- MAGIC
-- MAGIC Utilizando a palavra-chave `WITH`. 
-- MAGIC
-- MAGIC Note o `LPAD()`. [Esta função](https://spark.apache.org/docs/latest/api/sql/#lpad) insere caracteres à esqueda da string até que atinja certo tamanho. 
-- MAGIC
-- MAGIC A função `CONCAT_WS()` une os dados da data.  [Esta função](https://spark.apache.org/docs/latest/api/sql/#concat_ws) retorna uma string concatenada separada por um caractere específico.

-- COMMAND ----------

DROP TABLE IF EXISTS standardDate;
CREATE TABLE standardDate

WITH padStrings AS
(
SELECT 
  InvoiceNo,
  StockCode,
  Description,
  Quantity, 
  LPAD(month, 2, 0) AS month,
  LPAD(day, 2, 0) AS day,
  year,
  UnitPrice, 
  Country
FROM outdoorProducts
)
SELECT 
 InvoiceNo,
  StockCode,
  Description,
  Quantity, 
  concat_ws("/", month, day, year) sDate,
  UnitPrice,
  Country
FROM padStrings;






-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verificar a tabela
-- MAGIC Note a data. 

-- COMMAND ----------

SELECT * FROM standardDate LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verificar o schema
-- MAGIC
-- MAGIC Todos os valores ainda são string. 

-- COMMAND ----------

DESCRIBE standardDate;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Mudar para DateType
-- MAGIC
-- MAGIC Converte a data para o tipo correto. [docs](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html).
-- MAGIC
-- MAGIC Converte também o preço para Double. 

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW salesDateFormatted AS
SELECT
  InvoiceNo,
  StockCode,
  to_date(sDate, "MM/dd/yy") date,
  Quantity,
  CAST(UnitPrice AS DOUBLE)
FROM
  standardDate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visualizar os dados
-- MAGIC
-- MAGIC Extrai o dia da semana para analisar a quantidade de itens vendida em cada dia. Pode-se criar um gráfico de barras para visualizar melhor. 
-- MAGIC
-- MAGIC A `date_format()` mapeia o dia para o dia da semana. [A função](https://spark.apache.org/docs/latest/api/sql/#date_format) converte um `timestamp` para uma `string` no formato especificado. O `"E"` especifica o dia da semana. 

-- COMMAND ----------

SELECT
  date_format(date, "E") day,
  SUM(quantity) totalQuantity
FROM
  salesDateFormatted
GROUP BY (day)
ORDER BY day


-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
