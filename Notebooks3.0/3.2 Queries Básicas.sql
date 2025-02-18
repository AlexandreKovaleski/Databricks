-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Queries básicas com SQL
-- MAGIC
-- MAGIC Rode queries para resolver os problemas listados a seguir.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Início
-- MAGIC
-- MAGIC Utilize o script abaixo para ter acesso a dados de teste fornecedidos pela própria Databricks. Toda nova sessão deve ter esse script executado.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Criar tabela
-- MAGIC
-- MAGIC Uma [tabela do Databricks](https://docs.databricks.com/data/tables.html) é uma coleção de dados estruturados. A tabela vai conter 10 milhões de registros fictícios sobre pessoas, como primeiro e último nome, data de nascimento, salário, etc. O formato de arquivo é o [Parquet](https://databricks.com/glossary/what-is-parquet) comumente utilizado em cargas de big data.

-- COMMAND ----------

DROP TABLE IF EXISTS People10M;
CREATE TABLE People10M
USING parquet
OPTIONS (
path "/mnt/training/dataframes/people-10m.parquet",
header "true");

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Consultando tabelas
-- MAGIC Selecione usando a cláusula `SELECT` para visualizar os dados.

-- COMMAND ----------

SELECT * FROM People10M;

-- COMMAND ----------

select *, row_number() over (order by id) as rownum from People10M

-- COMMAND ----------




-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC Pode-se ver o schema da tabela usando a função `DESCRIBE` .
-- MAGIC
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> O **schema** é uma lista que define as colunas e seus tipos de dado. 

-- COMMAND ----------

DESCRIBE People10M;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Mostrando resultados da consulta
-- MAGIC
-- MAGIC Os resultados aparecem abaixo da célula. Use `WHERE` para limitar os resultados dado um conjunto de condições. 
-- MAGIC
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Extrai-se o ano de `birthDate` que é um timestamp

-- COMMAND ----------

SELECT
  firstName,
  middleName,
  lastName,
  birthDate
FROM
  People10M
WHERE
  year(birthDate) > 1990
  AND gender = 'F'

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Matemática
-- MAGIC
-- MAGIC O Spark SQL inclui várias <a href="https://spark.apache.org/docs/latest/api/sql/" target="_blank">funções pré-definidas</a> iguais ao SQL padrão. Pode-se usar elas para criar novas colunas baseadas em regras.

-- COMMAND ----------

 SELECT
  firstName,
  lastName,
  salary,
  salary * 0.2 AS savings
FROM
  People10M

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Views temporárias
-- MAGIC Úteis para exploração de dados, fornecem atalhos para consultas. Não são persistidas quando o cluster é encerrado e não aparecem na aba de "dados".

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PeopleSavings AS
SELECT
  firstName,
  lastName,
  year(birthDate) as birthYear,
  salary,
  salary * 0.2 AS savings
FROM
  People10M;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Obtendo os resultados
-- MAGIC
-- MAGIC Note se o resultado da criação foi um "OK" e rode uma consulta. 

-- COMMAND ----------

-- Selecione todos os dados da view temporária

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Montagens de queries
-- MAGIC Aqui temos um agrupamento e uma função de agregação com arredondamento. A ordenação é decrescente.

-- COMMAND ----------

SELECT
  birthYear,
  ROUND(AVG(salary), 2) AS avgSalary
FROM
  peopleSavings
GROUP BY
  birthYear
ORDER BY
  avgSalary DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Definindo uma nova tabela
-- MAGIC
-- MAGIC O código abaixo cria uma tabela usando Parquet. <a href="https://databricks.com/glossary/what-is-parquet#:~:text=Parquet%20is%20an%20open%20source,like%20CSV%20or%20TSV%20files" target="_blank">Parquet</a> é um formato open-source, baseado em colunas.
-- MAGIC
-- MAGIC
-- MAGIC Este conjunto de dados contém informações sobre a popularidade de nomes nos EUA entre 1880 - 2016.
-- MAGIC
-- MAGIC `Linha 1`: Nomes de tabelas devem ser únicos.
-- MAGIC
-- MAGIC Note os parâmetros.

-- COMMAND ----------

DROP TABLE IF EXISTS ssaNames;
CREATE TABLE ssaNames USING parquet OPTIONS (
  path "/mnt/training/ssn/names.parquet",
  header "true"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preview nos dados
-- MAGIC Selecione todos os registros da tabela recém criada acrescentado a condição `LIMIT` com o valor `5`. 

-- COMMAND ----------

-- Escreva a consulta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Unindo duas tabelas
-- MAGIC
-- MAGIC Para responder
-- MAGIC > Qual a quantidade de primeiros nomes que aparece na tabela `People10M`?
-- MAGIC
-- MAGIC Um JOIN pode ser usado.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Contar valores distintos
-- MAGIC
-- MAGIC As queries a seguir contam valores distintos.

-- COMMAND ----------

SELECT count(DISTINCT firstName)
FROM SSANames;

-- COMMAND ----------

SELECT count(DISTINCT firstName) 
FROM People10M;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criar views temporárias
-- MAGIC Duas views para facilitar o processo de leitura e escrita da união com JOIN. 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW SSADistinctNames AS 
  SELECT DISTINCT firstName AS ssaFirstName 
  FROM SSANames;

CREATE OR REPLACE TEMPORARY VIEW PeopleDistinctNames AS 
  SELECT DISTINCT firstName 
  FROM People10M

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Executar o join
-- MAGIC Para lembrar como funciona o Join em SQL consulte [artigo](https://en.wikipedia.org/wiki/Join_(SQL).  
-- MAGIC
-- MAGIC Por padrão é feito um JOIN do tipo `INNER`. Ou seja, a intersecção. 

-- COMMAND ----------

SELECT firstName 
FROM PeopleDistinctNames 
JOIN SSADistinctNames ON firstName = ssaFirstName

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quantos nomes?
-- MAGIC
-- MAGIC A query a seguir responde a questão. 

-- COMMAND ----------

SELECT count(*) 
FROM PeopleDistinctNames 
JOIN SSADistinctNames ON firstName = ssaFirstName;

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup
-- MAGIC
