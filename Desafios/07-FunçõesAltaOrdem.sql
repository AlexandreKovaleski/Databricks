-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Criar Tabelas
-- MAGIC Execute a célula abaixo para criar tabelas para as perguntas neste notebook.

-- COMMAND ----------

-- MAGIC %run ../Utilities/07-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 1: Transformar
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Use a função **`TRANSFORM`** e a tabela **`finances`** para calcular os **`juros`** para todos os cartões emitidos para cada usuário.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL que realize o seguinte:
-- MAGIC * Exibe o **`firstName`** e o **`lastName`** do titular do cartão, bem como uma nova coluna chamada **`interest`**
-- MAGIC * Usa **`TRANSFORM`** para extrair as despesas de cada cartão na coluna **`expenses`** e calcula os juros devidos com uma taxa de 6,25%.
-- MAGIC * Armazena os novos valores como um array na coluna **`interest`**
-- MAGIC * Armazena os resultados em uma tabela temporária chamada `q1Results`
-- MAGIC
-- MAGIC Uma solução corretamente executada deve retornar uma visualização que se parece com esta: (apresentando os dados resultantes)
-- MAGIC
-- MAGIC | firstName | lastName | interest |
-- MAGIC |----------- |---------|----------|
-- MAGIC |Lance|Da Costa|[138.9, 373.55, 158.97]|

-- COMMAND ----------

SELECT firstName,
       transform(expenses, exs -> exs.charges * 1.0625 ) AS interes
FROM finances

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 2: Exists
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Use a tabela da Questão 1, **`finances`**, para identificar os usuários cujos registros indicam que fizeram um pagamento em atraso.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL que realize o seguinte:
-- MAGIC * Exibe o **`firstName`** e o **`lastName`** do titular do cartão, bem como uma nova coluna chamada **`lateFee`**
-- MAGIC * Usa a função EXISTS para identificar os clientes que foram cobrados com uma taxa de pagamento em atraso.
-- MAGIC * Armazena os resultados em uma visualização temporária chamada **`q2Results`**
-- MAGIC
-- MAGIC Uma solução corretamente executada deve retornar um DataFrame que se parece com este exemplo: (apresentando os dados resultantes)
-- MAGIC
-- MAGIC | firstName | lastName | lateFee |
-- MAGIC |---------- |----------| ------- |
-- MAGIC |Lance|DaCosta |true|

-- COMMAND ----------

SELECT firstName,
       exists(expenses, exs -> TO_DATE(exs.lastPayment) < TO_DATE(exs.paymentDue)) AS interest
FROM finances

-- COMMAND ----------

select * from finances

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 3: Reduzir
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Use a função **`REDUCE`** para criar uma consulta na tabela **`charges`** que calcule as despesas totais em dólares e as despesas totais em ienes japoneses.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL que realize o seguinte:
-- MAGIC * Use a função **`REDUCE`** para calcular as despesas totais em dólares americanos (dado)
-- MAGIC * Use a função **`REDUCE`** para converter as despesas totais em ienes japoneses usando uma taxa de conversão onde 1 USD = 107,26 JPY
-- MAGIC * Armazene os resultados em uma tabela temporária chamada **`q3Results`**
-- MAGIC
-- MAGIC **OBSERVAÇÃO:** Na função `REDUCE`, o acumulador deve ser do mesmo tipo que a entrada. Você terá que fazer um `CAST` do acumulador como `DOUBLE` para usar esta função com esses dados. Exemplo: `CAST(0 AS DOUBLE)`
-- MAGIC
-- MAGIC Uma solução corretamente executada deve retornar um DataFrame que se parece com este exemplo: (apresentando os dados resultantes)
-- MAGIC
-- MAGIC | firstName | lastName | allCharges | totalDollars | totalYen |
-- MAGIC |---------- |----------| ------- | --------| ----------|
-- MAGIC |Lance|DaCosta |["2222.46", "5976.76", "2543.55"]|10742.77|1152269.51|

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW q3Results AS 
  SELECT firstName, 
         lastName, 
         allCharges,
         REDUCE(allCharges, CAST(0 AS DOUBLE), (charge , acc) -> charge + acc) AS totalDollars,
         REDUCE(allCharges, CAST(0 AS DOUBLE), (charge , acc) -> charge + acc , acc -> ROUND(acc * 107.26, 2)) AS totalYen
  FROM charges;

SELECT * FROM q3Results
