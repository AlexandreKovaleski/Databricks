-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Criar Tabelas
-- MAGIC Execute a célula abaixo para criar tabelas para as perguntas neste notebook.

-- COMMAND ----------

-- MAGIC %run ./05-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 1: Contagem
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Calcule a contagem para cada valor único no campo **`TrueFalse`** na tabela **`revenue1`**.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC Escreva uma consulta SQL que realize o seguinte:
-- MAGIC * Calcule o número de registros **`true`** e **`false`** no campo **`TrueFalse`** da tabela **`revenue1`**
-- MAGIC * Renomeie a nova coluna como **`count`**
-- MAGIC * Armazene os registros em uma visualização temporária chamada **`q1Results`** com o seguinte esquema. (apresentando os dados resultantes)
-- MAGIC
-- MAGIC | column | type |
-- MAGIC |--------|--------|
-- MAGIC | TrueFalse | boolean |
-- MAGIC | MinAmount | int |
-- MAGIC
-- MAGIC Uma solução corretamente concluída deve produzir um DataFrame semelhante a este exemplo de saída:
-- MAGIC
-- MAGIC |TrueFalse|         count |
-- MAGIC |---------|------------------|
-- MAGIC |     true|        4956|
-- MAGIC |    false|        5044|

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW q1Results AS
  SELECT count(TrueFalse) AS count,
  TrueFalse

  FROM revenue1

  GROUP BY TrueFalse;

SELECT * FROM q1Results



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 2: Função Máxima
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Calcule o valor máximo do campo **`Amount`** para cada valor único no campo **`TrueFalse`** na tabela **`revenue2`**.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC * Calcule o valor máximo de **`Amount`** para registros **`True`** e registros **`False`** no campo **`TrueFalse`** da tabela **`revenue2`**
-- MAGIC * Renomeie a nova coluna como **`maxAmount`**
-- MAGIC * Armazene os registros em uma visualização temporária chamada **`q2Results`** com o seguinte esquema. (apresentando os dados resultantes)
-- MAGIC    
-- MAGIC | column | type |
-- MAGIC |--------|--------|
-- MAGIC | TrueFalse | boolean |
-- MAGIC | maxAmount | double |
-- MAGIC
-- MAGIC Uma solução corretamente concluída deve produzir um DataFrame semelhante a este exemplo de saída:
-- MAGIC
-- MAGIC |TrueFalse|         MaxAmount|
-- MAGIC |---------|------------------|
-- MAGIC |     true|        2243937.93|
-- MAGIC |    false|2559457.1799999997|

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW q2Results AS
  SELECT
      TrueFalse,
      MAX(Amount) AS MaxAmount
  FROM
      revenue2
  GROUP BY
      TrueFalse;

SELECT * FROM q2Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 3: Função de Média
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Calcule a média do campo **`Amount`** para cada valor único no campo **`TrueFalse`** na tabela **`revenue3`**.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC * Calcule a média de **`Amount`** para registros **`True`** e registros **`False**` no campo **`TrueFalse`** da tabela **`revenue3`**
-- MAGIC * Renomeie a nova coluna como **`avgAmount`**
-- MAGIC * Armazene os registros em uma visualização temporária chamada **`q3Results`** com o seguinte esquema. (apresentando os dados resultantes)
-- MAGIC
-- MAGIC | column | type |
-- MAGIC |--------|--------|
-- MAGIC | TrueFalse | boolean |
-- MAGIC | avgAmount | double |
-- MAGIC
-- MAGIC Uma solução corretamente concluída deve produzir um DataFrame semelhante a este exemplo de saída:
-- MAGIC
-- MAGIC |TrueFalse|         AvgAmount|
-- MAGIC |---------|------------------|
-- MAGIC |     true|        2243937.93|
-- MAGIC |    false|2559457.1799999997|

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW q3Results AS
  SELECT
      TrueFalse,
      AVG(Amount) AS AvgAmount
  FROM
      revenue3
  GROUP BY
      TrueFalse;

SELECT * FROM q3Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 4: Tabela Dinâmica (Pivot)
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Calcule o valor total de **`Amount`** para os valores de **`YesNo`** iguais a **true** e **false** em 2002 e 2003 na tabela **`revenue4`**.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC * Converta o campo **`UTCTime`** em um carimbo de data e hora (Timestamp) e nomeie a nova coluna como **`Date`**
-- MAGIC * Extraia uma coluna **`Year`** da coluna **`Date`**
-- MAGIC * Filtre os anos maiores que 2001 e menores ou iguais a 2003
-- MAGIC * Agrupe por **`YesNo`** e crie uma tabela dinâmica para obter o valor total de **`Amount`** para cada ano e cada valor em **`YesNo`**
-- MAGIC * Represente cada valor total como um número decimal arredondado para duas casas decimais
-- MAGIC * Armazene os resultados em uma tabela temporária chamada **`q4results`**
-- MAGIC
-- MAGIC Uma solução corretamente executada deve produzir um DataFrame semelhante a este exemplo de saída: (apresentando os dados resultantes)
-- MAGIC
-- MAGIC |YesNo|    2002|    2003|
-- MAGIC |-----|--------|--------|
-- MAGIC | true| 61632.3| 8108.47|
-- MAGIC |false|44699.99|35062.22|

-- COMMAND ----------

select * from revenue4

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW q4Results AS
SELECT *
FROM (
    SELECT
      YesNo, 
      Amount,
      year(from_unixtime(UTCTime)) ano

    FROM revenue4
      )
PIVOT (
      round(sum(Amount), 2) SumAmount
      FOR ano IN ('2002', '2003')
);

SELECT * FROM q4Results


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 5: Valores Nulos e Agregações
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Calcule as somas de **`amount`** agrupadas por **`aisle`** após remover os valores nulos da tabela **`products`**.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC * Remova quaisquer linhas que contenham valores nulos nas colunas **`itemId`** ou **`aisle`**
-- MAGIC * Agregue as somas da coluna **`amount`** agrupadas por **`aisle`**
-- MAGIC * Armazene os resultados em uma visualização temporária chamada **`q5Results`**

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW q5Results AS
  SELECT sum(Amount) total,
  aisle

  FROM products

  WHERE aisle IS NOT NULL
  AND itemId IS NOT NULL

  GROUP BY aisle;

SELECT * FROM q5Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Questão 6: Gerar Subtotais com Rollup
-- MAGIC
-- MAGIC ### Resumo
-- MAGIC Calcule as médias de **`income`** agrupadas por **`itemName`** e **`month`** de modo que os resultados incluam médias para todos os meses, bem como um subtotal para um mês específico da tabela **`sales`**.
-- MAGIC
-- MAGIC ### Etapas para Completar
-- MAGIC * Corrija os valores nulos na coluna **`month`** gerados pela cláusula `ROLLUP`
-- MAGIC * Armazene os resultados em uma visualização temporária chamada **`q6Results`**
-- MAGIC
-- MAGIC Seus resultados devem se parecer com algo assim: (apresentando os dados resultantes)
-- MAGIC
-- MAGIC | itemName| month | avgRevenue |
-- MAGIC | --------| ----- | ---------- |
-- MAGIC | Anim | 10 | 4794.16 |
-- MAGIC | Anim | 7 | 5551.31 |
-- MAGIC | Anim | All months | 5046.54 |
-- MAGIC | Aute | 4 | 4069.51 |
-- MAGIC | Aute | 7 | 3479.31 |
-- MAGIC | Aute | 8 | 6339.28 |
-- MAGIC | Aute | All months |  4489.41 |
-- MAGIC | ... | ... | ... | 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW q6Results AS
  SELECT
      itemName,
      MONTH(date) AS month,
      AVG(revenue) AS avgIncome
  FROM
      sales
  GROUP BY
      ROLLUP(itemName, MONTH(date))
  ORDER BY
      itemName, month;

SELECT * FROM q6Results

-- COMMAND ----------

SELECT * FROM sales 
