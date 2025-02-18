-- Databricks notebook source
-- MAGIC
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lab 2 - Transformação de dados
-- MAGIC
-- MAGIC
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Neste notebook: </br></br>
-- MAGIC
-- MAGIC * Trabalhar com dados hierárquicos
-- MAGIC * Usar common table expressions para mostrar dados
-- MAGIC * Criar tabelas baseadas em outras existentes
-- MAGIC * Gerenciar valores nulos e timestamps

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ex 1: Criar tabela
-- MAGIC **Resumo:** Crie uma nova tabela chamada `eventsRaw` 
-- MAGIC
-- MAGIC Utilize este caminho: `/mnt/training/ecommerce/events/events.parquet`
-- MAGIC
-- MAGIC Passos: 
-- MAGIC * Rode um drop na tabela caso exista
-- MAGIC * Use o caminho especificado

-- COMMAND ----------

-- TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ex 2: Compreender o schema e metadados
-- MAGIC
-- MAGIC **Resumo:** Mostre o schema e outras informações detalhadas
-- MAGIC
-- MAGIC Note o uso dos tipos `ArrayType` e `StructType`
-- MAGIC
-- MAGIC Passos: 
-- MAGIC * Rode um único comando para responder a questão

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ex 3: Amostrar a tabela
-- MAGIC
-- MAGIC **Resumo:** Amostrar para olhar de perto os registros 
-- MAGIC
-- MAGIC Passos: 
-- MAGIC * Mostre 1% dos registros da tabela

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Ex 4: Criar uma nova tabela
-- MAGIC
-- MAGIC **Resumo:** Criar uma tabela `purchaseEvents` que incluem dados de evento com compras com o seguinte schema: 
-- MAGIC
-- MAGIC | ColumnName      | DataType| 
-- MAGIC |-----------------|---------|
-- MAGIC |purchases        |double   |
-- MAGIC |previousEventDate|date     |
-- MAGIC |eventDate        |date     |
-- MAGIC |city             |string   |
-- MAGIC |state            |string   |
-- MAGIC |userId           |string   |
-- MAGIC
-- MAGIC
-- MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Divida os valores timestamp por 1000000 (10e6) antes do casting. 
-- MAGIC
-- MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Dica:** Acesse os valores de StructType usando a notação dot.
-- MAGIC
-- MAGIC Passos: 
-- MAGIC * Drope a tabela se existir
-- MAGIC * Crie uma nova tabela usando `WITH`
-- MAGIC * Use common table expression para manipular os dados
-- MAGIC * Não inclua registros com `purchase_revenue_in_usd` igual a `NULL`
-- MAGIC * Ordene a tabela para que a cidade com mais compras apareça primeiro

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ex 5: Conte os registros
-- MAGIC
-- MAGIC **Resumo:** Conte os registros da tabela `purchaseEvents`. 
-- MAGIC

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ex 6: Enconte as cidades e estados com maior número de compras (purchases)
-- MAGIC **Resumo:** Escreva uma consulta que retorna a cidade e Estado com maior nr de compras. 
-- MAGIC

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Desafio: Produzir relatórios
-- MAGIC
-- MAGIC **Resumo:** Use a tabela `purchaseEvents` para produzir consultas explorando padrões de compra. Adicione visualizações correspondentes.  
-- MAGIC
-- MAGIC Passos: 
-- MAGIC * Crie visualizações para: 
-- MAGIC   * total de compras por dia da semana
-- MAGIC   * média de compras por data da compra
-- MAGIC   * total de compras por Estado
-- MAGIC   * Outros padrões que possam ser encontrados nos dados
-- MAGIC * Faça join da tabela com o caminho abaixo para obter uma lista de clientes e emails
-- MAGIC
-- MAGIC
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Acesse os dados que contém emails. Caminho: `/mnt/training/ecommerce/users/users.parquet`

-- COMMAND ----------

-- Total de compras por dia da semana

-- COMMAND ----------

-- Média de compras por dia da compra

-- COMMAND ----------

--Total de compras por Estado

-- COMMAND ----------

-- Criar tabela de usuários

-- COMMAND ----------

--Consulta unindo a nova tabela com a atual pelo id do usuário

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
