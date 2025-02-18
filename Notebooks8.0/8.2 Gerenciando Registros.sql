-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gerenciando Registros
-- MAGIC
-- MAGIC Na leitura anterior, demonstramos como criar uma tabela Delta. Utilizamos estratégias básicas de exploração de dados para identificar dois problemas nos dados. Neste notebook, demonstraremos como corrigir esses problemas e escrever em uma nova tabela de nível "gold" limpa que você pode usar para consultas. Além disso, demonstraremos como reparar e corrigir registros.
-- MAGIC
-- MAGIC Neste notebook, você irá:
-- MAGIC * Usar uma função de janela para interpolar valores ausentes
-- MAGIC * Atualizar uma tabela Delta
-- MAGIC * Verificar o histórico de versões em uma tabela Delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Começando
-- MAGIC Execute a célula abaixo para configurar o ambiente da sala de aula.

-- COMMAND ----------

-- MAGIC %run ../Includes/8-2-Setup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Reparando Registros
-- MAGIC
-- MAGIC Na leitura anterior, encontramos dois problemas nos dados:
-- MAGIC
-- MAGIC 1. Estávamos faltando registros de um único dispositivo por um período de três dias.
-- MAGIC 2. Havia pelo menos uma leitura defeituosa (frequência cardíaca inferior a zero) por dia em nosso conjunto.
-- MAGIC
-- MAGIC Vamos começar demonstrando como mesclar um conjunto de atualizações e inserções para corrigir esses problemas.
-- MAGIC
-- MAGIC Primeiro, vamos trabalhar nas leituras defeituosas do sensor. Anteriormente, você usou uma função de janela, emparelhada com a função `AVG`, para calcular um valor médio ao longo de um grupo de linhas. Aqui, usaremos uma função de janela, emparelhada com as funções embutidas `LAG` e `LEAD`, para interpolar valores e substituir as leituras defeituosas.
-- MAGIC
-- MAGIC **`LAG`**: obtém dados da linha anterior. [Saiba mais](https://spark.apache.org/docs/latest/api/sql/#lag). <br>
-- MAGIC **`LEAD`**: obtém dados da linha subsequente. [Saiba mais](https://spark.apache.org/docs/latest/api/sql/#lead).<br>
-- MAGIC
-- MAGIC Examine o código na próxima célula.
-- MAGIC
-- MAGIC `Linha 1`: Criamos ou substituímos uma visualização temporária chamada `updates`<br>
-- MAGIC `Linha 2`: Estamos usando um padrão CTAS para criar essa nova visualização<br>
-- MAGIC `Linha 3`: Selecionamos um subgrupo de colunas para incluir na função de janela definida nas linhas 5 - 8. Observe a expressão `(prev_amt+next_amt)/2`. Para qualquer entrada ausente, calculamos um novo ponto de dados que é a média da entrada anterior e da entrada subsequente. Esses valores são definidos na função de janela abaixo <br>
-- MAGIC `Linha 4`: Indica a função de janela como a fonte. O parêntese marca o início da função de janela<br>
-- MAGIC `Linha 5`: Seleciona todas as colunas de `health_tracker_silver`<br>
-- MAGIC `Linha 6`: `LAG` obtém a `heartrate` da linha anterior. Definimos a janela por `device_id` e `dte` para que o cálculo seja aplicado a cada valor ausente **por dispositivo** <br>
-- MAGIC `Linha 9`: Marca o fim da função de janela<br>
-- MAGIC `Linha 10`: Identifica que esse cálculo deve se aplicar **apenas** onde a leitura de frequência cardíaca é inferior a 0
-- MAGIC
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> **Interpolação** é um tipo de estimativa em que construímos novos pontos de dados com base em um conjunto de pontos de dados conhecidos.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW updates 
AS (
  SELECT name, (prev_amt+next_amt)/2 AS heartrate, time, dte, p_device_id
  FROM (
    SELECT *, 
    LAG(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS prev_amt, 
    LEAD(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS next_amt 
    FROM health_tracker_silver
  ) 
  WHERE heartrate < 0
)

-- COMMAND ----------

select * FROM updates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verificar o esquema
-- MAGIC
-- MAGIC Vamos querer usar os valores em `updates` para atualizar nossa tabela `health_tracker_silver`. Vamos verificar os esquemas de ambos para ver se coincidem.

-- COMMAND ----------

DESCRIBE updates

-- COMMAND ----------

DESCRIBE health_tracker_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dados que Chegam Tarde
-- MAGIC
-- MAGIC Estamos prontos para atualizar nossa tabela Silver com nossos valores interpolados, mas antes disso, descobrimos que aquelas leituras ausentes finalmente chegaram. Podemos preparar esses dados para mesclar com nossas outras atualizações.
-- MAGIC
-- MAGIC Execute a célula abaixo para ler os dados brutos.

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_data_2020_02_late;              

CREATE TABLE health_tracker_data_2020_02_late                        
USING json                                             
OPTIONS (
  path "dbfs:/mnt/training/healthcare/tracker/raw-late.json",
  inferSchema "true"
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preparar Inserções
-- MAGIC
-- MAGIC Podemos aplicar as mesmas transformações que usamos para criar nossa tabela `health_tracker_silver` a esses dados brutos. Isso nos dará uma visualização com o mesmo esquema de nossas outras tabelas e visualizações.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW inserts AS (
  SELECT
    value.name,
    value.heartrate,
    CAST(FROM_UNIXTIME(value.time) AS timestamp) AS time,
    CAST(FROM_UNIXTIME(value.time) AS DATE) AS dte,
    value.device_id p_device_id
  FROM
    health_tracker_data_2020_02_late
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preparar Upserts
-- MAGIC
-- MAGIC A palavra "upsert" é uma mistura das palavras "update" e "insert," e é isso que ele faz. Um upsert atualizará registros quando determinados critérios forem atendidos e, caso contrário, inserirá o registro. Criamos uma visualização que é a união de nossas visualizações `updates` e `inserts` e mantém todos os registros que gostaríamos de adicionar e modificar.
-- MAGIC
-- MAGIC Aqui, usamos `UNION ALL` para capturar todos os registros em ambas as visualizações, inclusive os duplicados.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
    SELECT * FROM updates 
    UNION ALL 
    SELECT * FROM inserts
    )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Realizar o Upsert
-- MAGIC
-- MAGIC Ao fazer um upsert em uma tabela Delta existente, use o Spark SQL para realizar a mesclagem a partir de outra tabela ou visualização registrada. O Registro de Transações registra a transação, e o Metastore reflete imediatamente as alterações.
-- MAGIC
-- MAGIC A mesclagem anexa tanto os novos/arquivos inseridos quanto os arquivos que contêm as atualizações ao diretório de arquivos Delta. O registro de transações informa ao leitor Delta qual arquivo usar para cada registro.
-- MAGIC
-- MAGIC Essa operação é semelhante ao comando SQL `MERGE`, mas possui suporte adicional para exclusões e outras condições em atualizações, inserções e exclusões. Em outras palavras, o uso do comando Spark SQL `MERGE` fornece suporte completo para uma operação de upsert.
-- MAGIC
-- MAGIC Use os comentários para entender melhor como este comando integra registros de nossas tabelas e visualizações existentes.
-- MAGIC
-- MAGIC Saiba mais sobre `MERGE INTO` [aqui](https://docs.databricks.com/spark/latest/spark-sql/language-manual/merge-into.html#merge-into--delta-lake-on-databricks).

-- COMMAND ----------

MERGE INTO health_tracker_silver                            
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id  
   
WHEN MATCHED THEN                                           
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                       
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viagem no Tempo
-- MAGIC
-- MAGIC Vamos verificar o número de registros nas diferentes versões de nossas tabelas.
-- MAGIC
-- MAGIC A Versão 1 mostra os dados após adicionarmos os registros de fevereiro. Lembre-se de que é aqui que descobrimos pela primeira vez os registros ausentes.
-- MAGIC
-- MAGIC A versão atual mostra tudo, incluindo os registros que upserted.

-- COMMAND ----------

-- VERSION 1
SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 1

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Descrever o Histórico
-- MAGIC Você pode verificar todo o histórico de uma tabela Delta, incluindo a operação, o usuário e outros detalhes para cada nova gravação na tabela.
-- MAGIC  

-- COMMAND ----------

DESCRIBE HISTORY health_tracker_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Escrever na tabela "gold"
-- MAGIC
-- MAGIC Até agora, ingerimos dados brutos (nível bronze) e aplicamos transformações para criar uma tabela silver. Usamos o Spark SQL para explorar e transformar ainda mais esses dados, adicionando novos valores quando encontramos erros de coleta e atualizando a tabela para refletir dados que chegaram atrasados. Agora que nossos dados estão limpos e refinados, podemos escrever em uma tabela "gold". Tabelas "gold" são usadas para manter agregações de nível de negócios. Ao criar esta tabela, também aplicamos funções de agregação a várias colunas.

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_gold;              

CREATE TABLE health_tracker_gold                        
USING DELTA
LOCATION "/health_tracker/gold"
AS 
SELECT 
  AVG(heartrate) AS meanHeartrate,
  STD(heartrate) AS stdHeartrate,
  MAX(heartrate) AS maxHeartrate
FROM health_tracker_silver
GROUP BY p_device_id


-- COMMAND ----------

SELECT
  *
FROM
  health_tracker_gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Limpeza
-- MAGIC Execute a próxima célula para limpar o ambiente da sala de aula.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Great work! Now that you've got a basic understanding of how data moves through the Delta architecture, we're ready to get back to analytics. In the next reading, we'll see how to write high-performance Spark queries with Databricks Delta. 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
