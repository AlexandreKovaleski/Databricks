-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC ## Otimizando o Delta
-- MAGIC
-- MAGIC Neste notebook, você verá alguns exemplos de como otimizar suas consultas usando o Delta Engine, que está incorporado no Databricks Runtime 7.0. Também faz parte do [Delta Lake](https://delta.io/) de código aberto.
-- MAGIC
-- MAGIC Os dados contêm informações sobre os horários de voos nos EUA a partir de 2008. Eles estão disponíveis para nós por meio dos [Conjuntos de Dados do Databricks](https://docs.databricks.com/data/databricks-datasets.html).
-- MAGIC
-- MAGIC Primeiro, criaremos uma tabela padrão usando o formato Parquet e, em seguida, executaremos uma consulta para observar o tempo de execução.
-- MAGIC
-- MAGIC Em seguida, executaremos a mesma consulta em uma tabela Delta usando otimizações do Delta Engine e compararemos as duas.
-- MAGIC
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> O Databricks inclui uma variedade de conjuntos de dados que você pode usar para continuar aprendendo ou apenas praticar! Confira a documentação para obter código Python copiável que você pode usar para ver quais conjuntos de dados estão disponíveis.
-- MAGIC
-- MAGIC Execute a célula abaixo para configurar o ambiente da sala de aula.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criar uma tabela Parquet
-- MAGIC Execute o comando abaixo para criar uma tabela Parquet.

-- COMMAND ----------

DROP TABLE IF EXISTS flights;
-- Crie uma tabela padrão e importe voos dos EUA para o ano de 2008
-- Cláusula USING: Especifique o formato parquet para uma tabela padrão
-- Cláusula PARTITIONED BY: Organize os dados com base na coluna "Origin" (código do aeroporto de origem).
-- Cláusula FROM: Importe dados de um arquivo CSV.
CREATE TABLE flights
USING
  parquet
PARTITIONED BY
  (Origin)
SELECT
  _c0 AS Year,
  _c1 AS MONTH,
  _c2 AS DayofMonth,
  _c3 AS DayOfWeek,
  _c4 AS DepartureTime,
  _c5 AS CRSDepartureTime,
  _c6 AS ArrivalTime,
  _c7 AS CRSArrivalTime,
  _c8 AS UniqueCarrier,
  _c9 AS FlightNumber,
  _c10 AS TailNumber,
  _c11 AS ActualElapsedTime,
  _c12 AS CRSElapsedTime,
  _c13 AS AirTime,
  _c14 AS ArrivalDelay,
  _c15 AS DepartureDelay,
  _c16 AS Origin,
  _c17 AS Destination,
  _c18 AS Distance,
  _c19 AS TaxiIn,
  _c20 AS TaxiOut,
  _c21 AS Cancelled,
  _c22 AS CancellationCode,
  _c23 AS Diverted,
  _c24 AS CarrierDelay,
  _c25 AS WeatherDelay,
  _c26 AS NASDelay,
  _c27 AS SecurityDelay,
  _c28 AS LateAircraftDelay
FROM                              -- Leitura de um CSV. 
  csv.`dbfs:/databricks-datasets/asa/airlines/2008.csv` 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Maior total mensal (Parquet)
-- MAGIC
-- MAGIC Execute a consulta para obter as 20 cidades com o maior total de voos mensais no primeiro dia da semana. Certifique-se de observar o horário em que a consulta é concluída.

-- COMMAND ----------

SELECT Month, Origin, count(*) as TotalFlights 
FROM flights
WHERE DayOfWeek = 1 
GROUP BY Month, Origin 
ORDER BY TotalFlights DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criar uma Tabela Delta
-- MAGIC Execute a consulta abaixo para comparar o Delta com o Parquet. Lembre-se de que este é o exato mesmo comando sendo executado na mesma configuração de cluster. Lembre-se de que as duas operações consomem aproximadamente a mesma quantidade de "trabalho" do Spark. Temos que ler um arquivo CSV enorme, particioná-lo por origem e armazená-lo em um novo formato colunar. Além disso, o Delta está criando um registro de transação e marcando os arquivos com metadados importantes e úteis!

-- COMMAND ----------

DROP TABLE IF EXISTS flights;
-- Crie uma tabela padrão e importe voos dos EUA para o ano de 2008
-- Cláusula USING: Especifique o formato "delta" em vez do formato parquet padrão
-- Cláusula PARTITIONED BY: Organize os dados com base na coluna "Origin" (código do aeroporto de origem).
-- Cláusula FROM: Importe dados de um arquivo CSV.
CREATE TABLE flights
USING
  delta
PARTITIONED BY
  (Origin)
SELECT
  _c0 AS Year,
  _c1 AS MONTH,
  _c2 AS DayofMonth,
  _c3 AS DayOfWeek,
  _c4 AS DepartureTime,
  _c5 AS CRSDepartureTime,
  _c6 AS ArrivalTime,
  _c7 AS CRSArrivalTime,
  _c8 AS UniqueCarrier,
  _c9 AS FlightNumber,
  _c10 AS TailNumber,
  _c11 AS ActualElapsedTime,
  _c12 AS CRSElapsedTime,
  _c13 AS AirTime,
  _c14 AS ArrivalDelay,
  _c15 AS DepartureDelay,
  _c16 AS Origin,
  _c17 AS Destination,
  _c18 AS Distance,
  _c19 AS TaxiIn,
  _c20 AS TaxiOut,
  _c21 AS Cancelled,
  _c22 AS CancellationCode,
  _c23 AS Diverted,
  _c24 AS CarrierDelay,
  _c25 AS WeatherDelay,
  _c26 AS NASDelay,
  _c27 AS SecurityDelay,
  _c28 AS LateAircraftDelay
FROM
  csv.`dbfs:/databricks-datasets/asa/airlines/2008.csv`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Otimizar a tabela
-- MAGIC
-- MAGIC Se a sua organização escreve continuamente dados em uma tabela Delta, com o tempo ela acumulará um grande número de arquivos, especialmente se você adicionar dados em lotes pequenos. Para analistas, uma reclamação comum na consulta a data lakes é a eficiência de leitura; e ter uma grande coleção de arquivos pequenos para examinar toda vez que os dados são consultados pode criar problemas de desempenho. Idealmente, um grande número de arquivos pequenos deve ser reescrito em um número menor de arquivos maiores regularmente, o que melhorará a velocidade das consultas de leitura de uma tabela. Isso é conhecido como compactação. Você pode compactar uma tabela usando o comando `OPTIMIZE` mostrado abaixo.
-- MAGIC
-- MAGIC O Z-ordering co-localiza informações de coluna (lembre-se de que o Delta é armazenamento colunar). A co-localidade é usada pelos algoritmos de pulo de dados do Delta Lake para reduzir drasticamente a quantidade de dados que precisa ser lida. Você pode especificar várias colunas para o ZORDER BY como uma lista separada por vírgulas. No entanto, a eficácia da co-localidade diminui com cada coluna adicional. Saiba mais sobre a otimização de tabelas Delta [aqui](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html).

-- COMMAND ----------

OPTIMIZE flights ZORDER BY (DayofWeek);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Execute a consulta novamente
-- MAGIC Execute a consulta abaixo para comparar o desempenho entre uma tabela Parquet padrão e uma tabela Delta otimizada.

-- COMMAND ----------

SELECT Month, Origin, count(*) as TotalFlights 
FROM flights
WHERE DayOfWeek = 1 
GROUP BY Month, Origin 
ORDER BY TotalFlights DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delta Cache 
-- MAGIC
-- MAGIC Usar o cache Delta é uma excelente maneira de otimizar o desempenho. Observação: o cache Delta *não* é o mesmo que o cache no Apache Spark, sobre o qual falamos no Módulo 4. Uma diferença notável é que o cache Delta é armazenado inteiramente no disco local, para que a memória não seja retirada de outras operações dentro do Spark. Quando ativado, o cache Delta cria automaticamente uma cópia de um arquivo remoto no armazenamento local, para que leituras sucessivas sejam significativamente mais rápidas. Infelizmente, para ativá-lo, você deve escolher um tipo de cluster que não está disponível na Edição Comunitária do Databricks.
-- MAGIC
-- MAGIC Para entender melhor as diferenças entre o cache Delta e o cache do Apache Spark, leia ["Delta and Apache Spark caching."](https://docs.databricks.com/delta/optimizations/delta-cache.html#delta-and-apache-spark-caching)

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
