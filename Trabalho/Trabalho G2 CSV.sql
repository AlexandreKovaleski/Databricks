-- Databricks notebook source
-- DBTITLE 1,Trabalho G2 - CSV
Divisão do grupo:

Alexandre Kovaleski Fochi - CSV
Felipe Guindani = JSON
Guilherme Graebim = JSON

-- COMMAND ----------

-- MAGIC %run ../Utilities/Classroom-Setup

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ytTrending
  USING csv
  OPTIONS (
    path "dbfs:/FileStore/shared_uploads/akovaleski@live.com/trending_yt_videos_113_countries.csv",
    header "true"
  )

-- COMMAND ----------

DESCRIBE ytTrending

-- COMMAND ----------

SELECT * FROM ytTrending

-- COMMAND ----------

-- DBTITLE 1,Criação de View Temporária
CREATE OR REPLACE TEMPORARY VIEW q1Results AS
  SELECT  title,
          channel_name,
          daily_rank,
          country,
          coalesce(view_count, 0) views,
          coalesce(like_count, 0) likes,
          description,
          snapshot_date

  FROM    ytTrending

  WHERE   channel_name IS NOT NULL
    AND   country = 'BR'

  ORDER BY snapshot_date DESC;

SELECT * FROM q1Results

-- COMMAND ----------

-- DBTITLE 1,Utilizando Cache na Tabela
CACHE TABLE q1Results

-- COMMAND ----------

-- DBTITLE 1,Selecionando a Tabela com Cache
SELECT * FROM q1Results

-- COMMAND ----------

-- DBTITLE 1,Removendo Cache da Tabela
UNCACHE TABLE IF EXISTS q1Results

-- COMMAND ----------

-- DBTITLE 1,Plotagem de Gráfico 1 - Views e Likes nos Últimos 10 Dias
SELECT  country,
        sum( CAST( view_count    AS INT ) ) amountViews,
        sum( CAST( like_count    AS INT ) ) amountLikes,
             CAST( snapshot_date AS DATE  ) date

FROM    ytTrending

WHERE   country = 'BR'

GROUP BY country, date

ORDER BY date DESC LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Plotagem de Gráfico 2 - Quantidade de Views dos Vídeos por País em 06/11/2023
CREATE OR REPLACE TEMPORARY VIEW q2Results AS 
    SELECT  DISTINCT
            country,
            sum( CAST( view_count AS INT ) ) amountViews

      FROM  ytTrending

     WHERE  view_count > 0
       AND  CAST( snapshot_date AS DATE  ) = '2023-11-06'

  GROUP BY  country;

SELECT * FROM q2Results

-- COMMAND ----------

-- DBTITLE 1,Particionando a Tabela Somente com Pais US
CREATE TABLE IF NOT EXISTS countryUS
PARTITIONED BY (country) AS
  SELECT title,
         channel_name,
         CAST( daily_rank AS INT ) ranking,
         coalesce( view_count, 0) views,
         coalesce( like_count, 0) likes,
         description,
         CAST( snapshot_date AS DATE ) date,
         country

    FROM ytTrending
   WHERE country = 'US';

SELECT *
  FROM countryUS 

-- COMMAND ----------

-- DBTITLE 1,Plotagem de Gráfico 3 - Quantidade de Likes no Canal Fortnite
SELECT channel_name,
       sum( likes ) amountLikes,
       date 

  FROM countryUS

 WHERE channel_name LIKE ('Fortnite')

 GROUP BY date, channel_name 
 ORDER BY date ASC

-- COMMAND ----------

-- MAGIC %run ../Utilities/Classroom-Cleanup
