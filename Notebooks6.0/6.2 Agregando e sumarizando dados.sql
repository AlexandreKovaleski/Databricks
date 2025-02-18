-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Agregação e Resumo de Dados
-- MAGIC
-- MAGIC Agora, vamos analisar algumas funções poderosas que podemos usar para agregar e resumir dados. Neste notebook, continuaremos a trabalhar com funções de ordem superior; desta vez, as aplicaremos a matrizes contendo dados numéricos. Além disso, trabalharemos com funções adicionais no Spark SQL que podem ser úteis ao apresentar dados.
-- MAGIC
-- MAGIC Neste notebook:
-- MAGIC * Aplicar funções de ordem superior a dados numéricos
-- MAGIC * Usar o comando `PIVOT` para criar tabelas dinâmicas
-- MAGIC * Usar os modificadores `ROLLUP` e `CUBE` para gerar subtotais
-- MAGIC * Usar funções de janela para realizar operações em um grupo de linhas
-- MAGIC * Usar as ferramentas de visualização do Databricks para visualizar e compartilhar dados
-- MAGIC
-- MAGIC Execute a célula abaixo para configurar nosso ambiente de sala de aula.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Funções de alta ordem e dados numéricos
-- MAGIC
-- MAGIC Cada uma das funções de alta ordem com as quais trabalhamos na última lição também pode ser usada com dados numéricos. Nesta lição, demonstraremos como cada uma das funções na lição anterior funciona com dados numéricos, bem como exploraremos algumas novas e poderosas funções de alta ordem.
-- MAGIC
-- MAGIC Execute as próximas duas células para criar e descrever a tabela com a qual estaremos trabalhando. Você pode reconhecer esta tabela de uma lição anterior. Lembre-se de que ela contém dados que medem a variabilidade ambiental em um conjunto de centros de dados. A tabela `DeviceData` contém as matrizes `temps` e `co2Level` que usamos para demonstrar funções de alta ordem.

-- COMMAND ----------

DROP TABLE IF EXISTS DCDataRaw;
CREATE TABLE DCDataRaw
USING parquet                           
OPTIONS (
    PATH "/mnt/training/iot-devices/data-centers/2019-q2-q3"
    );
    
CREATE TABLE IF NOT EXISTS DeviceData3     
USING parquet                               
WITH ExplodeSource                        
AS
  (
  SELECT 
  dc_id,
  to_date(date) AS date,
  EXPLODE (source)
  FROM DCDataRaw
  )
SELECT 
  dc_id,
  key device_type,                         
  date,
  value.description,
  value.ip,
  value.temps,
  value.co2_level co2Level
  
FROM ExplodeSource;

-- COMMAND ----------

DESCRIBE DeviceData3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visualização dos dados
-- MAGIC
-- MAGIC Vamos dar uma olhada em uma amostra dos dados para que possamos entender melhor os valores das matrizes.

-- COMMAND ----------

SELECT 
  temps, 
  co2Level
FROM DeviceData3
TABLESAMPLE (1 ROWS)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Filtrar
-- MAGIC
-- MAGIC O filtro opera em matrizes contendo dados numéricos da mesma forma que aquelas com dados de texto. Neste caso, imagine que desejamos coletar todas as temperaturas acima de um determinado limite. Execute a célula abaixo para visualizar o exemplo.

-- COMMAND ----------

SELECT 
  temps, 
  FILTER(temps, t -> t > 18) highTemps
FROM DeviceData2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exists
-- MAGIC O "Exists" opera em matrizes contendo dados numéricos da mesma forma que aquelas com dados de texto. Digamos que desejamos sinalizar os registros cujas temperaturas tenham excedido um valor especificado. Execute a célula abaixo para ver o exemplo.

-- COMMAND ----------

SELECT 
  temps, 
  EXISTS(temps, t -> t > 23) highTempsFlag
FROM DeviceData3

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Transform
-- MAGIC
-- MAGIC Quando usamos o `TRANSFORM` com dados numéricos, podemos aplicar qualquer função interna destinada a trabalhar com um único valor, ou podemos nomear nosso próprio conjunto de operações a serem aplicadas a cada valor na matriz. Esses dados incluem leituras de temperatura em Celsius. Cada linha contém uma matriz de 12 leituras de temperatura. Podemos usar o `TRANSFORM` para converter cada elemento de cada matriz em Fahrenheit. Para converter de Celsius para Fahrenheit, multiplicamos a temperatura em Celsius por 9, dividimos por 5 e depois adicionamos 32.
-- MAGIC
-- MAGIC Vamos analisar o código abaixo para entender melhor a função:
-- MAGIC
-- MAGIC **`TRANSFORM`**: o nome da função de ordem superior
-- MAGIC **`temps`**: o nome de nossa matriz de entrada
-- MAGIC **`t`**: o nome da variável iteradora. Você escolhe este nome e depois o usa na função lambda. Ela itera sobre a matriz, passando cada valor para a função um de cada vez.
-- MAGIC **`->`**: Indica o início da função
-- MAGIC **` ((t * 9) div 5) + 32`**: Esta é a função. Para cada valor na matriz de entrada, o valor é multiplicado por 9 e depois dividido por 5. Em seguida, adicionamos 32. Esta é a fórmula para a conversão de Celsius para Fahrenheit.
-- MAGIC Lembre-se de que o `TRANSFORM` recebe uma matriz, um iterador e uma função anônima como entrada. No código abaixo, `temps` é a coluna que contém a matriz e `t` é o iterador que percorre a lista. A função anônima `((t * 9) div 5) + 32` será aplicada a cada valor na lista.
-- MAGIC
-- MAGIC Nós usamos a função **`div`** para dividir uma expressão por outra sem incluir valores decimais. Isso é estritamente para organização neste exemplo. Para operações em que a precisão é importante, você usaria o operador `/`, que realiza uma divisão de ponto flutuante.
-- MAGIC

-- COMMAND ----------

SELECT 
  temps temps_C,
  TRANSFORM (temps, t -> ((t * 9) div 5) + 32 ) temps_F
FROM DeviceData3;


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ### Reduce 
-- MAGIC
-- MAGIC O `REDUCE` é mais avançado do que o `TRANSFORM`; ele usa duas funções lambda. Você pode usá-lo para reduzir os elementos de uma matriz a um único valor, mesclando os elementos em um buffer e aplicando uma função de finalização no buffer final.
-- MAGIC
-- MAGIC Vamos usar a função `REDUCE` para encontrar um valor médio, por dia, para nossas leituras de CO<sub>2</sub>. Dê uma olhada mais de perto nas partes individuais da função `REDUCE`, revisando a lista abaixo.
-- MAGIC
-- MAGIC `REDUCE(co2_level, 0, (c, acc) -> c + acc, acc ->(acc div size(co2_level)))`
-- MAGIC
-- MAGIC **`co2_level`** é a matriz de entrada.
-- MAGIC **`0`** é o ponto de partida para o buffer. Lembre-se de que precisamos manter um valor temporário do buffer cada vez que um novo valor é adicionado da matriz; começamos com zero neste caso para obter uma soma precisa dos valores na lista.
-- MAGIC **`(c, acc)`** é a lista de argumentos que usaremos para esta função. Pode ser útil pensar em `acc` como o valor do buffer e `c` como o valor que é adicionado ao buffer.
-- MAGIC **`c + acc`** é a função do buffer. À medida que a função itera sobre a lista, ela mantém o total (`acc`) e adiciona o próximo valor na lista (`c`).
-- MAGIC **`acc div size(co2_level)`** é a função de finalização. Depois de termos a soma de todos os números na matriz, dividimos pelo número de elementos para encontrar a média.
-- MAGIC
-- MAGIC Esta visualização será útil nos exercícios subsequentes, portanto, criaremos uma visualização temporária neste passo para que possamos acessá-la posteriormente.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW Co2LevelsTemporary
AS
  SELECT
    dc_id, 
    device_type,
    co2level,
    REDUCE(co2level, 0, (c, acc) -> c + acc, acc ->(acc div size(co2level))) as averageCo2Level
  FROM DeviceData3  
  SORT BY averageCo2Level DESC;

SELECT * FROM Co2LevelsTemporary

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Outras funções de alta ordem
-- MAGIC Existem muitas funções internas projetadas para trabalhar com dados do tipo matriz, bem como outras funções de alta ordem para explorar.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Tabelas dinâmicas: Exemplo 1
-- MAGIC As tabelas dinâmicas são suportadas no Spark SQL. Uma tabela dinâmica permite transformar linhas em colunas e agrupar por qualquer campo de dados. Vamos dar uma olhada mais de perto em nossa consulta.
-- MAGIC
-- MAGIC **`SELECT * FROM ()`**: A declaração `SELECT` dentro dos parênteses é o input para esta tabela. Observe que ela seleciona duas colunas da visualização `Co2LevelsTemporary`.
-- MAGIC **`PIVOT`**: O primeiro argumento na cláusula é uma função de agregação e a coluna a ser agregada. Em seguida, especificamos a coluna de pivô na subcláusula `FOR`. O operador `IN` contém os valores da coluna de pivô.

-- COMMAND ----------

SELECT * FROM (
  SELECT device_type, averageCo2Level 
  FROM Co2LevelsTemporary
)
PIVOT (
  ROUND(AVG(averageCo2Level), 2) avg_co2 
  FOR device_type IN ('sensor-ipad', 'sensor-inest', 
    'sensor-istick', 'sensor-igauge')
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tabelas Dinâmicas: Exemplo 2
-- MAGIC
-- MAGIC Neste exemplo, novamente retiramos dados de nossa tabela maior, `DeviceData`. Dentro da subconsulta, criamos a coluna `month` e usamos a função `REDUCE` para criar a coluna `averageCo2Level`.
-- MAGIC
-- MAGIC Na tabela dinâmica, tiramos a média dos valores de `averageCo2Level` agrupados por mês. Observe que renomeamos as colunas de mês de seus números para as abreviações em inglês.
-- MAGIC
-- MAGIC Saiba mais sobre tabelas dinâmicas neste [post](https://databricks.com/blog/2018/11/01/sql-pivot-converting-rows-to-columns.html).

-- COMMAND ----------

SELECT
  *
FROM
  (
    SELECT
      month(date) month,
      REDUCE(co2level, 0, (c, acc) -> c + acc, acc ->(acc div size(co2level))) averageCo2Level
    FROM
      DeviceData3
  ) PIVOT (
    avg(averageCo2Level) avg FOR month IN (7 JUL, 8 AUG, 9 SEPT, 10 OCT, 11 NOV)
  )

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Rollups
-- MAGIC
-- MAGIC Rollups são operadores usados com a cláusula `GROUP BY`. Eles permitem que você resuma dados com base nas colunas passadas para o operador `ROLLUP`.
-- MAGIC
-- MAGIC Os resultados desta consulta incluem os níveis médios de CO<sub>2</sub>, agrupados por `dc_id` e `device_type`. Os rollups estão criando subtotais para um grupo específico de dados. Os subtotais introduzem novas linhas, e as novas linhas conterão valores `NULL` para tudo, exceto o subtotal calculado.
-- MAGIC
-- MAGIC Podemos alterar isso usando a função `COALESCE()` introduzida em uma lição anterior.

-- COMMAND ----------

SELECT 
  COALESCE(dc_id, "All data centers") AS dc_id,
  COALESCE(device_type, "All devices") AS device_type,
  ROUND(AVG(averageCo2Level))  AS avgCo2Level
FROM Co2LevelsTemporary
GROUP BY dc_id, device_type
ORDER BY dc_id, device_type;

-- COMMAND ----------

SELECT 
  COALESCE(dc_id, "All data centers") AS dc_id,
  COALESCE(device_type, "All devices") AS device_type,
  ROUND(AVG(averageCo2Level))  AS avgCo2Level
FROM Co2LevelsTemporary
GROUP BY ROLLUP (dc_id, device_type)
ORDER BY dc_id, device_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cube
-- MAGIC `CUBE` é também um operador usado com a cláusula `GROUP BY`. Similar ao `ROLLUP`, você pode usar o `CUBE` para gerar valores de resumo para subelementos agrupados pelo valor da coluna. O `CUBE` é diferente do `ROLLUP` porque também gerará subtotais para todas as combinações das colunas de agrupamento especificadas na cláusula `GROUP BY`.
-- MAGIC
-- MAGIC Observe que a saída do exemplo abaixo mostra alguns dos valores adicionais gerados nesta consulta. Os dados de "Todos os centros de dados" (All data centers) foram agregados por tipo de dispositivo em todos os centros.

-- COMMAND ----------

SELECT 
  COALESCE(dc_id, "All data centers") AS dc_id,
  COALESCE(device_type, "All devices") AS device_type,
  ROUND(AVG(averageCo2Level))  AS avgCo2Level
FROM Co2LevelsTemporary
GROUP BY CUBE (dc_id, device_type)
ORDER BY dc_id, device_type;

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
