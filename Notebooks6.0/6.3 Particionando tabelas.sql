-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Particionamento de Tabelas
-- MAGIC
-- MAGIC Você pode afetar o desempenho das consultas particionando os dados em suas tabelas. Lembre-se de que examinamos algumas melhorias (e piorias) específicas de desempenho que podem ser causadas pelo particionamento. O particionamento de dados em uma consulta Spark SQL cria um subdiretório de dados de acordo com uma regra. Por exemplo, se eu particionar um conjunto de dados por ano, todos os dados em qualquer pasta nas subpastas dessa tabela terão o mesmo ano. Isso significa que, quando chegar a hora de consultar o conjunto e eu incluir algo como:<br> `WHERE year = 1990`, <br> o Spark pode evitar ler qualquer dado de pastas que **não** estejam na subpasta `1990`.
-- MAGIC
-- MAGIC No próximo conjunto de exercícios, demonstraremos exemplos de como particionar dados, como visualizar as partições da tabela e como usar widgets para ajustar os parâmetros da sua consulta.
-- MAGIC
-- MAGIC Por fim, demonstraremos como usar funções de janela, que usam um tipo diferente de particionamento para calcular valores em uma subseção de uma tabela.
-- MAGIC
-- MAGIC Execute a célula abaixo para configurar seu ambiente de sala de aula.

-- COMMAND ----------

-- MAGIC %run ../Includes/6-3-Setup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC No exemplo abaixo, criamos e usamos uma tabela chamada `AvgTemps`. Você pode reconhecer esta consulta de um notebook anterior. Esta tabela inclui leituras de temperatura tiradas ao longo de dias inteiros, bem como o valor calculado `avg_daily_temp_c`.
-- MAGIC
-- MAGIC Observe que esta tabela foi `PARTITIONED BY` pela coluna `device_type`. O resultado desse tipo de particionamento é que a tabela é armazenada em arquivos separados. Isso pode acelerar consultas subsequentes que podem filtrar determinadas partições. Essas **não** são as mesmas partições às quais nos referimos ao discutir a arquitetura básica do Spark.
-- MAGIC
-- MAGIC <img alt="Cuidado" title="Cuidado" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> A palavra **partição** é um pouco sobrecarregada em big data e computação distribuída. Ou seja, precisamos prestar atenção ao contexto para entender que tipo de partição está sendo discutido. Em particular, ao se referir à arquitetura interna do Spark, as partições se referem às unidades de dados que são fisicamente distribuídas em um cluster.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS AvgTemps2
PARTITIONED BY (device_type)
AS
  SELECT
    dc_id,
    date,
    temps,
    REDUCE(temps, 0, (t, acc) -> t + acc, acc ->(acc div size(temps))) as avg_daily_temp_c,
    device_type
  FROM DeviceData3;
  
SELECT * FROM AvgTemps2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use o comando `SHOW PARTITIONS` para ver como seus dados estão particionados. Neste caso, você pode verificar que os dados foram particionados de acordo com o tipo de dispositivo.

-- COMMAND ----------

SHOW PARTITIONS AvgTemps2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Crie um widget
-- MAGIC
-- MAGIC Os widgets de entrada permitem adicionar parâmetros aos seus notebooks e painéis. Você pode criar e remover widgets, bem como recuperar valores deles em uma consulta SQL. Uma vez criados, eles aparecem no topo do seu notebook. Você pode projetá-los para receber a entrada do usuário como:
-- MAGIC * dropdown: fornece uma lista de opções para o usuário selecionar
-- MAGIC * text: o usuário insere a entrada como texto
-- MAGIC * combobox: Combinação de texto e dropdown. O usuário seleciona um valor em uma lista fornecida ou insere um na caixa de texto.
-- MAGIC * multiselect: Selecionar um ou mais valores em uma lista de valores fornecidos
-- MAGIC
-- MAGIC Os widgets são melhores para:
-- MAGIC * Criar um notebook ou painel que é reexecutado com diferentes parâmetros
-- MAGIC * Explorar rapidamente os resultados de uma única consulta com diferentes parâmetros
-- MAGIC
-- MAGIC Saiba mais sobre widgets [aqui](https://docs.databricks.com/notebooks/widgets.html).
-- MAGIC
-- MAGIC Já criamos uma tabela particionada, portanto, temos uma coluna designada destinada a facilitar a leitura de dados com filtros. Neste exemplo, usaremos um widget para permitir que qualquer pessoa que visualize o notebook (ou painel correspondente) possa filtrar por `device_type` em nossa tabela.
-- MAGIC
-- MAGIC Neste exemplo, usamos um `DROPDOWN` para que o usuário possa selecionar entre todas as opções disponíveis. Nomeamos o widget `selectedDeviceType` e especificamos as `CHOICES` obtendo uma lista distinta de todos os valores na coluna `deviceType`.

-- COMMAND ----------

CREATE WIDGET DROPDOWN selectedDeviceType DEFAULT "sensor-inest" CHOICES
SELECT
  DISTINCT device_type
FROM
  DeviceData3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Usar o valor selecionado na query
-- MAGIC
-- MAGIC Usamos uma função definida pelo usuário, `getArgument()`, para recuperar o valor atual selecionado no widget. Essa funcionalidade está disponível no Databricks Runtime, mas não no Spark de código aberto.
-- MAGIC
-- MAGIC No exemplo abaixo, recuperamos o valor selecionado na cláusula `WHERE` no final da consulta. Execute o exemplo. Em seguida, altere o valor no widget. Observe que o comando abaixo é executado automaticamente. Por padrão, células que acessam a entrada de um determinado widget serão executadas automaticamente quando o valor de entrada for alterado. Você pode alterar os valores padrão usando o ícone ![settings](https://docs.databricks.com/_images/gear.png) no lado direito do painel de widgets na parte superior do notebook.
-- MAGIC

-- COMMAND ----------

SELECT 
  device_type,
  ROUND(AVG(avg_daily_temp_c),4) AS avgTemp,
  ROUND(STD(avg_daily_temp_c), 2) AS stdTemp
FROM AvgTemps2
WHERE device_type = getArgument("selectedDeviceType")
GROUP BY device_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Remover widget
-- MAGIC
-- MAGIC Você pode remover um widget com o seguinte comando, simplesmente referenciando-o pelo nome.

-- COMMAND ----------

REMOVE WIDGET selectedDeviceType

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Funções de janela
-- MAGIC
-- MAGIC Funções de janela calculam uma variável de retorno para cada linha de entrada de uma tabela com base em um grupo de linhas selecionadas pelo usuário, chamado de "frame". Para usar funções de janela, precisamos indicar que uma função está sendo usada como uma função de janela, adicionando uma cláusula `OVER` após uma função suportada em SQL. Dentro da cláusula `OVER`, você especifica quais linhas estão incluídas no frame associado a esta janela.
-- MAGIC
-- MAGIC No exemplo, a função que usaremos é `AVG`. Definimos a Especificação de Janela associada a esta função com `OVER(PARTITION BY ...)`. Os resultados mostram que a temperatura média mensal é calculada para um centro de dados em uma determinada data. A cláusula `WHERE` no final desta consulta está incluída para mostrar um mês inteiro de dados de um único data center.

-- COMMAND ----------

SELECT 
  dc_id,
  month(date),
  avg_daily_temp_c,
  AVG(avg_daily_temp_c)
  OVER (PARTITION BY month(date), dc_id) AS avg_monthly_temp_c
FROM AvgTemps2
WHERE month(date)in ("8", "9") AND dc_id = "dc-102";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTEs com funções de janela
-- MAGIC
-- MAGIC Aqui, integramos a mesma função de janela e usamos uma CTE (Expressão de Tabela Comum) para manipular ainda mais os valores calculados na expressão de tabela comum.

-- COMMAND ----------

WITH DiffChart AS
(
SELECT 
  dc_id,
  date,
  avg_daily_temp_c,
  AVG(avg_daily_temp_c)
  OVER (PARTITION BY month(date), dc_id) AS avg_monthly_temp_c  
FROM AvgTemps2
)
SELECT 
  dc_id,
  date,
  avg_daily_temp_c,
  avg_monthly_temp_c,
  avg_daily_temp_c - ROUND(avg_monthly_temp_c) AS degree_diff
FROM DiffChart
ORDER BY date;

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Cleanup
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
