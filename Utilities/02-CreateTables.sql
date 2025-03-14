-- Databricks notebook source
-- MAGIC
-- MAGIC %run ./Common

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # SETUP
-- MAGIC
-- MAGIC # Coding Challenge Setup
-- MAGIC def setup():
-- MAGIC   (spark.createDataFrame([
-- MAGIC     ("0244", 75, None, 36),
-- MAGIC     ("1432", 23, 3, 24),
-- MAGIC     ("3242", 14, 5, 5),
-- MAGIC     ("6693", 17, 14, 3),  
-- MAGIC     ("6693", 17, 14, 3),
-- MAGIC     ("6914", 5, None, 11),
-- MAGIC     ("7012", 129, 10, 11),  
-- MAGIC     ("7064", 34, None, 24),
-- MAGIC     ("7064", 34, None, 24),
-- MAGIC     ("9382", 15, 7, 8),
-- MAGIC     ("9958", 64, 2, 2)
-- MAGIC   ], ["itemId", "amount", "aisle", "price"]).createOrReplaceTempView("products"))
-- MAGIC
-- MAGIC setup()
-- MAGIC
-- MAGIC displayHTML("""
-- MAGIC Declared the following table:
-- MAGIC   <li><span style="color:green; font-weight:bold">products</span></li>
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Coding Challenge Setup
-- MAGIC from pyspark.sql.functions import to_timestamp
-- MAGIC
-- MAGIC def setup():
-- MAGIC   from pyspark.sql.types import StringType
-- MAGIC   from pyspark.sql.functions import abs, col
-- MAGIC   
-- MAGIC   return (createDummyData("mqr_2", name="name", yesNo="active", UTCTime="lastFinish", id="id", probability="winOdds")
-- MAGIC                  .withColumn("lastFinish", to_timestamp("lastFinish"))
-- MAGIC                  .drop("Percent", "index", "Amount")
-- MAGIC                  .createOrReplaceTempView("raceResults"))
-- MAGIC           
-- MAGIC
-- MAGIC setup()
-- MAGIC
-- MAGIC displayHTML("""
-- MAGIC Declared the following table:
-- MAGIC   <li><span style="color:green; font-weight:bold">raceResults</span></li>
-- MAGIC """)
-- MAGIC
