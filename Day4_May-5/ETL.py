# Databricks notebook source
# MAGIC %md
# MAGIC Reading path

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/Testfolder/
# MAGIC

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/Testfolder/circuits.csv")

# COMMAND ----------

input_path="dbfs:/FileStore/Testfolder/circuits.csv"
# df=spark.read.csv("dbfs:/FileStore/Testfolder/circuits.csv")
# df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/Testfolder/circuits.csv")
df=spark.read.csv(input_path,header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Task: 
# MAGIC -     1. rename circuitId-- circuit_id, circuitRef -- circuit_ref
# MAGIC -     2. add new column with current_date
# MAGIC -     3. drop url col

# COMMAND ----------

df.withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref"}).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df1 = df.withColumn("current_date",current_date())

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1 = df.withColumn("current_date",current_date())

# COMMAND ----------

df1.display()

# COMMAND ----------

df2 = df1.drop('url')

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Answer
# MAGIC - df.withColumnsRenamed({"circuitId":"circuit_id","circuitref":"circuit_ref"}).withColumn("current_Date",current_date()).drop("url").display()

# COMMAND ----------

# DBTITLE 1,Loading/ writing

