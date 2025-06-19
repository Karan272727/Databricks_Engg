# Databricks notebook source
# MAGIC %sql
# MAGIC create table db.tblame(id int,)

# COMMAND ----------

# MAGIC %md
# MAGIC creating spark session

# COMMAND ----------

spark

# COMMAND ----------

user_data=[(1,'Karan'),(2,'Ranbhor')]
user_schema="id int,name string" 

# COMMAND ----------

df =spark.createDataFrame(data=user_data, schema=user_schema)
df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Lazy Evaluation
# MAGIC - Transformation and Action
# MAGIC - Transformation
# MAGIC >   (_select,filter,sort,group,agg,join,alias,withColumnsRenamed,withColumnsRenamed)
# MAGIC - Action
# MAGIC >   .show(),.display())
# MAGIC - function
# MAGIC > col_, 
# MAGIC > current_date

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importing Functions

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aliasing

# COMMAND ----------

df.select("id".alias("emp_id"))

# COMMAND ----------

df.select(col("id").alias("emp_id"))

# COMMAND ----------

df1=df.select(col("id").alias("emp_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ColumnRenamed

# COMMAND ----------

df.withColumnRenamed("id","emp_id",).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## withcolumnsRenamed 
# MAGIC - Renaming Mulitple columns

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name"}).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importing Date Ingestion column

# COMMAND ----------

df2=df.withColumn("ingestion_date",current_date())

# COMMAND ----------

df2.display()
