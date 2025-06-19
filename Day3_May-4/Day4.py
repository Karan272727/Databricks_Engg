# Databricks notebook source
# MAGIC %md
# MAGIC Databrick Heirarchy
# MAGIC
# MAGIC - Catalog
# MAGIC - schema (Database)
# MAGIC - Tables, views

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC create table demo.emp(id int, name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into demo.emp values(1, 'Karan')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.demo.emp

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended hive_metastore.demo.emp

# COMMAND ----------

# MAGIC %md
# MAGIC - dbutils.help()
# MAGIC - DBFS: databricks File _System_

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/Testfolder")

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/raw")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/Testfolder/TestFile.txt","dbfs:/FileStore/raw")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/raw")
