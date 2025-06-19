# Databricks notebook source
# MAGIC %md
# MAGIC - Database
# MAGIC - Datawarehouse: Structured
# MAGIC - Data lake: semi, unstructured:blob and ADLS, S3, GCS
# MAGIC
# MAGIC - lakehouse = datawarehouse + data lake
# MAGIC - layer on top of the existing data lake(adls, s3, gcs)
# MAGIC
# MAGIC - Advance table format:
# MAGIC > - delta(databricks), iceberg(snowflake), hudi--> helps to build lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC - Spark, Apache is open source.
# MAGIC - Databrick is a Data plus AI company.
# MAGIC - Delta lake: is a storage framework which is used in mainly in databricks and is a linux foundation open source.

# COMMAND ----------

# MAGIC %md
# MAGIC - In conclusion:
# MAGIC - To make a datalake in **databrick** by default the storage _framework_ would be in **delta** format
# MAGIC - To make a datalake in **snowflake** by default the storage _framework_ would be in **iceberg** format
# MAGIC - delta.io website link

# COMMAND ----------

# MAGIC %md
# MAGIC - **Parquet** Format: stores data in **columnar** format.
# MAGIC > - csv, json
# MAGIC > - It uses compression techniques.
# MAGIC > - Compresses upto 97%
# MAGIC - Cons of parquet files:
# MAGIC > - Doesn't support ACID property
# MAGIC > - It doesn't help to build lakehouse
# MAGIC - To overcome the problem Databricks made an extension called **Delta lake** nothing but parquet format which helps to achieve **ACID** support.
# MAGIC - Same thing applies to iceberg format snowflake have some extension which act like a parquet.
# MAGIC
