-- Databricks notebook source
-- Managed table: this table are managed by datbrick and the default location: dbfs:/user/hive/warehouse/demo) where the files are stored.

-- when you DROP a managed table it drops everthing (table + metadata(parquet + delta logs))

create table tablename  (col1 datatype, col2 datatype)

df=spark.read.format("path")
df.write.saveAsTable("tblname")


-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Unmanaged tables: this tables are managed by the data engineers, and there location path may vary.
-- MAGIC - This tables are defined by keyword "location"
-- MAGIC - Sx: create table tblname(c1 datatype, c2 datatype) location 'dbfs:/eclerx/metadata'

-- COMMAND ----------

create table demo_vendors (id int, name string) location '/FileStore/eclerx_metadata/vendors'

-- COMMAND ----------

desc extended demo_vendors

-- COMMAND ----------

insert into demo_vendors values(1,'a');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Unmanaged tables 
-- MAGIC - If unmanaged table is dropped, it removes the records from it but keeps the log files

-- COMMAND ----------

drop table demo_vendors
