-- Databricks notebook source
-- MAGIC %md
-- MAGIC - delta helps to evolve schema
-- MAGIC - 

-- COMMAND ----------

create table emp(id int, name string, city string)

-- COMMAND ----------

desc  extended emp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Internals of the Delta lake
-- MAGIC - Parquet (Data Files): It holds the actual data, 
-- MAGIC - It will have delta logs(metadata)
-- MAGIC - > crc(cyclic redundant check)
-- MAGIC - > crc(json)
-- MAGIC - > crc(checkpoint)

-- COMMAND ----------

-- MAGIC %fs head
-- MAGIC dbfs:/user/hive/warehouse/emp/_delta_log/00000000000000000001.json

-- COMMAND ----------

insert into emp values (1, 'Karan','Mumbai');
insert into emp values (4, 'Neha','UAE');
insert into emp values (5, 'Stevan','USA');

-- COMMAND ----------

select * from emp
order by id

-- COMMAND ----------

-- DBTITLE 1,It shows how many operation happn
desc history emp

-- COMMAND ----------

-- DBTITLE 1,It shows number of files present in "numFiles" Field
desc detail emp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Timetravel_Functions

-- COMMAND ----------

-- DBTITLE 1,CTAS(create table as select)
select * from emp version as of 4

-- COMMAND ----------

select * from emp timestamp as of '2025-06-09T07:07:24.000+00:00'
