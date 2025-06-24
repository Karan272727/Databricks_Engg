-- Databricks notebook source
create table demo(id int, name string)

-- COMMAND ----------

desc extended demo

-- COMMAND ----------

-- DBTITLE 1,To check how many .json files have created in "demo" table
desc history demo

-- COMMAND ----------

-- DBTITLE 1,To check how many .parquet files have created in "demo" table
desc detail demo

-- COMMAND ----------

insert into demo values (1,'a');
insert into demo values (2,'b');
insert into demo values (3,'c');
insert into demo values (4,'d');
insert into demo values (5,'e');
insert into demo values (6,'f');
insert into demo values (7,'g');

-- COMMAND ----------

delete from demo
where id > 4

-- COMMAND ----------

select * from demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Z-ordering should be used with optimize command only, otherwise it doesn't work**

-- COMMAND ----------

optimize demo
zorder by (id)

-- Multiple coulmn name can be used in zordering command
-- Ex: zorder by (id, name) 
-- There are limitation in zordering, to overcome we use liquid clustering.

-- COMMAND ----------

desc history demo


-- COMMAND ----------

desc detail demo

-- COMMAND ----------

delete from demo

-- COMMAND ----------

-- DBTITLE 1,To get the deleted table back use below command
restore table demo to version as of 9

-- COMMAND ----------

-- It helps to remove stale files and no time travel is required.
-- By default it has retention period of seven days or 168 hours.
vacuum demo

-- COMMAND ----------

vacuum demo retain 0 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false


-- COMMAND ----------

vacuum demo retain 0 hours
