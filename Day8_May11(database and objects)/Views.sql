-- Databricks notebook source
-- View: virtual table
-- 1. Standard view (persisted) means it get saved in the schema
-- 2. Temp view 

-- COMMAND ----------

-- MAGIC %fs ls 

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df=spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv",header=True,inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.saveAsTable("bike_day")

-- COMMAND ----------

select * from bike_day

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Presisted View(Standard View)

-- COMMAND ----------

create or replace view max_month as 

select mnth,round(max(temp),2) as max from bike_day group by mnth order by max desc

-- COMMAND ----------

select * from max_month

-- COMMAND ----------

desc extended max_month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Temp view

-- COMMAND ----------

select * from hive_metastore.default.bike_day

-- COMMAND ----------

create or replace temp view holiday_count_temp as 

select mnth, count(*)  as count from hive_metastore.default.bike_day where holiday =1 group by mnth

-- COMMAND ----------

select * from holiday_count_temp

-- COMMAND ----------

show views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UDF
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Syntax
create function function_name(para datatype)
returns datatype
return logic/expression

-- COMMAND ----------

create or replace function fullname(first_name string, last_name string)
returns string
return concat(first_name, " ", last_name)

-- COMMAND ----------

create or replace function agegroup(age int) 
returns string 
return 
      case
        when age > 60 then 'senior'
        when age > 18 then 'adult'
        when age < 18 then 'teenage'
        else 'kid'
      end

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### using the udf fucntions 

-- COMMAND ----------

create or replace table example (id int, forname string, surname string, age int);
insert into example values(1, 'Karan', 'Ranbhor', 25),(2, 'Pawan', 'Kumar', 23), (3, 'Vindo', 'Garej', 34),(4,'Neha','Krej', 12)
