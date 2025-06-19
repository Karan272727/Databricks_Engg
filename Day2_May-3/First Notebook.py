# Databricks notebook source
print("Hi Karan")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Karan_database;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE Karan_database;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS employees (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   salary DOUBLE,
# MAGIC   department STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES
# MAGIC   (1, 'Alice', 90000, 'HR'),
# MAGIC   (2, 'Bob', 85000, 'Engineering'),
# MAGIC   (3, 'Charlie', 95000, 'Sales'),
# MAGIC   (4, 'David', 70000, 'Marketing'),
# MAGIC   (5, 'Eva', 72000, 'HR'),
# MAGIC   (6, 'Frank', 88000, 'Engineering'),
# MAGIC   (7, 'Grace', 67000, 'Sales'),
# MAGIC   (8, 'Hank', 94000, 'Marketing'),
# MAGIC   (9, 'Ivy', 81000, 'HR'),
# MAGIC   (10, 'Jack', 86000, 'Engineering'),
# MAGIC   (11, 'Kara', 73000, 'Sales'),
# MAGIC   (12, 'Liam', 92000, 'Marketing'),
# MAGIC   (13, 'Mia', 78000, 'HR'),
# MAGIC   (14, 'Nate', 87000, 'Engineering'),
# MAGIC   (15, 'Olivia', 69000, 'Sales'),
# MAGIC   (16, 'Paul', 93000, 'Marketing'),
# MAGIC   (17, 'Quinn', 75000, 'HR'),
# MAGIC   (18, 'Rachel', 89000, 'Engineering'),
# MAGIC   (19, 'Steve', 68000, 'Sales'),
# MAGIC   (20, 'Tina', 91000, 'Marketing');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees;
