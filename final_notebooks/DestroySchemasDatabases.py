# Databricks notebook source
# MAGIC %md
# MAGIC ##Delete the Steven directory

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven", True)
