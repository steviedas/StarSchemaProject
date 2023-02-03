# Databricks notebook source
# MAGIC %md
# MAGIC ##Delete all the databases in 'tmp/Steven/Bronze'

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven/Bronze", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delete all the databases in 'tmp/Steven/Silver'

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven/Silver", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delete all the databases in 'tmp/Steven/Gold'

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven/Gold", True)
