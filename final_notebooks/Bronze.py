# Databricks notebook source
# MAGIC %md
# MAGIC ##Remove the files from /tmp/Steven/Github/

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven/Github/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get the files from Github repository

# COMMAND ----------

!wget "https://github.com/steviedas/StarSchemaProject/raw/main/zips/payments.zip" -P "/dbfs/tmp/Steven/Github/"
!wget "https://github.com/steviedas/StarSchemaProject/raw/main/zips/riders.zip" -P "/dbfs/tmp/Steven/Github/"
!wget "https://github.com/steviedas/StarSchemaProject/raw/main/zips/stations.zip" -P "/dbfs/tmp/Steven/Github/"
!wget "https://github.com/steviedas/StarSchemaProject/raw/main/zips/trips.zip" -P "/dbfs/tmp/Steven/Github/"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delete all the files in Steven/Landing

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven/Landing/", True)

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/tmp/Steven/landing/*

# COMMAND ----------

# MAGIC %md
# MAGIC ##Unzip files to Steven/landing

# COMMAND ----------

# Make the landing directory
dbutils.fs.mkdirs("/tmp/Steven/Landing")

# COMMAND ----------

!pip install unzip

# COMMAND ----------

import subprocess
import glob
zip_files = glob.glob("/dbfs/tmp/Steven/Github/*.zip")
for zip_file in zip_files:
    extract_to_dir = "/dbfs/tmp/Steven/Landing"
    subprocess.call(["unzip", "-d", extract_to_dir, zip_file])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Bronze Schema

# COMMAND ----------

# MAGIC %run /Repos/steven.das@qualyfi.co.uk/StarSchemaProject/final_notebooks/SchemaCreation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write all the CSV's to Bronze as Delta Format

# COMMAND ----------

bronze_trips_df = spark.read.format('csv').load("/tmp/Steven/Landing/trips.csv", schema = trips_bronze_schema)
bronze_trips_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/trips")

bronze_stations_df = spark.read.format('csv').load("/tmp/Steven/Landing/stations.csv", schema = stations_bronze_schema)
bronze_stations_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/stations")

bronze_riders_df = spark.read.format('csv').load("/tmp/Steven/Landing/riders.csv", schema = riders_bronze_schema)
bronze_riders_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/riders")

bronze_payments_df = spark.read.format('csv').load("/tmp/Steven/Landing/payments.csv", schema = payments_bronze_schema)
bronze_payments_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/payments")
