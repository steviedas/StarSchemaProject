# Databricks notebook source
# MAGIC %fs
# MAGIC ls /tmp/

# COMMAND ----------

!pip install unzip

# COMMAND ----------

# MAGIC %md
# MAGIC ##Unzip files to Steven/landing

# COMMAND ----------

import subprocess
import glob

zip_files = glob.glob("/dbfs/tmp/landing/*.zip")

for zip_file in zip_files:
    extract_to_dir = "/dbfs/tmp/Steven/landing"
    subprocess.call(["unzip", "-d", extract_to_dir, zip_file])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delete all the files in Steven/landing

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/tmp/Steven/landing/*

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven/landing/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Bronze Schema

# COMMAND ----------

from pyspark.sql.types import *
trips_bronze_schema = StructType ([
    StructField("trip_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("rider_id", StringType(), True),
])

stations_bronze_schema = StructType ([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
])

riders_bronze_schema = StructType ([
    StructField("rider_id", StringType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("address", StringType(), True),
    StructField("birthday", StringType(), True),
    StructField("account_start", StringType(), True),
    StructField("account_end", StringType(), True),
    StructField("is_member", StringType(), True),
])

payments_bronze_schema = StructType ([
    StructField("payment_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("rider_id", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write all the CSV's to Bronze as Delta Format

# COMMAND ----------

csv_file_type = "csv"
table_file_type = "delta"

spark.read.format(csv_file_type).load("/tmp/Steven/landing/trips.csv", schema = trips_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/trips")

spark.read.format(csv_file_type).load("/tmp/Steven/landing/stations.csv", schema = station_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/stations")

spark.read.format(csv_file_type).load("/tmp/Steven/landing/riders.csv", schema = riders_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/riders")

spark.read.format(csv_file_type).load("/tmp/Steven/landing/payments.csv", schema = payments_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Schema

# COMMAND ----------

from pyspark.sql.types import *
trips_silver_schema = StructType ([
    StructField("trip_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", DateType(), True),
    StructField("ended_at", DateType(), True),
    StructField("start_station_id", IntegerType(), True),
    StructField("end_station_id", IntegerType(), True),
    StructField("rider_id", IntegerType(), True),
])

stations_silver_schema = StructType ([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
])

riders_silver_schema = StructType ([
    StructField("rider_id", IntegerType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("address", StringType(), True),
    StructField("birthday", DateType(), True),
    StructField("account_start_date", DateType(), True),
    StructField("account_end_date", DateType(), True),
    StructField("is_member", BooleanType(), True),
])

payments_silver_schema = StructType ([
    StructField("payment_id", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("amount", FloatType(), True),
    StructField("rider_id", IntegerType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load the delta files and specify new schema

# COMMAND ----------

df = spark.read.format("delta").load("/tmp/Steven/Bronze/trips/")
display(df)

# COMMAND ----------

print(df)

# COMMAND ----------

from pyspark.sql.functions import date_format

#df_1 = df.select("trip_id", date_format(df["started_at"], "dd/MM/yyyy HH:mm")).show()
#df_1 = df.withColumn("started_at", df["started_at"].cast("date"))
#df_1 = df.select(from_unixtime(unix_timestamp(df.started_at, 'dd/MM/yyyy HH:mm')).alias('start'))
df_1 = df \
.withColumn("started_at",from_unixtime(unix_timestamp(col("started_at"), 'dd/MM/yyyy HH:mm'))) \
.withColumn("started_at", date_format(col("started_at"), "dd/MM/yyyy HH:mm"))

# COMMAND ----------

print(df_1)

# COMMAND ----------

display(df_1)
