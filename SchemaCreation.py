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



# COMMAND ----------

# MAGIC %md
# MAGIC ##Write all the CSV's to Bronze as Delta Format

# COMMAND ----------

spark.read.format(csv_file_type).load("/tmp/Steven/landing/trips.csv", schema = trips_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/trips")

spark.read.format(csv_file_type).load("/tmp/Steven/landing/stations.csv", schema = stations_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/stations")

spark.read.format(csv_file_type).load("/tmp/Steven/landing/riders.csv", schema = riders_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/riders")

spark.read.format(csv_file_type).load("/tmp/Steven/landing/payments.csv", schema = payments_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delete all the files in 'tmp/Steven/Bronze'

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven/landing/Bronze/", True)

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/tmp/Steven/Bronze/*

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Schema

# COMMAND ----------

from pyspark.sql.types import *
trips_silver_schema = StructType ([
    StructField("trip_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
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

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import unix_timestamp, col

# Trips
bronze_trips_df = spark.read.format("delta").load("/tmp/Steven/Bronze/trips/")
bronze_trips_df_1 = bronze_trips_df.withColumn("started_at", unix_timestamp(col("started_at"), 'dd/MM/yyyy HH:mm').cast("timestamp")).withColumn("ended_at", unix_timestamp(col("ended_at"), 'dd/MM/yyyy HH:mm').cast("timestamp"))

# Stations
bronze_stations_df = spark.read.format("delta").load("/tmp/Steven/Bronze/stations/")

# Riders
bronze_riders_df = spark.read.format("delta").load("/tmp/Steven/Bronze/riders/")

# Payments
bronze_payments_df = spark.read.format("delta").load("/tmp/Steven/Bronze/payments/")


# Iterating over all columns to change data type accroding to silver schema
silver_trips_df = bronze_trips_df_1.select(*(bronze_trips_df_1[c].cast(trips_silver_schema[i].dataType) for i, c in enumerate(bronze_trips_df_1.columns)))
silver_stations_df = bronze_stations_df.select(*(bronze_stations_df[c].cast(stations_silver_schema[i].dataType) for i, c in enumerate(bronze_stations_df.columns)))
silver_riders_df = bronze_riders_df.select(*(bronze_riders_df[c].cast(riders_silver_schema[i].dataType) for i, c in enumerate(bronze_riders_df.columns)))
silver_payments_df = bronze_payments_df.select(*(bronze_payments_df[c].cast(payments_silver_schema[i].dataType) for i, c in enumerate(bronze_payments_df.columns)))

# COMMAND ----------

display(silver_trips_df)
print(bronze_trips_df_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write all the Bronze dataframes to Silver in Delta format

# COMMAND ----------

silver_trips_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/trips")

silver_stations_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/stations")

silver_riders_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/riders")

silver_payments_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/payments")
