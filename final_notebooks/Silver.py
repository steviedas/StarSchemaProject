# Databricks notebook source
# MAGIC %md
# MAGIC ##Silver Schema

# COMMAND ----------

# MAGIC %run /Repos/steven.das@qualyfi.co.uk/StarSchemaProject/final_notebooks/SchemaCreation

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

# MAGIC %md
# MAGIC ##Write all the Bronze dataframes to Silver in Delta format

# COMMAND ----------

silver_trips_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/trips")

silver_stations_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/stations")

silver_riders_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/riders")

silver_payments_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/payments")
