# Databricks notebook source
# MAGIC %md
# MAGIC ## Get access to all the schemas in the SchemaCreation file

# COMMAND ----------

# MAGIC %run /Repos/steven.das@qualyfi.co.uk/steven-repo/final_notebooks/SchemaCreation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the empty dataframes with the schema and write them to Bronze

# COMMAND ----------

from pyspark.sql.types import *

empty_rdd = sc.emptyRDD()

bronze_trips_df = spark.createDataFrame(empty_rdd, trips_bronze_schema)
bronze_trips_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/trips")

bronze_stations_df = spark.createDataFrame(empty_rdd, stations_bronze_schema)
bronze_stations_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/stations")

bronze_riders_df = spark.createDataFrame(empty_rdd, riders_bronze_schema)
bronze_riders_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/riders")

bronze_payments_df = spark.createDataFrame(empty_rdd, payments_bronze_schema)
bronze_payments_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the empty dataframes with the schema and write them to Silver

# COMMAND ----------

from pyspark.sql.types import *

empty_rdd = sc.emptyRDD()

silver_trips_df = spark.createDataFrame(empty_rdd, trips_silver_schema)
silver_trips_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/trips")

silver_stations_df = spark.createDataFrame(empty_rdd, stations_silver_schema)
silver_stations_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/stations")

silver_riders_df = spark.createDataFrame(empty_rdd, riders_silver_schema)
silver_riders_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/riders")

silver_payments_df = spark.createDataFrame(empty_rdd, payments_silver_schema)
silver_payments_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the empty dataframes with schema and write them to Gold

# COMMAND ----------

from pyspark.sql.types import *

empty_rdd = sc.emptyRDD()

gold_bike_df = spark.createDataFrame(empty_rdd, bike_gold_schema)
gold_bike_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_bikes")

gold_date_df = spark.createDataFrame(empty_rdd, date_gold_schema)
gold_date_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_dates")

gold_time_df = spark.createDataFrame(empty_rdd, time_gold_schema)
gold_time_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_times")

gold_payments_df = spark.createDataFrame(empty_rdd, payments_gold_schema)
gold_payments_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/fact_payments")

gold_trips_df = spark.createDataFrame(empty_rdd, trips_gold_schema)
gold_trips_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/fact_trips")

gold_stations_df = spark.createDataFrame(empty_rdd, stations_gold_schema)
gold_stations_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_stations")

gold_riders_df = spark.createDataFrame(empty_rdd, riders_gold_schema)
gold_riders_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_riders")
