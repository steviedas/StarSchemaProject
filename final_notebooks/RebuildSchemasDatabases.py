# Databricks notebook source
# MAGIC %md
# MAGIC ## Get access to all the schemas in the SchemaCreation file

# COMMAND ----------

# MAGIC %run /Repos/steven.das@qualyfi.co.uk/steven-repo/final_notebooks/SchemaCreation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Schema

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
# MAGIC ## Silver Schema

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
    StructField("account_start", DateType(), True),
    StructField("account_end", DateType(), True),
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
# MAGIC ## Gold Schema

# COMMAND ----------

from pyspark.sql.types import *

bike_gold_schema = StructType ([
    StructField("bike_id", IntegerType(), True),
    StructField("rideable_type", StringType(), True),
])

date_gold_schema = StructType ([
    StructField("date_id", IntegerType(), True),
    StructField("date", DateType(), True),
])

time_gold_schema = StructType ([
    StructField("time_id", IntegerType(), True),
    StructField("time", StringType(), True),
])

payments_gold_schema = StructType ([
    StructField("payment_id", IntegerType(), True),
    StructField("rider_id", IntegerType(), True),
    StructField("date_id", IntegerType(), True),
    StructField("amount", FloatType(), True),
])

trips_gold_schema = StructType ([
    StructField("trip_id", StringType(), True),
    StructField("rider_id", IntegerType(), True),
    StructField("bike_id", IntegerType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("started_at_date_id", IntegerType(), True),
    StructField("ended_at_date_id", IntegerType(), True),
    StructField("started_at_time_id", IntegerType(), True),
    StructField("ended_at_time_id", IntegerType(), True),
    StructField("rider_age", IntegerType(), True),
    StructField("trip_duration", IntegerType(), True),
])

stations_gold_schema = StructType ([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
])

riders_gold_schema = StructType ([
    StructField("rider_id", IntegerType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("address", StringType(), True),
    StructField("birthday", DateType(), True),
    StructField("account_start", DateType(), True),
    StructField("account_end", DateType(), True),
    StructField("is_member", BooleanType(), True),
])

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
