# Databricks notebook source
# MAGIC %md
# MAGIC ##Remove the files from /tmp/Steven/Github/

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven/Github/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get the files from Github repository

# COMMAND ----------

!wget "https://github.com/steviedas/steven-repo/raw/main/zips/payments.zip" -P "/dbfs/tmp/Steven/Github/"
!wget "https://github.com/steviedas/steven-repo/raw/main/zips/riders.zip" -P "/dbfs/tmp/Steven/Github/"
!wget "https://github.com/steviedas/steven-repo/raw/main/zips/stations.zip" -P "/dbfs/tmp/Steven/Github/"
!wget "https://github.com/steviedas/steven-repo/raw/main/zips/trips.zip" -P "/dbfs/tmp/Steven/Github/"

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
# MAGIC ##Delete all the files in 'tmp/Steven/Bronze'

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven/Bronze", True)

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/tmp/Steven/Bronze/*

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write all the CSV's to Bronze as Delta Format

# COMMAND ----------

spark.read.format('csv').load("/tmp/Steven/Landing/trips.csv", schema = trips_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/trips")

spark.read.format('csv').load("/tmp/Steven/Landing/stations.csv", schema = stations_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/stations")

spark.read.format('csv').load("/tmp/Steven/Landing/riders.csv", schema = riders_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/riders")

spark.read.format('csv').load("/tmp/Steven/Landing/payments.csv", schema = payments_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/payments")

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

# MAGIC %md
# MAGIC ##Write all the Bronze dataframes to Silver in Delta format

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven/Silver", True)

# COMMAND ----------

silver_trips_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/trips")

silver_stations_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/stations")

silver_riders_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/riders")

silver_payments_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Silver/payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read all the data from Silver into dataframes

# COMMAND ----------

silver_trips_delta_df = spark.read.format("delta").load("/tmp/Steven/Silver/trips/")
silver_stations_delta_df = spark.read.format("delta").load("/tmp/Steven/Silver/stations/")
silver_riders_delta_df = spark.read.format("delta").load("/tmp/Steven/Silver/riders/")
silver_payments_delta_df = spark.read.format("delta").load("/tmp/Steven/Silver/payments/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create the Bike Table

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define the window specification
w1 = Window.orderBy("rideable_type")

bike_df = silver_trips_delta_df.select("rideable_type").distinct().withColumn("bike_id", F.row_number().over(w1)).select("bike_id","rideable_type")

display(bike_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create the Date Table

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, split
from pyspark.sql.window import Window

#Create two dataframes, one containing only the start-date and one for the end_date
start_date_df = silver_trips_delta_df.select("started_at").withColumnRenamed("started_at","date")
end_date_df = silver_trips_delta_df.select("ended_at").withColumnRenamed("ended_at","date")

#Join the previous two dataframes together within one date column
date1_df = start_date_df.union(end_date_df).distinct()    

# Cast the date column as string
date1_df = date1_df.withColumn("date", col("date").cast("string"))

# Create new columns date_ext and time_ext. This table has date(yyyy-mm-dd hh:mm:ss), date_ext and time_ext columns
date_df1 = date1_df.withColumn('date_ext', split(col('date'), ' ')[0].substr(1, 11)).withColumn('time_ext', split(col('date'), ' ')[1].substr(0,5))

#Create a new dataframe from the payments table that has only one date column
payment_date_df1 = silver_payments_delta_df.select("date")

date_df2 = date_df1.select("date_ext").alias("date")
date_final_df = payment_date_df1.union(date_df2).distinct()

w2 = Window.orderBy("date")
date_df = date_final_df.withColumn("date_id", F.row_number().over(w2)).select("date_id","date")
date_df = date_df.withColumn("date", col("date").cast("date"))
display(date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create the Time Table

# COMMAND ----------

w = Window.orderBy('time_ext')
time_df = date_df1.select('time_ext').distinct().withColumn('time_id', F.row_number().over(w)).select('time_id', 'time_ext')
time_df = time_df.withColumnRenamed("time_ext", "time").withColumn("time", col("time"))
display(time_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create the Payments table

# COMMAND ----------

payments_df = silver_payments_delta_df.join(date_df, on=["date"], how="left").drop("date").select("payment_id", "rider_id", "date_id", "amount")
display(payments_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create the Trip Fact table

# COMMAND ----------

display(silver_trips_delta_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calculate the trip duration

# COMMAND ----------

from pyspark.sql import functions as F
trips1_df = silver_trips_delta_df.withColumn("trip_duration", ((F.unix_timestamp(silver_trips_delta_df["ended_at"]) - F.unix_timestamp(silver_trips_delta_df["started_at"]))/60))

display(trips1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add the bike_id to the Trip Fact table

# COMMAND ----------

trips2_df = trips1_df.join(bike_df, on=["rideable_type"], how="left").drop("rideable_type")
display(trips2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Changing all the startdates and enddates to the eqivalent startdate_id and enddate_id

# COMMAND ----------

trips3_df = trips2_df.withColumn("started_at", col("started_at").cast("string")).withColumn("ended_at", col("ended_at").cast("string"))
trips4_df = trips3_df.withColumn('started_at_date', split(col('started_at'), ' ')[0].substr(1, 11)) \
    .withColumn('started_at_time', split(col('started_at'), ' ')[1].substr(0,5)) \
    .withColumn('ended_at_date', split(col('ended_at'), ' ')[0].substr(1, 11)) \
    .withColumn('ended_at_time', split(col('ended_at'), ' ')[1].substr(0,5)) \
    .drop("started_at","ended_at")
display(trips4_df)

# COMMAND ----------

trips5_df = trips4_df \
    .join(date_df, trips4_df.started_at_date == date_df.date, 'left') \
    .withColumnRenamed("date", "date_at_start") \
    .withColumnRenamed("date_id", "started_at_date_id") \
    .join(date_df, trips4_df.ended_at_date == date_df.date, 'left') \
    .drop("date") \
    .withColumnRenamed("date_id", "ended_at_date_id") \
    .drop("started_at_date", "ended_at_date") \
    .join(time_df, trips4_df.started_at_time == time_df.time, 'left') \
    .drop("time") \
    .withColumnRenamed("time_id", "started_at_time_id") \
    .join(time_df, trips4_df.ended_at_time == time_df.time, 'left') \
    .drop("time") \
    .withColumnRenamed("time_id", "ended_at_time_id") \
    .drop("started_at_time", "ended_at_time")
display(trips5_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create a subset of the rider table

# COMMAND ----------

subset_riders_df = silver_riders_delta_df.select("rider_id", "birthday")
display(subset_riders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Join the birthday to trips facts table

# COMMAND ----------

from pyspark.sql.functions import datediff
trips6_df = trips5_df.join(subset_riders_df, on=["rider_id"], how="left")
trips_df = trips6_df.withColumn("rider_age", (datediff(col("date_at_start"),col("birthday"))/365).cast("int")) \
    .drop("date_at_start", "birthday") \
    .select("trip_id", "rider_id", "bike_id", "start_station_id", "end_station_id", "started_at_date_id", "ended_at_date_id", "started_at_time_id", "ended_at_time_id", "rider_age", "trip_duration")

trips_df = trips_df.withColumn('trip_duration', col('trip_duration').cast('int'))

display(trips_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write all the dataframes to Gold

# COMMAND ----------

dbutils.fs.rm("/tmp/Steven/Gold/", True)

# COMMAND ----------

bike_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_bikes")

date_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_dates")

time_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_times")

payments_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/fact_payments")

trips_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/fact_trips")

silver_stations_delta_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_stations")

silver_riders_delta_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_riders")
