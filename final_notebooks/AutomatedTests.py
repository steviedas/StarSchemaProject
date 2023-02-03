# Databricks notebook source
dim_bikes_df = spark.read.format("delta").load("/tmp/Steven/Gold/dim_bikes/")
dim_dates_df = spark.read.format("delta").load("/tmp/Steven/Gold/dim_dates/")
dim_times_df = spark.read.format("delta").load("/tmp/Steven/Gold/dim_times/")
dim_stations_df = spark.read.format("delta").load("/tmp/Steven/Gold/dim_stations/")
dim_riders_df = spark.read.format("delta").load("/tmp/Steven/Gold/dim_riders/")
fact_payments_df = spark.read.format("delta").load("/tmp/Steven/Gold/fact_payments/")
fact_trips_df = spark.read.format("delta").load("/tmp/Steven/Gold/fact_trips/")

# COMMAND ----------

bike_df.count()

# COMMAND ----------

display(bike_df)

# COMMAND ----------

assert bike_df.count() == 200, "This dataframe has an incorrect number of rows"
#assert bike_df.rideable_type[1] == "docked_bike"
