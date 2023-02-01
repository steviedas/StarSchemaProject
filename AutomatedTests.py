# Databricks notebook source
# MAGIC %md
# MAGIC ##Business Outcomes
# MAGIC ###1. Analyse how much time is spent per ride
# MAGIC a. Based on date and time factors such as day of the week and time of day \
# MAGIC b. Based on which station is the starting and / or ending station \
# MAGIC c. Based on age of the rider at time of the ride \
# MAGIC d. Based on whether the rider isi a member or a casual rider
# MAGIC 
# MAGIC ###2. Analyse how much money is spent
# MAGIC a. Per month, quarter, year \
# MAGIC b. Per member, based on the age of the rider at account start
# MAGIC 
# MAGIC ###3. EXTRA CREDIT
# MAGIC a. Based on how many rides the rider averages per month \
# MAGIC b. Based on how many minutes the rider spends on a bike per month

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load all the gold data

# COMMAND ----------

dim_bikes_df = spark.read.format("delta").load("/tmp/Steven/Gold/dim_bikes/")
dim_dates_df = spark.read.format("delta").load("/tmp/Steven/Gold/dim_dates/")
dim_times_df = spark.read.format("delta").load("/tmp/Steven/Gold/dim_times/")
dim_stations_df = spark.read.format("delta").load("/tmp/Steven/Gold/dim_stations/")
dim_riders_df = spark.read.format("delta").load("/tmp/Steven/Gold/dim_riders/")
fact_payments_df = spark.read.format("delta").load("/tmp/Steven/Gold/fact_payments/")
fact_trips_df = spark.read.format("delta").load("/tmp/Steven/Gold/fact_trips/")

# COMMAND ----------

# MAGIC %md
# MAGIC ####1a. Analyse how much time is spent per ride - based on date and time factors - day of the week

# COMMAND ----------

from pyspark.sql.functions import date_format, avg

# Join the trip_start_date to the fact_trips_df
one_a_week_df = fact_trips_df.join(dim_dates_df, fact_trips_df.started_at_date_id == dim_dates_df.date_id, 'left').withColumnRenamed("date", "trip_start_date").drop("date_id")

# Add a day_of_week column
one_a_week_df = one_a_week_df.withColumn("day_of_week", date_format(one_a_week_df["trip_start_date"], "E"))

# Group by the the day_of_week column and find the average
one_a_week_grouped_df = one_a_week_df.groupBy("day_of_week").agg(avg("trip_duration").cast('int').alias("average_trip_duration"))

display(one_a_week_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1a. Analyse how much time is spent per ride - based on date and time factors - time of day 

# COMMAND ----------

from pyspark.sql.functions import hour, avg

# Join the trip_start_time to the fact_trips_df
one_a_time_df = fact_trips_df.join(dim_times_df, fact_trips_df.started_at_time_id == dim_times_df.time_id, 'left').withColumnRenamed("time", "trip_start_time").drop("time_id")

# Add the hour_of_day column
one_a_time_df = one_a_time_df.withColumn("hour_of_day", hour(one_a_time_df["trip_start_time"]))

# Group by the the hour_of_day column and find the average
one_a_time_grouped_df = one_a_time_df.groupBy("hour_of_day").agg(avg("trip_duration").cast("int").alias("average_trip_duration"))

display(one_a_time_grouped_df)

# COMMAND ----------

display(fact_trips_df)
display(dim_times_df)
