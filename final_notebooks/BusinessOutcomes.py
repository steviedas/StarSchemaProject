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
one_a_week_grouped_df = one_a_week_df.groupBy("day_of_week").agg(avg("trip_duration").alias("average_trip_duration"))

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
one_a_time_grouped_df = one_a_time_df.groupBy("hour_of_day").agg(avg("trip_duration").alias("average_trip_duration")).orderBy("hour_of_day")

display(one_a_time_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1b. Analyse how much time is spent per ride - based on which station is the starting and/or ending station

# COMMAND ----------

from pyspark.sql.functions import avg

# Join the name column for both start_station_id and end_station_id to the fact_trips table joining on station_id
one_b_df = fact_trips_df \
    .join(dim_stations_df, fact_trips_df.start_station_id == dim_stations_df.station_id, 'left').drop("station_id", "latitude", "longitude").withColumnRenamed("name", "start_station_name") \
    .join(dim_stations_df, fact_trips_df.end_station_id == dim_stations_df.station_id, 'left').drop("station_id", "latitude", "longitude").withColumnRenamed("name", "end_station_name") \
    .select("start_station_name", "end_station_name", "trip_duration")

# Group by start_station_name and find the average trip_duration
start_station_grouped_df = one_b_df.groupBy("start_station_name").agg(avg("trip_duration").alias("average_trip_duration")).orderBy("start_station_name")

# Group by start_station_name and find the average trip_duration
end_station_grouped_df = one_b_df.groupBy("end_station_name").agg(avg("trip_duration").alias("average_trip_duration")).orderBy("end_station_name")

display(start_station_grouped_df)
display(end_station_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1c. Analyse how much time is spent per ride - based on age of the rider at time of the ride 

# COMMAND ----------

from pyspark.sql.functions import avg

# Select the relevant columns from the fact_trips_df
one_c_df = fact_trips_df.select("rider_age", "trip_duration")

# Group by rider_age and find average trip_duration
one_c_grouped_df = one_c_df.groupBy("rider_age").agg(avg("trip_duration").alias("average_trip_duration")).orderBy("rider_age")

display(one_c_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1d. Analyse how much time is spent per ride - based on whether the rider is a member or a casual rider

# COMMAND ----------

from pyspark.sql.functions import avg

#Join the dim_rider_df to the fact_trips on the rider_id
one_d_df = fact_trips_df.join(dim_riders_df, fact_trips_df.rider_id == dim_riders_df.rider_id, 'left').drop("rider_id", "first", "last", "address", "birthday", "account_start", "account_end")

# Select the relevant columns
one_d_df = one_d_df.select("is_member", "trip_duration")

# Group by is_member and calculate the average_trip_duration
one_d_grouped_df = one_d_df.groupBy("is_member").agg(avg("trip_duration").alias("average_trip_duration"))

display(one_d_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2a. Analyse how much money is spent - per month

# COMMAND ----------

from pyspark.sql.functions import year, month, sum, avg

# Join the fact_payments_df to the dim_dates_df on date_id
two_a_df = fact_payments_df.join(dim_dates_df, fact_payments_df.date_id == dim_dates_df.date_id, 'left').drop("date_id", "payment_id", "rider_id")

# Split out the date column to year and month
two_a_df1 = two_a_df.withColumn("year", year(two_a_df["date"]))
two_a_df1 = two_a_df1.withColumn("month", month(two_a_df["date"]))

# Group by year and then month and sum the amount column
two_a_df2 = two_a_df1.groupBy("year", "month").agg(sum("amount").alias("total_spent_per_month_per_year"))

# Group by month and calculate the average per month
two_a_df3 = two_a_df2.groupBy("month").agg(sum("total_spent_per_month_per_year").alias("average_spent_per_month"))
two_a_df3 = two_a_df3.orderBy("month")

display(two_a_df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2a. Analyse how much money is spent - per quarter

# COMMAND ----------

from pyspark.sql.functions import when, sum

# Add a new column for the quarter
quarterly_average = two_a_df3.withColumn("quarter", when(two_a_df3["month"].between(1, 3), 1)
                                                 .when(two_a_df3["month"].between(4, 6), 2)
                                                 .when(two_a_df3["month"].between(7, 9), 3)
                                                 .otherwise(4))

# Group the data by quarter and calculate the average
quarterly_average_grouped_df = quarterly_average.groupBy("quarter").agg(sum("average_spent_per_month").alias("quarterly_average")).orderBy("quarter")

# Display the result
display(quarterly_average_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2a. Analyse how much money is spent - per year

# COMMAND ----------

from pyspark.sql.functions import year, avg, sum

# Create a new column for year
two_a_year_df = two_a_df.withColumn("year", year(two_a_df["date"]))

# Group by the year column and average
two_a_year_df = two_a_year_df.groupBy("year").sum("amount").alias("average_spent_per_year").orderBy("year")
two_a_year_df = two_a_year_df.select("year", two_a_year_df["sum(amount)"].alias("average_spent_per_year"))

display(two_a_year_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2b. Analyse how much money is spent - per member based on the age of the rider at account start

# COMMAND ----------

from pyspark.sql.functions import year, datediff, avg, sum

# Join the fact_payments_df to the dim_riders_df on rider_id
two_b_df = fact_payments_df.join(dim_riders_df, fact_payments_df.rider_id == dim_riders_df.rider_id, 'left').drop("payment_id", "rider_id", "date_id", "first", "last", "address", "account_end", "is_member")

# Add an age at account start
two_b_df = two_b_df.withColumn("age_at_account_start", (datediff(two_b_df["account_start_date"], two_b_df["birthday"]) / 365).cast("int"))

two_b_grouped_df = two_b_df.groupBy("age_at_account_start").agg(sum("amount").alias("amount_spent_age")).orderBy("age_at_account_start")

display(two_b_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRA 1a - Analyse how much money is spent per member - based on how many rides the rider averages per year per month

# COMMAND ----------

from pyspark.sql.functions import date_format, sum, col

# Join the fact_trips_df to fact_payments_df on rider_id
extra_one_a_df = fact_trips_df.join(fact_payments_df, fact_trips_df.rider_id == fact_payments_df.rider_id, 'left').select("trip_id", fact_trips_df.rider_id, "amount", "started_at_date_id")

# Join the extra_one_a_df to dim_riders_df on rider_id and filter for members
extra_one_a_1df = extra_one_a_df.join(dim_riders_df, extra_one_a_df.rider_id == dim_riders_df.rider_id, 'left').select("trip_id", fact_trips_df.rider_id, "amount", "started_at_date_id", "is_member").filter(col("is_member") == True)

# Join extra_a_df to dim_dates_df
extra_one_a_2df = extra_one_a_1df.join(dim_dates_df, extra_one_a_1df.started_at_date_id == dim_dates_df.date_id, 'left').drop("date_id", "is_member", "trip_id")

from pyspark.sql.functions import sum, count, month, year, col

# Group the data by year and month and make the amount_spent and number_of_rides columns
extra_1a_df = extra_one_a_2df.groupBy(col("rider_id"), year("date").alias("year"), month("date").alias("month"), "started_at_date_id").agg(sum("amount").alias("amount_spent")) \
    .groupBy(col("rider_id"), "year", "month").agg(avg("amount_spent").alias("amount_spent"), count("rider_id").alias("number_of_trips"))

display(extra_1a_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRA 1b - Analyse how much money is spent per member - based on how many rides the rider averages per month

# COMMAND ----------

from pyspark.sql.functions import date_format, sum, col

# Join the fact_trips_df to fact_payments_df on rider_id
extra_one_b_df = fact_trips_df.join(fact_payments_df, fact_trips_df.rider_id == fact_payments_df.rider_id, 'left').select("trip_id", fact_trips_df.rider_id, "amount", "started_at_date_id")

# Join the extra_one_a_df to dim_riders_df on rider_id and filter for members
extra_one_b_1df = extra_one_b_df.join(dim_riders_df, extra_one_b_df.rider_id == dim_riders_df.rider_id, 'left').select("trip_id", fact_trips_df.rider_id, "amount", "started_at_date_id", "is_member").filter(col("is_member") == True)

# Join extra_a_df to dim_dates_df
extra_one_b_2df = extra_one_b_1df.join(dim_dates_df, extra_one_b_1df.started_at_date_id == dim_dates_df.date_id, 'left').drop("date_id", "is_member", "trip_id")

from pyspark.sql.functions import sum, count, month, year, col

# Group the data by year and month and make the amount_spent and number_of_rides columns
extra_1b_df = extra_one_b_2df.groupBy(col("rider_id"), month("date").alias("month"), "started_at_date_id").agg(sum("amount").alias("amount_spent")) \
    .groupBy(col("rider_id"), "month").agg(avg("amount_spent").alias("amount_spent"), count("rider_id").alias("number_of_trips"))

display(extra_1b_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRA 2a - Analyse how much money is spent per member - based on how many minutes the rider spends on a bike per year per month

# COMMAND ----------

from pyspark.sql.functions import date_format, sum, col, month, year

# Join the fact_trips_df to fact_payments_df on rider_id
extra_two_a_df = fact_trips_df.join(fact_payments_df, fact_trips_df.rider_id == fact_payments_df.rider_id, 'left').select(fact_trips_df.rider_id, "started_at_date_id", "trip_duration", "amount")

# Join the extra_b_df to dim_riders_df
extra_two_a_df1 = extra_two_a_df.join(dim_riders_df, extra_two_a_df.rider_id == dim_riders_df.rider_id, 'left').select(extra_two_a_df.rider_id, "started_at_date_id", "trip_duration", "amount", "is_member").filter(col("is_member") == True)

# Join extra_a_df to dim_dates_df
extra_two_a_df2 = extra_two_a_df1.join(dim_dates_df, extra_two_a_df1.started_at_date_id == dim_dates_df.date_id, 'left').drop("is_member", "date_id", "started_at_date_id")

# Group the data by year and month and make the amount_spent and total_trip_duration columns
extra_two_a_grouped_df = extra_two_a_df2.groupBy(col("rider_id"), year("date").alias("year"), month("date").alias("month"), col("trip_duration")).agg(sum("amount").alias("amount_spent")).orderBy("rider_id")
extra_two_a_grouped_df = extra_two_a_grouped_df.groupBy(col("rider_id"), "year", "month", col("amount_spent")).agg(sum("trip_duration").alias("total_trip_duration")).orderBy("rider_id")

display(extra_two_a_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRA 2b - Analyse how much money is spent per member - based on how many minutes the rider spends on a bike per month

# COMMAND ----------

from pyspark.sql.functions import date_format, sum, col, month, year

# Join the fact_trips_df to fact_payments_df on rider_id
extra_two_b_df = fact_trips_df.join(fact_payments_df, fact_trips_df.rider_id == fact_payments_df.rider_id, 'left').select(fact_trips_df.rider_id, "started_at_date_id", "trip_duration", "amount")

# Join the extra_b_df to dim_riders_df
extra_two_b_df1 = extra_two_b_df.join(dim_riders_df, extra_two_b_df.rider_id == dim_riders_df.rider_id, 'left').select(extra_two_b_df.rider_id, "started_at_date_id", "trip_duration", "amount", "is_member").filter(col("is_member") == True)

# Join extra_a_df to dim_dates_df
extra_two_b_df2 = extra_two_b_df1.join(dim_dates_df, extra_two_b_df1.started_at_date_id == dim_dates_df.date_id, 'left').drop("is_member", "date_id", "started_at_date_id")

# Group the data by year and month and make the amount_spent and total_trip_duration columns
extra_two_b_grouped_df = extra_two_b_df2.groupBy(col("rider_id"), month("date").alias("month"), col("trip_duration")).agg(sum("amount").alias("amount_spent")).orderBy("rider_id")
extra_two_b_grouped_df = extra_two_b_grouped_df.groupBy(col("rider_id"), "month", col("amount_spent")).agg(sum("trip_duration")).orderBy("rider_id")

display(extra_two_b_grouped_df)
