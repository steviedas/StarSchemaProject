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
one_a_time_grouped_df = one_a_time_df.groupBy("hour_of_day").agg(avg("trip_duration").alias("average_trip_duration"))

display(one_a_time_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1b. Analyse how much time is spent per ride - based on which station is the starting and / or ending station

# COMMAND ----------

from pyspark.sql.functions import avg

# Join the name column for both start_station_id and end_station_id to the fact_trips table joining on station_id
one_b_df = fact_trips_df \
    .join(dim_stations_df, fact_trips_df.start_station_id == dim_stations_df.station_id, 'left').drop("station_id", "latitude", "longitude").withColumnRenamed("name", "start_station_name") \
    .join(dim_stations_df, fact_trips_df.end_station_id == dim_stations_df.station_id, 'left').drop("station_id", "latitude", "longitude").withColumnRenamed("name", "end_station_name") \
    .select("start_station_name", "end_station_name", "trip_duration")

# Group by start_station_name and find the average trip_duration
start_station_grouped_df = one_b_df.groupBy("start_station_name").agg(avg("trip_duration").alias("average_trip_duration"))

# Group by start_station_name and find the average trip_duration
end_station_grouped_df = one_b_df.groupBy("end_station_name").agg(avg("trip_duration").alias("average_trip_duration"))

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
one_c_grouped_df = one_c_df.groupBy("rider_age").agg(avg("trip_duration").alias("average_trip_duration"))

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
monthly_totals = two_a_df1.groupBy("year", "month").agg(sum("amount").alias("total_spent_per_month_per_year"))

# Group by month and calculate the average per month
monthly_average = monthly_totals.groupBy("month").agg(avg("total_spent_per_month_per_year").alias("average_spent_per_month"))

display(monthly_average)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2a. Analyse how much money is spent - per quarter

# COMMAND ----------

from pyspark.sql.functions import when, sum

# Add a new column for the quarter
quarterly_average = monthly_average.withColumn("quarter", when(monthly_average["month"].between(1, 3), 1)
                                                 .when(monthly_average["month"].between(4, 6), 2)
                                                 .when(monthly_average["month"].between(7, 9), 3)
                                                 .otherwise(4))

# Group the data by quarter and calculate the average
result = quarterly_average.groupBy("quarter").agg(sum("average_spent_per_month").alias("quarterly_average"))

# Display the result
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2a. Analyse how much money is spent - per year

# COMMAND ----------

from pyspark.sql.functions import year, avg, sum

# Create a new column for year
two_a_year_df = two_a_df.withColumn("year", year(two_a_df["date"]))

# Group by the year column and average
two_a_year_df = two_a_year_df.groupBy("year").sum("amount").alias("average_spent_per_year")

display(two_a_year_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2b. Analyse how much money is spent - per member based on the age of the rider at account start

# COMMAND ----------

from pyspark.sql.functions import year, datediff, avg, sum

# Join the fact_payments_df to the dim_riders_df on rider_id
two_b_df = fact_payments_df.join(dim_riders_df, fact_payments_df.rider_id == dim_riders_df.rider_id, 'left').drop("payment_id", "rider_id", "date_id", "first", "last", "address", "account_end", "is_member")

# Add an age at account start
two_b_df = two_b_df.withColumn("age_at_account_start", (datediff(two_b_df["account_start"], two_b_df["birthday"]) / 365).cast("int"))

two_b_grouped_df = two_b_df.groupBy("age_at_account_start").agg(sum("amount").alias("amount_spent_age"))

display(two_b_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRA - Analyse how much money is spent per member - based on how many rides the rider averages per month

# COMMAND ----------

from pyspark.sql.functions import date_format, sum, col

# Join the fact_trips_df to fact_payments_df on rider_id
extra_a_df = fact_trips_df.join(fact_payments_df, fact_trips_df.rider_id == fact_payments_df.rider_id, 'left').select("trip_id", fact_trips_df.rider_id, "amount", "started_at_date_id")

extra_a_1df = extra_a_df.join(dim_riders_df, extra_a_df.rider_id == dim_riders_df.rider_id, 'left').select("trip_id", fact_trips_df.rider_id, "amount", "started_at_date_id", "is_member").filter(col("is_member") == True)

# Join extra_a_df to dim_dates_df
extra_a_df1 = extra_a_1df.join(dim_dates_df, extra_a_1df.started_at_date_id == dim_dates_df.date_id, 'left').drop("started_at_date_id", "date_id", "is_member")

# Change the formatting of the date column
extra_a_df2 = extra_a_df1.withColumn("month_year", date_format(extra_a_df1["date"], "MM-yyyy"))

# Group by rider_id and then by month_year 
extra_a_grouped_df = (extra_a_df2.groupBy("rider_id", "month_year").agg(sum("amount").alias("sum_amount")).select("rider_id", "month_year", "sum_amount"))

display(extra_a_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRA - Analyse how much money is spent per member - based on how many minutes the rider spends on a bike per month

# COMMAND ----------

from pyspark.sql.functions import date_format, sum

# Join the fact_trips_df to fact_payments_df on rider_id
extra_b_df = fact_trips_df.join(fact_payments_df, fact_trips_df.rider_id == fact_payments_df.rider_id, 'left').select(fact_trips_df.rider_id, "started_at_date_id", "trip_duration", "amount")

# Join the extra_b_df to dim_riders_df
extra_b_1df = extra_b_df.join(dim_riders_df, extra_b_df.rider_id == dim_riders_df.rider_id, 'left').select(extra_b_df.rider_id, "started_at_date_id", "trip_duration", "amount", "is_member").filter(col("is_member") == True)

# Join extra_a_df to dim_dates_df
extra_b_2df = extra_b_1df.join(dim_dates_df, extra_b_1df.started_at_date_id == dim_dates_df.date_id, 'left').drop("is_member", "date_id", "started_at_date_id")

# Change the formatting of the date column
extra_b_3df = extra_b_2df.withColumn("month_year", date_format(extra_b_2df["date"], "MM-yyyy")).drop("date")

display(extra_b_3df)

# COMMAND ----------

from pyspark.sql.functions import sum

result = (
    extra_b_3df
    .groupBy("rider_id", "month_year")
    .agg(sum("amount").alias("sum_amount"), sum("trip_duration").alias("sum_trip_duration")))
#    .withColumn("avg_amount_per_minute", col("sum_amount")/(col("sum_trip_duration") * 60))
#     .withColumn("total_amount_per_month", col("avg_amount_per_minute") * col("sum_trip_duration"))
#     .select("rider_id", "month_year", "total_amount_per_month")
# )

display(result)
