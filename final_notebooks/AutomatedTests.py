# Databricks notebook source
# MAGIC %md
# MAGIC ## Load all the data from the Gold folder

# COMMAND ----------

# MAGIC %run /Repos/steven.das@qualyfi.co.uk/StarSchemaProject/final_notebooks/BusinessOutcomes

# COMMAND ----------

# MAGIC %md
# MAGIC # Old Asserts

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bikes dimension table asserts

# COMMAND ----------

# ASSERT 1
assert dim_bikes_df.count() == 3, "This dataframe has an incorrect number of rows"

# ASSERT 2
# Get the second row of the dim_bikes_df DataFrame
dim_bikes_df_assert_2 = dim_bikes_df.take(2)[1]

# Check the values in the second row
assert dim_bikes_df_assert_2[0] == 2, "This dataframe has an incorrect value in row 2, column 1"
assert dim_bikes_df_assert_2[1] == "docked_bike", "This dataframe has an incorrect value in row 2, column 2"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dates dimension table asserts

# COMMAND ----------

# ASSERT 1
assert dim_dates_df.count() == 133, "This dataframe has an incorrect number of rows"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Times dimension table asserts

# COMMAND ----------

# ASSERT 1
assert dim_times_df.count() == 243, "This dataframe has an incorrect number of rows"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stations dimension table asserts

# COMMAND ----------

# ASSERT 1
assert dim_stations_df.count() == 838, "This dataframe has an incorrect number of rows"

# ASSERT 2
# Get the number of columns in the bike_df DataFrame
dim_stations_df_columns = len(dim_stations_df.columns)

# Check the number of columns
assert dim_stations_df_columns == 4, "This dataframe has an incorrect number of columns"

# COMMAND ----------

# MAGIC %md
# MAGIC # Business Outcomes Asserts

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checking that the schemas for the created the gold tables match the schemas in SchemaCreation

# COMMAND ----------

# MAGIC %run /Repos/steven.das@qualyfi.co.uk/StarSchemaProject/final_notebooks/SchemaCreation

# COMMAND ----------

assert dim_bikes_df.schema == bike_gold_schema, "Schema mismatch on: Bikes table"
assert dim_dates_df.schema == date_gold_schema, "Schema mismatch on: Dates table"
assert dim_times_df.schema == time_gold_schema, "Schema mismatch on: Times table"
assert dim_stations_df.schema == stations_gold_schema, "Schema mismatch on: Stations table"
assert dim_riders_df.schema == riders_gold_schema, "Schema mismatch on: Riders table"
assert fact_payments_df.schema == payments_gold_schema, "Schema mismatch on: Payments table"
assert fact_trips_df.schema == trips_gold_schema, "Schema mismatch on: Trips table"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1a. Analyse how much time is spent per ride - based on date and time factors - time of day 

# COMMAND ----------

# ASSERT 1
assert one_a_week_grouped_df.count() == 7, "This dataframe has an incorrect number of rows, there should only be 7 rows for 7 days"

# ASSERT 2
# Calculate the average of the average_trip_duration column
#one_a_week_grouped_df_average = one_a_week_grouped_df.agg({'average_trip_duration': 'mean'}).first()[0]

# Check the average value
#assert one_a_week_grouped_df_average == 16.859760676200302

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1a. Analyse how much time is spent per ride - based on date and time factors - time of day 

# COMMAND ----------

# ASSERT 1
assert one_a_time_grouped_df.count() <= 24, "This dataframe for question 1a, has more hours in the day than there should be!"

# COMMAND ----------

# MAGIC %md
# MAGIC ####1b. Analyse how much time is spent per ride - based on which station is the starting and/or ending station

# COMMAND ----------

# ASSERT 1
assert start_station_grouped_df.distinct().count() == 74, "This dataframe has an incorrect number of rows in question 1b for start station, there should be 74 rows"

# ASSERT 2
assert end_station_grouped_df.distinct().count() == 67, "This dataframe has an incorrect number of rows in question 1b for end station, there should be 67 rows"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1c. Analyse how much time is spent per ride - based on age of the rider at time of the ride 

# COMMAND ----------

# ASSERT 1
assert one_c_grouped_df.distinct().count() == 44, "This dataframe has an incorrect number of rows in question 1c, there should be 44 rows"

# q1e_count = timeSpentPerRide_RiderAge.filter(col("rider_age") < 5).count()
# assert q1e_count == 0, "Incorrect value found in Q1E, found rider younger than 5"

# COMMAND ----------

display(one_c_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1d. Analyse how much time is spent per ride - based on whether the rider is a member or a casual rider

# COMMAND ----------

from pyspark.sql.functions import col

# ASSERT 1
assert one_d_grouped_df.count() == 2, "This dataframe has an incorrect number of rows in question 1d, there should be 2 rows"

# ASSERT 2
# Get the number of columns in the bike_df DataFrame
one_d_grouped_df_columns = len(one_d_grouped_df.columns)

# Check the number of columns
assert one_d_grouped_df_columns == 2, "This dataframe has an incorrect number of columns in question 1d, there should only be two columns"

# ASSERT 3
# Extract the column you want to check
one_d_grouped_df_is_member = one_d_grouped_df.select(col("is_member"))

# Get the distinct values in that column
one_d_grouped_df_is_member_values = one_d_grouped_df_is_member.distinct().collect()

# Check if all the values in the list are either True or False
assert all(val[0] in [True, False] for val in one_d_grouped_df_is_member_values), "In question 1d, the produced dataframe has values are not boolean"

# COMMAND ----------

# MAGIC %md
# MAGIC ####2a. Analyse how much money is spent - per month

# COMMAND ----------

# ASSERT 1
assert two_a_df3.count() == 12, "This dataframe has too many months"

# COMMAND ----------

# MAGIC %md
# MAGIC ####2a. Analyse how much money is spent - per quarter

# COMMAND ----------

# ASSERT 1
assert quarterly_average_grouped_df.count() == 4, "This dataframe has too many quarters"

# COMMAND ----------

# MAGIC %md
# MAGIC ####2a. Analyse how much money is spent - per year

# COMMAND ----------

# ASSERT 1
assert two_a_year_df.distinct().count() == 10, "This dataframe has too many rows"

# COMMAND ----------

# MAGIC %md
# MAGIC ####2b. Analyse how much money is spent - per member based on the age of the rider at account start

# COMMAND ----------

# ASSERT 1
assert two_b_grouped_df.distinct().distinct().count() == 69, "This dataframe has too many rows"

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRA 1a - Analyse how much money is spent per member - based on how many rides the rider averages per year per month

# COMMAND ----------

# ASSERT 1
assert extra_1a_df.distinct().distinct().count() == 116, "This dataframe has too many rows"

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRA 1b - Analyse how much money is spent per member - based on how many rides the rider averages per year per month

# COMMAND ----------

# ASSERT 1
assert extra_1b_df.distinct().distinct().count() == 116, "This dataframe has too many rows"

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRA 2a - Analyse how much money is spent per member - based on how many minutes the rider spends on a bike per year per month

# COMMAND ----------

# ASSERT 1
assert extra_two_a_grouped_df.distinct().distinct().count() == 116, "This dataframe has too many rows"

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRA 2b - Analyse how much money is spent per member - based on how many minutes the rider spends on a bike per month

# COMMAND ----------

# ASSERT 1
assert extra_two_b_grouped_df.distinct().distinct().count() == 116, "This dataframe has too many rows"
