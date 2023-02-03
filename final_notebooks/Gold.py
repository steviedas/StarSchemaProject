# Databricks notebook source
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
# MAGIC ## Create the riders table

# COMMAND ----------

riders_df = silver_riders_delta_df.withColumnRenamed("account_start", "account_start_date").withColumnRenamed("account_end", "account_end_date")
display(riders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write all the dataframes to Gold

# COMMAND ----------

bike_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_bikes")

date_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_dates")

time_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_times")

payments_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/fact_payments")

trips_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/fact_trips")

silver_stations_delta_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_stations")

riders_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Gold/dim_riders")
