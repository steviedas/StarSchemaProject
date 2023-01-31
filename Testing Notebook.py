# Databricks notebook source
# MAGIC %md
# MAGIC ###Bronze Schema 

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

station_bronze_schema = StructType ([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
])

rider_bronze_schema = StructType ([
    StructField("rider_id", StringType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("address", StringType(), True),
    StructField("birthday", StringType(), True),
    StructField("account_start_date", StringType(), True),
    StructField("account_end_date", StringType(), True),
    StructField("is_member", StringType(), True),
])

payment_bronze_schema = StructType ([
    StructField("payment_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("rider_id", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver Schema

# COMMAND ----------

#Silver Schema
from pyspark.sql.types import *
trip_silver_schema = StructType ([
    StructField("trip_id", VarcharType(255), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", DateType(), True),
    StructField("ended_at", DateType(), True),
    StructField("start_station_id", IntegerType(), True),
    StructField("end_station_id", IntegerType(), True),
    StructField("rider_id", IntegerType(), True),
])

station_silver_schema = StructType ([
    StructField("station_id", VarcharType(255), True),
    StructField("name", VarcharType(255), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
])

rider_silver_schema = StructType ([
    StructField("rider_id", IntegerType(), True),
    StructField("first", VarcharType(255), True),
    StructField("last", VarcharType(255), True),
    StructField("address", VarcharType(255), True),
    StructField("birthday", DateType(), True),
    StructField("account_start_date", DateType(), True),
    StructField("account_end_date", DateType(), True),
    StructField("is_member", BooleanType(), True),
])

payment_silver_schema = StructType ([
    StructField("payment_id", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("amount", FloatType(), True),
    StructField("rider_id", IntegerType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold Schema

# COMMAND ----------

from pyspark.sql.types import *
trip_gold_schema = StructType ([
    StructField("trip_id", VarcharType(255), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", DateType(), True),
    StructField("ended_at", DateType(), True),
    StructField("start_station_id", IntegerType(), True),
    StructField("end_station_id", IntegerType(), True),
    StructField("rider_id", IntegerType(), True),
])

station_gold_schema = StructType ([
    StructField("station_id", VarcharType(255), True),
    StructField("name", VarcharType(255), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
])

rider_gold_schema = StructType ([
    StructField("rider_id", IntegerType(), True),
    StructField("first", VarcharType(255), True),
    StructField("last", VarcharType(255), True),
    StructField("address", VarcharType(255), True),
    StructField("birthday", DateType(), True),
    StructField("account_start_date", DateType(), True),
    StructField("account_end_date", DateType(), True),
    StructField("is_member", BooleanType(), True),
])

payment_gold_schema = StructType ([
    StructField("payment_id", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("amount", DecimalType(), True),
    StructField("rider_id", IntegerType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create a database for the each .csv file and write to Bronze

# COMMAND ----------

#File name, location and type
#file_name = "trip"
#csv_location = "/tmp/Steven/landing/####.csv"
#csv_file_type = "csv"
#table_file_type = "delta"
#bronze_location = "tmp/Steven/Bronze/"
#silver_location = "tmp/Steven/Silver"

#CSV options
#first_row_is_header = "false"
#delimiter = ","

#The applied options are for CSV files. For other file types, these will be ignored.
#bronze_{file_name}_df = spark.read.format(csv_file_type) \
#    .option("header", first_row_is_header) \
#    .option("sep", delimiter) \
#    .schema(trip_bronze_schema) \
#    .load(csv_location)
#{file_name}_df.write.format("delta").mode("overwrite").save(bronze_location)
#display(trips_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create databases in Silver using the Bronze delta files

# COMMAND ----------

#silver_{file_name}_df = spark.read.format(table_file_type) \
#    .load(bronze_location) \
#    .schema(trip_silver_schema) \
#    .write.format(table_file_type) \
#    .mode("overwrite") \
#    .save(silver_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create databases in Gold using the Silver Delta Files

# COMMAND ----------

#gold_{file_name}_df = spark.read.format(table_file_type) \
#    .load(silver_location) \
#    .schema(trip_silver_schema) \
#    .write.format(table_file_type) \
#    .mode("overwrite") \
#    .save(gold_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ###List of tables

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/Steven/landing/

# COMMAND ----------

# MAGIC %md
# MAGIC ###Destroy all the schemas/databases and the tables inside of them

# COMMAND ----------

#%fs
#destroy_path = '/tmp/Steven/landing/payment.csv'
#dbutils.fs.rm('destroy_path',True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Random stuff that could be useful

# COMMAND ----------

# Create a view or table
# trips_df.createOrReplaceTempView(file_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Query the temp table in SQL
# MAGIC --select * from trip
