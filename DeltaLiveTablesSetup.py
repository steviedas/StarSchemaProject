# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"
@dlt.table(
  comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)
def clickstream_raw():
  return (spark.read.format("json").load(json_path))

@dlt.table(
  comment="Wikipedia clickstream data cleaned and prepared for analysis."
)
@dlt.expect("valid_current_page_title", "current_page_title IS NOT NULL")
@dlt.expect_or_fail("valid_count", "click_count > 0")
def clickstream_prepared():
  return (
    dlt.read("clickstream_raw")
      .withColumn("click_count", expr("CAST(n AS INT)"))
      .withColumnRenamed("curr_title", "current_page_title")
      .withColumnRenamed("prev_title", "previous_page_title")
      .select("current_page_title", "click_count", "previous_page_title")
  )

@dlt.table(
  comment="A table containing the top pages linking to the Apache Spark page."
)
def top_spark_referrers():
  return (
    dlt.read("clickstream_prepared")
      .filter(expr("current_page_title == 'Apache_Spark'"))
      .withColumnRenamed("previous_page_title", "referrer")
      .sort(desc("click_count"))
      .select("referrer", "click_count")
      .limit(10)
  )

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

payments_csv_path = "/tmp/Steven/Landing/payments.csv"
riders_csv_path = "/tmp/Steven/Landing/riders.csv"
stations_csv_path = "/tmp/Steven/Landing/stations.csv"
trips_csv_path = "/tmp/Steven/Landing/trips.csv"

#Write to Bronze
@dlt.table(comment="The raw payments dataset, ingested from the extracted zip file from the Github repository.")
def bronze_payments():
  return (spark.read.format('csv').load("/tmp/Steven/Landing/payments.csv", schema = payments_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/payments"))

@dlt.table(comment="The raw riders dataset, ingested from the extracted zip file from the Github repository.")
def bronze_riders():
  return (spark.read.format('csv').load("/tmp/Steven/Landing/riders.csv", schema = riders_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/riders"))

@dlt.table(comment="The raw stations dataset, ingested from the extracted zip file from the Github repository.")
def bronze_stations():
  return (spark.read.format('csv').load("/tmp/Steven/Landing/stations.csv", schema = stations_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/stations"))

@dlt.table(comment="The raw trips dataset, ingested from the extracted zip file from the Github repository.")
def bronze_trips():
  return (spark.read.format('csv').load("/tmp/Steven/Landing/trips.csv", schema = trips_bronze_schema).write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/trips"))


# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

json_path = "/tmp/Steven/Github"

@dlt.table(
  comment="The raw data from the Github repository."
)
def <table_name>():
    df = spark.read.csv()
    
    final_df = df.......
    return (final_df)
