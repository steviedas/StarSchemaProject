# Databricks notebook source
# MAGIC %md
# MAGIC ##Example code

# COMMAND ----------

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

# MAGIC %md
# MAGIC ##Write the bronze tables to DeltaLiveTables

# COMMAND ----------

# MAGIC %run /Repos/steven.das@qualyfi.co.uk/steven-repo/final_notebooks/Bronze

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(comment="The raw payments dataset, ingested from the extracted zip file from the Github repository.")
def bronze_payments():
    return (bronze_payments_df)

@dlt.table(comment="The raw riders dataset, ingested from the extracted zip file from the Github repository.")
def bronze_riders():
    return (bronze_riders_df)

@dlt.table(comment="The raw stations dataset, ingested from the extracted zip file from the Github repository.")
def bronze_stations():
    return (bronze_stations_df)

@dlt.table(comment="The raw trips dataset, ingested from the extracted zip file from the Github repository.")
def bronze_trips():
    return (bronze_trips_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write the silver tables to DeltaLiveTables

# COMMAND ----------


