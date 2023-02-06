# Steven's Star Schema Project Implementation
![GitHub last commit](https://img.shields.io/github/last-commit/steviedas/steven-repo)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/steviedas/steven-repo)

[Star Schema Design](#star-schema-design) •
[Raw Data](#raw-data) •
[Creating the Schema](#creating-the-schema) •
[Destroy Schemas](#destroy-schemas-and-databases) •
[Rebuilding Schemas](#rebuild-the-schemas-and-databases) •
[Load Data in Bronze](#load-data-in-bronze) •
[Transform and Load Data in Gold](#transform-and-load-data-in-gold) •
[Business Outcomes](#business-outcomes) •
[Pictures](#pictures)

This project is based around a bike sharing program, that allows riders to purchase a pass at a kiosk or use a mobile application to unlock a bike at stations around the city and use the bike for a specified amount of time. The bikes can be returned to the same station or another station.

We created a star schema that ingests data into a Bronze layer. The incoming data data in the Bronze layer is then manipulated by enfocing a given schema and saved into the Silver layer. The data in the Silver layer is then transformed into the fact and dimension tables as laid out in the physical database design.

Star schema project files are located within this repository. This repository is designed to run in conjunction with Databricks, and hence, will use the Databricks File System (DBFS) to store each of the medallion layers.

## Star Schema Design
The various stages of the design of the star schema is shown in the various stages below.

   <details>
   <summary>Conceptual Database Design</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/steviedas/steven-repo/main/pictures/ConceptualDatabaseDesign.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540">
   ></p>
   >
   
   </details>

   <details>
   <summary>Logical Database Design</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/steviedas/steven-repo/main/pictures/LogicalDatabaseDesign.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540"
   ></p>
   >
   
   </details>
  
   <details>
   <summary>Physical Database Design</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/steviedas/steven-repo/main/pictures/PhysicalDatabaseDesign.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540"
   ></p>
   >
   
   </details>

## Raw Data
Within the repository, there is a folder called `zips`, contianing the relevant .zip files within. These zips contain the raw data in csv files.

Zip Files     | Contents
------------- | -------------
payments.zip  | payments.csv
riders.zip    | riders.csv
stations.zip  | stations.csv
trips.zip     | trips.csv

## Creating the Schema
Within the repository, the schemas for the each of the tables in each of the layers is written out in the file `SchemaCreation.py`.
An example of one of these schemas, is written below:

```python
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
```

## Destroy Schemas and Databases
This notebook deletes any databases already located in the DBFS. An example of this is shown below:

```python
dbutils.fs.rm("/tmp/Steven/Bronze", True)
```

## Rebuild the Schemas and Databases
This notebook creates empty dataframes in each layer, using the schema located in the `SchemaCreations.py`. An example of this is shown below:

```python
from pyspark.sql.types import *

empty_rdd = sc.emptyRDD()

bronze_trips_df = spark.createDataFrame(empty_rdd, trips_bronze_schema)
bronze_trips_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/trips")
```

## Load Data in Bronze
The notebook called `Bronze` does a few tasks. Below is a summary:
 <details>
   <summary>Pulls the zips folder from the repository to the Github folder in DBFS</summary>
   
   >```python
   >!wget "https://github.com/steviedas/steven-repo/raw/main/zips/payments.zip" -P "/dbfs/tmp/Steven/Github/"
   >```  
   
   </details>

<details>
   <summary>Unzips the zips file into the Landing folder in DBFS</summary>
   
   >```python
   >import subprocess
   >import glob
   >zip_files = glob.glob("/dbfs/tmp/Steven/Github/*.zip")
   >for zip_file in zip_files:
   >    extract_to_dir = "/dbfs/tmp/Steven/Landing"
   >    subprocess.call(["unzip", "-d", extract_to_dir, zip_file])
   >```  
   
   </details>

<details>
   <summary>Writes all the CSV's to Bronze as Delta Format</summary>
   
   >```python
   >bronze_trips_df = spark.read.format('csv').load("/tmp/Steven/Landing/trips.csv", schema = trips_bronze_schema)
   >bronze_trips_df.write.format("delta").mode("overwrite").save("/tmp/Steven/Bronze/trips")
   >```  
   
   </details>

## Load Data in Silver
The `Silver` notebook loads the created delta files from `tmp/Steven/Bronze` and writes to the empty delta files located in `tmp/Steven/Silver`

## Transform and load data in Gold
The `Gold` notebook creates all the facts and dimensions tables as shown in [Star Schema design](#star-schema-design). A list of the tables it creates is shown below:


|Fact Tables   | Dimension Tables |
|------------- | -----------------|
|fact_payments | dim_bikes        |
|fact_trips    | dim_dates        |
|              | dim_riders       |
|              | dim_stations     |
|              | dim_times        |

## Business Outcomes
The `Business Outcomes` notebook answers all the following Business Outcomes:
Analyse how much time is spent per ride:
* Based on date and time factors such as day of week and time of day
* Based on which station is the starting and/or ending station
* Based on age of the rider at time of the day
* Based on whether the rider is a member or a casual rider

Analyse how much money is spent
* Per month, quarter,  year
* Per member, based on the age of the rider at account start

EXTRA - Analyse how much money is spent per member
* Based on how many rides the rider averages per month
* Based on how many minutes the rider spends on a bike per month 

## Pictures
Pictures of the various file structures created and displays of the several dimension and fact tables, are shown below:

   <details>
   <summary>DBFS File Structure</summary>

   ><p align="center">
   ><img src="https://github.com/steviedas/steven-repo/raw/main/pictures/DeltaTables.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>

   <details>
   <summary>Bikes Dimension Table</summary>

   ><p align="center">
   ><img src="https://github.com/steviedas/steven-repo/raw/main/pictures/dim_bikes.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
  
   <details>
   <summary>Dates Dimension Table</summary>

   ><p align="center">
   ><img src="https://github.com/steviedas/steven-repo/raw/main/pictures/dim_dates.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
   
   <details>
      
   <summary>Riders Dimension Table</summary>

   ><p align="center">
   ><img src="https://github.com/steviedas/steven-repo/raw/main/pictures/dim_riders.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
   
   <details>
      
   <summary>Stations Dimension Table</summary>

   ><p align="center">
   ><img src="https://github.com/steviedas/steven-repo/raw/main/pictures/dim_stations.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
   
   <details>
      
   <summary>Times Dimension Table</summary>

   ><p align="center">
   ><img src="https://github.com/steviedas/steven-repo/raw/main/pictures/dim_times.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
   
   <details>
      
   <summary>Fact Payments Table</summary>

   ><p align="center">
   ><img src="https://github.com/steviedas/steven-repo/raw/main/pictures/fact_payments.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
   
   <details>
      
   <summary>Fact Trips Table</summary>

   ><p align="center">
   ><img src="https://github.com/steviedas/steven-repo/raw/main/pictures/fact_trips.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
