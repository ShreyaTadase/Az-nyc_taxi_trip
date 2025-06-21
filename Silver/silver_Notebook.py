# Databricks notebook source

spark.conf.set("fs.azure.account.auth.type.nyctaxi02storage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxi02storage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxi02storage.dfs.core.windows.net", "9e7812d6-****-4daa-b502-1b3**d67b060")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxi02storage.dfs.core.windows.net","wGb8Q~J***VjE.XyWLBL7l5***5gWPytAz-fwaTD" )
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxi02storage.dfs.core.windows.net", "https://login.microsoftonline.com/5f0c202a-a440-40aa-a79f-3a2076da8b22/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@nyctaxi02storage.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md 
# MAGIC data reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC reading csv data

# COMMAND ----------

# MAGIC %md
# MAGIC trip type varible

# COMMAND ----------

df_trip_type = spark.read.format('csv')\
                    .option("inferSchema",True)\
                    .option("header",True)\
                    .load("abfss://bronze@nyctaxi02storage.dfs.core.windows.net/trip_type")

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC trip_zone

# COMMAND ----------

df_trip_zone = spark.read.format("csv")\
                    .option("inferSchema",True)\
                    .option("header",True)\
                    .load("abfss://bronze@nyctaxi02storage.dfs.core.windows.net/trip_zone")

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

myschema = '''  
                    VendorID bigint,
                    lpep_pickup_datetime timestamp,
                    lpep_dropoff_datetime timestamp, 
                    store_and_fwd_flag string, 
                    RatecodeID bigint, 
                    PULocationID bigint, 
                    DOLocationID bigint, 
                    passenger_count bigint, 
                    trip_distance double, 
                    fare_amount double, 
                    extra double, 
                    mta_tax double, 
                    tip_amount double, 
                    tolls_amount double, 
                    ehail_fee double, 
                    improvement_surcharge double, 
                    total_amount double, 
                    payment_type bigint, 
                    trip_type bigint, 
                    congestion_surcharge double
'''

# COMMAND ----------

df_trip = spark.read.format("parquet")\
                .schema(myschema)\
                .option("header",True)\
                .option("recursiveFileLookup",True)\
                .load("abfss://bronze@nyctaxi02storage.dfs.core.windows.net/trips2023data")

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC data transformation

# COMMAND ----------

## with column rename
df_trip_type = df_trip_type.withColumnRenamed("description","trip_description")

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format("parquet")\
                  .mode("append")\
                  .option("path","abfss://silver@nyctaxi02storage.dfs.core.windows.net/trip_type")\
                  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC TRip zone

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn('zone1', split(col('zone'), '/')[0])\
                           .withColumn('zone2',split(col('zone'),'/')[1])

         

# COMMAND ----------

df_trip_zone.display()   

# COMMAND ----------

df_trip_zone.write.format("parquet")\
            .mode("append")\
            .option("path","abfss://silver@nyctaxi02storage.dfs.core.windows.net/trip_zone")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC trip data

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.withColumn('trip_date',to_date(col('lpep_pickup_datetime')))\
                 .withColumn('trip_year',year(col('lpep_pickup_datetime')))\
                 .withColumn('trip_month',month(col('lpep_pickup_datetime')))

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip= df_trip.select('VendorID','PULocationID','DOLocationID','trip_distance','fare_amount','total_amount')
df_trip.display()

# COMMAND ----------

df_trip.write.format("parquet")\
            .mode("append")\
            .option("path","abfss://silver@nyctaxi02storage.dfs.core.windows.net/trip2023data")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Analysis

# COMMAND ----------

display(df_trip)
