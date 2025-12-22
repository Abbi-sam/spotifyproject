# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Autoloader

# COMMAND ----------

df_load = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
      .option("cloudFiles.schemaLocation", "abfss://silver@spotifystorageaz.dfs.core.windows.net/DimUser/checkpoint") \
        .option("schemaEvolutionMode","addNewColumns") \
          .load("abfss://bronze@spotifystorageaz.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df_load)

# COMMAND ----------

df_load = df_load.drop("_rescued_data")
display(df_load)

# COMMAND ----------

df_load = df_load.withColumn("user_name", upper(col("user_name")))
display(df_load)

# COMMAND ----------

df_load.writeStream.format("delta")\
    .outputMode("append")\
        .option("checkpointLocation", "/abfss://silver@spotifystorageaz.dfs.core.windows.net/DimUser/checkpoint")\
            .trigger(once=True)\
                .option("path", "abfss://silver@spotifystorageaz.dfs.core.windows.net/DimUser/data")\
                    .toTable("spotify_cat.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dim Artist

# COMMAND ----------

df_loadart = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
      .option("cloudFiles.schemaLocation", "abfss://silver@spotifystorageaz.dfs.core.windows.net/DimArtist/checkpoint") \
        .option("schemaEvolutionMode","addNewColumns") \
          .load("abfss://bronze@spotifystorageaz.dfs.core.windows.net/DimArtist")\
            .toTable("spotify_cat.silver.DimArtist")

# COMMAND ----------

display(df_loadart)

# COMMAND ----------

df_loadart = df_loadart.drop("_rescued_data")
df_loadart = df_loadart.dropDuplicates(["artist_id"])
display(df_loadart)

# COMMAND ----------

df_loadart.writeStream.format("delta") \
    .outputMode("append"). \
        option("checkpointLocation", "/abfss://silver@spotifystorageaz.dfs.core.windows.net/DimArtist/checkpoint")\
            .trigger(once=True)\
                .option("path", "abfss://silver@spotifystorageaz.dfs.core.windows.net/DimArtist/data")\
                    .toTable("spotify_cat.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ##DIM Track

# COMMAND ----------

df_loadtrack = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
      .option("cloudFiles.schemaLocation", "abfss://silver@spotifystorageaz.dfs.core.windows.net/DimTrack/checkpoint") \
        .option("schemaEvolutionMode","addNewColumns") \
          .load("abfss://bronze@spotifystorageaz.dfs.core.windows.net/DimTrack")

# COMMAND ----------

df_loadtrack = df_loadtrack.drop("_rescued_data")
display(df_loadtrack)

# COMMAND ----------

df_loadtrack = df_loadtrack.withColumn("durationFlag",when(col("duration_sec") < 150, "low")\
    .when(col("duration_sec") > 300, "medium")\
    .otherwise("high"))
df_loadtrack = df_loadtrack.withColumn("track_name", regexp_replace(col("track_name"),'-',' '))

# COMMAND ----------

display(df_loadtrack)

# COMMAND ----------

df_loadtrack.writeStream.format("delta") \
    .outputMode("append"). \
        option("checkpointLocation", "/abfss://silver@spotifystorageaz.dfs.core.windows.net/DimTrack/checkpoint")\
            .trigger(once=True)\
                .option("path", "abfss://silver@spotifystorageaz.dfs.core.windows.net/DimTrack/data")\
                    .toTable("spotify_cat.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dim Date

# COMMAND ----------

df_loaddate = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
      .option("cloudFiles.schemaLocation", "abfss://silver@spotifystorageaz.dfs.core.windows.net/DimDate/checkpoint") \
        .option("schemaEvolutionMode","addNewColumns") \
          .load("abfss://bronze@spotifystorageaz.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_loaddate = df_loaddate.drop("_rescued_data")
df_loaddate.writeStream.format("delta") \
    .outputMode("append"). \
        option("checkpointLocation", "/abfss://silver@spotifystorageaz.dfs.core.windows.net/DimDate/checkpoint")\
            .trigger(once=True)\
                .option("path", "abfss://silver@spotifystorageaz.dfs.core.windows.net/DimDate/data")\
                    .toTable("spotify_cat.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ##FactStream

# COMMAND ----------

df_loadfact = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
      .option("cloudFiles.schemaLocation", "abfss://silver@spotifystorageaz.dfs.core.windows.net/FactStream/checkpoint") \
        .option("schemaEvolutionMode","addNewColumns") \
          .load("abfss://bronze@spotifystorageaz.dfs.core.windows.net/FactStream")

# COMMAND ----------

display(df_loadfact)

# COMMAND ----------

df_loadfact = df_loadfact.drop("_rescued_data")
df_loadfact.writeStream.format("delta") \
    .outputMode("append"). \
        option("checkpointLocation", "/abfss://silver@spotifystorageaz.dfs.core.windows.net/FactStream/checkpoint")\
            .trigger(once=True)\
                .option("path", "abfss://silver@spotifystorageaz.dfs.core.windows.net/FactStream/data")\
                    .toTable("spotify_cat.silver.FactStream")

# COMMAND ----------

