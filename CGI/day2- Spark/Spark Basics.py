# Databricks notebook source
# MAGIC %sql
# MAGIC Data Structures of Spark
# MAGIC 1. RDD (Resilient Distributed Datasets) (1 D)
# MAGIC 2. DataFrames (2D) Like a table (Rows and Columns) (python)
# MAGIC 3. Datasets (Scala)

# COMMAND ----------

# DBTITLE 1,Spark Doc Link
https://spark.apache.org/docs/latest/api/python/reference/index.html

# COMMAND ----------

spark

# COMMAND ----------

# DBTITLE 1,getting started
data=([(1,'A'),(2,'B')])
schema="id int, name string"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL
# MAGIC 1. Read csv
# MAGIC 2. Transformation
# MAGIC 3. Write table

# COMMAND ----------

# DBTITLE 1,Read CSV
df=spark.read.csv("/Volumes/cgi_dev/naval/raw/Spotify_Songs.csv",header=True)

# COMMAND ----------

df=spark.read.csv("/Volumes/cgi_dev/naval/raw/Spotify_Songs.csv",header=True,inferSchema=True)

# COMMAND ----------

csv
json
parquet
delta

# COMMAND ----------

# DBTITLE 1,Write
df.write.saveAsTable("cgi_dev.naval.spofity_songs_pyspark")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cgi_dev.naval.spofity_songs_pyspark
