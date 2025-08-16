# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.rawdata

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawdata")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawdata/Flights")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.bronze;
# MAGIC CREATE SCHEMA workspace.silver;
# MAGIC CREATE SCHEMA workspace.gold;

# COMMAND ----------

CREATE VOLUME workspace.bronze.BronzeVolume;
CREATE VOLUME workspace.silver.SilverVolume;
CREATE VOLUME workspace.gold.GoldVolume;