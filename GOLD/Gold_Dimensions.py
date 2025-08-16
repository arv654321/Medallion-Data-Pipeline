# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC  **Fetching Parameters**

# COMMAND ----------

key_cols = "['flight_id']"
key_cols_list = eval(key_cols)

catalog = "workspace"

cdc_col = "Modified_Date"

backdated_refresh = ""

source_object = "Silver_Flights"

source_schema = "silver"

target_object = "DimFlights"

target_schema = "gold"

surrogate_key = "DimFlightsKey"

# COMMAND ----------

# key_cols = "['airport_id']"
# key_cols_list = eval(key_cols)

# catalog = "workspace"

# cdc_col = "Modified_Date"

# backdated_refresh = ""

# source_object = "Silver_Airports"

# source_schema = "silver"

# target_object = "DimAirports"

# target_schema = "gold"

# surrogate_key = "DimAirportsKey"

# COMMAND ----------

# key_cols = "['passenger_id']"
# key_cols_list = eval(key_cols)

# catalog = "workspace"

# cdc_col = "Modified_Date"

# backdated_refresh = ""

# source_object = "Silver_Passengers"

# source_schema = "silver"

# target_object = "DimPassengers"

# target_schema = "gold"

# surrogate_key = "DimPassengersKey"

# COMMAND ----------

# MAGIC %md
# MAGIC ##  **Incremental Data Ingestion**

# COMMAND ----------

# MAGIC %md
# MAGIC **Last Load Date**

# COMMAND ----------

# No BackDated Refresh
if len(backdated_refresh)==0:
    # If table exists in the destination
    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
        last_load = spark.sql(f"SELECT MAX(cdc_col) FROM {catalog}.{target_schema}.{target_object}").collect()[0][0]
    else:
        last_load = "1900-01-01 00:00:00"

# BackDated Refresh
else:
    last_load = backdated_refresh

# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM {catalog}.{source_schema}.{source_object} WHERE {cdc_col} > '{last_load}'")

# COMMAND ----------

# MAGIC %md
# MAGIC   **OLD vs NEW Records**

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"): 

  # Key Columns String For Incremental
  key_cols_string_incremental = ", ".join(key_cols_list)

  df_trg = spark.sql(f"""SELECT {key_cols_string_incremental}, {surrogate_key}, create_date, update_date 
                      FROM {catalog}.{target_schema}.{target_object}""")


else:

  # Key Columns String For Initial
  key_cols_string_init = [f"'' AS {i}" for i in key_cols_list]
  key_cols_string_init = ", ".join(key_cols_string_init)
  
  df_trg = spark.sql(f"""SELECT {key_cols_string_init}, CAST('0' AS INT) AS {surrogate_key}, CAST('1900-01-01 00:00:00' AS timestamp) AS          create_date, CAST('1900-01-01 00:00:00' AS timestamp) AS update_date WHERE 1=0""")

# COMMAND ----------

df_trg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC  **JOIN Condition**

# COMMAND ----------

join_condition = " AND ".join([f"src.{i} = trg.{i}" for i  in key_cols_list])

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
                    SELECT src.*, 
                           trg.{surrogate_key},
                           trg.create_date,
                           trg.update_date
                    FROM src
                    LEFT JOIN trg
                    ON {join_condition}
                    """)

# COMMAND ----------

df_join.display()

# COMMAND ----------

# OLD RECORDS
df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())

# NEW RECOERDS
df_new = df_join.filter(col(f'{surrogate_key}').isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriching DFs

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing DF_old**

# COMMAND ----------

df_old_enr = df_old.withColumn('update_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing DF_new**

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"): 
    max_surrogate_key = spark.sql(f"""
                            SELECT max({surrogate_key}) FROM {catalog}.{target_schema}.{target_object}
                        """).collect()[0][0]
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                    .withColumn('create_date', current_timestamp())\
                    .withColumn('update_date', current_timestamp())    

else:
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                    .withColumn('create_date', current_timestamp())\
                    .withColumn('update_date', current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Union of old and new DFs

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPSERT

# COMMAND ----------

from delta.tables import DeltaTable 

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
                        .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
                        .whenNotMatchedInsertAll()\
                        .execute()

else: 

    df_union.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")


# COMMAND ----------

# MAGIC %sql
# MAGIC  select * from workspace.gold.Dim_Passengers