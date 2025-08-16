# Databricks notebook source
src_array = [
    {"src":"Bookings"},
    {"src":"Flights"},
    {"src":"Customers"},
    {"src":"Airports"}
]


# COMMAND ----------

dbutils.jobs.taskValues.set(key="output_key",value=src_array)