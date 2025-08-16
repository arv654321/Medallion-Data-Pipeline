import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Bookings Data
rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "flight_id IS NOT NULL"
}


@dlt.table(
    name="stg_bookings"
)
def stg_bookings():
    df = spark.readStream.format("delta")\
              .load("/Volumes/workspace/bronze/bronzevolume/Bookings/Data")
    return df


@dlt.view(
    name =  "transformed_bookings"
)
def transformed_bookings():
    df = spark.readStream.table("stg_bookings")
    df = df.withColumn("amount",col("amount").cast(DoubleType()))\
           .withColumn("booking_date",to_date(col("booking_date")))\
           .withColumn("Modified_Date",current_timestamp())\
           .drop("_rescued_data")
    return df


@dlt.table(
    name="Silver_Bookings"
)
@dlt.expect_all_or_drop(rules)
def Silver_Bookings():
    df = spark.readStream.table("transformed_bookings")
    return df


# Flights Data
@dlt.view(
    name = "transformed_flights"
)
def transformed_flights():
    df = spark.readStream.format("Delta")\
              .load("/Volumes/workspace/bronze/bronzevolume/Flights/Data/")
    df = df.drop("_rescued_data").withColumn("Modified_Date",current_timestamp())
    return df

dlt.create_streaming_table("Silver_Flights")

dlt.create_auto_cdc_flow(
    target = "Silver_Flights",
    source = "transformed_flights",
    keys = ["flight_id"],
    sequence_by = col("Modified_Date"),
    stored_as_scd_type = "1"
)


# Passenger Data

@dlt.view(
    name = "transformed_passengers"
)

def transformed_passengers():
    df = spark.readStream.format("Delta")\
              .load("/Volumes/workspace/bronze/bronzevolume/Customers/Data/")
    df = df.drop("_rescued_data").withColumn("Modified_Date",current_timestamp())
    return df

dlt.create_streaming_table("Silver_Passengers")

dlt.create_auto_cdc_flow(
    target = "Silver_Passengers",
    source = "transformed_passengers",
    keys = ["passenger_id"],
    sequence_by = col("Modified_Date"),
    stored_as_scd_type = "1"
)


# Airports Data

@dlt.view(
    name = "transformed_airports"
)

def transformed_airports():
    df = spark.readStream.format("Delta")\
              .load("/Volumes/workspace/bronze/bronzevolume/Airports/Data/")
    df = df.drop("_rescued_data").withColumn("Modified_Date",current_timestamp())
    return df

dlt.create_streaming_table("Silver_Airports")

dlt.create_auto_cdc_flow(
    target = "Silver_Airports",
    source = "transformed_airports",
    keys = ["airport_id"],
    sequence_by = col("Modified_Date"),
    stored_as_scd_type = "1"
)

# Silver Business View

@dlt.view(
    name = "Silver_Business"
)
def Silver_Business():
    df = dlt.readStream("Silver_Bookings")\
              .join(dlt.readStream("Silver_Flights"),["flight_id"])\
              .join(dlt.readStream("Silver_Passengers"),["passenger_id"])\
              .join(dlt.readStream("Silver_Airports"),["airport_id"])\
              .drop("Modified_Date")
    return df

