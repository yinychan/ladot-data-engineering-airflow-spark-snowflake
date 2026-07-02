import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

import pandas as pd
from sodapy import Socrata

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

# client = Socrata("data.lacity.org", None)
# results = client.get("4f5p-udkv", limit=5)
# results_df = pd.DataFrame.from_records(results)

# spark.createDataFrame(results_df).schema
parking_inventory_policies_schema = types.StructType([
    types.StructField('spaceid', types.StringType(), True),
    types.StructField('blockface', types.StringType(), True),
    types.StructField('metertype', types.StringType(), True),
    types.StructField('ratetype', types.StringType(), True),
    types.StructField('raterange', types.StringType(), True),
    types.StructField('timelimit', types.StringType(), True),
    types.StructField('latlng', types.StringType(), True) # to be created as a VARIANT column in Snowflake
])

meter_occupancy_schema = types.StructType([
    types.StructField('spaceid', types.StringType(), True),
    types.StructField('eventtime', types.TimestampType(), True),
    types.StructField('occupancystate', types.StringType(), True)
])

parking_citations_schema = types.StructType([
    types.StructField('ticket_number', types.StringType(), True),
    types.StructField('issue_date', types.TimestampType(), True),
    types.StructField('issue_time', types.StringType(), True), # We will need to left-pad this column with zeros to make it a 4-digit string before converting to a DecimalType(4, 0) in Snowflake
    types.StructField('meter_id', types.StringType(), True),
    types.StructField('marked_time', types.StringType(), True),
    types.StructField('rp_state_plate', types.StringType(), True),
    types.StructField('plate_expiry_date', types.StringType(), True),
    types.StructField('make', types.StringType(), True),
    types.StructField('body_style', types.StringType(), True),
    types.StructField('color', types.StringType(), True),
    types.StructField('location', types.StringType(), True),
    types.StructField('route', types.StringType(), True),
    types.StructField('agency', types.IntegerType(), True),
    types.StructField('violation_code', types.StringType(), True),
    types.StructField('violation_description', types.StringType(), True),
    types.StructField('fine_amount', types.DecimalType(10, 2), True),
    types.StructField('agency_desc', types.StringType(), True),
    types.StructField('color_desc', types.StringType(), True),
    types.StructField('body_style_desc', types.StringType(), True),
    types.StructField('loc_lat', types.DoubleType(), True),
    types.StructField('loc_long', types.DoubleType(), True),
    types.StructField('geocodelocation', types.StringType(), True), # to be created as a VARIANT column in Snowflake
])

# 1. Find the original data types

# 2. Retrive parquets from S3

# 3. Retype