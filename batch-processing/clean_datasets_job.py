import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
import os
from dotenv import load_dotenv

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")  
aws_s3_bucket = os.getenv("AWS_S3_BUCKET")

spark = SparkSession.builder \
    .master("local[*]") \
    .appName(aws_s3_bucket) \
    .config("spark.jars.packages", 
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

# 1. Define schemas based on Socrata API outputs for each dataset
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
    types.StructField('eventtime', types.StringType(), True), # We will need to convert this column to a TIMESTAMP_NTZ(9) in Snowflake
    types.StructField('occupancystate', types.StringType(), True)
])

parking_citations_schema = types.StructType([
    types.StructField('ticket_number', types.StringType(), True),
    types.StructField('issue_date', types.StringType(), True), # We will need to convert this column to a TIMESTAMP_NTZ(9) in Snowflake
    types.StructField('issue_time', types.StringType(), True), # We will need to left-pad this column with zeros to make it a 4-digit string before converting to a NUMBER(4,0) in Snowflake
    types.StructField('meter_id', types.StringType(), True),
    types.StructField('marked_time', types.StringType(), True),
    types.StructField('rp_state_plate', types.StringType(), True),
    types.StructField('plate_expiry_date', types.StringType(), True),
    types.StructField('make', types.StringType(), True),
    types.StructField('body_style', types.StringType(), True),
    types.StructField('color', types.StringType(), True),
    types.StructField('location', types.StringType(), True),
    types.StructField('route', types.StringType(), True),
    types.StructField('agency', types.StringType(), True), # We will need to convert this column to a IntegerType() in Snowflake
    types.StructField('violation_code', types.StringType(), True),
    types.StructField('violation_description', types.StringType(), True),
    types.StructField('fine_amount', types.StringType(), True), # We will need to convert this column to a DecimalType(10, 2) in Snowflake
    types.StructField('agency_desc', types.StringType(), True),
    types.StructField('color_desc', types.StringType(), True),
    types.StructField('body_style_desc', types.StringType(), True),
    types.StructField('loc_lat', types.StringType(), True), # We will need to convert this column to a DoubleType() in Snowflake
    types.StructField('loc_long', types.StringType(), True), # We will need to convert this column to a DoubleType() in Snowflake
    types.StructField('geocodelocation', types.StringType(), True), # to be created as a VARIANT column in Snowflake
])

# 2. Retrive parquets from S3
# URL: s3://ladot-meter-parking-de-project-aws-data-lake/
data_lake_path = f"s3a://{aws_s3_bucket}/"
df_policies = spark.read.schema(parking_inventory_policies_schema).parquet(f"{data_lake_path}/parking_inventory_policies/")
df_occupancy = spark.read.schema(meter_occupancy_schema).parquet(f"{data_lake_path}/meter_occupancy/")
df_citations = spark.read.schema(parking_citations_schema).parquet(f"{data_lake_path}/parking_citations/")

# 3. Re-type and Clean dataframes
df_citations_cleaned = df_citations \
    .withColumn(
        "issue_time", 
        F.lpad(F.col("issue_time"), 4, "0")
    ) \
    .withColumn(
        "issue_date",
        F.to_timestamp(F.col("issue_date"))
    ) \
    .withColumn(
        "agency",
        F.col("agency").cast(types.IntegerType())
    ) \
    .withColumn(
        "fine_amount",
        F.col("fine_amount").cast(types.DecimalType(10, 2))
    ) \
    .withColumn(
        "loc_lat",
        F.col("loc_lat").cast(types.DoubleType())
    ) \
    .withColumn(
        "loc_long",
        F.col("loc_long").cast(types.DoubleType())
    )

df_occupancy_cleaned = df_occupancy \
    .withColumn(
        "eventtime",
        F.to_timestamp(F.col("eventtime"))
    )

# 4. Write cleaned dataframes back to S3
df_policies.write.mode("overwrite").parquet(f"{data_lake_path}/silver/parking_inventory_policies/")
df_occupancy_cleaned.write.mode("overwrite").parquet(f"{data_lake_path}/silver/meter_occupancy/")
df_citations_cleaned.write.mode("overwrite").parquet(f"{data_lake_path}/silver/parking_citations/")