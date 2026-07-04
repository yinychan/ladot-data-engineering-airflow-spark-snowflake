## Batch Processing with Apache Spark and PySpark

Since our source data came from Scorata API, our fields were provided to us as strings, and part of our batch processing will be to re-type the data back into their original format as listed in LA City Open Data's documentation.

## Contents

- [Overview](#overview)
    - [Batch vs Stream](#batch-vs-stream)
    - [Data Cleaning](#data-cleaning)
- [Setting Up Spark and PySpark](#setting-up-spark-and-pyspark)
    - [Installation Prerequisites](#installation-prerequisites)
    - [Prepare for Processing Script](#prepare-for-processing-script)
- [Planning](#planning)
    - [Re-typing](#re-typing)
    - [Spark Data Schema](#spark-data-schema)
    - [Dimension and Fact Tables](#dimension-and-fact-tables)
- [Reading From S3](#reading-from-s3)
- [Write Re-Type and Clean Transformations](#write-re-type-and-clean-transformations)
- [Store Cleaned Datasets (Silver)](#store-cleaned-datasets-silver)
- [Write Target Staging Tables inside Snowflake](#write-target-staging-tables-inside-snowflake)
    - [Data Mapping](#data-mapping)
    - [Data Definition Language (DDL)](#data-definition-language-ddl)
    - [SQL Ingestion Script](#sql-ingestion-script)

## Overview 

### Batch vs Stream

Data processing can be done in one of 2 ways: batch and streaming. Batch processing deal with data collected over time and processed in bulk. Conversely, Stream processing deals with continuous data ingestion and analysis.  For our purposes with LADOT parking data, we will implement batch processing.

Batch processes can be scheduled to run in a time increment that makes most sense for your project. E.g. hourly, daily, weekly, or by the minute.

### Data Cleaning

To get our data ready for analytics, we need to clean the raw data we retrived from the Socrata API. We will use Apache Spark to transform the raw text fields into their proper analytical data types. This will optimize performance and prevents staging errors in Snowflake.

We will also be working with PySpark which is the Python API for Spark.

## Setting Up Spark and PySpark

### Installation Prerequisites

These step-by-steps use Homebrew and Spark on v.4.1.2. Spark requires both Java and Python. Instructions are written for macOS using `uv`. 

__1. Check for Homebrew__

```
brew --version
brew update # if necessary for you
```

__2. Check for Java__

Spark runs on Java 17 or Java 21, we'll do 21.

```
java -version
brew install openjdk@21
```

__3. Check for Python__

```
uv run python --version
uv python list
uv python pin 3.14
```

__4. Add PySpark__

```
cd batch-processing
uv init
uv add pyspark
```

__5. Set Environment Variables__

```
nano ~/.zshrc
export JAVA_HOME=$(brew --prefix openjdk@21)
export PATH="$JAVA_HOME/bin:$PATH"

# Ensures standalone uv takes priority
export PATH="$HOME/.local/bin:$PATH"

# for Spark to run on localhost
export SPARK_LOCAL_IP="127.0.0.1"
```

Save and Exit: Ctrl + X, "yes"

__6. Confirm it's all there__

```
java -version
which uv
```

__7. Run Spark locally__

In your terminal: 

```
uv run pyspark
```

If you see "Welcome to Spark version 4.1.2", it ran successfully. You can go to `localhost:4040` in your browser for Spark UI.

### Prepare for Processing Script

__1. Create the file__

```
cd batch-processing/
touch clean_datasets_job.py
```

__2. Import Spark and necessary packages__

```
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
```

## Planning

### Re-typing

Because the Socrata API exported the fields as generic text strings, we need to re-type our columns back to the original datatypes as defined by LA City Open Data. We re-type for 3 main critical reasons:

1. To reduce costs in storage and query performance. If a column is typed as `Number` or `Date`, Snowflake stores it in a highly compressed, binary format. It tracks the exact minimum and maximum values of that block in its metadata. E.g. querying a column such as `issue_date` as a `TimestampType` allows Snowflake to skip millions of rows by checking the metadata. If `issue_date` remained a `StringType`, Snowflake would scan every row of text data, which costs would have a high cost in time and compute.

2. To run basic math in SQL. Ee.g. if `fine_amount` is kept as a string, we would not be able to execute lines such as `SELECT SUM(fine_amount)` or `SELECT AVG(fine_amount)` when needed. If numerical values were kept as string types, sorting would become alphabetical, not numerical. e.g. `ORDER BY fine_amount` would list `100.00` before `25.00` because alphabetically "1" comes before "2".

3. To maintain data integrity and avoid data corruption. E.g. if `issue_date` remained a `StringType`, values such as "N/A" or "00/00/0000" will leak into production rather than returning `NULL` if it were a strongly-typed pipeline.

### Spark Data Schema

__1. Gather data structure from source data__

- meter_occupancy/
    - spaceid (Text)
    - eventtime (Floating Timestamp)
    - occupancystate (Text)

- parking_inventory_policies/
    - spaceid (Text)
    - metertype (Text)
    - ratetype (Text)
    - raterange (Text)
    - timelimit (Text)
    - blockface (Text)
    - latlng (Point or Location)

- parking_citations/
    - ticket_number (Text)
    - fine_amount (Number)
    - issue_date (Floating Timestamp)
    - issue_time (Text)
    - marked_time (Text)
    - violation_code (Text)
    - violation_description (Text)
    - agency (Text)
    - agency_desc (Text)
    - location (Text)
    - loc_lat (Number)
    - loc_long (Number)
    - geocodelocation (Point)
    - rp_state_plate (Text)
    - plate_expiry_date (Text / Number)
    - make (Text)
    - body_style (Text)
    - color (Text)
    - body_style_desc (Text)
    - color_desc (Text)

__2. Define current Schema using PySpark__

In `clean_datasets_job.py`, write:

```
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
```

### Dimension and Fact Tables

We won't need this yet, but while we're here looking at our original data structure, let's also see how we'd want our dimension tables and fact tables to be structured for future anaytics tasks.

Dims:
- dim_vehicles (GROUP BY or SELECT DISTINCT to deduplicate)
    - MD5 primary key
    - rp_state_plate
    - plate_expiry_date
    - make
    - body_style_desc
    - color_desc

- dim_meter
    - meter_key (from fields spaceid, but we'll assume is same as meter_id)
    - meter_type
    - rate_type
    - rate_range
    - time_limit
    - location_key

- dim_location (lat/lan/geolocation) (GROUP BY or SELECT DISTINCT to deduplicate)
    - location_key
    - address_number_street (from fields location and blockface)
    - latitude
    - longitude

- dim_agency (GROUP BY or SELECT DISTINCT to deduplicate)
    - aagency_key VARCHAR PRIMARY KEY (agency field from source)
    - description
    
- dim_date (GROUP BY or SELECT DISTINCT to deduplicate)
    - date_key DATE PRIMARY KEY (native date key for fast partition pruning)
    - day
    - month
    - year

- dim_time_of_day (GROUP BY or SELECT DISTINCT to deduplicate)
    - time_key TIME PRIMARY KEY (native time key)
    - hour_number INT
    - minute_number INT

- Violation (GROUP BY or SELECT DISTINCT to deduplicate)
    - MD5 Violoation key
    - code
    - description

Facts:
- Citations
    - ticket_number
    - fine_amount
    - Date_key
    - TimeOfDay_key
    - violation_key
    - agency_key
    - location_key
    - meter_key
    - vehicle_key

- Occupancy
    - occupancy_key
    - meter_key
    - Date_key
    - TimeOfDay_key

## Reading From S3

__1. Create .env variables__

```
# In your terminal
touch .env

uv add python-dotenv
```

In your `.env` file, add the following keys along with your values:

```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_S3_BUCKET=
```

In your `clean_datasets_job.py` file, read your environment variables

```
...
import os
from dotenv import load_dotenv

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")  
aws_s3_bucket = os.getenv("AWS_S3_BUCKET")
...
```

__2. Configure Hadoop__

Even though we're running Apache Spark locally, Spark still uses Hadoop core libraries as its primary storage file-system interface. Let's config Hadoop to make a connection to S3. We are also using Hadoop's highly optimized file system driver called `s3a`.

```
spark = SparkSession.builder \
    .master("local[*]") \
    .appName(aws_s3_bucket) \  # we can name our app the same as our bucket for consistency
    .config("spark.jars.packages", 
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

... # I have my data schema definitions here

data_lake_path = f"s3a://{aws_s3_bucket}/"
df_policies = spark.read.schema(parking_inventory_policies_schema).parquet(f"{data_lake_path}/parking_inventory_policies/")
df_occupancy = spark.read.schema(meter_occupancy_schema).parquet(f"{data_lake_path}/meter_occupancy/")
df_citations = spark.read.schema(parking_citations_schema).parquet(f"{data_lake_path}/parking_citations/")
```

__3. Test run the connection__

In your terminal:

```
uv run python clean_datasets_job.py
```

A few key items you should see to indicate successful dependency resolution:

```
downloading https://repo1....
[SUCCESSFUL ] org.apache.hadoop#hadoop-aws;3.4.1!hadoop-aws.jar (125ms)
downloading https://repo1....
[SUCCESSFUL ] com.amazonaws#aws-java-sdk-bundle;1.12.262!aws-java-sdk-bundle.jar (7811ms)
downloading https://repo1....
[SUCCESSFUL ] software.amazon.awssdk#bundle;2.24.6!bundle.jar (8560ms)
downloading https://repo1....
[SUCCESSFUL ] org.wildfly.openssl#wildfly-openssl;1.1.3.Final!wildfly-openssl.jar (166ms)
```

I got a couple warnings I can safely ignore:

```
# standard when running Hadoop on MacOS
WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable

...

# utility warning, Spark muting verbose debug logs
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
```

No erros or exceptions thrown. It looks like I'm good to move forward.

## Write Re-Type and Clean Transformations

We re-type and clean what we can in Spark. Later, there will be 3 remaining re-type tasks we still need to do in the Snowflake end:

1. In `parking_inventory_policies_schema` > `latlng` is to be created as a VARIANT column.
2. In `parking_citations_schema` > `issue_time` will need to be converting to a NUMBER(4,0) in Snowflake to keep the miliarty time format e.g. 0100
3. In `parking_citations_schema` > `geocodelocation` is to be created as a VARIANT column.

```
...

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
```

## Store Cleaned Datasets (Silver)

We write the re-typed and cleaned dataframes back to S3. In `clean_datasets_job.py`:

```
...

df_policies.write.mode("overwrite").parquet(f"{data_lake_path}/silver/parking_inventory_policies/")
df_occupancy_cleaned.write.mode("overwrite").parquet(f"{data_lake_path}/silver/meter_occupancy/")
df_citations_cleaned.write.mode("overwrite").parquet(f"{data_lake_path}/silver/parking_citations/")
```

Great, we should be ready to get this cleaned and sent to S3. In your terminal:

```
uv run python clean_datasets_job.py
```

#### Success!

My terminal output looks something like this:

```
...

26/07/03 10:29:53 WARN MemoryManager: Total allocation exceeds 95.00% (1,020,054,720 bytes) of heap memory
Scaling row group sizes to 95.00% for 8 writers
[Stage 2:===================================>                   (189 + 8) / 294]
```

This means Spark is chunking the process and successfully running the script.

Additionally, you can check for a `_SUCCESS` file flag in your S3 directories. Log into you AWS dashboard and navigate to your S3 bucket. Click into your bucket directory to locate the `_SUCCESS` files.

For me, they were found here: 

```
Amazon S3 > Buckets > ladot-meter-parking-de-project-aws-data-lake > silver/ > meter_occupancy/ > _SUCCESS
Amazon S3 > Buckets > ladot-meter-parking-de-project-aws-data-lake > silver/ > parking_citations/ > _SUCCESS
Amazon S3 > Buckets > ladot-meter-parking-de-project-aws-data-lake > silver/ > parking_inventory_policies/ > _SUCCESS
```

## Write Target Staging Tables inside Snowflake

### Data Mapping

We need to make note of a few data mappings to move between Spark and Snowflake:

1. Match Spark data types with Snowflake's native SQL data types:

- TimestampType() -> TIMESTAMP
- IntegerType() -> INTEGER
- DecimalType() -> NUMBER(10,2)
- DoubleType() -> DOUBLE
- StringType() -> VARCHAR

2. Apply `VARIANT` type for `latlang` and `geocodelocation`.

### Data Definition Language (DDL)

Let's write the `CREATE TABLE` scripts for all 3 of our datasets before placing them in our Snowflake workspace. 

1. Staging Table for Meter Parking Inventory Policies
```
CREATE TABLE staging_parking_inventory_policies (
    spaceid VARCHAR,
    blockface VARCHAR,
    metertype VARCHAR,
    ratetype VARCHAR,
    raterange VARCHAR,
    timelimit VARCHAR,
    latlng VARIANT
);
```

2. Staging Table for Meter Occupancy
```
CREATE TABLE staging_meter_occupancy (
    spaceid VARCHAR,
    eventtime TIMESTAMP,
    occupancystate VARCHAR
);
```

3. Staging Table for Meter Parking Citations
```
CREATE TABLE staging_parking_citations (
    ticket_number VARCHAR,
    issue_date TIMESTAMP,
    issue_time VARCHAR, -- We will retype this at a later stage
    meter_id VARCHAR,
    marked_time VARCHAR,
    rp_state_plate VARCHAR,
    plate_expiry_date VARCHAR,
    make VARCHAR,
    body_style VARCHAR,
    color VARCHAR,
    location VARCHAR,
    route VARCHAR,
    agency INTEGER,
    violation_code VARCHAR,
    violation_description VARCHAR,
    fine_amount NUMBER(10,2),
    agency_desc VARCHAR,
    color_desc VARCHAR,
    body_style_desc VARCHAR,
    loc_lat DOUBLE,
    loc_long DOUBLE,
    geocodelocation VARIANT  
);
```

#### Success!

In your Snowflake console, you should see `Table STAGING_PARKING_CITATIONS successfully created.` upon succes.

### SQL Ingestion Script

Now, we connect our Snowflake staging tables to S3 and copy from S3. Remember from our Data Warehouse phase, we [created a stage called `silver_stage`](/warehouse/README.md#secure-cloud-authentication).

We use `PATTERN` to filter out non-parquet files.

```
COPY INTO staging_parking_inventory_policies 
FROM (
    SELECT
        $1:spaceid::VARCHAR,
        $1:blockface::VARCHAR,
        $1:metertype::VARCHAR,
        $1:ratetype::VARCHAR,
        $1:raterange::VARCHAR,
        $1:timelimit::VARCHAR,
        $1:latlang::VARIANT
    FROM @silver_stage/parking_inventory_policies/
)
PATTERN = '.*part-.*\.parquet';

COPY INTO staging_meter_occupancy
FROM (
    SELECT
        $1:spaceid::VARCHAR,
        $1:eventtime::TIMESTAMP,
        $1:occupancystate::VARCHAR
    FROM @silver_stage/meter_occupancy/
)
PATTERN = '.*part-.*\.parquet';

COPY INTO staging_parking_citations 
FROM (
  SELECT 
    $1:ticket_number::VARCHAR,
    $1:issue_date::TIMESTAMP,
    $1:issue_time::VARCHAR, -- Preserves your clean 4-character padding string layout
    $1:meter_id::VARCHAR,
    $1:marked_time::VARCHAR,
    $1:rp_state_plate::VARCHAR,
    $1:plate_expiry_date::VARCHAR,
    $1:make::VARCHAR,
    $1:body_style::VARCHAR,
    $1:color::VARCHAR,
    $1:location::VARCHAR,
    $1:route::VARCHAR,
    $1:agency::INTEGER,
    $1:violation_code::VARCHAR,
    $1:violation_description::VARCHAR,
    $1:fine_amount::NUMBER(10,2),
    $1:agency_desc::VARCHAR,
    $1:color_desc::VARCHAR,
    $1:body_style_desc::VARCHAR,
    $1:loc_lat::DOUBLE,
    $1:loc_long::DOUBLE,
    $1:geocodelocation::VARIANT
  FROM @silver_stage/parking_citations/
)
PATTERN = '.*part-.*\.parquet';
```

Run these in your Snowflake workspace. Because we're dealing with millions of rows for the parking citations dataset, the process will take a while (a handful of minutes for me).

#### Success!

Your Snowflake console should indicate something like `294 (chunks) s3://[bucket]/silver/[directory]/*.parquet LOADED 520000 ... etc` listing out all the chunks and total number of rows processed. 

Aamzing, we're done with the Batch Processing phase now.

## Back to main

Excellent, you can [continue back at the main project](../README.md).
