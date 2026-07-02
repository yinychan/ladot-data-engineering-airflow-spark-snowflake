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

__2. Define exact mapping using PySpark__

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
    types.StructField('agency', types.DecimalType(), True),
    types.StructField('violation_code', types.StringType(), True),
    types.StructField('violation_description', types.StringType(), True),
    types.StructField('fine_amount', types.DecimalType(), True),
    types.StructField('agency_desc', types.StringType(), True),
    types.StructField('color_desc', types.StringType(), True),
    types.StructField('body_style_desc', types.StringType(), True),
    types.StructField('loc_lat', types.DoubleType(), True),
    types.StructField('loc_long', types.DoubleType(), True),
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



### Partitioning

`PARTITIONED BY` clause