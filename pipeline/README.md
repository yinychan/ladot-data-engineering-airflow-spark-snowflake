## Data Ingestion

We will be using LA City's Open Data. 

## Contents

- [Data sources and API docs](#data-sources-and-api-docs)
- [Extract the data](#extract-the-data)
- [Connect to Database](#connect-to-database)
- [Insert the Data](#insert-the-data)
- [Extraction Using Socrata API](#extraction-using-socrata-api)

### Data sources and API docs

Sign Up and generate their API Token:
https://data.lacity.org/login. Enter it as your `APP_TOKEN` value in your `.env`.

We will be handling 3 datasets
- [LADOT Metered Parking Inventory & Policies](https://data.lacity.org/Transportation/LADOT-Metered-Parking-Inventory-Policies/s49e-q6j2/about_data)
- [LADOT Parking Meter Occupancy](https://data.lacity.org/Transportation/LADOT-Parking-Meter-Occupancy/e7h6-4a3e/about_data)
- [Parking Citations](https://data.lacity.org/Transportation/Parking-Citations/4f5p-udkv/about_data)

SODA3 API via Socrata documentation:
- https://dev.socrata.com/foundry/data.lacity.org/s49e-q6j2 (LADOT Metered Parking Inventory & Policies)
- https://dev.socrata.com/foundry/data.lacity.org/e7h6-4a3e (LADOT Parking Meter Occupancy)
- https://dev.socrata.com/foundry/data.lacity.org/4f5p-udkv (Parking Citations)

API Endpoints (requires authentication using the above `APP_TOKEN`):
- https://data.lacity.org/api/v3/views/s49e-q6j2/export.csv (LADOT Metered Parking Inventory & Policies) All data (~34416 rows)
- https://data.lacity.org/api/v3/views/e7h6-4a3e/export.csv (LADOT Parking Meter Occupancy) All data (~4213 rows)
- https://data.lacity.org/api/v3/views/4f5p-udkv/export.csv (Parking Citations) All data (~25089303 rows)

They also offer the endpoints in json format, paginated with 1000 entries at a time by default. For our purposes, we will run `POST` requests through the csv endpoints.

We start with the script for ingesting from one of the endpoints first to get it working.

### Extract the data. 
Make sure you're working in your `pipeline folder` directory.
```
ls

#as needed, run:
cd pipeline
```

In your `pipeline.py` file, add to the top of the page:
```
import requests
import os
import pandas as pd
```

Add the pandas package
```
uv add pandas
```

In `pipeline.py`, after your imports, set up variables to insert into request:
```
app_token = os.getenv("APP_TOKEN")

headers = {
    "X-App-Token": app_token,
    "Content-Type": "application/json",
}
payload_csv = {
    "query": "SELECT *",
    "orderingSpecifier": "discard"
}
meter_occupancy_csv = "https://data.lacity.org/api/v3/views/e7h6-4a3e/export.csv"
```

Now, we retrieve data from the API
```
meter_occupancy_response = requests.post(meter_occupancy_csv, headers=headers, json=payload_csv, timeout=100)
meter_occupancy_response.raise_for_status() # Check if the request was successful
```

We put the response into dataframe. Because the response comes back as string of text, we have to wrap it in `io.StringIO`. It helps us read the string response as if it were an actual file.
```
# Add to top of pipeline.py file with all the other imports:
from io import StringIO

# Add this after the line with `raise_for_status`
df = pd.read_csv(StringIO(meter_occupancy_response.text))
```

2. Let's make sure we've extracted the data we expect
```
# After generating a dataframe, in pipeline.py:
print(df.head())
print(df.dtypes)
print(df.shape)
print(df.columns)
```

In your terminal:
```
uv run python pipeline.py
```

Your terminal should output something like this:
```
  SpaceID            EventTime_UTC OccupancyState
0   C1193  2026 May 26 07:00:18 AM       OCCUPIED
1   CB968  2026 Jun 04 04:25:08 PM       OCCUPIED
2   SV273  2026 Jun 04 02:53:15 PM         VACANT
3  CB4391  2026 May 29 02:16:09 AM       OCCUPIED
4  CB4359  2026 Jun 04 02:18:00 PM       OCCUPIED
SpaceID           str
EventTime_UTC     str
OccupancyState    str
dtype: object
(4236, 3)
Index(['SpaceID', 'EventTime_UTC', 'OccupancyState'], dtype='str')
```
### Connect to Database
3. Great, now we're ready to connect to our database
```
# Add to top of pipeline.py file with all the other imports:
from sqlalchemy import create_engine
```

This means, we'll need to add the package to our virtual environment
```
uv add sqlalchemy psycopg2-binary
```

Create the engine
```
# in pipeline.py, continuing at the end of file
psql_user = os.getenv("POSTGRES_USER")
psql_password = os.getenv("POSTGRES_PASSWORD")
psql_db = os.getenv("POSTGRES_DB")
psql_port = os.getenv("POSTGRES_PORT", "5432") # default to 5432 if not set
psql_host = os.getenv("POSTGRES_HOST", "localhost") # default to localhost if not set

engine = create_engine(
    f"postgresql://{psql_user}:{psql_password}@{psql_host}:{psql_port}/{psql_db}"
)
```

### Insert the Data
4. We're ready to insert the data into our database

First, we add the `tqdm` package which will help us monitor the import progress
```
uv add tqdm
```
Make sure to import it to `pipeline.py`
```
# Add to the top of page with other imports
from tqdm.auto import tqdm
```

Comment out or delete the previously created dataframe. We're creating a new one that is iterable
```
# df = pd.read_csv(StringIO(meter_occupancy_response.text))
# print(df.head())
# print(df.dtypes)
# print(df.shape)
# print(df.columns)

df_iterable = pd.read_csv(
    StringIO(meter_occupancy_response.text),
    iterator=True,
    chunksize=100
)

first = True

for df_chunk in tqdm(df_iterable):

    if first:
        # Create table schema (no data)
        df_chunk.head(0).to_sql(
            name="meter_occupancy",
            con=engine,
            if_exists="replace"
        )
        first = False
        print("Table created")

    # Insert chunk
    df_chunk.to_sql(
        name="meter_occupancy",
        con=engine,
        if_exists="append"
    )

    print("Inserted:", len(df_chunk))
```

Now, let's test run the entire script in your codespace
```
uv run python pipeline.py
```

You should see an output like this:
```
0it [00:00, ?it/s]Table created
Inserted: 100
...
Inserted: 36
43it [00:00, 68.79it/s]
```

We check our database to see that it's inserted. In the terminal window where you've run `uv run pgcli ...`, let's inspect the database
```
\dt

SELECT * FROM meter_occupancy LIMIT 10;
```

Success here looks something like this:
```
+-------+---------+-------------------------+----------------+
| index | SpaceID | EventTime_UTC           | OccupancyState |
|-------+---------+-------------------------+----------------|
| 0     | WP150   | 2026 Jun 04 05:52:15 PM | OCCUPIED       |
| 1     | LZ108   | 2026 Jun 04 05:56:10 PM | OCCUPIED       |
| 2     | CB588   | 2026 Jun 04 04:31:17 PM | OCCUPIED       |
| 3     | C1087   | 2026 Jun 04 05:31:18 PM | OCCUPIED       |
| 4     | C291    | 2026 Jun 04 06:39:05 PM | OCCUPIED       |
| 5     | WP69    | 2026 Jun 04 06:51:57 PM | OCCUPIED       |
| 6     | WP67    | 2026 Jun 04 07:04:38 PM | VACANT         |
| 7     | V167    | 2026 Jun 04 03:37:15 PM | OCCUPIED       |
| 8     | SV75    | 2026 Jun 04 05:54:32 PM | VACANT         |
| 9     | HO281B  | 2026 Jun 04 06:01:53 PM | OCCUPIED       |
+-------+---------+-------------------------+----------------+
```

### Extraction Using Socrata API

While `pipeline.py` exemplifies a more generic example of data extraction from a .csv endpoint, the datasets I want to use from LADOT Open Data are powered by [Socrata Open Data API](https://dev.socrata.com/), so I will need my final version to implement with the [sodapy package](https://github.com/afeld/sodapy).

You can reference my `pipeline-sodapy.py` file. A couple notes on that set of code:

1. We're calling the `.get_all()` method to read data over all the results before chunking. We'll see how that performs when reading over 25 million rows for the "Parking Citations" dataset, but for now this works for what we need.

2. We write a function to yield successive chunks of the results as a list we can convert into a DataFrame

```
# Notice new imports at the top of pipeline-sodapy.py
...
from sodapy import Socrata
from itertools import islice
import json

...
def chunked_iterable(iterable, size = 1000):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk
```

3. The main difference with this `pipeline-sodapy.py` script here is we're converting the data into a dataframe _after_ chunking rather than before as we did in `pipeline.py`. The intention is to prevent an out-of-memory crash when we need to run 25 million rows of data. 

A second "fool proof" catch we need to consider for when we run the other datasets, we need to flatten data types that are lists and dictionaries before we insert them into the db.

```
...
for chunk in chunked_iterable(results):
    # Convert the chunk to a DataFrame
    df_chunk = pd.DataFrame.from_records(chunk)
    
    df_chunk = df_chunk.map(
        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x # flatten dictionaries and lists into string for to_sql()
    )
    ...
```

4. Let's see how this works out. We run the script using original the meter occupancy dataset.

```
uv run python pipeline-sodapy.py
```

Now, let's switch out the dataset ID and dataset name for parking citations

```
dataset_identifier = "4f5p-udkv"
dataset_name = "parking_citations"
```

Run the script again

```
uv run python pipeline-sodapy.py
```

Beautiful! You should see it inserting thousands of rows into your local PostgreSQL. Quite literally, thousands of these in your terminal output:

```
Inserted: 1000
Inserted: 1000
Inserted: 1000
```

Let's just double check in the local db. We'll only look at a few columns and rows at a time (__*important*__). In your terminal within the `pipeline/` directory:

```
export $(xargs < .env)
uv run pgcli -h localhost -p $POSTGRES_PORT -u $POSTGRES_USER -d $POSTGRES_D
# enter password

\dt
select index, ticket_number, fine_amount, geocodelocation from parking_citations limit 10;
```

Outout should look something like:

```
la_meter_parking_db> select index, ticket_number, fine_amount, geocodelocation from parking_ci
 tations limit 10;
+-------+---------------+-------------+----------------------------------------------------------------+
| index | ticket_number | fine_amount | geocodelocation                                                |
|-------+---------------+-------------+----------------------------------------------------------------|
| 0     | 4602073232    | 68          | {"type": "Point", "coordinates": [-118.29959251, 34.0382668]}  |
| 1     | 4601302834    | 68          | {"type": "Point", "coordinates": [-118.4310588, 34.16337]}     |
| 2     | 4601978496    | 63          | {"type": "Point", "coordinates": [-118.260923, 34.05286752]}   |
| 3     | 4602159520    | 68          | {"type": "Point", "coordinates": [-118.30409538, 33.78880056]} |
| 4     | 4602061811    | 68          | {"type": "Point", "coordinates": [-118.59280069, 34.20887326]} |
| 5     | 4601664760    | 63          | {"type": "Point", "coordinates": [-118.27405053, 34.05840158]} |
| 6     | 4602043423    | 93          | {"type": "Point", "coordinates": [-118.20276478, 34.05680302]} |
| 7     | 4602639440    | 68          | {"type": "Point", "coordinates": [-118.17533122, 34.06184663]} |
| 8     | 4602094615    | 58          | {"type": "Point", "coordinates": [-118.25296108, 34.04697011]} |
| 9     | 4601664771    | 63          | {"type": "Point", "coordinates": [-118.27509548, 34.04401265]} |
+-------+---------------+-------------+----------------------------------------------------------------+
SELECT 10
```

Wonderful, it's looking good. You can stop your `pipeline-sodapy.py` execution or allow it to keep running. I'm going to stop it since I'm ready to move to the next step.

## Back to main

Excellent, you can [continue back at the main project](../README.md).