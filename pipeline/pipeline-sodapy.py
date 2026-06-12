import requests
import os
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from sodapy import Socrata
from itertools import islice
import json

#1. set up variables to insert into request 
app_token = os.getenv("APP_TOKEN")

# https://github.com/afeld/sodapy# The Socrata Open Data API (SODA) client library for Python provides a convenient way to interact with Socrata datasets.
client = Socrata("data.lacity.org", app_token)
dataset_identifier = "e7h6-4a3e" # This is the identifier for the meter occupancy dataset on data.lacity.org
dataset_name = "meter_occupancy" # This is the name of the table we will create in our database

# dataset_identifier = "s49e-q6j2" # This is the identifier for the Metered Parking Inventory & Policies dataset on data.lacity.org
# dataset_name = "meter_inventory_policies" # This is the name of the table we will create in our database

# dataset_identifier = "4f5p-udkv" # This is the identifier for the parking citations dataset on data.lacity.org
# dataset_name = "parking_citations" # This is the name of the table we will create in our database


#2. retrieve data from API using sodapy client
results = client.get_all(dataset_identifier) # This will retrieve all records from the dataset. For large datasets, you may want to use pagination or filtering to retrieve data in chunks.

#3. connect to the database
psql_user = os.getenv("POSTGRES_USER", "root")
psql_password = os.getenv("POSTGRES_PASSWORD", "root")
psql_db = os.getenv("POSTGRES_DB", "la_meter_parking_db")
psql_port = os.getenv("POSTGRES_PORT", "5432") # default to 5432 if not set
psql_host = os.getenv("POSTGRES_HOST", "localhost") # default to localhost if not set

engine = create_engine(
    f"postgresql://{psql_user}:{psql_password}@{psql_host}:{psql_port}/{psql_db}"
)

#4. We need to chunk the results into smaller pieces to avoid memory issues when converting to a DataFrame. Let's say we want to chunk it into pieces of 1000 records.
def chunked_iterable(iterable, size = 1000):
    """
    Yields successive chunks of a specified size from an iterable.
    """
    it = iter(iterable) # converts our data into an iterator object
    while True: # initiate an infinite loop to keep yielding chunks until we exhaust the iterator
        chunk = list(islice(it, size)) # use islice to take the next successive chunk from the iterator and convert it to a list. We need to pass a list downstream to convert to a DataFrame, so we convert the islice object to a list here.
        if not chunk: # exit the loop if the chunk is empty
            break
        yield chunk

#5. Now we can iterate over the chunked results and insert them into the database in batches. This will help manage memory usage and also allow us to monitor progress more effectively.
first = True

for chunk in chunked_iterable(results):
    # Convert the chunk to a DataFrame
    df_chunk = pd.DataFrame.from_records(chunk)

    df_chunk = df_chunk.map(
        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x # convert any nested dictionaries or lists to JSON strings to avoid issues when inserting into the database
    )

    if first:
        df_chunk.head(0).to_sql(
            name=dataset_name,
            con=engine,
            if_exists="replace"
        )
        first = False
        print("Table created")

    # Insert chunk
    df_chunk.to_sql(
        name=dataset_name,
        con=engine,
        if_exists="append"
    )

    print("Inserted:", len(df_chunk))
