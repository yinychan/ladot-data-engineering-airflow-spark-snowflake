import requests
import os
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from tqdm.auto import tqdm #to monitor import progress

#1. set up variables to insert into request
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

#2. retrieve data from API
meter_occupancy_response = requests.post(meter_occupancy_csv, headers=headers, json=payload_csv, timeout=100)
meter_occupancy_response.raise_for_status() # Check if the request was successful


#5. connect to the database
psql_user = os.getenv("POSTGRES_USER")
psql_password = os.getenv("POSTGRES_PASSWORD")
psql_db = os.getenv("POSTGRES_DB")
psql_port = os.getenv("POSTGRES_PORT", "5432") # default to 5432 if not set
psql_host = os.getenv("POSTGRES_HOST", "localhost") # default to localhost if not set

engine = create_engine(
    f"postgresql://{psql_user}:{psql_password}@{psql_host}:{psql_port}/{psql_db}"
)

#6. inserting data
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