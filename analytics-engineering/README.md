## Analytics Engineering

A few things we'll be working with in this stage: 

1. Since our data is already inside Snowflake, we'll use dbt-snowflake as the adapter that connects dbt with Snowflake.
2. we'll be modeling with star schema using dimension and fact tables. Look back at [Data Denormalization from our Data Warehouse section of the pipeline](/warehouse/README.md#data-denormalization)

## Contents

- [Planning](#planning)
    - [Dimension and Fact Tables](#dimension-and-fact-tables)
    - [Setting up dbt](#setting-up-dbt-data-build-tool)
    - [Initialize a new project](#initialize-a-new-project)
    - [Create File Structure](#create-file-structure)
- [Staging](#staging)
- [Dimension Tables](#dimension-tables)
    - [Execute](#execute)
    - [Repeat with remaining dimension tables](#repeat-with-remaining-dimension-tables)
- [Fact Tables](#fact-tables)

## Planning

We can reference our batch processing where we set up our [Spark Data Schema for staging](/batch-processing/README.md#spark-data-schema)

### Dimension and Fact Tables

After looking at our original data structure, let's also see how we'd want our dimension tables and fact tables to be structured for future anaytics tasks.

Dims:
- dim_vehicle (GROUP BY or SELECT DISTINCT to deduplicate)
    - vehicle_key MD5 primary key
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
    - agency_key VARCHAR PRIMARY KEY (agency field from source)
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

- dim_violation (GROUP BY or SELECT DISTINCT to deduplicate)
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

### Setting up dbt (Data build tool)

First, make sure you have uv set up

```
cd /analytics-engineering
uv --version
```

You'll see an output similar to `uv 0.11.18 ...`. You're ready to add `dbt-snowflake`.

```
uv init
uv add dbt-snowflake
```

### Initialize a new project

Let's make sure everything is good to go:

```
uv run dbt --version
```

While initializing, my version of dbt encountered an error that required me to pin a previous version of Python (I'm on dbt version v1.11, requiring Python version Python 3.13 or earlier). [Check your version compatibility before moving foward](https://docs.getdbt.com/faqs/Core/install-python-compatibility?version=2.0&name=Fusion).


Scaffold a fresh project using your preferred project name. I will name mine `ladot_transformations`.

```
uv run dbt init ladot_transformations
```

You will be prompted with a number of questions from your terminal. Here they are so you can gather the information ahead of time.

```
Enter a number: # choose [1] for snowflake
account (https://<this_value>.snowflakecomputing.com):
user (dev username):
[1] password
[2] keypair
[3] sso
Desired authentication type option (enter a number):
private_key_path (path/to/private.key): 
role (dev role): 
warehouse (warehouse name): 
database (default database that dbt will build objects in):
schema (default schema that dbt will build objects in):
threads (1 or more) [1]: # for local development, we'll stick with 1. Press enter
```

To generate a private key, navigate to the directory where you want to store your private key. This is what I did for myself:

In a new terminal tab, run:

```
cd ~/.dbt/
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out dbt_snowflake_key.p8 -nocrypt
openssl rsa -in dbt_snowflake_key.p8 -pubout -out dbt_snowflake_key.pub
cat dbt_snowflake_key.pub
```

Copy the entire text block that begins with `-----BEGIN PUBLIC KEY-----` and ends with `-----END PUBLIC KEY-----`. 

Go to your Snowflake Workspace and run this single line:

```
ALTER USER yinychan SET RSA_PUBLIC_KEY='-----BEGIN PUBLIC KEY-----
ABCDEFGHIJKLMNOPQRSTUVWXYZ...
...
...
...
YOUR KEY SHOULD BE A LONG TEXT STRING
...
-----END PUBLIC KEY-----'
```

Your Snowflake console should say "Statement executed successfully." after you run the `SET RSA_PUBLIC_KEY` command.

Moving back to the terminal tab where you ran `uv run dbt init ...`, the value for `private_key_path` should equal something like `/Your/user/path/.dbt/dbt_snowflake_key.p8`.

Once initialized, you'll see a success message similar to `... Profile ladot_transformations written to /your-user-directory-path/.dbt/profiles.yml using target's profile_template.yml ...`. Excellent, your project is scaffolded.

Let's test out the connection:

```
uv run dbt debug
```

If it works, the output should include a line that says `Connection test: [OK connection ok]`.

### Create File Structure

We will roughly follow Snowflake's [example demo file structure](https://github.com/Snowflake-Labs/getting-started-with-dbt-on-snowflake/tree/main/tasty_bytes_dbt_demo). We want to create the following file structure:

```
ladot_transformations/
├── dbt_project.yml (created by default)
├── profiles.yml
└── models/
    ├── staging/
    │   ├── sources.yml
    │   ├── raw_meter_occupancy.sql
    │   ├── raw_parking_inventory_policies.sql
    │   └── raw_parking_citations.sql
    └── marts/
```

Go into your `/ladot_transformations/` directory and first create the `profiles.yml` file:

```
cd ladot_transformations/
touch profiles.yml
```

If you recall, in our "Initialize a new project" step, a `~/.dbt/profiles.yml` file was created from `dbt init`. Copy the contents of that file into this file: `ladot_transformations/profile.yml`.

Then, run the following command to ensure the connection works:

```
cd ladot_transformations/
uv run dbt debug
```

Your console output should show something like:

```
...
17:22:29    Connection test: [OK connection ok]
17:22:29  All checks passed!
```

## Staging

Our first objective is to build our foundational staging views to act as a buffer between the base tables and our final star table.

We'll be working with the 3 files we created in the `staging` folder with the `raw_` prefix. Since we have defined the data source in our `sources.yml` file, we continue with the [dbt documentation on selecting from a source](https://docs.getdbt.com/docs/build/sources?version=2.0&name=Fusion#selecting-from-a-source).

As an example, in `raw_meter_occupancy.sql`:

```
SELECT
    spaceid,
    eventtime,
    occupancystate
FROM {{ source('raw_ladot', 'staging_meter_occupancy') }}

```

Once similar SELECT queries are set up for the other tables `raw_parking_inventory_policies.sql` and `raw_parking_citations.sql`, we are ready to execute the scripts. Make sure you're in your dbt project directory

```
cd ladot_transformations/
uv run dbt run
```

Success is if your terminal output includes a message similar to these:

```
...
Finished running 3 view models in 0 hours 0 minutes and 4.84 seconds (4.84s).
Completed successfully
Done. PASS=3 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=3
...
```

## Dimension Tables (Gold)

Now that our staging views are established, we can start creating our [dimension tables](#dimension-and-fact-tables). We'll start with `dim_vehicle`.

```
touch models/marts/dim_vehicle.sql
```

If you don't already have a `packages.yml` file, create one:

```
cd ladot_transformations/
touch packages.yml
```

Then, add the following to your `packages.yml` file:

```
packages:
  - package: dbt-labs/dbt_utils
    version: 1.4.1
```

Install the package:

```
uv run dbt deps
```

For our `dim_vehicle.sql`, we will be working with this set of columns:

- vehicle_key (MD5 primary key)
- rp_state_plate
- plate_expiry_date
- make
- body_style_desc
- color_desc

We start by telling dbt to instruct Snowflake to build this dim table as a physical database table (rather than as a view). By materializing this dimension as a table, we calculate our vehicle profiles once during the batch run rather than re-reading raw staging data everytime we run a report if we leave it as a view.

```
# In dim_vehicle.sql
{{ config(
    materialized='table'
) }}
```

Let's break down the SELECT a little:

```
SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key([
        'rp_state_plate', 
        'plate_expiry_date', 
        'make',
        'body_style_desc',
        'color_desc'
    ]) }} as vehicle_key, 
    rp_state_plate, 
    plate_expiry_date, 
    make, 
    body_style_desc, 
    color_desc 
FROM {{ ref('raw_parking_citations') }} 
WHERE rp_state_plate IS NOT NULL 
  AND plate_expiry_date IS NOT NULL
```

1. `DISTINCT` instructs Snowflake to look at the entire row payload and strip away all identical rows so we are left with exactly one single row per unique vehicle profile.
2. We use `dbt_utils.generate_surrogate_key` to help us generate unique primary keys for this table.
3. We only retrieve rows that have values for `rp_state_plate` and `plate_expiry_date`

### Execute

Now, we run the SQL command in your terminal:

```
uv run dbt run --select dim_vehicle
```

It's executed successfully if you see a console message that includes:

```
...
Finished running 1 table model in 0 hours 0 minutes and 8.74 seconds (8.74s).
Completed successfully
Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
...
```

### Repeat with remaining dimension tables

The remaining files / tables will repeat the same process:

```
dim_location.sql
dim_meter.sql
dim_agency.sql
dim_date.sql
dim_time_of_day.sql
dim_violation.sql
```

## Fact Tables (Gold)

While their purpose is to connect our dimension tables, we create them the same way:

```
touch models/marts/fact_citation.sql
touch models/marts/fact_occupancy.sql
```

Reference my `fact_*.sql` files for SQL syntax. Then, execute:

```
uv run dbt run --select fact_citation fact_occupancy
```

Success! You should see this response in your terminal output:

```
...
Finished running 2 table models in 0 hours 1 minutes and 14.50 seconds (74.50s).
Completed successfully
Done. PASS=2 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=2
...
```

## Back to main

Excellent, you can [continue back at the main project](../README.md).