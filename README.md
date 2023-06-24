# Universal Data

Universal Data is a lightweight ELT/ETL that's perfect for api data extraction and transformation.

## Special Features

-   Extract data from any api with a simple configuration
-   Auto-detection of data types
-   Transform data with simple python functions

## Architecture

Python scripts + Postgres (database to store pipeline configuration and data)

ELT-T (Extract - Load - Transform - Transfer)

-   Extract: python script to extract data from an api
-   Load: in a postgres database spin up for it (source\_{source_id})
-   Transform: base python script to infer data types and transform data
-   Transfer: (optional) python script to transfer data to a target database

### Structure

-   `extract/`
    -   `sources/`: the config files for sources
    -   `parser.py`: api response parser (json, xml, ...)
    -   `scraper.py`: all the scraping methods
    -   `utils.py`: utils fonctions
-   `load/`
    -   `base.py`: sqlalchemy model to load data into the destination
-   `transfer/`
    -   `transfer.py`: transfer data from source to destination
-   `transform/`
    -   `transformations/`: post-processing transformations for specific sources
    -   `model.py`: functions for modelisation / normalization
-   `database.py`: utils for database/datawarehouse
-   `run.py`: script to run a pipeline/task(s)
-   `server.py`: server to manage all pipelines

## Use

### Install

-   `pip install -r requirements.txt`
-   Modify .env file
-   Create database with `python database.py`

### Add pipeline / source / target

-   Everything can be manage directly on the PostgreSQL database

```sql
-- Create a new client
INSERT INTO "public"."clients"("id", "name") VALUES(1, 'default')

-- Create a new source
INSERT INTO "public"."sources"("id", "name", "config", "client_id")
    VALUES(1, 'hacker_news', NULL, 1)

-- Create a new target
INSERT INTO "public"."targets"("id", "type", "uri", "client_id")
    VALUES(1, 'universal-load', 'postgresql+psycopg2://localhost:5432/universal-load', 1)

-- Create a new pipeline
INSERT INTO "public"."pipelines"("source_id", "target_id", "active") VALUES(1, 1, TRUE)
```

### Create a new source config

-   Create a new file in `sources/`

### Run

Run server

`python server.py`

Run a pipeline

`python run.py 1 --extract --transform --transfer`
