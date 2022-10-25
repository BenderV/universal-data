# The Universal Data Connector

Automatically generate connectors using AI.

### Structure

- `extract/`
  - `parser.py`: api response parser (json, xml, ...)
  - `scraper.py`: all the scraping methods
  - `utils.py`: utils fonctions
- `transform/`:
  - `model.py`: functions for modelisation / normalization
- `load/`
  - `base.py`: sqlalchemy model to load data into the destination
- `sources`: the config files for sources
- `store`: tempory directoy to store data
- `database.py`: utils for database/datawarehouse
- `run.py`: script to run the program
- `server.py`: server to manage all pipelines

### Usage

Run direct config

- `python run.py sources/hacker_news.yml --target postgresql+psycopg2://localhost:5432/universal-load`

Run with database config

- `python server.py`

## Strategies

### Slicing

Take into account errors, request time, doc suggestion

#### Max Range (pagination + ordered)

Search like.
`from` should be at the minimum, `to` to today.
Result is ordered/paginated with latest result updated first.
It's similar to a search method

- Largest interval possible

#### Unordered (pagination + no order)

The response have pagination but it's not ordered so we may want to have smaller interval

- Stock => maximum
- Flux => from last time only

#### Result Limit (no pagination)

The response can't contain "X" number of result and so the pagination should reflect that.

- Large interval to small
