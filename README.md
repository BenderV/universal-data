# The Universal Data Connector

Automatically generate connectors using AI.

### Structure

- `sources/`: the config files for sources
- `store/`: tempory directoy to store data
- `database.py`: utils for database/datawarehouse
- `load.py`: functions to load data into the destination
- `model.py`: functions for modelisation / normalization
- `parser.py`: api response parser (json, xml, ...)
- `run.py`: script to run the program
- `scraper.py`: all the scraping methods
- `utils.py`: utils fonctions

### Usage

`python run.py sources/hacker_news.yml`

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
