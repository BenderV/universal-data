# The Universal Data Connector

Automatically generate connectors using AI.

### Structure

- `sources/`: the config files for sources
- `move.py`: functions to export data
- `parser.py`: api response parser (json, xml, ...)
- `run.py`: script to run the program
- `scraper.py`: all the scraping methods
- `utils.py`: utils fonctions

### Usage

`python run.py sources/hacker_news.yml`
