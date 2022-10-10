import scraper
from arxiv import CONFIG as ARXIV_CONFIG
from bioarxiv import CONFIG as BIOARXIV_CONFIG
from hacker_news import CONFIG as HN_CONFIG
from notion import CONFIG as NOTION_CONFIG

scraper.runner(ARXIV_CONFIG)
scraper.runner(BIOARXIV_CONFIG)
scraper.runner(HN_CONFIG)
scraper.runner(NOTION_CONFIG)
