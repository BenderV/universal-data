"""
Response is XML
Documentation: https://api.biorxiv.org/
https://api.biorxiv.org/pubs/medrxiv/2020-03-01/2020-03-30/5
"""

import scraper

CONFIG = {
    "host": "https://api.biorxiv.org",
    "entities": [
        {
            "name": "Biorxiv Articles",
            "entity": "articles",
            "format": "json",
            "type": "Slicing",
            "key": "collection",
            "method": "GET",
            "request": {
                "url": "/pubs/biorxiv/{from_date}/{to_date}/{cursor}/json",
            },
            "slice": {
                "date_format": "%Y-%m-%d",
                "type": "url",
            },
            "pagination": {
                "default": 0,
                "step": 100,
                # "ref": "collection",  # collection, so we will take len(collection)
                "key": "cursor",
                "type": "url",
            },
        },
        {
            "name": "Medrxiv Articles",
            "entity": "articles",
            "format": "json",
            "type": "Slicing",
            "key": "collection",
            "method": "GET",
            "request": {
                "url": "/pubs/medrxiv/{from_date}/{to_date}/{cursor}/json",
            },
            "slice": {
                "date_format": "%Y-%m-%d",
                "type": "url",
            },
            "pagination": {
                "default": 0,
                "step": 100,
                # "ref": "collection",  # collection, so we will take len(collection)
                "key": "cursor",
                "type": "url",
            },
        },
    ],
}


if __name__ == "__main__":
    scraper.runner(CONFIG)
