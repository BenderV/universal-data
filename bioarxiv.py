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
            "name": "Articles",
            "format": "json",
            "type": "Listing",
            # "variant": {
            #     "server": ["bioarxiv", "medrxiv"],
            # },
            "key": "collection",
            # "date_format": "YYYY-MM-DD",
            "method": "GET",
            "request": {
                "url": "/pubs/biorxiv/1900-01-01/2022-10-10/{cursor}/json",
                # "url": "/pubs/bioarxiv/{from}-{to}/{cursor}/json",
                # "server": 2,  # default
                # "from": "2020-03-01",
                # "to": "2021-03-01",
                # "to": {
                #     "_value": ..
                #     "_format": ..
                # ... ?
                # }
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
