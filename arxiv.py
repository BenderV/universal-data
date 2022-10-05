"""
Response is XML
Documentation: https://arxiv.org/help/api/user-manual
"""

import scraper

CONFIG = {
    "host": "http://export.arxiv.org/api",
    "entities": [
        {
            "name": "Articles",
            "format": "atom:1.0",
            "type": "Listing",
            "path": "/query",
            "key": "entries",
            "method": "GET",
            "request": {
                "params": {
                    "max_results": 2,  # default
                    "search_query": "cat:cs.CV+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.AI+OR+cat:cs.NE+OR+cat:cs.RO",  # ⚠️ it's mandatory
                    "sortBy": "lastUpdatedDate",
                    "sortOrder": "descending",
                }
            },
            "pagination": {
                "step": 2,  # should be len(result) ?
                "key": "start",
                "type": "params",
            },
        },
    ],
}

scraper.runner(CONFIG)
