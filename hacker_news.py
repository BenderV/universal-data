import scraper

CONFIG = {
    "host": "https://hacker-news.firebaseio.com/v0",
    "entities": [
        {
            "name": "Item",
            "type": "Looping",
            "path": "/item/{0}.json",
            # "schema": {"by": {"relation": "User", "key": "id"}},
            "max_value": {
                "type": "Int",
                "url": "/maxitem.json",
                "key_path": None,  # direct
            },
        },
        {
            "name": "User",
            "type": "DirectFetch",
            "path": "/user/{0}.json",
            "id": {"entity": "Item", "key": "by"},
        },
    ],
}

scraper.runner(CONFIG)
