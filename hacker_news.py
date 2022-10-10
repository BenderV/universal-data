import scraper

CONFIG = {
    "host": "https://hacker-news.firebaseio.com/v0",
    "entities": [
        {
            "name": "Item",
            "format": "json",
            "type": "Looping",
            # "schema": {"by": {"relation": "User", "key": "id"}},
            "request": {
                "url": "/item/{0}.json",
            },
            "max_value": {
                "type": "Int",
                "url": "/maxitem.json",
                "key_path": None,  # direct
            },
        },
        {
            "name": "User",
            "format": "json",
            "type": "DirectFetch",
            "request": {
                "url": "/user/{0}.json",
            },
            "id": {"entity": "Item", "key": "by"},
        },
    ],
}

if __name__ == "__main__":
    scraper.runner(CONFIG)
