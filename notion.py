import scraper

CONFIG = {
    "host": "https://api.notion.com/v1",
    "headers": {
        # Shoud be separated
        # "Authorization": "Bearer {token}",
        "Content-Type": "application/json",
        "Authorization": "Bearer secret_zBAolxSkAC1Jh8sKzIguaRQ511FWZ1ViyAJjRwG6I7f",
        "Notion-Version": "2022-06-28",
    },
    "entities": [
        {
            "name": "User",
            "format": "json",
            "type": "Listing",
            "key": "results",
            "method": "GET",
            "request": {
                "url": "/users",
                "params": {
                    "page_size": 1,
                },
            },
            "pagination": {
                "ref": "next_cursor",
                "key": "start_cursor",
                "type": "params",
            },
        },
        {
            "name": "Pages",
            "format": "json",
            "type": "Listing",
            "key": "results",
            "method": "POST",
            "request": {
                "url": "/search",
                "json": {
                    "page_size": 2,
                    "sort": {
                        "direction": "ascending",
                        "timestamp": "last_edited_time",
                    },
                },
            },
            "pagination": {
                "ref": "next_cursor",
                "key": "start_cursor",
                "type": "json",
            },
        },
    ],
}

if __name__ == "__main__":
    scraper.runner(CONFIG)
