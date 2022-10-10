import scraper

CONFIG = {
    "host": "https://api.typeform.com",
    "headers": {
        # Shoud be separated
        "Content-Type": "application/json",
        "Authorization": "Bearer tfp_geY4SoC6xQwEDQ1R1TTsrakfPsekL6zQXmK2pmyEF2o_h29T837UjtJs",
    },
    "entities": [
        {
            "name": "Forms",
            "entity": "forms",
            "format": "json",
            "type": "Listing",
            "key": "items",
            "method": "GET",
            "request": {
                "url": "/forms",
                "params": {
                    "page_size": 200,
                    "sort_by": "last_updated_at",
                    "order_by": "desc",
                },
            },
            "pagination": {
                "default": 1,
                "step": 1,
                "key": "page",
                "type": "params",
            },
        },
    ],
}

if __name__ == "__main__":
    scraper.runner(CONFIG)
