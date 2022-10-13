import json
import logging
from collections import defaultdict
from datetime import datetime
from parser import atom_parse

import urllib3
import yaml
from requests import Session

# from move import transfert
from utils import PropertyTree, deep_get, dict_to_obj_tree, partial_format

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import urllib.parse

logger = logging.getLogger("scaper")
logger.setLevel(logging.DEBUG)


class LiveServerSession(Session):
    """https://stackoverflow.com/a/51026159"""

    def __init__(self, prefix_url=None, headers={}, *args, **kwargs):
        super(LiveServerSession, self).__init__(*args, **kwargs)
        self.prefix_url = prefix_url
        self.headers.update(headers)

    def request(self, method, url, *args, **kwargs):
        if "params" in kwargs:
            # Avoid encoding
            # https://stackoverflow.com/a/23497912/2131871
            kwargs["params"] = urllib.parse.urlencode(kwargs["params"], safe=":+")
        url = self.prefix_url + url  # urljoin(self.prefix_url, url)
        return super(LiveServerSession, self).request(
            method, url, *args, **kwargs, verify=False
        )


class Crawler:
    def __init__(self, config):
        self.items = defaultdict(list)  # FETCHED
        self.config = config
        self.session = LiveServerSession(
            config.host, headers=config.select("headers", {})
        )
        self.retrievers = [
            type2class(entity.type)(self, entity) for entity in self.config.entities
        ]
        logger.debug(f"crawler host: {config.host}")

    def run(self):
        # Take all routes from config and run them

        for retriever in self.retrievers:
            retriever.start()

        print(self.items)

    def _fetch(self, method, **attributes):
        try:
            logger.info(method, attributes)

            # dumps = json.dumps(attributes)
            # dumps2 = partial_format(attributes, **self.config.params.dict())
            # attributes2 = json.loads(dumps2)
            response = self.session.request(method, **attributes)
            response.raise_for_status()
        except Exception as err:
            logger.error(err)
            logger.error(err.response.text)
            raise
        return response

    def add_item(self, entity, item):
        self.items[entity].append(item)
        for retriever in self.retrievers:
            retriever.check_item(entity, item)


# Create class retriever / route design / ... ?
class Strategy:
    def __init__(self, crawler, config):
        self.crawler = crawler
        self.config = config  # config could be just the name ?

    def start(self):
        raise NotImplementedError

    def check_item(self, entity, item):
        for dep in getattr(self.config, "dependencies", []):
            if dep.entity == entity:
                value = item[dep.entity_key]
                key = dep.key
                self.run({key: value})

    def add_item(self, item):
        self.crawler.add_item(self.config.entity, item)

    def _fetch(self, **attributes):
        method = getattr(
            self.config, "method", "GET"
        )  # TO HAVE IT EXPLICITELY IN CONFIG
        response = self.crawler._fetch(method, **attributes)

        if self.config.format == "atom:1.0":  # should be moved ?
            return atom_parse(response.text)
        return response.json()


class Looping(Strategy):
    max_value = None  # To Save and update ?

    def start(self):
        print("run looping")
        max_value = self._fetch_max_value()
        logger.debug(f"max_value: {max_value}")  # TODO: to save
        for ind, value in enumerate(range(max_value, 0, -1)):
            if ind > 2:
                break  # Testing purpose
            url = self.config.request.url.format(value)
            result = self._fetch(url=url)
            self.add_item(result)

    def _fetch_value(self, data, path):
        """TODO"""
        return data

    def _fetch_max_value(self):
        res = self._fetch(url=self.config.max_value.url)
        value = self._fetch_value(res, self.config.max_value.key_path)
        return value


class DirectFetch(Strategy):
    def start(self):
        pass

    def run(self, params):
        url = self.config.request.url.format(**params)
        result = self._fetch(url=url)
        self.add_item(result)


class List(Crawler):
    """Listing without pagination"""

    def start(self):
        response = self._fetch(url=self.config.request.url)
        results = deep_get(response, self.config.key)
        for result in results:
            self.add_item(result)


class Listing(Strategy):
    """Listing with pagination"""

    def _fetch_list(self, cursor=None):

        request_attributes = getattr(self.config, "request", PropertyTree()).dict()
        if hasattr(self.config, "pagination") and cursor is not None:  # if pagination
            type = self.config.pagination.type
            if isinstance(request_attributes[type], dict):
                request_attributes[type][self.config.pagination.key] = cursor
            elif isinstance(request_attributes[type], str):
                request_attributes[type] = request_attributes[type].format(
                    cursor=cursor
                )
            else:
                raise Exception('Request params should be "dict" or "str"')

        print(request_attributes)
        response = self._fetch(**request_attributes)
        results = deep_get(response, self.config.key)
        for result in results:
            self.add_item(result)
        return response

    def start(self):
        cursor = getattr(self.config.pagination, "default", None)

        for ind in range(1, 3):  # Testing purpose
            response = self._fetch_list(cursor)
            if hasattr(self.config.pagination, "ref"):
                cursor = deep_get(response, self.config.pagination.ref)
            else:
                cursor = (1 + ind) * self.config.pagination.step
            if not cursor:
                break


class Slicing(Listing):
    """Slice with date"""

    from_date = datetime(1900, 1, 1)
    to_date = datetime.now()

    def format_date(self, date):
        return date.strftime(self.config.slice.date_format)

    def start(self):
        request_attributes = getattr(self.config, "request", PropertyTree()).dict()
        params = self.config.slice
        if isinstance(request_attributes[params.type], str):
            request_attributes[params.type] = partial_format(
                request_attributes[params.type],
                from_date=self.format_date(self.from_date),
                to_date=self.format_date(self.to_date),
            )
            self.config.request = dict_to_obj_tree(request_attributes)
        else:
            raise Exception('Request params should be "dict" or "str"')

        return super(Slicing, self).start()


def type2class(type):
    if type == "Looping":
        return Looping
    elif type == "DirectFetch":
        return DirectFetch
    elif type == "Listing":
        return Listing
    elif type == "Slicing":
        return Slicing
    else:
        raise NotImplementedError


def runner(config):
    config = dict_to_obj_tree(config)
    crawler = Crawler(config)
    crawler.run()
