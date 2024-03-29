import json
import logging
import os
from collections import defaultdict
from datetime import datetime

import urllib3
from loguru import logger

from extract.parser import atom_parse, xml_parse
from extract.utils import (
    PropertyTree,
    apply_nested,
    deep_get,
    dict_to_obj_tree,
    partial_format,
)
from load.base import DataWarehouse

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import urllib.parse

import backoff
import requests
from dotenv import load_dotenv
from requests_cache import CacheMixin
from requests_cache.backends.filesystem import FileCache
from requests_ratelimiter import LimiterMixin

load_dotenv()
logger = logging.getLogger("scaper")
logger.setLevel(logging.DEBUG)

logging.getLogger("requests_cache").setLevel("DEBUG")

CACHE_DIR = os.environ["CACHE_DIR"]


class NoResultException(Exception):
    pass


class EndOfPaginationException(Exception):
    pass


class CachedLimiterSession(CacheMixin, LimiterMixin, requests.Session):
    """Session class with caching and rate-limiting behavior. Accepts arguments for both
    LimiterSession and CachedSession.
    """


class LimitedSession(LimiterMixin, requests.Session):
    """Session class with caching and rate-limiting behavior. Accepts arguments for both
    LimiterSession and CachedSession.
    """


class FileCacheWithPostgres(FileCache):
    def save_response(self, response, cache_key=None, expires=None):
        logger.debug("cache_key", cache_key)
        super().save_response(response, cache_key, expires)


file_cache_backend = FileCacheWithPostgres(
    cache_name=CACHE_DIR,
)


class CustomSession(LimitedSession):
    """https://stackoverflow.com/a/51026159"""

    def __init__(self, rate_limit=5, prefix_url=None, headers={}, *args, **kwargs):
        super().__init__(
            per_second=rate_limit,  # 100 requests per second max
            cache_name=CACHE_DIR,
            use_cache_dir=False,
            backend=file_cache_backend,
            serializer="json",
            *args,
            **kwargs,
        )
        self.prefix_url = prefix_url
        self.headers.update(headers)

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5,
    )
    def request(self, method, *args, **kwargs):
        if "params" in kwargs:
            # Avoid encoding
            # https://stackoverflow.com/a/23497912/2131871
            kwargs["params"] = urllib.parse.urlencode(kwargs["params"], safe=":+")
        print(method, self.prefix_url, args, kwargs)
        kwargs["url"] = self.prefix_url + kwargs["url"]  # urljoin(self.prefix_url, url)
        return super(CustomSession, self).request(method, *args, **kwargs, verify=False)


class File:
    def save_state(self, id, state):
        with open(f"store/state_{id}.json", "w+") as f:
            f.write(json.dumps(state))

    def load_state(self, id):
        try:
            with open(f"store/state_{id}.json", "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error('FileNotFoundError: "store/state_%s.json"', id)
            return {}


class Crawler:
    def __init__(self, config, debug=False, loader=lambda x: x, memory=File):
        self.debug = debug
        self.config = config
        self.session = CustomSession(
            rate_limit=config.select("rate_limit", 20),
            crawler=self,
            prefix_url=config.host,
            headers=config.select("headers", {}),
        )
        self.retrievers = [
            type2class(entity.type)(self, entity) for entity in self.config.routes
        ]
        self.loader = loader
        self.memory = memory
        logger.debug(f"crawler host: {config.host}")

    def run(self):
        logger.info(f"Start crawling {self.config.id}")
        # Take all routes from config and run them

        for retriever in self.retrievers:
            retriever.start()

    def _fetch(self, method, **attributes):
        try:
            logger.info(method, attributes)

            if hasattr(self.config, "params"):
                # Iterate on nested dict and format
                attributes = apply_nested(
                    attributes,
                    lambda x: partial_format(x, **self.config.params.dict()),
                )

            response = self.session.request(method, **attributes)
            response.raise_for_status()
        except Exception as err:
            logger.error(err)
            logger.error(err.response)
            raise
        return response

    def export_item(self, entity, item):
        self.loader.load(self.config.id, entity, item)

    def add_items(self, entity, items):
        self.loader.load(self.config.id, entity, items)
        for item in items:
            for retriever in self.retrievers:
                retriever.check_item(entity, item)


# Create class retriever / route design / ... ?
class Strategy:
    def __init__(self, crawler, config):
        self.crawler = crawler
        self.config = config  # config could be just the name ?
        self.params = {}  # custom params
        self.state = {}

    def start(self):
        self.load_state()
        if hasattr(self.config, "dependencies"):
            return
        return self._start()

    def _start():
        raise NotImplementedError

    def save_state(self):
        self.crawler.memory.save_state(self.config.id, self.state)

    def load_state(self):
        self.state = self.crawler.memory.load_state(self.config.id)

    def check_item(self, entity, item):
        for dep in getattr(self.config, "dependencies", []):
            if dep.entity == entity:
                value = item[dep.entity_key]
                key = dep.key

                # if key in self.config.params:
                #     pass
                self.params[key] = value

                self._start()

    def add_items(self, items):
        for item in items:
            item.update(self.params)
        self.crawler.add_items(self.config.entity, items)

    def _fetch(self, **attributes):
        method = getattr(
            self.config, "method", "GET"
        )  # TO HAVE IT EXPLICITELY IN CONFIG
        # Iterate on nested dict and format
        attributes = apply_nested(
            attributes,
            lambda x: partial_format(x, **self.params),
        )
        response = self.crawler._fetch(method, **attributes)

        if self.config.format == "atom:1.0":  # should be moved ?
            return atom_parse(response.text)
        if self.config.format == "xml":
            return xml_parse(response.text)
        return response.json()


class Looping(Strategy):
    max_value = None  # To Save and update ?

    def start(self):
        max_value = self._fetch_max_value()
        logger.debug(f"max_value: {max_value}")  # TODO: to save
        for ind, value in enumerate(range(max_value, 0, -1)):
            if self.crawler.debug and ind > 3:
                break
            # Safety measure: stop if too many requests
            if ind > 1000:
                raise Exception("Too many requests")
            url = self.config.request.url.format(value)
            result = self._fetch(url=url)

            self.add_items([result])

    def _fetch_value(self, data, path):
        """TODO"""
        return data

    def _fetch_max_value(self):
        res = self._fetch(url=self.config.max_value.url)
        value = self._fetch_value(res, self.config.max_value.key_path)
        return value


class DirectFetch(Strategy):
    def _start(self):
        url = self.config.request.url
        result = self._fetch(url=url)
        self.add_items([result])


class List(Strategy):
    """Listing without pagination"""

    def _start(self):
        response = self._fetch(url=self.config.request.url)
        results = deep_get(response, self.config.key)
        self.add_items(results)


class Listing(Strategy):
    """Listing with pagination"""

    @backoff.on_exception(
        backoff.expo,
        NoResultException,
        max_tries=3,
    )
    def _fetch_list(self, cursor=None):
        request_attributes = getattr(self.config, "request", PropertyTree()).dict()
        if hasattr(self.config, "pagination") and cursor is not None:  # if pagination
            type = self.config.pagination.type
            if type not in request_attributes:
                request_attributes[type] = {self.config.pagination.key: cursor}
            if isinstance(request_attributes[type], dict):
                request_attributes[type][self.config.pagination.key] = cursor
            elif isinstance(request_attributes[type], str):
                request_attributes[type] = request_attributes[type].format(
                    cursor=cursor
                )
            else:
                raise Exception('Request params should be "dict" or "str"')

        response = self._fetch(**request_attributes)
        results = deep_get(response, self.config.key)
        self.add_items(results)

        if hasattr(self.config.pagination, "stop_func"):
            logger.debug(f"evaluate with stop func: {self.config.pagination.stop_func}")
            # TODO: should probably wrap in its own function to enforce params
            should_stop = eval(self.config.pagination.stop_func)
            logger.debug(f"should_stop: {should_stop}")
            if should_stop:
                raise EndOfPaginationException()

        if not results:
            raise NoResultException("No results")

        if hasattr(self.config.pagination, "ref_func"):
            cursor = eval(self.config.pagination.ref_func)
        elif hasattr(self.config.pagination, "ref"):
            cursor = deep_get(response, self.config.pagination.ref)
        elif hasattr(self.config.pagination, "step"):
            cursor += self.config.pagination.step
        else:
            cursor += len(results)

        return cursor

    def _start(self):
        cursor = self.state.get("cursor") or getattr(
            self.config.pagination, "default", None
        )

        # Safety measure: we don't want to loop forever
        ind = -1
        while True:
            ind += 1

            if self.crawler.debug and ind > 3:
                break

            try:
                cursor = self._fetch_list(cursor)
            except (NoResultException, EndOfPaginationException):
                break

            self.state["cursor"] = cursor
            self.save_state()

            if not cursor:
                break


class Slicing(Listing):
    """Slice with date"""

    from_date = datetime(1900, 1, 1)
    to_date = datetime(9999, 12, 31)  # datetime.now()

    def format_date(self, date):
        return date.strftime(self.config.slice.date_format)

    def _start(self):
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

        return super(Slicing, self)._start()


def type2class(type):
    if type == "Looping":
        return Looping
    elif type == "DirectFetch":
        return DirectFetch
    elif type == "Listing":
        return Listing
    elif type == "Slicing":
        return Slicing
    elif type == "List":
        return List
    else:
        raise NotImplementedError


def runner(config: dict, target: str, debug=False, memory=File(), params={}):
    if params:
        config = apply_nested(
            config,
            lambda x: partial_format(x, **params),
        )
    loader = DataWarehouse(target)
    config_tree = dict_to_obj_tree(config)
    crawler = Crawler(config_tree, debug=debug, memory=memory, loader=loader)
    crawler.run()
