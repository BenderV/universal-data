import json
import logging
from collections import defaultdict
from datetime import datetime
from parser import atom_parse

from requests import Session

from move import transfert
from utils import PropertyTree, deep_get, dict_to_obj_tree, partial_format

FETCHED = defaultdict(list)


import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import urllib.parse

logging.basicConfig(level=logging.DEBUG)


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
    def __init__(self, config, session):
        self.config = config
        self.session = session

    def run():
        yield

    def _fetch(self, **attributes):
        method = getattr(
            self.config, "method", "GET"
        )  # TO HAVE IT EXPLICITELY IN CONFIG
        try:
            logging.debug(method, attributes)
            response = self.session.request(method, **attributes)
            response.raise_for_status()
        except Exception as err:
            logging.error(err)
            logging.error(err.response.text)
            raise

        if self.config.format == "atom:1.0":
            return atom_parse(response.text)
        return response.json()


class Looping(Crawler):
    max_value = None  # To Save and update ?

    def run(self):
        max_value = self._fetch_max_value()
        logging.debug("max_value", max_value)  # TODO: to save
        for ind, value in enumerate(range(max_value, 0, -1)):
            if ind > 2:
                break  # Testing purpose
            url = self.config.request.url.format(value)
            result = self._fetch(url=url)
            FETCHED[self.config.entity].append(result)
            yield result

    def _fetch_value(self, data, path):
        """TODO"""
        return data

    def _fetch_max_value(self):
        res = self.session.get(self.config.max_value.url)
        value = self._fetch_value(res.json(), self.config.max_value.key_path)
        return value


class List(Crawler):
    """Listing without pagination"""

    def run(self):
        response = self._fetch(url=self.config.request.url)
        results = deep_get(response, self.config.key)
        for result in results:
            FETCHED[self.config.entity].append(result)
        return response


class Listing(Crawler):
    """Listing with pagination"""

    def _fetch_list(self, cursor=None):
        # if pagination
        request_attributes = getattr(self.config, "request", PropertyTree()).dict()
        if hasattr(self.config, "pagination") and cursor is not None:
            type = self.config.pagination.type
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
        for result in results:
            FETCHED[self.config.entity].append(result)
        return response

    def run(self):
        cursor = getattr(self.config.pagination, "default", None)

        for ind in range(1, 3):  # Testing purpose
            response = self._fetch_list(cursor)
            if hasattr(self.config.pagination, "ref"):
                cursor = deep_get(response, self.config.pagination.ref)
            else:
                cursor = (1 + ind) * self.config.pagination.step
            if not cursor:
                break
            yield response


class Slicing(Listing):
    """Slice with date"""

    from_date = datetime(1900, 1, 1)
    to_date = datetime.now()

    def format_date(self, date):
        return date.strftime(self.config.slice.date_format)

    def run(self):
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

        return super(Slicing, self).run()


class DirectFetch(Crawler):
    def run(self):
        # Scan for task
        for name, result in FETCHED.items():
            if self.config.id.entity == name:
                # HN sent null some times... ?
                value = result[self.config.id.key]
                url = self.config.request.url.format(value)
                result = self._fetch(url=url)
                FETCHED[self.config.entity].append(result)
                yield result


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
    logging.debug(f"host: {config.host}")
    session = LiveServerSession(config.host, headers=config.select("headers", {}))

    crawlers = []
    for entity in config.entities:
        crawler = type2class(entity.type)(entity, session)
        crawlers.append(crawler)

    for crawler in crawlers:
        logging.debug(f"Run Crawnler: {crawler.config.name}")
        for result in crawler.run():
            pass

    logging.debug(FETCHED)
    # for name, rows in FETCHED.items():
    #    transfert(name, rows, "id")
    with open("output.json", "w") as f:
        json.dump(FETCHED, f)


# Handle response
# Parse
# Next actions
