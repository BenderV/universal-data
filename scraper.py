import json
import re
from parser import atom_parse

from requests import Session

from utils import PropertyTree, deep_get, dict_to_obj_tree

FETCHED = []


import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import urllib.parse


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

    def _fetch(self, url, **kwargs):
        method = getattr(
            self.config, "method", "GET"
        )  # TO HAVE IT EXPLICITELY IN CONFIG
        if kwargs:
            attributes = kwargs
        else:
            attributes = getattr(self.config, "request", None)
            if attributes:
                attributes = attributes.dict()
            else:
                attributes = {}

        try:
            print(method, url, kwargs)
            response = self.session.request(method, url, **attributes)
            response.raise_for_status()
        except Exception as err:
            print(err)
            print(err.response.text)
            raise

        if self.config.format == "atom:1.0":
            return atom_parse(response.text)
        return response.json()


class Looping(Crawler):
    max_value = None  # To Save and update ?

    def run(self):
        max_value = self._fetch_max_value()
        print(max_value)  # TODO: to save
        for ind, value in enumerate(range(max_value, 0, -1)):
            if ind > 2:
                break  # Testing purpose
            url = self.config.path.format(value)
            result = self._fetch(url)
            FETCHED.append((self.config.name, result))
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
        response = self._fetch(self.config.path)
        results = deep_get(response, self.config.key)
        for result in results:
            FETCHED.append((self.config.name, result))
        return response


class Listing(Crawler):
    """Listing with pagination"""

    def _fetch_list(self, cursor=None):
        # if pagination
        request_attributes = getattr(self.config, "request", PropertyTree()).dict()
        if hasattr(self.config, "pagination") and cursor is not None:
            type = self.config.pagination.type
            request_attributes[type][self.config.pagination.key] = cursor
        response = self._fetch(self.config.path, **request_attributes)
        results = deep_get(response, self.config.key)
        for result in results:
            FETCHED.append((self.config.name, result))
        return response

    def run(self):
        cursor = None

        for ind in range(0, 3):  # Testing purpose
            response = self._fetch_list(cursor)
            if hasattr(self.config.pagination, "ref"):
                cursor = deep_get(response, self.config.pagination.ref)
            else:
                cursor = (1 + ind) * self.config.pagination.step
            if not cursor:
                break
            yield response


class Search(Crawler):
    def run(self):
        response = self._fetch(self.config.path)
        results = deep_get(response, self.config.key)
        for result in results:
            FETCHED.append((self.config.name, result))
        yield from results


# class Listing(Crawler):
#     # start_cursor: next_cursor

#     def run(self):
#         for value in range(0, 3):  # SAFER While True
#             result = self._fetch(
#                 value,
#             )
#             FETCHED.append((self.config.name, result))
#             yield result


class DirectFetch(Crawler):
    def run(self):
        # Scan for task
        for name, result in FETCHED:
            if self.config.id.entity == name:
                value = result[self.config.id.key]
                url = self.config.path.format(value)
                result = self._fetch(url)
                FETCHED.append((self.config.name, result))
                yield result


def type2class(type):
    if type == "Looping":
        return Looping
    elif type == "DirectFetch":
        return DirectFetch
    elif type == "Listing":
        return Listing
    elif type == "Search":
        return Search
    else:
        raise NotImplementedError


def runner(config):
    config = dict_to_obj_tree(config)
    session = LiveServerSession(config.host, headers=config.select("headers", {}))

    crawlers = []
    for entity in config.entities:
        crawler = type2class(entity.type)(entity, session)
        crawlers.append(crawler)

    for crawler in crawlers:
        print("Run Crawnler", crawler.config.name)
        for result in crawler.run():
            pass

    print(FETCHED)


# Handle response
# Parse
# Next actions
