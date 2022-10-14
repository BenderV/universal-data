import json
import re
from functools import reduce
from typing import Any, Dict


class PropertyTree:
    def __repr__(self):
        return json.dumps(self.__dict__, indent=4)

    def dict(self):
        """apply recursively on all properties"""
        item = {}
        for k, v in self.__dict__.items():
            if isinstance(v, PropertyTree):
                item[k] = v.dict()
            elif isinstance(v, list):
                item[k] = [sv.dict() for sv in v]
            else:
                item[k] = v
        return item

    def __repr__(self):
        return json.dumps(self.dict())

    # TODO: fix
    def select(self, path, fallback=None):
        """
        Select the key from a nested dict, if the key is not found, return None
        path = 'a.b.c'
        """
        obj = self.dict()
        return reduce(
            lambda d, k: d.get(k, fallback) if d else fallback, path.split("."), obj
        )


def dict_to_obj_tree(yaml_config: Dict[str, Any]) -> PropertyTree:
    tree = PropertyTree()
    for key, value in yaml_config.items():
        if type(value) == dict:
            setattr(tree, key, dict_to_obj_tree(value))
        elif type(value) == list:
            setattr(tree, key, [dict_to_obj_tree(v) for v in value])
        else:
            setattr(tree, key, value)
    return tree


def deep_get(dictionary, keys, default=None):
    return reduce(
        lambda d, key: d.get(key, default) if isinstance(d, dict) else default,
        keys.split("."),
        dictionary,
    )


def partial_format(s, **kwargs):
    """
    https://stackoverflow.com/a/63924089
    """
    parts = re.split(r"(\{[^}]*\})", s)
    for k, v in kwargs.items():
        for idx, part in enumerate(parts):
            if re.match(
                rf"\{{{k}[!:}}]", part
            ):  # Placeholder keys must always be followed by '!', ':', or the closing '}'
                parts[idx] = parts[idx].format_map({k: v})
    return "".join(parts)


def apply_nested(obj, func):
    # Iterate on nested dict and format
    for k, v in obj.items():
        if isinstance(v, dict):
            obj[k] = apply_nested(v, func)
        elif isinstance(v, str):
            obj[k] = func(v)
        elif isinstance(v, list):
            obj[k] = [apply_nested(i, func) for i in v]
        else:
            obj[k] = v
    return obj
