# Extract type from json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from transform.types import (
    generate_jsonschema,
    jsonschema_to_postgres_types,
    remove_null_from_jsonschema,
)

examples_types = [
    {
        "name": "ben",
        "age": 100,
        "is_human": True,
        "height": 1.8,
        "address": {
            "street": "123 Main St",
            "city": "New York",
            "state": "NY",
            "zip": "10001",
        },
        "friends": ["joe", "jane", "jim"],
        "pets": [{"name": "fido", "type": "dog"}, {"name": "fluffy", "type": "cat"}],
        "other": [
            {
                "key": "a",
            },
            4,
            "b",
            True,
            1.2,
        ],
        "friend_ids": [1, 2, 3],
    }
]

properties = {
    "name": {"type": "string"},
    "age": {"type": "integer"},
    "is_human": {"type": "boolean"},
    "height": {"type": "number"},
    "address": {
        "type": "object",
        "properties": {
            "street": {"type": "string"},
            "city": {"type": "string"},
            "state": {"type": "string"},
            "zip": {"type": "string"},
        },
        "required": ["city", "state", "street", "zip"],
    },
    "friends": {"type": "array", "items": {"type": "string"}},
    "pets": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {"name": {"type": "string"}, "type": {"type": "string"}},
            "required": ["name", "type"],
        },
    },
    "friend_ids": {"type": "array", "items": {"type": "integer"}},
    "other": {
        "items": {
            "anyOf": [
                {"type": ["boolean", "number", "string"]},
                {
                    "properties": {"key": {"type": "string"}},
                    "required": ["key"],
                    "type": "object",
                },
            ]
        },
        "type": "array",
    },
}

types_pg = {
    "name": "text",
    "age": "numeric",
    "is_human": "boolean",
    "height": "numeric",
    "address": "jsonb",
    "friends": "text[]",
    "pets": "jsonb[]",
    "other": "text[]",
    "friend_ids": "numeric[]",
}


def test_extract_type_from_json():
    jsonschema = generate_jsonschema(examples_types)
    assert jsonschema["properties"] == properties


def test_jsonschema_to_postgres_types():
    jsonschema = generate_jsonschema(examples_types)
    assert jsonschema_to_postgres_types(jsonschema) == types_pg


def test_jsonschema_to_postgres_type_with_null():
    examples_with_null = [
        {
            "id": 1,
            "name": "ben",
            "age": 100,
        },
        {
            "id": 2,
            "name": None,
        },
    ]

    types_pg_2 = {
        "id": "numeric",
        "name": "text",
        "age": "numeric",
    }

    jsonschema = generate_jsonschema(examples_with_null)
    assert jsonschema_to_postgres_types(jsonschema) == types_pg_2


def test_remove_null():
    jsonschema = {
        "$schema": "http://json-schema.org/schema#",
        "type": "object",
        "properties": {
            "zero": {"type": "null"},
            "id": {"type": ["integer", "null"]},
            "test": {"type": "array", "items": {"type": ["null", "string"]}},
            "meta": {
                "anyOf": [
                    {"type": "null"},
                    {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "name": {"type": "string"},
                        },
                        "required": ["id", "name"],
                    },
                ]
            },
        },
        "required": ["id", "meta"],
    }
    jsonschema_cleaned = {
        "$schema": "http://json-schema.org/schema#",
        "type": "object",
        "properties": {
            "zero": {"type": "null"},
            "id": {"type": "integer"},
            "test": {"type": "array", "items": {"type": "string"}},
            "meta": {
                "type": "object",
                "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
                "required": ["id", "name"],
            },
        },
        "required": ["id", "meta"],
    }
    assert remove_null_from_jsonschema(jsonschema) == jsonschema_cleaned


if __name__ == "__main__":
    test_extract_type_from_json()
    test_jsonschema_to_postgres_types()
    test_remove_null()
