import sqlalchemy as sa
from genson import SchemaBuilder
from loguru import logger
from sqlalchemy import dialects


def generate_jsonschema(rows, schema=None):
    """Unused for now."""
    builder = SchemaBuilder()
    if schema is not None:
        builder.add_schema(schema)
    for row in rows:
        builder.add_object(row)
    return remove_null_from_jsonschema(builder.to_schema())


def jsonschema_to_postgres_types(schema):
    """
    Convert a JSON schema to a list of columns and their types.
    """

    columns = {}
    for key, value in schema["properties"].items():
        if "anyOf" in value:  # array of different types
            columns[key] = "text"
        elif value["type"] == "number":
            columns[key] = "numeric"
        elif value["type"] == "integer":
            columns[key] = "numeric"
        elif value["type"] == "string":
            columns[key] = "text"
        elif value["type"] == "boolean":
            columns[key] = "boolean"
        elif value["type"] == "object":
            columns[key] = "jsonb"
        elif value["type"] == "array":
            if "items" not in value:
                columns[key] = "jsonb[]"  # empty array
            elif "anyOf" in value["items"]:  # array of different types
                columns[key] = "text[]"
            elif value["items"]["type"] == "string":
                columns[key] = "text[]"
            elif value["items"]["type"] == "number":
                columns[key] = "numeric[]"
            elif value["items"]["type"] == "integer":
                columns[key] = "numeric[]"
            elif value["items"]["type"] == "boolean":
                columns[key] = "boolean[]"
            elif value["items"]["type"] == "object":
                columns[key] = "jsonb[]"
            else:
                logger.warning(f"Unsupported type for {key}: {value['items']['type']}")
                columns[key] = "text[]"
        elif value["type"] == "null":
            columns[key] = "text"  # debug
        else:
            raise ValueError(f"Unknown type {key} {value['type']}")
    return columns


def pg_type_to_sqlalchemy_type(pg_type):
    """
    Convert a Postgres type to a SQLAlchemy type.
    """
    if pg_type == "text":
        return sa.Text
    elif pg_type == "numeric":
        return sa.Numeric
    elif pg_type == "boolean":
        return sa.Boolean
    elif pg_type == "jsonb":
        return dialects.postgresql.JSONB
    elif pg_type == "text[]":
        return dialects.postgresql.ARRAY(sa.Text)
    elif pg_type == "numeric[]":
        return dialects.postgresql.ARRAY(sa.Numeric)
    elif pg_type == "boolean[]":
        return dialects.postgresql.ARRAY(sa.Boolean)
    elif pg_type == "jsonb[]":
        return dialects.postgresql.ARRAY(dialects.postgresql.JSONB)
    else:
        raise ValueError(f"Unknown type {pg_type}")


def remove_null_from_jsonschema(jsonschema):
    def remove_null(v):
        if v.get("type") == "null":
            # del jsonschema["properties"][k]
            pass
        if v.get("type") == "array":
            if "items" not in v:  # Empty array
                v["items"] = {"type": "string"}
            else:
                v["items"] = remove_null(v["items"])
        elif isinstance(v.get("type"), list):
            v["type"] = [x for x in v["type"] if x != "null"]
            if len(v["type"]) == 1:
                v["type"] = v["type"][0]
        elif "anyOf" in v:
            if {"type": "null"} in v["anyOf"]:
                v["anyOf"].remove({"type": "null"})
            if len(v["anyOf"]) == 1:
                v = remove_null(v["anyOf"][0])
            # else:
            #    raise ValueError("anyOf with more than one item")
        return v

    properties = {}

    for k, v in jsonschema["properties"].items():
        properties[k] = remove_null(v)

    jsonschema["properties"] = properties
    return jsonschema
