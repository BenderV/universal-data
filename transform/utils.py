from collections import defaultdict
from concurrent.futures import process

import yaml
from genson import SchemaBuilder
from load.base import DataWarehouse, Entity
from sqlalchemy import dialects


def generate_jsonschema(rows):
    """Unused for now."""
    builder = SchemaBuilder()
    for row in rows:
        builder.add_object(row.data)
    return builder.to_schema()


def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)
