from collections import defaultdict
from concurrent.futures import process
from typing import Dict, Optional
import yaml
from genson import SchemaBuilder
from load.base import DataWarehouse, Entity
from sqlalchemy import dialects
import sqlalchemy as sa

def generate_jsonschema(rows):
    """Unused for now."""
    builder = SchemaBuilder()
    for row in rows:
        builder.add_object(row)
    return builder.to_schema()
from collections import defaultdict


def airtable_unnest_tables(rows):
    """Specific code for Airtable
    We should remove this code in the future, but making it generic :)
    """
    tables = defaultdict(list)

    for row_ in rows:
        table_name = row_["tableName"]
        # deep copy
        row = {**row_}
        processed_row = row.pop("fields")
        processed_row.update(row)
        tables[table_name].append(processed_row)
    return tables


def create_upsert_method(meta: sa.MetaData, extra_update_fields: Optional[Dict[str, str]] = None):
    """
    Create upsert method that satisfied the pandas's to_sql API.
    """
    def method(table, conn, keys, data_iter):
        # select table that data is being inserted to (from pandas's context)
        sql_table = sa.Table(table.name, meta, autoload=True)
        
        # list of dictionaries {col_name: value} of data to insert
        values_to_insert = [dict(zip(keys, data)) for data in data_iter]
        
        # create insert statement using postgresql dialect.
        # For other dialects, please refer to https://docs.sqlalchemy.org/en/14/dialects/
        insert_stmt = sa.dialects.postgresql.insert(sql_table, values_to_insert)

        # create update statement for excluded fields on conflict
        update_stmt = {exc_k.key: exc_k for exc_k in insert_stmt.excluded}
        if extra_update_fields:
            update_stmt.update(extra_update_fields)
        
        # create upsert statement.
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=sql_table.primary_key.columns, # index elements are primary keys of a table
            set_=update_stmt # the SET part of an INSERT statement
        )
        
        # execute upsert statement
        conn.execute(upsert_stmt)

    return method
