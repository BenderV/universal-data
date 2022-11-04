from collections import defaultdict
from concurrent.futures import process
from typing import Dict, Optional

import pandas as pd
import sqlalchemy as sa
import yaml
from genson import SchemaBuilder
from load.base import DataWarehouse, Entity
from sqlalchemy import dialects, text
from sqlalchemy.inspection import inspect


def generate_jsonschema(rows, schema=None):
    """Unused for now."""
    builder = SchemaBuilder()
    if schema is not None:
        builder.add_schema(schema)
    for row in rows:
        builder.add_object(row)
    return builder.to_schema()


POSTGRES_TYPES = {
    'array': 'jsonb',
    'boolean': 'boolean',
    'object': 'jsonb',
    'string': 'text',
    'integer': 'numeric',
    'number': 'numeric',
}

def extract_type_from_df(df):
    dtypes_dict = {
        column: dialects.postgresql.JSONB
        for column in df.columns
        if any(
            isinstance(df.iloc[row][column], dict) or isinstance(df.iloc[row][column], list)
            for row in range(0, len(df))
        )
    }
    return dtypes_dict


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


class Transformer:
    """
    def process_row(input_):
        pass

    transformer = Transformer(
        engine = engine,
        input_table = "public.__ud_dedup",
        output_table = "public.__ud_normalized",
        transform = process_row
    )

    transformer.run()
    """

    chunk_size = 1000
    output_table_jsonschema = None

    def __init__(self, engine: sa.engine.Engine, output_table: str, transform: callable, input_table: str = None, input_sql: str = None, primary_key=None):
        self.engine = engine
        if input_sql and input_table:
            raise ValueError("Only one of input_sql and input_table can be set")
        self.input_table = input_table
        self.input_sql = input_sql or f"SELECT * FROM {input_table}"
        self.primary_key = primary_key
    
        self.output_table = output_table
        self.transform = transform
        self.meta = sa.MetaData(self.engine)

    def _extract_primary_keys(self, table_name):
        table = sa.Table(table_name, self.meta, autoload=True, autoload_with=self.engine)
        return [key.name for key in table.primary_key]

    def _get_primary_key(self):
        if not self.primary_key:
            primary_keys = self._extract_primary_keys(self.input_table)
            if len(primary_keys) > 1:
                raise Exception("Only support single primary key")
            self.primary_key = primary_keys[0]
        
        return self.primary_key

    def _output_table_exist(self):
        insp = sa.inspect(self.engine)
        table_exist = insp.has_table(self.output_table)
        return table_exist

    def _create_table(self):
        schema = self.output_table_jsonschema
        
        columns = []
        for col_name, value in schema['properties'].items():
            if "type" not in value:
                if "anyOf" in value:
                    col_type = POSTGRES_TYPES['object']
                else:
                    raise Exception(f"Type not found for {col_name}")
            else:
                if isinstance(value['type'], list):
                    if 'null' in value['type'] and len(value['type']) == 2:
                        value['type'] = [t for t in value['type'] if t != 'null'][0]
                    else:
                        raise Exception(f"Array type not supported yet: {col_name} {value['type']}")
                col_type = POSTGRES_TYPES.get(value['type'])
                if col_type is None:
                    if value['type'] == "null":
                        col_type = "text"   
                    else:
                        raise ValueError(f'Unknown type for col {col_name}: {value}')
                suffix = "PRIMARY KEY" if col_name == self._get_primary_key() else ""
            columns.append(f'"{col_name}" {col_type} {suffix}')

        columns = ", ".join(columns)
        
        query = f"""
            CREATE TABLE "{self.output_table}" (
                {columns}
            )
        """
        self.engine.execute(f"""DROP TABLE IF EXISTS "{self.output_table}";""")
        print('Drop table')
        print(query)
        # Need to escape to avoid % in the query
        self.engine.execute(text(query))
        print('Table created')

    def _fetch_rows_to_insert(self, limit=None):
        self._get_primary_key()

        query_filter = f"""
            AND "{self.primary_key}" NOT IN (
                SELECT __key
                FROM "{self.output_table}"
            )
        """ if self._output_table_exist() else ""
        
        query = f"""
            SELECT *
            FROM (
                {self.input_sql}
            ) AS t
            WHERE True
            {query_filter}
            LIMIT {self.chunk_size if limit is None else limit}
        """
        rows_raw = self.engine.execute(query).all()
        rows = [{
            '__key': row[self.primary_key],
            # '__created_at': row.created_at.isoformat(),
            # '__updated_at': row.updated_at.isoformat(),
            **self.transform(dict(row))
        } for row in rows_raw]
        return rows

    def _update_json_schema(self, rows):
        schema = generate_jsonschema(rows, self.output_table_jsonschema)
        self.output_table_jsonschema = schema
        return schema

    def _insert_data(self, rows):
        print(f'insert_data {self.output_table}: {len(rows)} rows')
        df = pd.DataFrame(rows)
        meta = sa.MetaData(self.engine)
        upsert_method = create_upsert_method(meta)

        try:
            df.to_sql(
                self.output_table,
                self.engine,
                index=False,
                if_exists="append",
                chunksize=self.chunk_size,
                method=upsert_method,
                dtype=extract_type_from_df(df)
            )
        except (sa.exc.ProgrammingError, sa.exc.CompileError):
            self._update_json_schema(rows)
            self._create_table()
        except Exception as eee:
            # if 'CompileError' in str(eee):
            self._update_json_schema(rows)
            self._create_table()
            
    def run(self):
        rows = self._fetch_rows_to_insert()
        table_exist = self._output_table_exist()
        if not table_exist:
            self._update_json_schema(rows)
            self._create_table()
        
    
        self._insert_data(rows)
        while True:
            rows = self._fetch_rows_to_insert()
            if not rows:
                break
            self._insert_data(rows)
