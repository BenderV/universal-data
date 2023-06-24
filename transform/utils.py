from collections import defaultdict
from time import sleep
from typing import Dict, Optional

import sqlalchemy as sa
from loguru import logger
from sqlalchemy import text
from sqlalchemy.inspection import inspect

from transform.types import (
    generate_jsonschema,
    jsonschema_to_postgres_types,
    pg_type_to_sqlalchemy_type,
)

MAX_RETRIES = 10


class UpdatedTableException(Exception):
    """Raised when the table is updated by another process."""

    pass


def fix_array_string(items):
    if items and isinstance(items, list):
        return [str(i) for i in items]
    return [str(items)]


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


def create_upsert_method(
    meta: sa.MetaData, extra_update_fields: Optional[Dict[str, str]] = None
):
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
            index_elements=sql_table.primary_key.columns,  # index elements are primary keys of a table
            set_=update_stmt,  # the SET part of an INSERT statement
        )

        # execute upsert statement
        conn.execute(upsert_stmt)

    return method


class Transformer:
    """Transform class, take a table from Postgres,
    Apply the data transformation
    And apply it to the output table
    """

    chunk_size = 1000
    output_table_jsonschema = None
    update_retries = 0

    def __init__(
        self,
        engine: sa.engine.Engine,
        output_table: str,
        transform: callable,
        input_table: str = None,
        input_sql: str = None,
        primary_key=None,
        process_key=None,
    ):
        self.engine = engine
        self.input_sql = input_sql
        self.input_table = input_table

        if input_table and not input_sql:
            self.input_sql = self._create_input_sql_from_table()

        self.primary_key = primary_key
        self.output_table = output_table
        self.transform = transform
        self.meta = sa.MetaData(self.engine)
        self.process_key = process_key

    def _extract_primary_keys(self, table_name):
        table = sa.Table(
            table_name, self.meta, autoload=True, autoload_with=self.engine
        )
        return [key.name for key in table.primary_key]

    def _create_input_sql_from_table(self):
        """Remove __hash column from input table"""
        columns = inspect(self.engine).get_columns(self.input_table)
        columns = [column["name"] for column in columns if column["name"] != "__hash"]
        columns = ", ".join(columns)
        return f"SELECT {columns} FROM {self.input_table}"

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
        if table_exist:
            columns = insp.get_columns(self.output_table)
            if "__hash" not in [column["name"] for column in columns]:
                return False
        return table_exist

    def _create_table(self):
        schema = self.output_table_jsonschema
        columns = jsonschema_to_postgres_types(schema)
        pkey = self._get_primary_key()
        sql_columns = ", ".join(
            [
                f"\"{key}\" {value} {'PRIMARY KEY' if key == pkey else ''}"
                for key, value in columns.items()
            ]
        )
        query = f"""
            CREATE TABLE "{self.output_table}" (
                {sql_columns}
            )
        """
        self.engine.execute(f"""DROP TABLE IF EXISTS "{self.output_table}";""")
        print("Drop table")
        print(query)
        # Need to escape to avoid % in the query
        self.engine.execute(text(query))
        print("Table created")
        sleep(1)

    def _fetch_rows_to_insert(self, limit=None, reset=False):
        self._get_primary_key()

        if self.process_key:
            query = f"""
                SELECT t.*, md5(t::text) AS __hash
                FROM (
                    {self.input_sql}
                ) AS t
                WHERE t."{self.process_key}" IS NOT TRUE
                LIMIT {self.chunk_size if limit is None else limit}
            """
        else:
            query_filter = (
                f"""  
                LEFT JOIN LATERAL (
                    SELECT "{self.primary_key}"
                    FROM "{self.output_table}" AS s
                    WHERE t."{self.primary_key}" = s."{self.primary_key}"
                    AND t.__hash = s.__hash
                ) AS dest ON TRUE
                WHERE True
                AND dest."{self.primary_key}" IS NULL
            """
                if self._output_table_exist() and reset is False
                else ""
            )

            query = f"""
                SELECT t.*, __hash
                FROM (
                    SELECT t.*, md5(t::text) AS __hash
                    FROM (
                        {self.input_sql}
                    ) AS t
                ) AS t
                {query_filter}
                LIMIT {self.chunk_size if limit is None else limit}
            """

        rows_raw = self.engine.execute(query).all()

        rows = []
        for row_raw in rows_raw:
            row = {
                "__key": row_raw[self.primary_key],
                "__hash": row_raw["__hash"],
                # '__created_at': row.created_at.isoformat(),
                # '__updated_at': row.updated_at.isoformat(),
                **self.transform(dict(row_raw)),
            }
            rows.append(row)
        return rows

    def _update_json_schema(self, rows):
        schema = generate_jsonschema(rows, self.output_table_jsonschema)
        self.output_table_jsonschema = schema
        return schema

    def _update_process_key(self, rows):
        keys = [row["__key"] for row in rows]
        query = f"""
            UPDATE "{self.input_table}"
            SET "{self.process_key}" = TRUE
            WHERE "{self.primary_key}" IN ({", ".join([f"'{key}'" for key in keys])})
        """
        self.engine.execute(query)

    def _reset_process_key(self):
        query = f"""
            UPDATE "{self.input_table}"
            SET "{self.process_key}" = FALSE
        """
        self.engine.execute(query)

    # TODO: add types comparaison
    def _check_table_match_rows_columns(self, types):
        """Check if the columns of the output table match the types of the rows
        Work only for columns name not types
        """
        columns = inspect(self.engine).get_columns(self.output_table)
        columns_name = {column["name"] for column in columns}
        rows_columns = types.keys()
        additional_columns = set(rows_columns) - set(columns_name)
        if additional_columns:
            raise UpdatedTableException(
                f"Columns {additional_columns} are not in the output table"
            )
        return True

    def _insert_data(self, rows):
        import pandas as pd

        print(f"insert_data {self.output_table}: {len(rows)} rows")
        pg_types = jsonschema_to_postgres_types(self.output_table_jsonschema)
        dtypes = {k: pg_type_to_sqlalchemy_type(t) for k, t in pg_types.items()}

        self._check_table_match_rows_columns(dtypes)

        df = pd.DataFrame(rows)
        meta = sa.MetaData(self.engine)
        upsert_method = create_upsert_method(meta)

        # TODO: have better enforced normalization
        for key in df.columns:
            if dtypes[key] == sa.Text:
                df[key] = df[key].astype(str)
            elif (
                str(dtypes[key]) == "ARRAY"
                and dtypes[key].__repr__() == "ARRAY(Text())"
            ):
                # print(f"FIX {key}")
                df[key] = df[key].apply(fix_array_string)

        df.to_sql(
            self.output_table,
            self.engine,
            index=False,
            if_exists="append",
            chunksize=self.chunk_size,
            method=upsert_method,
            dtype=dtypes,
        )

        if self.process_key:
            self._update_process_key(rows)

    def run(self):
        table_exist = self._output_table_exist()
        # Check if we need to update values
        rows = self._fetch_rows_to_insert()
        if not rows:
            return

        # Get large sample to recreate the jsonschema (hack)
        rows = self._fetch_rows_to_insert(limit=10000, reset=True)
        self._update_json_schema(rows)
        if not table_exist:
            self._create_table()

        while True:
            rows = self._fetch_rows_to_insert()
            if not rows:
                break
            self._update_json_schema(rows)
            try:
                self._insert_data(rows)
            except (UpdatedTableException, Exception) as e:
                logger.warning("Error while inserting data")
                logger.warning(e)
                self.update_retries += 1
                if self.update_retries > MAX_RETRIES:
                    raise e
                logger.info("Retry n°{}".format(self.update_retries))
                self._create_table()
                if self.process_key:
                    self._reset_process_key()
