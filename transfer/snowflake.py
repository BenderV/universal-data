import json
import os

import sqlalchemy as sa
from loguru import logger
from snowflake.sqlalchemy.custom_types import OBJECT
from sqlalchemy.dialects.postgresql import JSONB

from transfer.default import DefaultLoader


def normalize_type(type):
    mapper = {
        "VARCHAR": "TEXT",
        "NUMBER": "NUMERIC",
        "BOOLEAN": "BOOLEAN",
        "OBJECT": "JSONB",
        "ARRAY": "JSONB[]",
        "NULL": "TEXT",
    }
    if type not in mapper.keys() and type not in mapper.values():
        logger.warning(f"Type {type} not mapped")
    return mapper.get(type, type)


class SnowflakeLoader(DefaultLoader):
    def support_uri(self):
        return self.dest_engine.dialect.name == "snowflake"

    @property
    def dest_table(self):
        return sa.Table(
            self.table_name,
            sa.MetaData(),
            autoload=True,
            autoload_with=self.dest_engine,
        )

    def _create_table_from_source(self):
        """
        Create the table in the destination
        """
        dest_table = sa.Table(
            self.table_name,
            sa.MetaData(),
            *[
                sa.Column(
                    c.name,
                    c.type if c.type.__class__ != JSONB else OBJECT(),
                    nullable=c.nullable,
                )
                for c in self.source_table.columns
            ],
        )
        dest_table.drop(self.dest_engine, checkfirst=True)
        dest_table.create(self.dest_engine)

    def _check_table_have_same_columns(self):
        """
        Check if the table have the same columns as the source table
        And same types
        """

        def normalize_column(column):
            return (column.name, normalize_type(column.type.__class__.__name__))

        source_columns = [normalize_column(c) for c in self.source_table.columns]
        dest_columns = [normalize_column(c) for c in self.dest_table.columns]
        same_columns = source_columns == dest_columns
        if not same_columns:
            logger.warning(
                f"Table {self.table_name} have different columns in source and destination"
            )
        return same_columns

    def _upload(self, rows):
        # TODO: Create a real temp file (instead of using table_name)
        with open(f"/tmp/{self.table_name}.json", "w") as f:
            f.write(json.dumps(rows, default=str))

        with self.dest_engine.connect() as conn:
            conn.execute(
                f"CREATE OR REPLACE FILE FORMAT __ud_{self.table_name}_file_format\
                TYPE = 'JSON';"
            )
            conn.execute(
                f"CREATE OR REPLACE STAGE __ud_{self.table_name}_stage \
                FILE_FORMAT = __ud_{self.table_name}_file_format;"
            )
            conn.execute(
                f"PUT file:///tmp/{self.table_name}.json @__ud_{self.table_name}_stage"
            )
            conn.execute(f"SELECT * FROM @__ud_{self.table_name}_stage")
            conn.execute(
                f"COPY INTO {self.table_name} \
                FROM @__ud_{self.table_name}_stage \
                FILE_FORMAT = (type = 'JSON' strip_outer_array = true) \
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;"
            )

        # Remove tmp file
        os.remove(f"/tmp/{self.table_name}.json")

    def _remove(self, hashs):
        # Remove rows from dest table that match column __hash with hashs
        self.dest_engine.execute(
            f"""
            DELETE FROM {self.table_name}
            WHERE __hash IN ({", ".join([f"'{h}'" for h in hashs])})
            """
        )
