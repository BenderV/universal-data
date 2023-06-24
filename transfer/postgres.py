import sqlalchemy as sa
from loguru import logger

from transfer.default import DefaultLoader


class PostgresLoader(DefaultLoader):
    def support_uri(self):
        return self.dest_engine.dialect.name == "postgresql"

    def _check_table_have_same_columns(self):
        """
        Check if the table have the same columns as the source table
        And same types
        """
        source_columns = [(c.name, str(c.type)) for c in self.source_table.columns]
        dest_columns = [(c.name, str(c.type)) for c in self.dest_table.columns]
        same_columns = source_columns == dest_columns
        if not same_columns:
            logger.warning(
                f"Table {self.table_name} have different columns in source and destination"
            )
        return same_columns

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
            self.source_table.name,
            sa.MetaData(),
            *[
                sa.Column(
                    c.name,
                    c.type,
                    nullable=c.nullable,
                )
                for c in self.source_table.columns
            ],
        )
        dest_table.drop(self.dest_engine, checkfirst=True)
        dest_table.create(self.dest_engine)

    def _upload(self, rows):
        self.dest_engine.execute(self.dest_table.insert(), rows)

    def _remove(self, hashs):
        # Remove rows from dest table that match column __hash with hashs
        self.dest_engine.execute(
            f"""
            DELETE FROM "{self.table_name}"
            WHERE __hash IN ({", ".join([f"'{h}'" for h in hashs])})
            """
        )
