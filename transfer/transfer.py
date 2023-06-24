from transfer.postgres import PostgresLoader
from transfer.snowflake import SnowflakeLoader

LOADERS = [PostgresLoader, SnowflakeLoader]


def transfer(source_uri, dest_uri, table_name):
    for loader in LOADERS:
        loader = loader(source_uri, dest_uri, table_name)
        if loader.support_uri():
            loader.transfer()
            return
    raise NotImplementedError("No loader compatible with the source uri")
