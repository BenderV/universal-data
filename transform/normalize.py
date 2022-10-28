from enum import Enum

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import dialects
from transform.utils import generate_jsonschema, create_upsert_method

POSTGRES_TYPES = {
    'array': 'jsonb',
    'boolean': 'boolean',
    'object': 'jsonb',
    'string': 'text',
}

class EntityNormalization:
    schema = 'public'
    chunk_size = 1000

    def __init__(self, database, source_id, entity):
        self.db = database
        self.source_id = source_id
        self.entity = entity
        self.table_name = f"_raw_{source_id}_{entity}" # target table name

    def _table_exist(self):
        insp = sa.inspect(self.db.engine)
        table_exist = insp.has_table(self.table_name, schema=self.schema)
        return table_exist

    def _fetch_rows_to_insert(self):
        query_filter = f"""
            AND __key NOT IN (
                SELECT __key
                FROM {self.table_name}
            )
        """ if self._table_exist() else ""
        
        query = f"""
            SELECT *
            FROM __ud_dedup
            WHERE True
            {query_filter}
            AND source_id = '{self.source_id}'
            AND entity = '{self.entity}'
            LIMIT {self.chunk_size}
        """
        rows_raw = self.db.engine.execute(query).all()
        rows = [{
            '__key': row['__key'],
            '__created_at': row.created_at.isoformat(),
            **row.data
        } for row in rows_raw]
        return rows

    def create_table(self, schema):
        keys = schema['properties'].keys()
        
        columns = []
        for col_name, value in schema['properties'].items():
            col_type = POSTGRES_TYPES.get(value['type'])
            suffix = "PRIMARY KEY" if col_name == '__key' else ""
            columns.append(f"{col_name} {col_type} {suffix}")

        columns = ", ".join(columns)
        
        query = f"""
            CREATE TABLE {self.table_name} (
                {columns}
            )
        """
        self.db.engine.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        self.db.engine.execute(query)
    
    def insert_data(self, rows):
        df = pd.DataFrame(rows)
        meta = sa.MetaData(self.db.engine)
        upsert_method = create_upsert_method(meta)
            
        dtypes_dict = {
            column: dialects.postgresql.JSONB
            for column in df.columns
            if any(
                isinstance(df.iloc[row][column], dict) or isinstance(df.iloc[row][column], list)
                for row in range(0, len(df))
            )
        }

        df.to_sql(
          self.table_name,
          self.db.engine,
          schema=self.schema,
          index=False,
          if_exists="append",
          chunksize=self.chunk_size,
          method=upsert_method,
          dtype=dtypes_dict
        )
      
    def normalize(self):
        rows = self._fetch_rows_to_insert()
        
        insp = sa.inspect(self.db.engine)
        table_exist = insp.has_table(self.table_name, schema=self.schema)
        if not table_exist:
            schema = generate_jsonschema(rows)
            self.create_table(schema)
        
        self.insert_data(rows)
        while True:
            rows = self._fetch_rows_to_insert()
            if not rows:
                break
            self.insert_data(rows)

if __name__ == "__main__":
    db = EntityNormalization(uri="postgresql://localhost:5432/biolook")
    db.normalize()
