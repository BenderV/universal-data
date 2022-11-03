from enum import Enum

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import dialects

from transform.utils import Transformer


class EntityNormalization:
    def __init__(self, database, source_id, entity):
        self.db = database
        self.source_id = source_id
        self.entity = entity

    def normalize(self):
        input_sql = f"""
            SELECT *
            FROM __ud_dedup
            WHERE source_id = '{self.source_id}'
            AND entity = '{self.entity}'
        """

        transformer = Transformer(
            engine = self.db.engine,
            input_sql = input_sql,
            primary_key = '__key',
            output_table = f"_raw_{self.source_id}_{self.entity}",
            transform = lambda row: row["data"]
        )

        transformer.run()
    
        # # Hack ...
        if self.source_id == 'airtable' and self.entity == 'tables_records':
            print(f"Hack airtable")
            def process_row(row):
                fields = row["fields"]
                # TODO: Need to move this to utils.Transformer
                # 59 for Postgres, 300 for Snowflake / BigQuery
                fields_with_name_length = {k[:59]: v for k, v in fields.items()}
    
                return {
                    "id": row["id"],
                    **fields_with_name_length,
                }

            query = """SELECT DISTINCT "tableName" FROM _raw_airtable_tables_records"""
            rows = self.db.engine.execute(query).all()
            for row in rows:
                transformer = Transformer(
                    engine = self.db.engine,
                    input_sql = f"""
                        SELECT *
                        FROM _raw_airtable_tables_records
                        WHERE "tableName" = '{row.tableName}'
                    """,
                    primary_key = 'id',
                    output_table = row.tableName,
                    transform = process_row
                )
                transformer.run()        
            
        if self.source_id == 'teamtailor':
            def process_row(row):
                links = row.get('links', {})
                del links['self']
                return {
                    "id": row["id"],
                    **row["attributes"],
                    **links
                }

            transformer = Transformer(
                engine = self.db.engine,
                input_table = f"_raw_teamtailor_{self.entity}",
                output_table = f"teamtailor_{self.entity}",
                transform = process_row
            )
            
            transformer.run()
            

if __name__ == "__main__":
    db = EntityNormalization(uri="postgresql://localhost:5432/biolook")
    db.normalize()
