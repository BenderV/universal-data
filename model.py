from collections import defaultdict
from concurrent.futures import process

import pandas as pd
import yaml
from genson import SchemaBuilder
from sqlalchemy import dialects

from load import DataWarehouse, Entity


def generate_jsonschema(rows):
    """Unused for now."""
    builder = SchemaBuilder()
    for row in rows:
        builder.add_object(row.data)
    return builder.to_schema()


def airtable_unnest_tables(rows):
    """
    TODO: we should remove specific code...
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

class Normalizer(DataWarehouse):
    def normalize_entity(self, entity, rows):
        # TODO: add append
        df = pd.DataFrame(rows)
        dtypes_dict = {
            column: dialects.postgresql.JSONB
            for column in df.columns
            if any(
                isinstance(df.iloc[row][column], dict) or isinstance(df.iloc[row][column], list)
                for row in range(0, len(df))
            )
        }
        df.to_sql(entity, self.engine, if_exists="append", index=False, dtype=dtypes_dict)

    def normalize(self, source_id):
        entities = [r[0] for r in self.session.query(Entity.entity).filter_by(source_id=source_id, processed=False).distinct().all()]

        for entity_name in entities:
            with open(f"sources/{source_id}.yml") as f:
                source_config = yaml.safe_load(f)
            entities_config = source_config["entities"].find((lambda x: x["entity"] == entity_name))
            entities_config['type'] == 'Listing'
            
            query = self.session.query(Entity).filter_by(entity=entity_name, processed=False)
            rows = [r.data for r in query.all()]
            print(entity_name, len(rows)) 
            self.normalize_entity(entity_name, rows)
            if "tables_records" == entity_name:
                self.airtable_postprocessing(rows)
            query.update({Entity.processed: True})
            self.session.commit()

    def airtable_postprocessing(self, rows):
        tables_records = airtable_unnest_tables(rows)
        for table_name, table_rows in tables_records.items():
            self.normalize_entity(table_name, table_rows)
