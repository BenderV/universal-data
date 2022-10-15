# +
import argparse
import json
import sys
from collections import defaultdict

import pandas as pd
from sqlalchemy import create_engine, dialects


def split_by_entity_type(df):

    entities = {}
    for entity in df.tableName.unique():
        entities[entity] = df[df.entity == entity]
    return entities


def airtable_postprocessing(rows):
    """
    TODO: we should remove specific code...
    """
    tables = defaultdict(list)

    for row2 in rows:
        table_name = row2["tableName"]
        # deep copy
        row = {**row2}
        processed_row = row.pop("fields")
        processed_row.update(row)
        tables[table_name].append(processed_row)
    return tables


def upload(uri, entities, schema="public"):
    engine = create_engine(uri)

    # God, it's terrible, we shouldn't have that....
    if "tables_records" in entities.keys():
        tables_records = airtable_postprocessing(entities["tables_records"])
        entities.update(tables_records)

    for entity, data in entities.items():
        df = pd.DataFrame(data)
        dtypesDict = {
            column: dialects.postgresql.JSONB
            for column in df.columns
            if any(
                isinstance(df.iloc[row][column], dict)
                or isinstance(df.iloc[row][column], list)
                for row in range(0, len(df))
            )
        }
        df.to_sql(
            entity,
            con=engine,
            if_exists="replace",
            index=False,
            dtype=dtypesDict,
            schema=schema,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", type=str, required=True)
    parser.add_argument("--schema", type=str, default="public")
    args = parser.parse_args()
    file = "\n".join([l for l in sys.stdin])
    data = json.loads(file)
    upload(args.target, data, args.schema)
