# +
import argparse
import json
import sys

import pandas as pd
from sqlalchemy import create_engine, dialects


def upload(uri, entities, schema="public"):
    engine = create_engine(uri)

    for entity, data in entities.items():
        df = pd.DataFrame(data)
        dtypesDict = {
            column: dialects.postgresql.JSONB
            for column in df.columns
            if any(isinstance(df.iloc[row][column], dict) for row in range(0, len(df)))
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
