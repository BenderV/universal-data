import pandas as pd
from genson import SchemaBuilder

from load import DataWarehouse, Entity


def generate_jsonschema(rows):
    """Unused for now."""
    builder = SchemaBuilder()
    for row in rows:
        builder.add_object(row.data)
    return builder.to_schema()


class Normalizer(DataWarehouse):
    def normalize_entity(self, entity, rows):
        # TODO: add append
        df = pd.DataFrame(rows)
        # dtypesDict = {
        #     column: dialects.postgresql.JSONB
        #     for column in df.columns
        #     if any(
        #         isinstance(df.iloc[row][column], dict) or isinstance(df.iloc[row][column], list)
        #         for row in range(0, len(df))
        #     )
        # }
        df.to_sql(entity, self.engine, if_exists="replace", index=False)

    def normalize(self):
        entities = [r[0] for r in self.session.query(Entity.entity).distinct().all()]
        for entity_name in entities:
            rows = [
                r[0]
                for r in self.session.query(Entity.data)
                .filter_by(entity=entity_name)
                .all()
            ]
            print(entity_name, len(rows))
            self.normalize_entity(entity_name, rows)
