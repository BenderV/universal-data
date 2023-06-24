import importlib
import os
import sys

from transform.utils import Transformer


def load_module(module):
    module_path = module

    if module_path in sys.modules:
        return sys.modules[module_path]

    return __import__(module_path, fromlist=[module])


class EntityNormalization:
    def __init__(self, database, source_id, entity):
        self.db = database
        self.source_id = source_id
        self.entity = entity

    def normalize(self):
        input_sql = f"""
            SELECT *
            FROM __ud_entities
            WHERE source_id = '{self.source_id}'
            AND entity = '{self.entity}'
        """

        transformer = Transformer(
            engine=self.db.engine,
            input_table="__ud_entities",
            input_sql=input_sql,
            process_key="processed",
            primary_key="__key",
            output_table=f"_raw_{self.source_id}_{self.entity}",
            transform=lambda row: row["data"],
        )

        transformer.run()

        # Run post processing transformations

        path = f"transform/transformations/{self.source_id}.py"
        if os.path.exists(path):
            module = load_module(f"transform.transformations.{self.source_id}")
            print(f"Running post processing transformations for {self.source_id}")
            module.run(self)
