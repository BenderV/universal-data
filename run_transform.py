from transform.normalize import EntityNormalization
from transform.dedup import Deduplicator
from load.base import DataWarehouse
import argparse

def transform(target_uri):
    db = DataWarehouse(target_uri)
    dedup = Deduplicator(db)
    dedup.create_or_update_deduplicate_view()

    entities = dedup._get_active_entities()
    for entity in entities:
        print('Normalize entity:', entity)
        en = EntityNormalization(db, entity['source_id'], entity['entity'])
        en.normalize()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", type=str)
    args, unknown_args = parser.parse_known_args()
    transform(args.target)
