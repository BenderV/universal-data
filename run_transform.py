from transform.normalize import EntityNormalization
from transform.dedup import Deduplicator
from load.base import DataWarehouse

def run_transform(target_uri):
    db = DataWarehouse(target_uri)
    dedup = Deduplicator(db)
    dedup.create_or_update_deduplicate_view()

    entities = dedup._get_active_entities()
    for entity in entities:
        print('Normalize entity:', entity)
        en = EntityNormalization(db, entity['source_id'], entity['entity'])
        en.normalize()
