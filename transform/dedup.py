""""
https://stackoverflow.com/questions/9766940/how-to-create-an-sql-view-with-sqlalchemy
https://github.com/sqlalchemy/sqlalchemy/wiki/Views
"""
import yaml

def read_source_name(source_name):
    with open(f'sources/{source_name}.yml') as f:
        source_config = yaml.safe_load(f)
    return source_config

class Deduplicator:

    def __init__(self, database):
        self.db = database

    def _get_config(self):
        """Return array
        [{
            "source_id": "biorxiv",
            "entity": "articles",
            "keys": ["doi", "version"],
        }]
        """
        pass

    def _get_active_entities(self):
        with self.db.engine.begin() as conn:
            rows = conn.execute("""
                SELECT DISTINCT source_id, entity
                FROM __ud_entities
            """).all()
            return [dict(r) for r in rows]

    def _get_config(self):
        source_entities = self._get_active_entities()
        for source_entity in source_entities:
            config = read_source_name(source_entity['source_id'])
            keys = config['entities'].get(source_entity['entity'])
            source_entity['keys'] = keys
        return source_entities

    def create_or_update_deduplicate_view(self):
        source_entity_configs = self._get_config()

        with self.db.engine.begin() as conn:
            queries_source_entity_with_key = []
            for config in source_entity_configs:
                source_id, entity, keys = config.values()
                key_columns = ', '.join([f"data->>'{key}'" for key in keys])
                query_source_entity = f"""
                    SELECT md5(({key_columns})::text) AS "__key", *
                    FROM __ud_entities
                    WHERE source_id = '{source_id}' AND entity = '{entity}'
                """
                queries_source_entity_with_key.append(query_source_entity)
 
            dedup_query = f"""
                CREATE OR REPLACE VIEW "__ud_dedup" AS
                SELECT *
                FROM (
                    SELECT 
                        ROW_NUMBER() OVER (PARTITION BY ("__key") ORDER BY "created_at" DESC) AS inverse_rank,
                        *
                    FROM (
                        {" UNION ".join(queries_source_entity_with_key)}
                    ) AS rows_all
                ) AS rows_rank
                WHERE inverse_rank = 1
            """
            
            conn.execute(dedup_query)



if __name__ == "__main__":
    data_warehouse = Deduplicator(
        "postgresql+psycopg2://localhost:5432/biolook"
    )
    data_warehouse._get_active_entities()
