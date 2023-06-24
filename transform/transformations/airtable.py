from transform.utils import Transformer


def process_row(row):
    fields = row["fields"]
    # TODO: Need to move this to utils.Transformer
    # 59 for Postgres, 300 for Snowflake / BigQuery
    fields_with_name_length = {k[:59]: v for k, v in fields.items()}

    return {
        "id": row["id"],
        **fields_with_name_length,
    }


def run(self):
    query = """SELECT DISTINCT "tableName" FROM _raw_airtable_tables_records"""
    rows = self.db.engine.execute(query).all()
    for row in rows:
        transformer = Transformer(
            engine=self.db.engine,
            input_sql=f"""
                SELECT "id", "fields", "tableName", "createdTime"
                FROM _raw_airtable_tables_records
                WHERE "tableName" = '{row.tableName}'
            """,
            primary_key="id",
            output_table=row.tableName,
            transform=process_row,
        )
        transformer.run()
