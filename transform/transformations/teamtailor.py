from transform.utils import Transformer


def process_row(row):
    links = row.get("links", {})
    relationships = {}
    for key, relation in row.get("relationships", {}).items():
        data = relation.get("data")
        if not data:
            continue
        if isinstance(data, dict):
            relationships[key + "_id"] = data["id"]
        if isinstance(data, list):
            relationships[key + "_ids"] = [d["id"] for d in data]

    del links["self"]
    return {"id": row["id"], **row["attributes"], **relationships, **links}


def run(self):
    transformer = Transformer(
        engine=self.db.engine,
        input_table=f"_raw_teamtailor_{self.entity}",
        output_table=f"teamtailor_{self.entity}",
        transform=process_row,
    )
    transformer.run()
