"""
Specific code for Airtable
We should remove this code in the future, but making it generic :)
"""

def airtable_unnest_tables(rows):
    tables = defaultdict(list)

    for row_ in rows:
        table_name = row_["tableName"]
        # deep copy
        row = {**row_}
        processed_row = row.pop("fields")
        processed_row.update(row)
        tables[table_name].append(processed_row)
    return tables

