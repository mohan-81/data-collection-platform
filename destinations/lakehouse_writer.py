import os
import uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType
from pyiceberg.partitioning import PartitionSpec

# ---------------------------------------------------
# Convert pandas dataframe → Iceberg schema
# ---------------------------------------------------
def dataframe_to_schema(df):

    fields = []
    field_id = 1

    for col in df.columns:
        fields.append(
            NestedField(
                field_id,
                col,
                StringType(),
                required=False
            )
        )
        field_id += 1

    return Schema(*fields)


# ---------------------------------------------------
# Create Iceberg table if not exists
# ---------------------------------------------------
def get_or_create_table(catalog, table_name, df):

    try:
        table = catalog.load_table(table_name)

    except Exception:

        schema = dataframe_to_schema(df)

        table = catalog.create_table(
            table_name,
            schema,
            PartitionSpec()
        )

    return table

# ---------------------------------------------------
# Write dataframe to Iceberg table
# ---------------------------------------------------
def write_iceberg_table(base_path, table_name, df):

    warehouse = base_path

    catalog = load_catalog(
        "filesystem",
        **{
            "type": "filesystem",
            "warehouse": warehouse
        }
    )

    table = get_or_create_table(
        catalog,
        table_name,
        df
    )

    table.append(df)

    return len(df)


# ---------------------------------------------------
# Public function used by destinations
# ---------------------------------------------------
def push_iceberg(dest, source, rows):

    if not rows:
        return 0

    base_path = dest.get("path") or dest.get("host")

    df = pd.DataFrame(rows)

    for c in df.columns:
        df[c] = df[c].astype(str)

    df["fetched_at"] = str(pd.Timestamp.utcnow())

    table_name = f"default.{source}_iceberg"

    count = write_iceberg_table(
        base_path,
        table_name,
        df
    )

    print(f"[ICEBERG] Written {count} rows")

    return count