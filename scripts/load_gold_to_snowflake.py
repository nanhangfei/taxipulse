"""Load a month of gold trips from MinIO into Snowflake for dbt to model.

Snowflake (SaaS) cannot reach local MinIO, so we pull the gold parquet down with
pyarrow and push it up with the connector's PUT+COPY (write_pandas) — the same
internal-stage pattern Stage 1 used for the raw load.

Idempotency: DELETE the target month, then append. A second run of the same
month removes those rows and re-inserts identical ones — net no-op, no dupes.

Column case: DataFrame columns are upper-cased so write_pandas creates
"PICKUP_DATETIME" etc., which unquoted dbt refs (pickup_datetime) resolve to.

Usage: python scripts/load_gold_to_snowflake.py <year> <month>
Env: SNOWFLAKE_* (key-pair, as in dbt profiles.yml) + MINIO_ROOT_USER/PASSWORD,
optional MINIO_ENDPOINT (default http://localhost:9000 for host access).
"""
import os
import sys

import pyarrow.dataset as ds
from pyarrow import fs
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

TARGET_TABLE = "FCT_TRIPS_GOLD"
GOLD_PATH = "gold/fct_trips"  # s3://<this>, partitioned year=/month=
SCHEMA = "RAW"  # objects are unquoted-uppercase in Snowflake; the connector
                # quotes identifiers, so pass upper-case to match.


def read_gold_month(year: int, month: int):
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    scheme, _, host = endpoint.partition("://")
    s3 = fs.S3FileSystem(
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        endpoint_override=host,
        scheme=scheme,
    )
    dataset = ds.dataset(GOLD_PATH, filesystem=s3, format="parquet", partitioning="hive")
    table = dataset.to_table(
        filter=(ds.field("year") == year) & (ds.field("month") == month)
    )
    df = table.to_pandas()
    df.columns = [c.upper() for c in df.columns]
    return df


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"].upper(),
        schema=SCHEMA,
    )


def main(year: int, month: int) -> None:
    df = read_gold_month(year, month)
    print(f"read {len(df):,} gold rows for {year}-{month:02d} from MinIO")

    conn = connect()
    try:
        cur = conn.cursor()
        # DELETE the month first for idempotency; ignore "table does not exist"
        # on the very first load (write_pandas creates it below).
        try:
            cur.execute(
                f"DELETE FROM {SCHEMA}.{TARGET_TABLE} WHERE YEAR = %s AND MONTH = %s",
                (year, month),
            )
            print(f"deleted existing {year}-{month:02d} rows: {cur.rowcount}")
        except snowflake.connector.errors.ProgrammingError as e:
            if "does not exist" not in str(e):
                raise
            print("target table not present yet — will be auto-created")

        ok, nchunks, nrows, _ = write_pandas(
            conn, df, TARGET_TABLE,
            database=os.environ["SNOWFLAKE_DATABASE"].upper(), schema=SCHEMA,
            auto_create_table=True, overwrite=False,
            quote_identifiers=True, use_logical_type=True,
        )
        print(f"write_pandas ok={ok} rows={nrows:,} chunks={nchunks}")
    finally:
        conn.close()


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]))
