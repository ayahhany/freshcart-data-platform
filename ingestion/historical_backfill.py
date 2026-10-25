
import os
import argparse
import psycopg
from psycopg import sql


SOURCE_SCHEMA = "app"
BRONZE_SCHEMA = "bronze_app"
BATCH_SIZE = int(os.getenv("BACKFILL_BATCH_SIZE", "5000"))

# Stable logical keys, including the composite key for product_prices.
TABLE_KEYS = {
    "customers": ["customer_id"],
    "order_items": ["order_item_id"],
    "orders": ["order_id"],
    "payments": ["payment_id"],
    "product_prices": ["product_id", "valid_from"],
    "products": ["product_id"],
    "stores": ["store_id"],
}


def connect_source():
    return psycopg.connect(
        host=os.getenv("SOURCE_HOST", "source-db"),
        port=int(os.getenv("SOURCE_PORT", "5432")),
        dbname=os.getenv("SOURCE_DB", "freshcart_source"),
        user=os.environ["SOURCE_USER"],
        password=os.environ["SOURCE_PASSWORD"],
        application_name="freshcart_historical_backfill",
    )


def connect_warehouse():
    return psycopg.connect(
        host=os.getenv("DW_HOST", "data-warehouse"),
        port=int(os.getenv("DW_PORT", "5432")),
        dbname=os.getenv("DW_DB", "freshcart_dw"),
        user=os.environ["DW_USER"],
        password=os.environ["DW_PASSWORD"],
        application_name="freshcart_historical_backfill",
    )



def ensure_checkpoint_table(warehouse):
    with warehouse.transaction():
        warehouse.execute("""
            CREATE TABLE IF NOT EXISTS staging_app._historical_backfill_tables (
                table_name text PRIMARY KEY,
                completed_at timestamptz NOT NULL,
                rows_processed bigint NOT NULL
            )
        """)


def is_completed(warehouse, table):
    with warehouse.transaction():
        result = warehouse.execute("""
            SELECT rows_processed
            FROM staging_app._historical_backfill_tables
            WHERE table_name = %s
        """, (table,)).fetchone()

    if result is not None:
        print(
            f"Skipping {table}: already completed "
            f"({result[0]:,} rows processed previously)",
            flush=True,
        )
        return True

    return False


def mark_completed(warehouse, table, rows_processed):
    with warehouse.transaction():
        warehouse.execute("""
            INSERT INTO staging_app._historical_backfill_tables
                (table_name, completed_at, rows_processed)
            VALUES (%s, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (table_name) DO UPDATE SET
                completed_at = EXCLUDED.completed_at,
                rows_processed = EXCLUDED.rows_processed
        """, (table, rows_processed))

def get_columns(conn, table):
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (SOURCE_SCHEMA, table),
    ).fetchall()

    columns = [row[0] for row in rows]
    if not columns:
        raise RuntimeError(f"Source table not found: {table}")

    return columns


def load_table(source, warehouse, table, keys):
    columns = get_columns(source, table)

    missing_keys = set(keys) - set(columns)
    if missing_keys:
        raise RuntimeError(
            f"{table} is missing logical key columns: {sorted(missing_keys)}"
        )

    source_select = sql.SQL(
        "SELECT {} FROM {}.{}"
    ).format(
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.Identifier(SOURCE_SCHEMA),
        sql.Identifier(table),
    )

    target_columns = columns + [
        "_source_schema",
        "_source_table",
        "_ingested_at",
    ]

    insert_sql = sql.SQL(
        "INSERT INTO {}.{} ({}) VALUES ({}, %s, %s, CURRENT_TIMESTAMP) "
        "ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        sql.Identifier(BRONZE_SCHEMA),
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, target_columns)),
        sql.SQL(", ").join(
            [sql.Placeholder()] * len(columns)
        ),
        sql.SQL(", ").join(map(sql.Identifier, keys)),
        sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(column), sql.Identifier(column)
            )
            for column in target_columns
            if column not in keys
        ),
    )

    last_key = None
    total = 0

    while True:
        if last_key is None:
            query = source_select + sql.SQL(
                " ORDER BY {} LIMIT %s"
            ).format(
                sql.SQL(", ").join(map(sql.Identifier, keys))
            )
            params = (BATCH_SIZE,)
        else:
            key_expression = (
                sql.Identifier(keys[0])
                if len(keys) == 1
                else sql.SQL("({})").format(
                    sql.SQL(", ").join(map(sql.Identifier, keys))
                )
            )
            key_placeholders = (
                sql.Placeholder()
                if len(keys) == 1
                else sql.SQL("({})").format(
                    sql.SQL(", ").join(
                        sql.Placeholder() for _ in keys
                    )
                )
            )
            query = source_select + sql.SQL(
                " WHERE {} > {} ORDER BY {} LIMIT %s"
            ).format(
                key_expression,
                key_placeholders,
                sql.SQL(", ").join(map(sql.Identifier, keys)),
            )
            key_params = (
                (last_key,)
                if len(keys) == 1
                else tuple(last_key)
            )
            params = key_params + (BATCH_SIZE,)

        rows = source.execute(query, params).fetchall()
        if not rows:
            break

        batch = []
        for row in rows:
            values = list(row)
            values.extend([SOURCE_SCHEMA, table])
            batch.append(tuple(values))

        with warehouse.transaction():
            warehouse.cursor().executemany(insert_sql, batch)

        last_key = tuple(rows[-1][columns.index(key)] for key in keys)
        if len(keys) == 1:
            last_key = last_key[0]

        total += len(rows)
        print(f"  {table}: upserted {total:,} rows", flush=True)

    print(f"Finished {table}: {total:,} source rows processed", flush=True)
    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table",
        choices=list(TABLE_KEYS.keys()),
        help="Process only one table instead of all tables.",
    )
    args = parser.parse_args()

    total = 0

    with connect_source() as source, connect_warehouse() as warehouse:
        ensure_checkpoint_table(warehouse)

        tables = (
            {args.table: TABLE_KEYS[args.table]}
            if args.table
            else TABLE_KEYS
        )

        for table, keys in tables.items():
            if is_completed(warehouse, table):
                continue

            print(f"\nStarting {table}...", flush=True)

            with source.transaction():
                source.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                processed = load_table(source, warehouse, table, keys)

            mark_completed(warehouse, table, processed)
            total += processed

    print(
        f"\nBackfill finished. Rows processed in this run: {total:,}",
        flush=True,
    )


if __name__ == "__main__":
    main()