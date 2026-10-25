
import argparse
import os
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql


SOURCE_SCHEMA = "app"
BRONZE_SCHEMA = "bronze_app"
STATE_SCHEMA = "staging_app"
STATE_TABLE = "_incremental_days"
CAIRO = ZoneInfo("Africa/Cairo")

BATCH_SIZE = int(os.getenv("INCREMENTAL_BATCH_SIZE", "1000"))

SOURCE_TABLES = {
    "customers": ["customer_id"],
    "order_items": ["order_item_id"],
    "orders": ["order_id"],
    "payments": ["payment_id"],
    "product_prices": ["product_id", "valid_from"],
    "products": ["product_id"],
    "stores": ["store_id"],
}


def get_source_connection():
    return psycopg.connect(
        host=os.getenv("SOURCE_HOST", "source-db"),
        port=int(os.getenv("SOURCE_PORT", "5432")),
        dbname=os.getenv("SOURCE_DB", "freshcart_source"),
        user=os.environ["SOURCE_USER"],
        password=os.environ["SOURCE_PASSWORD"],
    )


def get_warehouse_connection():
    return psycopg.connect(
        host=os.getenv("DW_HOST", "data-warehouse"),
        port=int(os.getenv("DW_PORT", "5432")),
        dbname=os.getenv("DW_DB", "freshcart_dw"),
        user=os.environ["DW_USER"],
        password=os.environ["DW_PASSWORD"],
    )


def ensure_state_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(STATE_SCHEMA)
            )
        )
        cur.execute(
            sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.{} (
                    business_date date PRIMARY KEY,
                    loaded_at timestamptz NOT NULL,
                    rows_upserted bigint NOT NULL
                )
            """).format(
                sql.Identifier(STATE_SCHEMA),
                sql.Identifier(STATE_TABLE),
            )
        )


def get_source_columns(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (SOURCE_SCHEMA, table_name),
        )
        columns = [row[0] for row in cur.fetchall()]

    if not columns:
        raise RuntimeError(f"Source table not found: {table_name}")

    for required in ("created_at", "updated_at"):
        if required not in columns:
            raise RuntimeError(
                f"Source table {table_name} has no {required} column"
            )

    missing_keys = [
        key for key in SOURCE_TABLES[table_name] if key not in columns
    ]
    if missing_keys:
        raise RuntimeError(
            f"Missing logical key columns in {table_name}: {missing_keys}"
        )

    return columns


def fetch_batch(
    source_conn,
    table_name,
    columns,
    day_start,
    next_day_start,
    last_cursor=None,
):
    """
    Fetch rows changed or created during the Cairo business day.

    Pagination uses stable logical keys rather than updated_at. Each table
    is read inside a repeatable-read snapshot in load_table().
    """
    keys = SOURCE_TABLES[table_name]

    query = sql.SQL("""
        SELECT {}
        FROM {}.{}
        WHERE (
            (created_at >= %s AND created_at < %s)
            OR
            (updated_at >= %s AND updated_at < %s)
        )
    """).format(
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        sql.Identifier(SOURCE_SCHEMA),
        sql.Identifier(table_name),
    )

    params = [day_start, next_day_start, day_start, next_day_start]

    if last_cursor is not None:
        query += sql.SQL(" AND ({}) > ({})").format(
            sql.SQL(", ").join(sql.Identifier(k) for k in keys),
            sql.SQL(", ").join(sql.Placeholder() for _ in keys),
        )
        params.extend(last_cursor)

    query += sql.SQL(" ORDER BY {} LIMIT %s").format(
        sql.SQL(", ").join(sql.Identifier(k) for k in keys)
    )
    params.append(BATCH_SIZE)

    with source_conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def upsert_batch(warehouse_conn, table_name, columns, rows, ingested_at):
    keys = SOURCE_TABLES[table_name]
    metadata_columns = [
        "_source_schema",
        "_source_table",
        "_ingested_at",
    ]
    target_columns = columns + metadata_columns

    assignments = [
        sql.SQL("{} = EXCLUDED.{}").format(
            sql.Identifier(column),
            sql.Identifier(column),
        )
        for column in target_columns
        if column not in keys
    ]

    insert_query = sql.SQL("""
        INSERT INTO {}.{} ({})
        VALUES ({})
        ON CONFLICT ({})
        DO UPDATE SET {}
    """).format(
        sql.Identifier(BRONZE_SCHEMA),
        sql.Identifier(table_name),
        sql.SQL(", ").join(
            sql.Identifier(column) for column in target_columns
        ),
        sql.SQL(", ").join(
            sql.Placeholder() for _ in target_columns
        ),
        sql.SQL(", ").join(sql.Identifier(key) for key in keys),
        sql.SQL(", ").join(assignments),
    )

    values = [
        tuple(row) + (SOURCE_SCHEMA, table_name, ingested_at)
        for row in rows
    ]

    with warehouse_conn.cursor() as cur:
        cur.executemany(insert_query, values)


def load_table(
    source_conn,
    warehouse_conn,
    table_name,
    day_start,
    next_day_start,
):
    total_rows = 0
    last_cursor = None
    keys = SOURCE_TABLES[table_name]

    # Start a fresh, consistent source snapshot for this table.
    with source_conn.transaction():
        source_conn.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        )

        columns = get_source_columns(source_conn, table_name)

        while True:
            rows = fetch_batch(
                source_conn,
                table_name,
                columns,
                day_start,
                next_day_start,
                last_cursor=last_cursor,
            )

            if not rows:
                break

            ingested_at = datetime.now(timezone.utc)

            # Each batch commits independently. If a later batch fails,
            # rerunning the day safely upserts previously loaded rows.
            with warehouse_conn.transaction():
                upsert_batch(
                    warehouse_conn,
                    table_name,
                    columns,
                    rows,
                    ingested_at,
                )

            total_rows += len(rows)

            # The rows are ordered by the stable logical key(s).
            last_row = rows[-1]
            last_cursor = tuple(
                last_row[columns.index(key)] for key in keys
            )

            print(
                f"{table_name}: upserted {total_rows:,} rows so far",
                flush=True,
            )

            if len(rows) < BATCH_SIZE:
                break

    print(
        f"{table_name}: completed with {total_rows:,} rows",
        flush=True,
    )
    return total_rows


def load_business_day(source_conn, warehouse_conn, business_date):
    today_cairo = datetime.now(CAIRO).date()

    if business_date >= today_cairo:
        raise ValueError(
            f"{business_date} is not a completed Cairo business day. "
            f"Today in Cairo is {today_cairo}."
        )

    day_start = datetime.combine(
        business_date, time.min, tzinfo=CAIRO
    ).astimezone(timezone.utc)

    next_day_start = datetime.combine(
        business_date + timedelta(days=1),
        time.min,
        tzinfo=CAIRO,
    ).astimezone(timezone.utc)

    print(
        f"\nStarting Cairo business date {business_date}; "
        f"UTC window [{day_start.isoformat()}, "
        f"{next_day_start.isoformat()})",
        flush=True,
    )

    total = 0

    for table_name in SOURCE_TABLES:
        total += load_table(
            source_conn,
            warehouse_conn,
            table_name,
            day_start,
            next_day_start,
        )

    # Mark the day complete only after all seven tables succeed.
    # If any table fails, no completion marker is written for this day.
    with warehouse_conn.transaction():
        with warehouse_conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    INSERT INTO {}.{} (
                        business_date,
                        loaded_at,
                        rows_upserted
                    )
                    VALUES (%s, %s, %s)
                    ON CONFLICT (business_date)
                    DO UPDATE SET
                        loaded_at = EXCLUDED.loaded_at,
                        rows_upserted = EXCLUDED.rows_upserted
                """).format(
                    sql.Identifier(STATE_SCHEMA),
                    sql.Identifier(STATE_TABLE),
                ),
                (
                    business_date,
                    datetime.now(timezone.utc),
                    total,
                ),
            )

    print(
        f"Completed {business_date}: {total:,} rows upserted.",
        flush=True,
    )


def main():
    if BATCH_SIZE < 1:
        raise ValueError("INCREMENTAL_BATCH_SIZE must be positive")

    parser = argparse.ArgumentParser(
        description=(
            "Load one completed Cairo business day or backfill a date range."
        )
    )

    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--business-date",
        type=date.fromisoformat,
        help="Load one Cairo business date (YYYY-MM-DD).",
    )
    date_group.add_argument(
        "--start-date",
        type=date.fromisoformat,
        help="Inclusive start date for a historical backfill (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        help=(
            "Inclusive end date for a backfill. Requires --start-date; "
            "defaults to yesterday when --start-date is provided."
        ),
    )

    args = parser.parse_args()
    today_cairo = datetime.now(CAIRO).date()
    yesterday = today_cairo - timedelta(days=1)

    if args.end_date and not args.start_date:
        parser.error("--end-date requires --start-date")

    if args.business_date and args.end_date:
        parser.error("--end-date cannot be used with --business-date")

    if args.business_date:
        dates = [args.business_date]
    elif args.start_date:
        end_date = args.end_date or yesterday

        if args.start_date > end_date:
            parser.error("--start-date must be on or before --end-date")

        dates = []
        current_date = args.start_date
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
    else:
        dates = [yesterday]

    if any(day >= today_cairo for day in dates):
        parser.error(
            f"Only completed Cairo business days can be loaded. "
            f"Today is {today_cairo}."
        )

    source_conn = None
    warehouse_conn = None

    try:
        source_conn = get_source_connection()
        warehouse_conn = get_warehouse_connection()

        with warehouse_conn.transaction():
            ensure_state_table(warehouse_conn)

        for business_date in dates:
            load_business_day(
                source_conn,
                warehouse_conn,
                business_date,
            )

    finally:
        if source_conn is not None:
            source_conn.close()
        if warehouse_conn is not None:
            warehouse_conn.close()


if __name__ == "__main__":
    main()
