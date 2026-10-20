import os
from datetime import datetime, timezone

import psycopg
from psycopg import sql


# Source table names and their primary keys.
# product_prices has a composite primary key.
SOURCE_TABLES = {
    "customers": ["customer_id"],
    "order_items": ["order_item_id"],
    "orders": ["order_id"],
    "payments": ["payment_id"],
    "product_prices": ["product_id", "valid_from"],
    "products": ["product_id"],
    "stores": ["store_id"],
}

SOURCE_SCHEMA = "app"
STAGING_SCHEMA = "staging_app"
BRONZE_SCHEMA = "bronze_app"
BATCH_SIZE = 10_000


def get_source_connection():
    """Connect using the source's read-only account."""
    return psycopg.connect(
        host=os.getenv("SOURCE_HOST", "source-db"),
        port=os.getenv("SOURCE_PORT", "5432"),
        dbname=os.getenv("SOURCE_DB", "freshcart_source"),
        user=os.environ["SOURCE_USER"],
        password=os.environ["SOURCE_PASSWORD"],
        application_name="freshcart_initial_full_load",
    )


def get_warehouse_connection():
    """Connect to the separate analytics warehouse."""
    return psycopg.connect(
        host=os.getenv("DW_HOST", "data-warehouse"),
        port=os.getenv("DW_PORT", "5432"),
        dbname=os.getenv("DW_DB", "freshcart_dw"),
        user=os.environ["DW_USER"],
        password=os.environ["DW_PASSWORD"],
        application_name="freshcart_initial_full_load",
    )


def get_source_columns(source_conn, table_name):
    """Read source column definitions without changing the source."""
    query = """
        SELECT
            column_name,
            data_type,
            udt_name,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """

    with source_conn.cursor() as cur:
        cur.execute(query, (SOURCE_SCHEMA, table_name))
        columns = cur.fetchall()

    if not columns:
        raise RuntimeError(
            f"Source table {SOURCE_SCHEMA}.{table_name} was not found."
        )

    return columns


def postgres_type(column):
    """Map source PostgreSQL types to warehouse column types."""
    data_type = column[1]
    udt_name = column[2]
    char_length = column[3]
    precision = column[4]
    scale = column[5]

    if data_type == "character varying":
        if char_length is not None:
            return sql.SQL("varchar({})").format(sql.Literal(char_length))
        return sql.SQL("varchar")

    if data_type == "character":
        if char_length is not None:
            return sql.SQL("char({})").format(sql.Literal(char_length))
        return sql.SQL("char")

    if data_type == "numeric":
        if precision is not None and scale is not None:
            return sql.SQL("numeric({}, {})").format(
                sql.Literal(precision),
                sql.Literal(scale),
            )
        return sql.SQL("numeric")

    if data_type == "USER-DEFINED":
        return sql.Identifier(udt_name)

    return sql.SQL(data_type)


def create_staging_table(warehouse_conn, table_name, source_columns):
    """Create a staging table preserving source columns and primary keys."""
    definitions = []

    for column in source_columns:
        column_name = column[0]
        nullable = column[6] == "YES"

        definition = sql.SQL("{} {}{}").format(
            sql.Identifier(column_name),
            postgres_type(column),
            sql.SQL("") if nullable else sql.SQL(" NOT NULL"),
        )
        definitions.append(definition)

    # Metadata describes where and when the row was ingested.
    definitions.extend([
        sql.SQL("_source_schema text NOT NULL"),
        sql.SQL("_source_table text NOT NULL"),
        sql.SQL("_ingested_at timestamptz NOT NULL"),
    ])

    primary_key = SOURCE_TABLES[table_name]
    definitions.append(
        sql.SQL("PRIMARY KEY ({})").format(
            sql.SQL(", ").join(
                sql.Identifier(key) for key in primary_key
            )
        )
    )

    query = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {}.{} ({})"
    ).format(
        sql.Identifier(STAGING_SCHEMA),
        sql.Identifier(table_name),
        sql.SQL(", ").join(definitions),
    )

    with warehouse_conn.cursor() as cur:
        cur.execute(query)


def load_table(source_conn, warehouse_conn, table_name, ingested_at):
    """Stream one source table into staging using PostgreSQL COPY."""
    source_columns = get_source_columns(source_conn, table_name)
    column_names = [column[0] for column in source_columns]

    target_columns = column_names + [
        "_source_schema",
        "_source_table",
        "_ingested_at",
    ]

    source_query = sql.SQL("SELECT {} FROM {}.{}").format(
        sql.SQL(", ").join(
            sql.Identifier(name) for name in column_names
        ),
        sql.Identifier(SOURCE_SCHEMA),
        sql.Identifier(table_name),
    )

    copy_query = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN"
    ).format(
        sql.Identifier(STAGING_SCHEMA),
        sql.Identifier(table_name),
        sql.SQL(", ").join(
            sql.Identifier(name) for name in target_columns
        ),
    )

    # Each table is committed separately in staging.
    # A failed run never publishes partial data to Bronze.
    with warehouse_conn.transaction():
        create_staging_table(
            warehouse_conn, table_name, source_columns
        )

        with warehouse_conn.cursor() as cur:
            cur.execute(
                sql.SQL("TRUNCATE TABLE {}.{}").format(
                    sql.Identifier(STAGING_SCHEMA),
                    sql.Identifier(table_name),
                )
            )

        loaded = 0

        with source_conn.cursor(name=f"full_load_{table_name}") as src:
            src.itersize = BATCH_SIZE
            src.execute(source_query)

            with warehouse_conn.cursor() as dst:
                with dst.copy(copy_query) as copy:
                    while True:
                        rows = src.fetchmany(BATCH_SIZE)
                        if not rows:
                            break

                        for row in rows:
                            copy.write_row(
                                tuple(row)
                                + (SOURCE_SCHEMA, table_name, ingested_at)
                            )

                        loaded += len(rows)
                        print(
                            f"  {table_name}: processed {loaded:,} rows",
                            flush=True,
                        )

        # Count the source table within the same consistent snapshot.
        with source_conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(SOURCE_SCHEMA),
                    sql.Identifier(table_name),
                )
            )
            source_count = cur.fetchone()[0]

        with warehouse_conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(STAGING_SCHEMA),
                    sql.Identifier(table_name),
                )
            )
            staging_count = cur.fetchone()[0]

        if loaded != source_count or staging_count != source_count:
            raise RuntimeError(
                f"Count mismatch for {table_name}: "
                f"streamed={loaded:,}, source={source_count:,}, "
                f"staging={staging_count:,}"
            )

    print(
        f"Validated {table_name}: {source_count:,} rows",
        flush=True,
    )
    return source_count


def publish_tables(warehouse_conn):
    """Atomically replace the Bronze tables with validated staging tables."""
    with warehouse_conn.transaction():
        for table_name in SOURCE_TABLES:
            with warehouse_conn.cursor() as cur:
                # Initial-load publication. If a Bronze table already exists,
                # replace it in the same transaction as the other tables.
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                        sql.Identifier(BRONZE_SCHEMA),
                        sql.Identifier(table_name),
                    )
                )

                cur.execute(
                    sql.SQL("ALTER TABLE {}.{} SET SCHEMA {}").format(
                        sql.Identifier(STAGING_SCHEMA),
                        sql.Identifier(table_name),
                        sql.Identifier(BRONZE_SCHEMA),
                    )
                )

    print("All seven tables published to bronze_app.", flush=True)


def main():
    ingested_at = datetime.now(timezone.utc)
    total_rows = 0

    # One repeatable-read snapshot keeps all source-table reads consistent.
    with get_source_connection() as source_conn:
        source_conn.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
        )

        with get_warehouse_connection() as warehouse_conn:
            for table_name in SOURCE_TABLES:
                print(
                    f"Loading {SOURCE_SCHEMA}.{table_name} into staging...",
                    flush=True,
                )
                total_rows += load_table(
                    source_conn,
                    warehouse_conn,
                    table_name,
                    ingested_at,
                )

            print(
                f"All staging tables validated: {total_rows:,} rows.",
                flush=True,
            )

            publish_tables(warehouse_conn)

    print(
        f"Initial full load completed successfully: "
        f"{total_rows:,} rows published.",
        flush=True,
    )


if __name__ == "__main__":
    main()
