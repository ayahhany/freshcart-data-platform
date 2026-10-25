import os
from datetime import datetime, timezone

import psycopg
from psycopg import sql


SOURCE_SCHEMA = "app"
STAGING_SCHEMA = "staging_app"
BRONZE_SCHEMA = "bronze_app"

BATCH_SIZE = 250
CHECKPOINT_TABLE = "_load_checkpoints"

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
    """Connect to the read-only FreshCart source database."""
    return psycopg.connect(
        host=os.getenv("SOURCE_HOST", "source-db"),
        port=int(os.getenv("SOURCE_PORT", "5432")),
        dbname=os.getenv("SOURCE_DB", "freshcart_source"),
        user=os.environ["SOURCE_USER"],
        password=os.environ["SOURCE_PASSWORD"],
        autocommit=True,
    )


def get_warehouse_connection():
    """Connect to the FreshCart data warehouse."""
    return psycopg.connect(
        host=os.getenv("DW_HOST", "data-warehouse"),
        port=int(os.getenv("DW_PORT", "5432")),
        dbname=os.getenv("DW_DB", "freshcart_dw"),
        user=os.environ["DW_USER"],
        password=os.environ["DW_PASSWORD"],
        autocommit=True,
    )


def get_source_columns(source_conn, table_name):
    """Return source column metadata in physical column order."""
    with source_conn.cursor() as cur:
        cur.execute(
            """
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
            """,
            (SOURCE_SCHEMA, table_name),
        )
        columns = cur.fetchall()

    if not columns:
        raise RuntimeError(
            f"No columns found for {SOURCE_SCHEMA}.{table_name}"
        )

    return columns


def postgres_type(column):
    """Build a PostgreSQL type expression from source metadata."""
    data_type = column[1]
    udt_name = column[2]
    char_length = column[3]
    precision = column[4]
    scale = column[5]

    if data_type in ("character varying", "character"):
        if char_length is not None:
            return sql.SQL("{}({})").format(
                sql.SQL(data_type),
                sql.Literal(char_length),
            )
        return sql.SQL(data_type)

    if data_type == "numeric":
        if precision is not None:
            if scale is not None:
                return sql.SQL("numeric({}, {})").format(
                    sql.Literal(precision),
                    sql.Literal(scale),
                )
            return sql.SQL("numeric({})").format(
                sql.Literal(precision),
            )
        return sql.SQL("numeric")

    if data_type == "USER-DEFINED":
        return sql.Identifier(udt_name)

    return sql.SQL(data_type)


def create_staging_table(warehouse_conn, table_name, source_columns):
    """Create staging with source columns, metadata, and source primary key."""
    definitions = []

    for column in source_columns:
        column_name = column[0]
        nullable = column[6] == "YES"

        definitions.append(
            sql.SQL("{} {}{}").format(
                sql.Identifier(column_name),
                postgres_type(column),
                sql.SQL("") if nullable else sql.SQL(" NOT NULL"),
            )
        )

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


def ensure_checkpoint_table(warehouse_conn):
    """Create the durable checkpoint table without disturbing staging data."""
    query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {}.{} (
            table_name text PRIMARY KEY,
            last_key_1 text,
            last_key_2 text
        )
    """).format(
        sql.Identifier(STAGING_SCHEMA),
        sql.Identifier(CHECKPOINT_TABLE),
    )

    with warehouse_conn.cursor() as cur:
        cur.execute(query)


def get_checkpoint(warehouse_conn, table_name):
    """Read the last committed primary-key values for a table."""
    query = sql.SQL(
        "SELECT last_key_1, last_key_2 FROM {}.{} WHERE table_name = %s"
    ).format(
        sql.Identifier(STAGING_SCHEMA),
        sql.Identifier(CHECKPOINT_TABLE),
    )

    with warehouse_conn.cursor() as cur:
        cur.execute(query, (table_name,))
        return cur.fetchone()


def get_staging_count(warehouse_conn, table_name):
    """Return the current staging row count."""
    query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
        sql.Identifier(STAGING_SCHEMA),
        sql.Identifier(table_name),
    )

    with warehouse_conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]


def build_batch_query(table_name, source_columns, last_keys):
    """Build a primary-key keyset query for the next bounded batch."""
    column_names = [column[0] for column in source_columns]
    primary_key = SOURCE_TABLES[table_name]
    column_by_name = {
        column[0]: column for column in source_columns
    }

    select_columns = sql.SQL(", ").join(
        sql.Identifier(name) for name in column_names
    )

    query = sql.SQL("SELECT {} FROM {}.{}").format(
        select_columns,
        sql.Identifier(SOURCE_SCHEMA),
        sql.Identifier(table_name),
    )

    params = []

    # A NULL first key means this table has not loaded any batches yet.
    if last_keys[0] is not None:
        key_types = [
            postgres_type(column_by_name[key])
            for key in primary_key
        ]

        if len(primary_key) == 1:
            where_clause = sql.SQL("{} > CAST(%s AS {})").format(
                sql.Identifier(primary_key[0]),
                key_types[0],
            )
        else:
            left_keys = sql.SQL(", ").join(
                sql.Identifier(key) for key in primary_key
            )
            right_keys = sql.SQL(", ").join(
                sql.SQL("CAST(%s AS {})").format(key_type)
                for key_type in key_types
            )

            where_clause = sql.SQL("({}, {}) > ({})").format(
                sql.Identifier(primary_key[0]),
                sql.Identifier(primary_key[1]),
                right_keys,
            )

            # Build a proper row-value comparison for the composite key.
            where_clause = sql.SQL("ROW({}) > ROW({})").format(
                left_keys,
                right_keys,
            )

        query += sql.SQL(" WHERE ") + where_clause
        params.extend(last_keys[:len(primary_key)])

    query += sql.SQL(" ORDER BY {} LIMIT %s").format(
        sql.SQL(", ").join(
            sql.Identifier(key) for key in primary_key
        )
    )
    params.append(BATCH_SIZE)

    return query, params


def save_batch_and_checkpoint(
    warehouse_conn,
    table_name,
    target_columns,
    rows,
    source_column_names,
    ingested_at,
):
    """Commit a batch and its checkpoint in the same transaction."""
    primary_key = SOURCE_TABLES[table_name]
    column_indexes = {
        name: index for index, name in enumerate(source_column_names)
    }
    last_row = rows[-1]

    last_key_1 = str(last_row[column_indexes[primary_key[0]]])
    last_key_2 = (
        str(last_row[column_indexes[primary_key[1]]])
        if len(primary_key) == 2
        else None
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

    checkpoint_query = sql.SQL("""
        UPDATE {}.{}
        SET last_key_1 = %s,
            last_key_2 = %s
        WHERE table_name = %s
    """).format(
        sql.Identifier(STAGING_SCHEMA),
        sql.Identifier(CHECKPOINT_TABLE),
    )

    with warehouse_conn.transaction():
        with warehouse_conn.cursor() as cur:
            with cur.copy(copy_query) as copy:
                for row in rows:
                    copy.write_row(
                        tuple(row)
                        + (
                            SOURCE_SCHEMA,
                            table_name,
                            ingested_at,
                        )
                    )

            cur.execute(
                checkpoint_query,
                (last_key_1, last_key_2, table_name),
            )

            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Could not update checkpoint for {table_name}"
                )


def load_table(source_conn, warehouse_conn, table_name, ingested_at):
    """Load a table in committed batches and resume from its last checkpoint."""
    source_columns = get_source_columns(source_conn, table_name)
    column_names = [column[0] for column in source_columns]
    primary_key = SOURCE_TABLES[table_name]

    target_columns = column_names + [
        "_source_schema",
        "_source_table",
        "_ingested_at",
    ]

    # Initialize the table and checkpoint atomically.
    with warehouse_conn.transaction():
        create_staging_table(
            warehouse_conn,
            table_name,
            source_columns,
        )

        with warehouse_conn.cursor() as cur:

            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(STAGING_SCHEMA),
                    sql.Identifier(table_name),
                )
            )
            staging_count = cur.fetchone()[0]

            cur.execute(
                sql.SQL("""
                    SELECT last_key_1, last_key_2
                    FROM {}.{}
                    WHERE table_name = %s
                """).format(
                    sql.Identifier(STAGING_SCHEMA),
                    sql.Identifier(CHECKPOINT_TABLE),
                ),
                (table_name,),
            )
            checkpoint = cur.fetchone()

            if checkpoint is None:
                # Never discard existing staging data to initialize a checkpoint.
                if staging_count > 0:
                    raise RuntimeError(
                        f"{table_name} has {staging_count:,} staged rows "
                        "but no checkpoint. Refusing to truncate or reload "
                        "the table; inspect its state first."
                    )

                cur.execute(
                    sql.SQL("""
                        INSERT INTO {}.{} (
                            table_name, last_key_1, last_key_2
                        ) VALUES (%s, NULL, NULL)
                    """).format(
                        sql.Identifier(STAGING_SCHEMA),
                        sql.Identifier(CHECKPOINT_TABLE),
                    ),
                    (table_name,),
                )
                last_keys = [None, None]

            else:
                last_keys = list(checkpoint)

                if last_keys[0] is None and last_keys[1] is not None:
                    raise RuntimeError(
                        f"{table_name} has an invalid checkpoint: "
                        "the second key exists but the first key is NULL."
                    )

                if len(primary_key) == 2:
                    if last_keys[0] is not None and last_keys[1] is None:
                        raise RuntimeError(
                            f"{table_name} has an incomplete composite-key "
                            "checkpoint. Refusing to resume."
                        )
                elif last_keys[1] is not None:
                    raise RuntimeError(
                        f"{table_name} has an unexpected second checkpoint "
                        "key. Refusing to resume."
                    )

                if staging_count == 0 and last_keys[0] is not None:
                    raise RuntimeError(
                        f"{table_name} has a checkpoint but no staged rows. "
                        "Refusing to reset the checkpoint automatically."
                    )

                if staging_count > 0 and last_keys[0] is None:
                    raise RuntimeError(
                        f"{table_name} has staged rows but its checkpoint "
                        "is empty. Refusing to resume."
                    )

                if staging_count > 0 and last_keys[0] is not None:
                    key_columns = sql.SQL(", ").join(
                        sql.Identifier(key) for key in primary_key
                    )

                    checkpoint_values = [
                        last_keys[0],
                        last_keys[1],
                    ][:len(primary_key)]

                    cast_types = {
                        "integer": "integer",
                        "timestamp with time zone": "timestamptz",
                    }

                    source_types = {
                        column[0]: column[1]
                        for column in source_columns
                    }



                    unsupported_types = {
                        key: source_types[key]
                        for key in primary_key
                        if source_types[key] not in cast_types
                    }

                    if unsupported_types:
                        raise RuntimeError(
                            f"Unsupported primary-key types for "
                            f"{table_name}: {unsupported_types}"
                        )

                    cast_values = sql.SQL(", ").join(
                        sql.SQL("CAST(%s AS {})").format(
                            sql.SQL(cast_types[source_types[key]])
                        )
                        for key in primary_key
                    )



                    cur.execute(
                        sql.SQL("""
                            SELECT
                                EXISTS (
                                    SELECT 1
                                    FROM {}.{}
                                    WHERE ROW({}) = ROW({})
                                ),
                                EXISTS (
                                    SELECT 1
                                    FROM {}.{}
                                    WHERE ROW({}) > ROW({})
                                )
                        """).format(
                            sql.Identifier(STAGING_SCHEMA),
                            sql.Identifier(table_name),
                            key_columns,
                            cast_values,
                            sql.Identifier(STAGING_SCHEMA),
                            sql.Identifier(table_name),
                            key_columns,
                            cast_values,
                        ),
                        checkpoint_values * 2,
                    )

                    checkpoint_exists, rows_after_checkpoint = cur.fetchone()

                    if not checkpoint_exists or rows_after_checkpoint:
                        raise RuntimeError(
                            f"{table_name} staging data does not match its "
                            "checkpoint. Refusing to resume automatically."
                        )


    loaded = get_staging_count(warehouse_conn, table_name)

    if last_keys[0] is not None:
        print(
            f"Resuming {table_name} after primary key "
            f"{tuple(last_keys[:len(primary_key)])}; "
            f"{loaded:,} rows currently staged",
            flush=True,
        )
    else:
        print(f"Starting load for {table_name}", flush=True)

    while True:
        batch_query, params = build_batch_query(
            table_name,
            source_columns,
            last_keys,
        )

        with source_conn.cursor() as src:
            src.execute(batch_query, params)
            rows = src.fetchall()

        if not rows:
            break

        save_batch_and_checkpoint(
            warehouse_conn,
            table_name,
            target_columns,
            rows,
            column_names,
            ingested_at,
        )

        last_row = rows[-1]
        key_indexes = {name: column_names.index(name) for name in primary_key}
        last_keys = [
            str(last_row[key_indexes[key]]) for key in primary_key
        ]

        if len(last_keys) == 1:
            last_keys.append(None)

        loaded += len(rows)

        print(
            f"  {table_name}: committed {loaded:,} rows",
            flush=True,
        )

    # Validate after the final batch. The source must remain stable during
    # this initial load; a changing source will cause validation to fail.
    with source_conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(SOURCE_SCHEMA),
                sql.Identifier(table_name),
            )
        )
        source_count = cur.fetchone()[0]

    staging_count = get_staging_count(warehouse_conn, table_name)

    metadata_query = sql.SQL("""
        SELECT COUNT(*)
        FROM {}.{}
        WHERE _source_schema = %s
          AND _source_table = %s
    """).format(
        sql.Identifier(STAGING_SCHEMA),
        sql.Identifier(table_name),
    )

    with warehouse_conn.cursor() as cur:
        cur.execute(metadata_query, (SOURCE_SCHEMA, table_name))
        valid_metadata_count = cur.fetchone()[0]

    if not (
        source_count == staging_count == valid_metadata_count
    ):
        raise RuntimeError(
            f"Validation failed for {table_name}: "
            f"source={source_count:,}, "
            f"staging={staging_count:,}, "
            f"valid_metadata={valid_metadata_count:,}"
        )

    print(
        f"Validated {table_name}: {source_count:,} rows",
        flush=True,
    )
    return source_count


def get_validated_staging_count(
    source_conn,
    warehouse_conn,
    table_name,
):
    """Reuse staging only when counts and row provenance all match."""
    with warehouse_conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name = %s
            )
            """,
            (STAGING_SCHEMA, table_name),
        )
        table_exists = cur.fetchone()[0]

    if not table_exists:
        return None

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

        cur.execute(
            sql.SQL("""
                SELECT COUNT(*)
                FROM {}.{}
                WHERE _source_schema = %s
                  AND _source_table = %s
            """).format(
                sql.Identifier(STAGING_SCHEMA),
                sql.Identifier(table_name),
            ),
            (SOURCE_SCHEMA, table_name),
        )
        valid_metadata_count = cur.fetchone()[0]

    if (
        source_count == staging_count == valid_metadata_count
    ):
        print(
            f"Reusing validated staging table {table_name}: "
            f"{staging_count:,} rows",
            flush=True,
        )
        return staging_count

    print(
        f"Staging table {table_name} needs loading or repair "
        f"(source={source_count:,}, staging={staging_count:,}, "
        f"valid_metadata={valid_metadata_count:,})",
        flush=True,
    )
    return None


def publish_tables(warehouse_conn):
    """Atomically replace Bronze tables with the validated staging tables."""
    with warehouse_conn.transaction():
        with warehouse_conn.cursor() as cur:
            for table_name in SOURCE_TABLES:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                        sql.Identifier(BRONZE_SCHEMA),
                        sql.Identifier(table_name),
                    )
                )

            for table_name in SOURCE_TABLES:
                cur.execute(
                    sql.SQL("ALTER TABLE {}.{} SET SCHEMA {}").format(
                        sql.Identifier(STAGING_SCHEMA),
                        sql.Identifier(table_name),
                        sql.Identifier(BRONZE_SCHEMA),
                    )
                )

            # Clear checkpoints only after all tables have been moved.
            # A future run will initialize fresh staging tables.
            cur.execute(
                sql.SQL("DELETE FROM {}.{}").format(
                    sql.Identifier(STAGING_SCHEMA),
                    sql.Identifier(CHECKPOINT_TABLE),
                )
            )

    print(
        "Published all validated tables to bronze_app.",
        flush=True,
    )


def main():
    ingested_at = datetime.now(timezone.utc)

    print("Starting FreshCart full-load ingestion.", flush=True)

    with get_source_connection() as source_conn:
        with get_warehouse_connection() as warehouse_conn:
            ensure_checkpoint_table(warehouse_conn)

            total_rows = 0

            for table_name in SOURCE_TABLES:
                validated_count = get_validated_staging_count(
                    source_conn,
                    warehouse_conn,
                    table_name,
                )

                if validated_count is not None:
                    total_rows += validated_count
                    continue

                total_rows += load_table(
                    source_conn,
                    warehouse_conn,
                    table_name,
                    ingested_at,
                )

            print(
                f"All seven tables validated. Total rows: "
                f"{total_rows:,}",
                flush=True,
            )

            publish_tables(warehouse_conn)

    print("FreshCart full-load ingestion completed successfully.", flush=True)


if __name__ == "__main__":
    main()
