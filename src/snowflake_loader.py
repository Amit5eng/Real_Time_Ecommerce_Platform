import os
from pathlib import Path

import snowflake.connector


PROJECT_DIR = Path(__file__).resolve().parent.parent
GOLD_DIR = PROJECT_DIR / "data" / "gold"


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse="COMPUTE_WH",
        database="ECOMMERCE_DB",
        schema="ANALYTICS",
    )


TABLES = {
    "payment_metrics": {
        "file": GOLD_DIR / "payment_metrics" / "part-00000.parquet",
        "table": "PAYMENT_METRICS",
        "columns": """
            PAYMENT_COUNT,
            TOTAL_PAYMENT_AMOUNT,
            AVERAGE_PAYMENT_AMOUNT
        """,
        "select": """
            $1:payment_count::INTEGER,
            $1:total_payment_amount::NUMBER(18,2),
            $1:average_payment_amount::NUMBER(18,2)
        """,
    },
    "city_payment_metrics": {
        "file": GOLD_DIR / "city_payment_metrics" / "part-00000.parquet",
        "table": "CITY_PAYMENT_METRICS",
        "columns": """
            CITY,
            PAYMENT_COUNT,
            TOTAL_PAYMENT_AMOUNT,
            AVERAGE_PAYMENT_AMOUNT
        """,
        "select": """
            $1:city::VARCHAR,
            $1:payment_count::INTEGER,
            $1:total_payment_amount::NUMBER(18,2),
            $1:average_payment_amount::NUMBER(18,2)
        """,
    },
    "event_type_metrics": {
        "file": GOLD_DIR / "event_type_metrics" / "part-00000.parquet",
        "table": "EVENT_TYPE_METRICS",
        "columns": """
            EVENT_TYPE,
            EVENT_COUNT
        """,
        "select": """
            $1:event_type::VARCHAR,
            $1:event_count::INTEGER
        """,
    },
    "product_metrics": {
        "file": GOLD_DIR / "product_metrics" / "part-00000.parquet",
        "table": "PRODUCT_METRICS",
        "columns": """
            PRODUCT_ID,
            EVENT_COUNT,
            TOTAL_QUANTITY,
            TOTAL_AMOUNT
        """,
        "select": """
            $1:product_id::VARCHAR,
            $1:event_count::INTEGER,
            $1:total_quantity::INTEGER,
            $1:total_amount::NUMBER(18,2)
        """,
    },
}


def main():
    print("Snowflake Gold Loader Started")

    # Validate all local files before connecting or changing Snowflake tables.
    for name, config in TABLES.items():
        file_path = config["file"]

        if not file_path.is_file():
            raise FileNotFoundError(f"Gold file not found: {file_path}")

        if file_path.stat().st_size == 0:
            raise ValueError(f"Gold file is empty: {file_path}")

        print(f"Validated local file: {name}")

    conn = get_connection()
    cursor = conn.cursor()
    transaction_started = False

    try:
        # Create the temporary stage before starting the DML transaction.
        cursor.execute("""
            CREATE OR REPLACE TEMPORARY STAGE GOLD_STAGE
            FILE_FORMAT = (TYPE = PARQUET)
        """)

        # Upload every dataset to a separate stage path.
        for name, config in TABLES.items():
            file_path = config["file"].resolve().as_posix()
            stage_path = f"@GOLD_STAGE/{name}/"

            put_sql = (
                f"PUT 'file://{file_path}' {stage_path} "
                "OVERWRITE = TRUE AUTO_COMPRESS = FALSE PARALLEL = 1"
            )

            print(f"Uploading {name} to Snowflake stage...")
            cursor.execute(put_sql)

            upload_results = cursor.fetchall()
            if not upload_results:
                raise RuntimeError(f"No upload result returned for {name}")

            print(f"Upload completed: {name}")

        # Disable autocommit so the target-table changes are grouped.
        conn.autocommit(False)
        cursor.execute("BEGIN TRANSACTION")
        transaction_started = True

        for name, config in TABLES.items():
            table = config["table"]
            stage_path = f"@GOLD_STAGE/{name}"

            print(f"Replacing data in {table}...")

            cursor.execute(f"DELETE FROM ANALYTICS.{table}")

            copy_sql = f"""
                COPY INTO ANALYTICS.{table}
                ({config["columns"]})
                FROM (
                    SELECT {config["select"]}
                    FROM {stage_path}
                )
                FILE_FORMAT = (TYPE = PARQUET)
                ON_ERROR = ABORT_STATEMENT
                PURGE = FALSE
                FORCE = TRUE
            """

            cursor.execute(copy_sql)
            copy_results = cursor.fetchall()

            # Reject a table load if Snowflake reports a file-level error.
            for result in copy_results:
                if len(result) > 1 and result[1] != "LOADED":
                    raise RuntimeError(
                        f"COPY did not load a file successfully for {table}: "
                        f"{result}"
                    )

            cursor.execute(
                f"SELECT COUNT(*) FROM ANALYTICS.{table}"
            )
            row_count = cursor.fetchone()[0]

            expected_rows = {
                "PAYMENT_METRICS": 1,
                "CITY_PAYMENT_METRICS": 6,
                "EVENT_TYPE_METRICS": 5,
                "PRODUCT_METRICS": 900,
            }[table]

            if row_count != expected_rows:
                raise RuntimeError(
                    f"Validation failed for {table}: "
                    f"expected {expected_rows} rows, found {row_count}."
                )

            print(f"{table}: {row_count} rows verified")

        conn.commit()
        transaction_started = False

        print("\nAll four Gold tables loaded and validated.")
        print("Snowflake Gold Loader Completed Successfully")

    except Exception:
        if transaction_started:
            try:
                conn.rollback()
                print("Transaction rolled back.")
            except Exception:
                print(
                    "WARNING: Rollback failed; inspect Snowflake table state."
                )

        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
