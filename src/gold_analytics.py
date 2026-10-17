from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent.parent

SILVER_DIR = PROJECT_DIR / "data" / "processed" / "ecommerce_events"
GOLD_DIR = PROJECT_DIR / "data" / "gold"

PAYMENT_GOLD_DIR = GOLD_DIR / "payment_metrics"
CITY_PAYMENT_GOLD_DIR = GOLD_DIR / "city_payment_metrics"
EVENT_TYPE_GOLD_DIR = GOLD_DIR / "event_type_metrics"
PRODUCT_GOLD_DIR = GOLD_DIR / "product_metrics"


# --------------------------------------------------
# Read Silver Parquet Files
# --------------------------------------------------

def read_silver_data():
    parquet_files = list(SILVER_DIR.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found in {SILVER_DIR}"
        )

    tables = []

    for file in parquet_files:
        table = pq.read_table(file)
        tables.append(table)

    return pa.concat_tables(tables)


# --------------------------------------------------
# Helper: Write Gold Dataset
# --------------------------------------------------

def write_gold(table, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "part-00000.parquet"

    pq.write_table(
        table,
        output_file,
        compression="snappy"
    )

    return output_file


# --------------------------------------------------
# 1. Payment Metrics
# --------------------------------------------------

def create_payment_metrics(table):

    payment_table = table.filter(
        pc.equal(
            table["event_type"],
            pa.scalar("payment_completed")
        )
    )

    payment_count = payment_table.num_rows

    total_amount = pc.sum(
        payment_table["amount"]
    ).as_py()

    average_amount = pc.mean(
        payment_table["amount"]
    ).as_py()

    result = pa.table({
        "payment_count": [payment_count],
        "total_payment_amount": [round(total_amount, 2)],
        "average_payment_amount": [round(average_amount, 2)]
    })

    write_gold(result, PAYMENT_GOLD_DIR)

    print("Payment Metrics:")
    print(result)


# --------------------------------------------------
# 2. City Payment Metrics
# --------------------------------------------------

def create_city_payment_metrics(table):

    payment_table = table.filter(
        pc.equal(
            table["event_type"],
            pa.scalar("payment_completed")
        )
    )

    cities = payment_table["city"].unique()

    rows = []

    for city in cities:
        city_value = city.as_py()

        city_table = payment_table.filter(
            pc.equal(
                payment_table["city"],
                pa.scalar(city_value)
            )
        )

        payment_count = city_table.num_rows

        total_amount = pc.sum(
            city_table["amount"]
        ).as_py()

        average_amount = pc.mean(
            city_table["amount"]
        ).as_py()

        rows.append({
            "city": city_value,
            "payment_count": payment_count,
            "total_payment_amount": round(total_amount, 2),
            "average_payment_amount": round(average_amount, 2)
        })

    result = pa.Table.from_pylist(rows)

    write_gold(result, CITY_PAYMENT_GOLD_DIR)

    print("\nCity Payment Metrics:")
    print(result)


# --------------------------------------------------
# 3. Event Type Metrics
# --------------------------------------------------

def create_event_type_metrics(table):

    event_types = table["event_type"].unique()

    rows = []

    for event_type in event_types:
        event_type_value = event_type.as_py()

        event_count = table.filter(
            pc.equal(
                table["event_type"],
                pa.scalar(event_type_value)
            )
        ).num_rows

        rows.append({
            "event_type": event_type_value,
            "event_count": event_count
        })

    result = pa.Table.from_pylist(rows)

    write_gold(result, EVENT_TYPE_GOLD_DIR)

    print("\nEvent Type Metrics:")
    print(result)


# --------------------------------------------------
# 4. Product Metrics
# --------------------------------------------------

def create_product_metrics(table):

    products = table["product_id"].unique()

    rows = []

    for product in products:
        product_value = product.as_py()

        product_table = table.filter(
            pc.equal(
                table["product_id"],
                pa.scalar(product_value)
            )
        )

        event_count = product_table.num_rows

        total_quantity = pc.sum(
            product_table["quantity"]
        ).as_py()

        total_amount = pc.sum(
            product_table["amount"]
        ).as_py()

        rows.append({
            "product_id": product_value,
            "event_count": event_count,
            "total_quantity": total_quantity,
            "total_amount": round(total_amount, 2)
        })

    result = pa.Table.from_pylist(rows)

    write_gold(result, PRODUCT_GOLD_DIR)

    print("\nProduct Metrics:")
    print(result)


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    print("=" * 60)
    print("Gold Analytics Started")
    print("=" * 60)

    table = read_silver_data()

    print(f"Silver Record Count: {table.num_rows}")

    create_payment_metrics(table)
    create_city_payment_metrics(table)
    create_event_type_metrics(table)
    create_product_metrics(table)

    print("\n" + "=" * 60)
    print("Gold Analytics Completed Successfully")
    print("=" * 60)


if __name__ == "__main__":
    main()