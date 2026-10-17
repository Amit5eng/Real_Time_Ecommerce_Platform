from pathlib import Path

import pyarrow.parquet as pq


PROJECT_DIR = Path("/opt/airflow/project")

input_path = PROJECT_DIR / "data" / "processed" / "payment_summary"

parquet_files = sorted(input_path.glob("*.parquet"))

if not parquet_files:
    raise FileNotFoundError(
        f"No Parquet files found in: {input_path}"
    )

print(f"Found {len(parquet_files)} Parquet files.")

total_payment_amount = 0.0
total_payment_count = 0

city_data = {}

processed_rows = 0

print("\n===== READING PAYMENT SUMMARY =====")

for index, parquet_file in enumerate(parquet_files, start=1):

    table = pq.read_table(
        parquet_file,
        columns=[
            "window",
            "city",
            "total_payment_amount",
            "payment_count",
            "average_payment_amount"
        ]
    )

    rows = table.to_pylist()

    for row in rows:

        amount = float(row["total_payment_amount"] or 0)
        count = int(row["payment_count"] or 0)
        average = float(row["average_payment_amount"] or 0)

        total_payment_amount += amount
        total_payment_count += count

        city = row["city"]

        if city not in city_data:
            city_data[city] = {
                "total_payment_amount": 0.0,
                "total_payment_count": 0,
                "average_values": []
            }

        city_data[city]["total_payment_amount"] += amount
        city_data[city]["total_payment_count"] += count
        city_data[city]["average_values"].append(average)

        if processed_rows < 20:
            print(
                row["window"],
                city,
                round(amount, 2),
                count,
                round(average, 2)
            )

        processed_rows += 1

    if index % 25 == 0 or index == len(parquet_files):
        print(
            f"Processed {index}/{len(parquet_files)} Parquet files."
        )


print("\n===== OVERALL PAYMENT METRICS =====")

overall_average_payment = (
    total_payment_amount / total_payment_count
    if total_payment_count
    else 0
)

print(
    f"Total Payment Amount     : "
    f"{total_payment_amount:.2f}"
)

print(
    f"Total Payment Count      : "
    f"{total_payment_count}"
)

print(
    f"Overall Average Payment  : "
    f"{overall_average_payment:.2f}"
)


print("\n===== CITY LEVEL PAYMENT METRICS =====")

city_metrics = []

for city, data in city_data.items():

    average_payment = (
        sum(data["average_values"])
        / len(data["average_values"])
        if data["average_values"]
        else 0
    )

    city_metrics.append(
        (
            city,
            data["total_payment_amount"],
            data["total_payment_count"],
            average_payment
        )
    )


city_metrics.sort(
    key=lambda x: x[1],
    reverse=True
)


for city, total_amount, count, average in city_metrics:

    print(
        f"{city:12} | "
        f"Total Amount: {total_amount:10.2f} | "
        f"Count: {count:5} | "
        f"Average: {average:10.2f}"
    )


print("\nAnalytics job completed successfully.")
