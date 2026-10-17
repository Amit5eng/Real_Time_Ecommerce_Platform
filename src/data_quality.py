from pathlib import Path

import pyarrow.parquet as pq


PROJECT_DIR = Path("/opt/airflow/project")

input_path = PROJECT_DIR / "data" / "processed" / "payment_summary"

required_columns = {
    "window",
    "city",
    "total_payment_amount",
    "payment_count",
    "average_payment_amount",
}

parquet_files = sorted(input_path.glob("*.parquet"))

if not parquet_files:
    raise FileNotFoundError(
        f"No Parquet files found in: {input_path}"
    )

print(f"Checking {len(parquet_files)} Parquet files.")


total_rows = 0
failed_checks = []


print("\n===== DATA QUALITY CHECK =====")


for index, parquet_file in enumerate(parquet_files, start=1):

    table = pq.read_table(parquet_file)

    actual_columns = set(table.column_names)

    # --------------------------------------------------
    # 1. Schema validation
    # --------------------------------------------------

    missing_columns = required_columns - actual_columns

    if missing_columns:
        failed_checks.append(
            f"{parquet_file.name}: Missing columns "
            f"{sorted(missing_columns)}"
        )

    # --------------------------------------------------
    # 2. Convert to Python rows
    # --------------------------------------------------

    rows = table.to_pylist()

    total_rows += len(rows)

    for row in rows:

        # --------------------------------------------------
        # 3. Null validation
        # --------------------------------------------------

        for column in required_columns:

            if column in row and row[column] is None:
                failed_checks.append(
                    f"{parquet_file.name}: NULL value "
                    f"in column '{column}'"
                )

        # --------------------------------------------------
        # 4. Payment amount validation
        # --------------------------------------------------

        amount = row.get("total_payment_amount")

        if amount is not None and amount < 0:
            failed_checks.append(
                f"{parquet_file.name}: Negative payment amount "
                f"{amount}"
            )

        # --------------------------------------------------
        # 5. Payment count validation
        # --------------------------------------------------

        payment_count = row.get("payment_count")

        if payment_count is not None and payment_count <= 0:
            failed_checks.append(
                f"{parquet_file.name}: Invalid payment count "
                f"{payment_count}"
            )

        # --------------------------------------------------
        # 6. Average payment validation
        # --------------------------------------------------

        average_payment = row.get("average_payment_amount")

        if average_payment is not None and average_payment < 0:
            failed_checks.append(
                f"{parquet_file.name}: Negative average payment "
                f"{average_payment}"
            )


    if index % 25 == 0 or index == len(parquet_files):
        print(
            f"Checked {index}/{len(parquet_files)} files."
        )


# --------------------------------------------------
# Final validation
# --------------------------------------------------

print("\n===== DATA QUALITY SUMMARY =====")

print(f"Files Checked : {len(parquet_files)}")
print(f"Rows Checked  : {total_rows}")
print(f"Failures      : {len(failed_checks)}")


if total_rows == 0:
    failed_checks.append(
        "Processed dataset contains zero rows."
    )


if failed_checks:

    print("\n===== DATA QUALITY FAILURES =====")

    for failure in failed_checks[:20]:
        print(failure)

    if len(failed_checks) > 20:
        print(
            f"... and {len(failed_checks) - 20} more failures."
        )

    raise RuntimeError(
        f"Data quality validation failed with "
        f"{len(failed_checks)} failure(s)."
    )


print("\nData quality validation PASSED.")
