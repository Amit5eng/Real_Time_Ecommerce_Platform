import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


PROJECT_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = PROJECT_DIR / "data" / "raw" / "ecommerce_events"

BUCKET = "realtime-ecommerce-raw-amitchaudhary-2026"
PREFIX = "raw/ecommerce_events"
REGION = "ap-southeast-2"


def main():
    if not BRONZE_DIR.exists():
        raise FileNotFoundError(
            f"Bronze directory does not exist: {BRONZE_DIR}"
        )

    s3 = boto3.client("s3", region_name=REGION)
    files = sorted(BRONZE_DIR.rglob("*.parquet"))

    if not files:
        print(f"No Parquet files found in {BRONZE_DIR}")
        return

    uploaded = 0
    skipped = 0

    for file_path in files:
        relative_path = file_path.relative_to(BRONZE_DIR).as_posix()
        key = f"{PREFIX}/{relative_path}"
        file_size = file_path.stat().st_size

        try:
            response = s3.head_object(Bucket=BUCKET, Key=key)

            if response["ContentLength"] == file_size:
                print(f"SKIP: {relative_path}")
                skipped += 1
                continue

        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code not in ("404", "NoSuchKey", "NotFound"):
                raise

        s3.upload_file(str(file_path), BUCKET, key)
        print(f"UPLOADED: s3://{BUCKET}/{key}")
        uploaded += 1

    print(f"Upload complete. Uploaded: {uploaded}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
