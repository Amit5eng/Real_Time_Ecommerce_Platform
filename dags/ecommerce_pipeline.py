
from datetime import datetime, timedelta
from pathlib import Path
import logging
import subprocess
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_DIR = Path("/opt/airflow/project")
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def run_script(script_name, failure_message):
    """Run a project script and stream its output into Airflow logs."""
    script_path = PROJECT_DIR / "src" / script_name

    if not script_path.is_file():
        raise FileNotFoundError(
            f"Required script not found: {script_path}"
        )

    logger.info("Starting script: %s", script_name)

    try:
        with subprocess.Popen(
            [sys.executable, "-u", str(script_path)],
            cwd=str(PROJECT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        ) as process:
            if process.stdout is not None:
                for line in process.stdout:
                    logger.info("[%s] %s", script_name, line.rstrip())

            return_code = process.wait()

    except OSError:
        logger.exception("Could not start script: %s", script_name)
        raise

    if return_code != 0:
        raise RuntimeError(
            f"{failure_message} Script '{script_name}' "
            f"exited with code {return_code}."
        )

    logger.info("Completed script successfully: %s", script_name)


def check_processed_data():
    output_path = PROJECT_DIR / "data" / "processed" / "payment_summary"

    if not output_path.is_dir():
        raise FileNotFoundError(
            f"Processed data directory not found: {output_path}"
        )

    parquet_files = list(output_path.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found in: {output_path}"
        )

    logger.info("Found %d Parquet files.", len(parquet_files))


with DAG(
    dag_id="ecommerce_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "data-engineering"],
) as dag:

    check_data = PythonOperator(
        task_id="check_processed_data",
        python_callable=check_processed_data,
        execution_timeout=timedelta(minutes=5),
    )

    upload_bronze_to_s3 = PythonOperator(
        task_id="upload_bronze_to_s3",
        python_callable=run_script,
        op_args=[
            "s3_bronze_uploader.py",
            "Bronze S3 upload failed.",
        ],
        execution_timeout=timedelta(minutes=30),
    )

    data_quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_script,
        op_args=[
            "data_quality.py",
            "Data quality validation failed.",
        ],
        execution_timeout=timedelta(minutes=30),
    )

    gold_analytics = PythonOperator(
        task_id="run_gold_analytics",
        python_callable=run_script,
        op_args=[
            "gold_analytics.py",
            "Gold analytics job failed.",
        ],
        execution_timeout=timedelta(minutes=45),
    )

    snowflake_load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=run_script,
        op_args=[
            "snowflake_loader.py",
            "Snowflake loading failed.",
        ],
        execution_timeout=timedelta(minutes=30),
    )

    analytics = PythonOperator(
        task_id="run_analytics",
        python_callable=run_script,
        op_args=[
            "analytics.py",
            "Analytics job failed.",
        ],
        execution_timeout=timedelta(minutes=30),
    )

    check_data >> upload_bronze_to_s3 >> data_quality >> gold_analytics >> snowflake_load >> analytics