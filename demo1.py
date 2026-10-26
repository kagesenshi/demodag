from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task

# Configure standard logger
logger = logging.getLogger("airflow.task")

# Default arguments applied to all tasks in the DAG
DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="demo_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="A foundational ETL demonstration utilizing the TaskFlow API",
    schedule="0 6 * * *",  # Runs daily at 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["demo", "etl"],
)
def demo_etl_pipeline():
    """Defines a sample end-to-end data processing workflow."""

    @task
    def extract() -> list[dict]:
        """Simulates extracting records from an upstream source."""
        logger.info("Extracting records from upstream data source...")
        records = [
            {"order_id": 1001, "amount": 150.50, "currency": "USD"},
            {"order_id": 1002, "amount": 89.20, "currency": "USD"},
            {"order_id": 1003, "amount": 210.00, "currency": "USD"},
        ]
        return records

    @task
    def transform(orders: list[dict]) -> dict:
        """Processes raw records and computes aggregations."""
        logger.info(f"Transforming {len(orders)} orders...")
        total_revenue = sum(order["amount"] for order in orders)
        avg_order_value = total_revenue / len(orders) if orders else 0.0

        summary = {
            "processed_count": len(orders),
            "total_revenue": round(total_revenue, 2),
            "average_ticket": round(avg_order_value, 2),
        }
        logger.info(f"Aggregations calculated: {summary}")
        return summary

    @task
    def load(summary: dict) -> None:
        """Simulates persisting transformed data into downstream storage."""
        logger.info(
            f"Persisting summary data to warehouse: "
            f"Orders={summary['processed_count']}, Revenue=${summary['total_revenue']}"
        )

    # Define task execution flow via dependency mapping
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)


# Instantiate the DAG
demo_dag = demo_etl_pipeline()
