from datetime import datetime

from airflow import DAG

from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
)

from airflow.providers.google.cloud.transfers.gcs_to_gcs import (
    GCSToGCSOperator,
)

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryInsertJobOperator,
)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)


PROJECT_ID = "b55uma"

SOURCE_BUCKET = "customer-pipeline-practice"
SOURCE_FILE = "customer_details.csv"

DEST_BUCKET = "umacustomer-parallel2026"

DATASET_ID = "parallelexecution_ds"
STAGING_TABLE = "parallel_customer_staging"
HISTORY_TABLE = "parallel_customer_history"
VIEW_NAME = "parallel_customer_view"

LOCATION = "US"


with DAG(
    dag_id="parallel_execution_umacustomer",
    start_date=datetime(2026, 1, 1),
    schedule='* 6 * * * *',
    catchup=False,
    tags=["gcs", "bigquery", "operators"],
) as dag:

    # 1. Create a new GCS bucket
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DEST_BUCKET,
        project_id=PROJECT_ID,
        location=LOCATION,
        storage_class="STANDARD",
    )

    # 2. Copy the source CSV to the new bucket
    copy_csv = GCSToGCSOperator(
        task_id="copy_csv",
        source_bucket=SOURCE_BUCKET,
        source_objects=[SOURCE_FILE],
        destination_bucket=DEST_BUCKET,
        destination_object="customers.csv",
        move_object=False,
    )

    # 3. Create a new BigQuery dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        location=LOCATION,
        if_exists="ignore",
    )

    # 4. Create the staging table
    create_staging_table = BigQueryCreateTableOperator(
        task_id="create_staging_table",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=STAGING_TABLE,
        if_exists="ignore",
        table_resource={
            "schema": {
                "fields": [
                    {"name": "user_id", "type": "STRING"},
                    {"name": "age", "type": "STRING"},
                    {"name": "gender", "type": "STRING"},
                    {"name": "occupation", "type": "STRING"},
                    {"name": "zip_code", "type": "STRING"},
                    {"name": "desc", "type": "STRING"},
                ]
            }
        },
    )

    # 5. Load the CSV into the staging table
    load_staging_table = GCSToBigQueryOperator(
        task_id="load_staging_table",
        bucket=DEST_BUCKET,
        source_objects=["customers.csv"],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}"
        ),
        project_id=PROJECT_ID,
        location=LOCATION,
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "age", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
            {"name": "occupation", "type": "STRING", "mode": "NULLABLE"},
            {"name": "zip_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "desc", "type": "STRING", "mode": "NULLABLE"},
        ],
        allow_jagged_rows=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )

    # 6. Create the history table
    create_history_table = BigQueryCreateTableOperator(
        task_id="create_history_table",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=HISTORY_TABLE,
        if_exists="ignore",
        table_resource={
            "schema": {
                "fields": [
                    {"name": "user_id", "type": "INT64"},
                    {"name": "age", "type": "INT64"},
                    {"name": "gender", "type": "STRING"},
                    {"name": "zip_code", "type": "STRING"},
                    {"name": "loaded_at", "type": "TIMESTAMP"},
                ]
            }
        },
    )

    # 7. Insert valid records into history
    insert_history = BigQueryInsertJobOperator(
        task_id="insert_history",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO
                    {PROJECT_ID}.{DATASET_ID}.{HISTORY_TABLE}
                    (user_id, age, gender, zip_code, loaded_at)
                    SELECT
                        SAFE_CAST(user_id AS INT64),
                        SAFE_CAST(age AS INT64),
                        gender,
                        zip_code,
                        CURRENT_TIMESTAMP()
                    FROM
                    {PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}
                    WHERE SAFE_CAST(user_id AS INT64) IS NOT NULL
                      AND SAFE_CAST(age AS INT64) IS NOT NULL
                """,
                "useLegacySql": False,
            }
        },
    )

    # 8. Export history to GCS as CSV
    export_history = BigQueryToGCSOperator(
        task_id="export_history",
        source_project_dataset_table=(
            f"{PROJECT_ID}.{DATASET_ID}.{HISTORY_TABLE}"
        ),
        destination_cloud_storage_uris=[
            f"gs://{DEST_BUCKET}/customers_history.csv"
        ],
        export_format="CSV",
        print_header=True,
    )

    # 9. Create a BigQuery view over the history table
    create_customer_view = BigQueryCreateTableOperator(
        task_id="create_customer_view",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=VIEW_NAME,
        if_exists="log",
        table_resource={
            "view": {
                "query": f"""
                    SELECT
                        user_id,
                        age,
                        gender,
                        zip_code,
                        loaded_at
                    FROM
                    {PROJECT_ID}.{DATASET_ID}.{HISTORY_TABLE}
                """,
                "useLegacySql": False,
            }
        },
    )

    (
        [create_bucket,create_dataset]
        >> copy_csv
        >> [create_staging_table,create_history_table]
        >> load_staging_table
        >> insert_history
        >> export_history
        >> create_customer_view
    )