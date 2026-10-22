from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_json,
)

from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality


# Define the local timezone used for DAG scheduling.
local_tz = pendulum.timezone("Europe/Malta")


# Define default parameters shared by all three DAGs.
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz),
}


# Define the PostgreSQL schemas used by the ELT process.
staging_schema = "staging"
core_schema = "core"


# DAG 1: Extract YouTube data and save it to a JSON file.
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",
    catchup=False,
) as dag_produce:

    # Retrieve the uploads playlist ID for the target YouTube channel.
    playlist_id = get_playlist_id()

    # Retrieve all video IDs from the uploads playlist.
    video_ids = get_video_ids(playlist_id)

    # Extract metadata and statistics for the retrieved videos.
    extract_data = extract_video_data(video_ids)

    # Save the extracted video data to a dated JSON file.
    save_to_json_task = save_to_json(extract_data)

    # Trigger the database update DAG after the JSON file is saved.
    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    # Define the execution order of the extraction tasks.
    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db


    # DAG 2: Load JSON data into staging and transform it into core.
    with DAG(
        dag_id="update_db",
        default_args=default_args,
        description="DAG to process JSON file and insert data into both staging and core schemas",
        catchup=False,
        schedule=None,
    ) as dag_update:

        # Synchronize the staging table with the extracted JSON data.
        update_staging = staging_table()

        # Transform staging records and synchronize the core table.
        update_core = core_table()

        # Trigger data quality checks after both database layers are updated.
        trigger_data_quality = TriggerDagRunOperator(
            task_id="trigger_data_quality",
            trigger_dag_id="data_quality",
        )

        # Ensure staging is updated before core, then start quality validation.
        update_staging >> update_core >> trigger_data_quality


        # DAG 3: Validate data quality in the staging and core schemas.
        with DAG(
            dag_id="data_quality",
            default_args=default_args,
            description="DAG to check the data quality on both layers in the database",
            catchup=False,
            schedule=None,
        ) as dag_quality:

            # Validate the raw data stored in the staging schema.
            soda_validate_staging = yt_elt_data_quality(staging_schema)

            # Validate the transformed data stored in the core schema.
            soda_validate_core = yt_elt_data_quality(core_schema)

            # Run core validation only after staging validation completes.
            soda_validate_staging >> soda_validate_core