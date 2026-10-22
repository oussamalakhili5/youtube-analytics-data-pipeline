from datawarehouse.data_utils import (
    get_conn_cursor,
    close_conn_cursor,
    create_schema,
    create_table,
    get_video_ids,
)
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)

# Define the table used to store YouTube video data.
table = "yt_api"


@task
def staging_table():
    """Synchronize the staging table with the latest extracted YouTube data.

    Load data from JSON, create the staging schema and table,
    insert new videos, update existing videos, and delete records
    that are no longer present in the source data.
    """
    schema = "staging"

    conn, cur = None, None

    try:
        # Establish a PostgreSQL connection and cursor.
        conn, cur = get_conn_cursor()

        # Load the extracted YouTube data from the JSON file.
        YT_data = load_data()

        # Ensure that the staging schema and table exist.
        create_schema(schema)
        create_table(schema)

        # Retrieve the video IDs already stored in the staging table.
        table_ids = get_video_ids(cur, schema)

        # Insert new videos or update existing records.
        for row in YT_data:

            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)

            else:
                if row["video_id"] in table_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)

                    # Collect the video IDs currently available in the JSON source.
                    ids_in_json = {row["video_id"] for row in YT_data}

                    # Identify records that exist in staging but not in the source data.
                    ids_to_delete = set(table_ids) - ids_in_json

                    # Delete outdated records when necessary.
                    if ids_to_delete:
                        delete_rows(cur, conn, schema, ids_to_delete)

                        logger.info(f"{schema} table update completed")

    except Exception as e:
        # Log the error and propagate it to Airflow.
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        # Ensure the database cursor and connection are closed.
        if conn and cur:
            close_conn_cursor(conn, cur)


@task
def core_table():
    """Synchronize the refined core table with the staging data.

    Read records from staging, transform each video record,
    insert new videos, update existing records, and remove
    records that are no longer present in staging.
    """
    schema = "core"

    conn, cur = None, None

    try:
        # Establish a PostgreSQL connection and cursor.
        conn, cur = get_conn_cursor()

        # Ensure that the core schema and table exist.
        create_schema(schema)
        create_table(schema)

        # Retrieve video IDs already present in the core table.
        table_ids = get_video_ids(cur, schema)

        # Track the video IDs currently available in staging.
        current_video_ids = set()

        # Retrieve all records from the staging table.
        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        # Transform each staging record before loading it into core.
        for row in rows:

            current_video_ids.add(row["Video_ID"])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)

            else:
                transformed_row = transform_data(row)

                # Update existing records or insert new ones.
                if transformed_row["Video_ID"] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)

                else:
                    insert_rows(cur, conn, schema, transformed_row)

                    # Identify core records that no longer exist in staging.
                    ids_to_delete = set(table_ids) - current_video_ids

                    # Remove outdated records from the core table.
                    if ids_to_delete:
                        delete_rows(cur, conn, schema, ids_to_delete)

                        logger.info(f"{schema} table update completed")

    except Exception as e:
        # Log any exception that occurs during the core table update.
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        # Ensure the database cursor and connection are closed.
        if conn and cur:
            close_conn_cursor(conn, cur)