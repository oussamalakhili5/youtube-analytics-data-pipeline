from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

# Name of the table used to store YouTube video data.
table = "yt_api"


def get_conn_cursor():
    """Create a PostgreSQL connection and return a dictionary cursor.

    Returns:
        tuple: A PostgreSQL connection and a RealDictCursor.
        """
    hook = PostgresHook(
        postgres_conn_id="postgres_db_yt_elt",
        database="elt_db"
    )
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    return conn, cur


def close_conn_cursor(conn, cur):
    """Close the database cursor and connection.

    Args:
        conn: The active PostgreSQL connection.
        cur: The cursor associated with the connection.
        """
    cur.close()
    conn.close()


def create_schema(schema):
    """Create a PostgreSQL schema if it does not already exist.

    Args:
        schema (str): The name of the schema to create.
        """
    conn, cur = get_conn_cursor()

    # Create the schema only if it does not exist.
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)

    # Commit the transaction to persist the changes.
    conn.commit()

    close_conn_cursor(conn, cur)


def create_table(schema):
    """Create the YouTube data table in the specified schema.

    The staging table stores raw data, while other schemas use
    a refined table structure that includes a Video_Type column
    and stores Duration as a TIME value.

    Args:
        schema (str): The schema in which the table will be created.
        """
    conn, cur = get_conn_cursor()

    if schema == "staging":
        # Define the table structure for raw YouTube data.
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR(20) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
        );
        """
    else:
        # Define the refined table structure for transformed data.
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" TIME NOT NULL,
            "Video_Type" VARCHAR(10) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
        );
        """

        cur.execute(table_sql)

        # Commit the table creation.
        conn.commit()

        close_conn_cursor(conn, cur)


def get_video_ids(cur, schema):
    """Retrieve all video IDs from the specified schema.

    Args:
        cur: An active PostgreSQL cursor.
        schema (str): The schema from which video IDs are retrieved.

        Returns:
            list: A list containing the existing video IDs.
            """
    cur.execute(f'SELECT "Video_ID" FROM {schema}.{table};')
    ids = cur.fetchall()

    # Extract video IDs from the dictionary rows.
    video_ids = [row["Video_ID"] for row in ids]

    return video_ids