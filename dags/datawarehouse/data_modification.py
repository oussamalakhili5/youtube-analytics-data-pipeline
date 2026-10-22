import logging

# Configure the module-level logger.
logger = logging.getLogger(__name__)

# Define the table used to store YouTube video data.
table = "yt_api"


def insert_rows(cur, conn, schema, row):
    """Insert a YouTube video record into the specified schema.

    Args:
        cur: Active PostgreSQL cursor used to execute SQL queries.
        conn: Active PostgreSQL connection used to commit the transaction.
        schema: Target database schema, such as 'staging' or 'core'.
        row: Dictionary containing the video data to insert.
        """
    try:

        # Insert raw YouTube API data into the staging schema.
        if schema == "staging":

            video_id = "video_id"

            cur.execute(
                f"""
                INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
                """,
                row,
            )

            # Insert transformed data into the target schema.
        else:

            video_id = "Video_ID"

            cur.execute(
                f"""
                INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s)
                """,
                row,
            )

        # Commit the transaction to save the inserted record.
        conn.commit()

        # Log the ID of the inserted video.
        logger.info(f"Inserted row with Video_ID: {row[video_id]}")

    except Exception as e:
        # Log the error if the insertion fails.
        logger.error(f"Error inserting row with Video_ID: {row[video_id]}")
        raise e


def update_rows(cur, conn, schema, row):
    """Update an existing YouTube video record.

    Field names depend on the target schema:
        staging uses the original API field names, while core uses
        the database column names.

        Args:
            cur: Active PostgreSQL cursor used to execute SQL queries.
            conn: Active PostgreSQL connection used to commit the transaction.
            schema: Target database schema, such as 'staging' or 'core'.
            row: Dictionary containing the updated video data.
            """
    try:

        # Map the field names used in the staging schema.
        if schema == "staging":
            video_id = "video_id"
            upload_date = "publishedAt"
            video_title = "title"
            video_views = "viewCount"
            likes_count = "likeCount"
            comments_count = "commentCount"

            # Map the field names used in the core schema.
        else:
            video_id = "Video_ID"
            upload_date = "Upload_Date"
            video_title = "Video_Title"
            video_views = "Video_Views"
            likes_count = "Likes_Count"
            comments_count = "Comments_Count"

            # Update the video title and engagement statistics.
            # Match the record using its video ID and upload date.
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s,
                "Likes_Count" = %({likes_count})s,
                "Comments_Count" = %({comments_count})s
                WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
                """,
                row,
            )

            # Commit the transaction to save the updated record.
            conn.commit()

            # Log the ID of the updated video.
            logger.info(f"Updated row with Video_ID: {row[video_id]}")

    except Exception as e:
        # Log the error if the update fails.
        logger.error(f"Error updating row with Video_ID: {row[video_id]} - {e}")
        raise e


def delete_rows(cur, conn, schema, ids_to_delete):
    """Delete video records using their video IDs.

    Args:
        cur: Active PostgreSQL cursor used to execute SQL queries.
        conn: Active PostgreSQL connection used to commit the transaction.
        schema: Target database schema, such as 'staging' or 'core'.
        ids_to_delete: List of video IDs to delete.
        """
    try:

        # Format the provided video IDs for the SQL IN clause.
        ids_to_delete = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""

        # Delete records whose video IDs match the provided list.
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN {ids_to_delete};
            """
        )

        # Commit the transaction to save the deletions.
        conn.commit()

        # Log the IDs of the deleted videos.
        logger.info(f"Deleted rows with Video_IDs: {ids_to_delete}")

    except Exception as e:
        # Log the error if the deletion fails.
        logger.error(f"Error deleting rows with Video_IDs: {ids_to_delete} - {e}")
        raise e