def test_api_key(api_key):
    """Verify that the API key fixture contains the expected mock value."""
    assert api_key == "MOCK_KEY1234"


def test_channel_handle(channel_handle):
    """Verify that the channel handle fixture matches the expected value."""
    assert channel_handle == "MRCHEESE"


def test_postgres_conn(mock_postgres_conn_vars):
    """Verify that the mocked PostgreSQL connection settings are correct."""
    conn = mock_postgres_conn_vars

    # Check the expected PostgreSQL connection credentials and settings.
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"


def test_dags_integrity(dagbag):
    """Verify that all expected Airflow DAGs load correctly and have the expected task counts."""

    # 1. Ensure that no DAG import errors occurred.
    assert dagbag.import_errors == {}, (
        f"Import errors found: {dagbag.import_errors}"
    )
    print("===========")
    print(dagbag.import_errors)

    # 2. Verify that all expected DAG IDs are present.
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())

    print("===========")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing."

        # 3. Ensure that exactly three DAGs were loaded.
    assert dagbag.size() == 3

    print("===========")
    print(dagbag.size())

        # 4. Verify the expected number of tasks in each DAG.
    expected_task_counts = {
        "produce_json": 5,
        "update_db": 3,
        "data_quality": 2,
    }

    print("===========")

    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)

        assert expected_count == actual_count, (
            f"DAG {dag_id} has {actual_count} tasks, "
            f"expected {expected_count}."
        )

        print(dag_id, len(dag.tasks))