import os

import duckdb

TABLES = [
    "gold_dim_file_reg",
    "gold_dim_network_foreign_ip",
    "gold_dim_network_local_ip",
    "gold_dim_network_open_port",
    "gold_dim_network_socket",
    "gold_dim_process",
    "gold_fact_file_reg",
    "gold_fact_network_ip",
    "gold_fact_network_packet",
    "gold_fact_process",
    "gold_fact_process_network",
    "gold_file_host",
    "gold_file_service",
    "gold_file_user",
    "gold_tech_chrono",
    "gold_tech_table_count",
]


def connection():
    try:
        db_format = os.environ["RSBD_FORMAT"]
        db_path = os.environ["RSBD_PATH"]
    except KeyError:
        raise ValueError("Empty path. Go to home page for connection settings.")
    if db_format.lower() == "duckdb":
        con = duckdb.connect(database=db_path, read_only=True)
    else:
        con = duckdb.connect(database=":memory:")
        for table in TABLES:
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM '{db_path}/{table}.{db_format}';")
    return con


def add_user_red_list(con, sidebar):
    user = con.execute(
        """
        SELECT DISTINCT usr.name
        FROM gold_dim_process pro
        LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
        ORDER BY name
    """
    ).df()
    hide_user = sidebar.multiselect("Hide user", user)
    return hide_user


def add_pid_red_list(con, sidebar):
    pid = con.execute(
        """
        SELECT
            DISTINCT pid
        FROM gold_dim_process
        ORDER BY pid
    """
    ).df()
    hide_pid = sidebar.multiselect("Hide PID", pid)
    return hide_pid


# Unwanted commands


def add_command_red_list(con, sidebar):
    command = con.execute(
        """
        SELECT
            DISTINCT command
        FROM gold_dim_process
        ORDER BY command
    """
    ).df()
    hide_command = sidebar.multiselect("Hide command", command)
    return hide_command
