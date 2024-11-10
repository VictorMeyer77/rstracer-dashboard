from datetime import timedelta
from timeit import default_timer as timer
import streamlit as st

from pages import add_user_red_list, add_pid_red_list, add_command_red_list, connection


start_timer = timer()
db = connection("/Users/victormeyer/Dev/Self/rstracer/export/data/", "parquet")

st.set_page_config(
    page_title="Process",
    page_icon="⚙️",
    layout="wide",
)
st.header("Process", divider=True)

# DATE SLIDE BAR

# Time selection

(min_date, max_date) = db.execute(
    """
    SELECT
        MIN(created_at) AS min_date_process,
        MAX(created_at) AS max_date_process
    FROM gold_fact_process
"""
).fetchone()

st.sidebar.header("Parameters", divider=True)
(slider_date_min, slider_date_max) = st.sidebar.slider(
    "Analysis Interval",
    min_value=min_date,
    max_value=max_date,
    value=(min_date, max_date),
    format="DD-MM-YY hh:mm:ss",
    step=timedelta(seconds=1),
)

# Red list

hide_user = add_user_red_list(db, st.sidebar)
hide_pid = add_pid_red_list(db, st.sidebar)
hide_command = add_command_red_list(db, st.sidebar)

# Mem & Cpu Analysis

resource_per_command = db.execute(
    """
SELECT
    SUM(fact.pcpu) AS pcpu,
    SUM(fact.pmem) AS pmem,
    COALESCE(pro.command, 'Unknown') AS command,
    fact.created_at
FROM
    gold_fact_process fact
LEFT JOIN
    gold_dim_process pro ON fact.pid = pro.pid
LEFT JOIN
    gold_file_user usr ON pro.uid = usr.uid
WHERE
    fact.created_at >= ? AND fact.created_at <= ?
AND pro.pid NOT IN ?
AND  usr.name NOT IN ?
AND pro.command NOT IN ?
GROUP BY fact.created_at, pro.command
ORDER BY fact.created_at
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

st.subheader("CPU Usage by Command", divider=True)
st.area_chart(
    resource_per_command, x="created_at", y="pcpu", color="command", stack="center", x_label="date", y_label="CPU usage"
)
st.subheader("Memory Usage by Command", divider=True)
st.area_chart(
    resource_per_command,
    x="created_at",
    y="pmem",
    color="command",
    stack="center",
    x_label="date",
    y_label="Memory usage (%)",
)

# Process by network
st.subheader("Network I/O by Command", divider=True)

packet_process = db.execute(
    """
SELECT
    TO_TIMESTAMP(FLOOR(EXTRACT('epoch' FROM packet.created_at) / 10) * 10) AT TIME ZONE 'UTC' AS time,
    COALESCE(pro.command, 'Unknown') AS command,
    ROUND(SUM(length) / (1024 * 1024), 2) AS size
FROM gold_fact_process_network net_pro
INNER JOIN gold_fact_network_packet packet ON net_pro.packet_id = packet._id
LEFT JOIN gold_dim_process pro ON net_pro.pid = pro.pid
LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
WHERE packet.created_at >= ? AND packet.created_at <= ?
AND pro.pid NOT IN ?
AND usr.name NOT IN ?
AND pro.command NOT IN ?
GROUP BY time, command
ORDER BY time
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

st.area_chart(
    data=packet_process,
    x="time",
    y="size",
    color="command",
    stack="center",
    x_label="date",
    y_label="size (Mo)",
)

# Process count

st.subheader("Process Repartition", divider=True)

# Process by Commands

process_by_command_count = db.execute(
    """
WITH process AS
(
    SELECT DISTINCT
        fact.pid,
        pro.command
    FROM
        gold_fact_process fact
    LEFT JOIN
        gold_dim_process pro ON fact.pid = pro.pid
    LEFT JOIN
        gold_file_user usr ON pro.uid = usr.uid
    WHERE
        fact.created_at >= ? AND fact.created_at <= ?
    AND pro.pid NOT IN ?
    AND usr.name NOT IN ?
    AND pro.command NOT IN ?
)
SELECT
    COALESCE(command, 'Unknwon') AS command,
    COUNT(*) AS count
FROM process
GROUP BY command
ORDER BY count DESC
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

st.text("Process total launched by command")
st.bar_chart(
    process_by_command_count,
    x="command",
    y="count",
    x_label="command",
    y_label="count",
    color="command",
)

# Process by User

process_by_user_count = db.execute(
    """
WITH process AS
(
    SELECT DISTINCT
        fact.pid,
        usr.name AS user
    FROM
        gold_fact_process fact
    LEFT JOIN
        gold_dim_process pro ON fact.pid = pro.pid
    LEFT JOIN
        gold_file_user usr ON pro.uid = usr.uid
    WHERE
        fact.created_at >= ? AND fact.created_at <= ?
    AND pro.pid NOT IN ?
    AND usr.name NOT IN ?
    AND pro.command NOT IN ?
)
SELECT
    COALESCE(user, 'Unknwon') AS user,
    COUNT(*) AS count
FROM process
GROUP BY user
ORDER BY count DESC
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

st.text("Process total launched by user")
st.bar_chart(
    process_by_user_count,
    x="user",
    y="count",
    x_label="user",
    y_label="count",
    color="user",
)

# Metadata

st.subheader("Process Actions", divider=True)
metadata_columns = st.columns(3)

# Process per children count

pids_per_process = db.execute(
    """
WITH ppid_count AS
(
    SELECT
        COUNT(DISTINCT fact.pid) AS count,
        dim.ppid AS pid,
    FROM
        gold_fact_process fact
    LEFT JOIN
        gold_dim_process pro ON fact.pid = pro.pid
    LEFT JOIN
        gold_file_user usr ON pro.uid = usr.uid
    LEFT JOIN gold_dim_process dim ON fact.pid = dim.pid
    WHERE
        fact.created_at >= ? AND fact.created_at <= ?
    AND pro.pid NOT IN ?
    AND usr.name NOT IN ?
    AND pro.command NOT IN ?
    GROUP BY dim.ppid
)
SELECT
    ppid_count.pid,
    ppid_count.count AS children,
    pro.command,
FROM ppid_count LEFT JOIN gold_dim_process pro ON ppid_count.pid = pro.pid
ORDER BY ppid_count.count DESC
LIMIT 20
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

with metadata_columns[0]:
    st.text("Process with most children (Top 20)")
    st.dataframe(pids_per_process, hide_index=True)


# Oldest process

pids_per_age = db.execute(
    """
SELECT DISTINCT
    fact.pid,
    pro.command,
    AGE(dim.inserted_at, dim.started_at) AS age,
FROM
    gold_fact_process fact
LEFT JOIN
    gold_dim_process pro ON fact.pid = pro.pid
LEFT JOIN
    gold_file_user usr ON pro.uid = usr.uid
LEFT JOIN gold_dim_process dim ON fact.pid = dim.pid
WHERE
    fact.created_at >= ? AND fact.created_at <= ?
AND pro.pid NOT IN ?
AND usr.name NOT IN ?
AND pro.command NOT IN ?
ORDER BY age DESC
LIMIT 20
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

with metadata_columns[1]:
    st.text("Oldest processes (Top 20)")
    st.dataframe(pids_per_age, hide_index=True)

# Most used commands

full_commands_count = db.execute(
    """
SELECT DISTINCT
    COUNT(DISTINCT fact.pid) AS count,
    pro.full_command
FROM
    gold_fact_process fact
LEFT JOIN
    gold_dim_process pro ON fact.pid = pro.pid
LEFT JOIN
    gold_file_user usr ON pro.uid = usr.uid
LEFT JOIN gold_dim_process dim ON fact.pid = dim.pid
WHERE
    fact.created_at >= ? AND fact.created_at <= ?
AND pro.pid NOT IN ?
AND usr.name NOT IN ?
AND pro.command NOT IN ?
GROUP BY pro.full_command
ORDER BY count DESC
LIMIT 20
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

with metadata_columns[2]:
    st.text("Most used commands (Top 20)")
    st.dataframe(full_commands_count.rename(columns={"full_command": "command"}), hide_index=True)

# Statistics

st.sidebar.header("Statistics", divider=True)

# Process count

process_total = db.execute(
    """
SELECT
    COUNT(DISTINCT ROW(fact.pid, dim.started_at)) AS count,
FROM
    gold_fact_process fact
LEFT JOIN
    gold_dim_process pro ON fact.pid = pro.pid
LEFT JOIN
    gold_file_user usr ON pro.uid = usr.uid
LEFT JOIN gold_dim_process dim ON fact.pid = dim.pid
WHERE
    fact.created_at >= ? AND fact.created_at <= ?
AND pro.pid NOT IN ?
AND usr.name NOT IN ?
AND pro.command NOT IN ?
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).fetchone()[0]

st.sidebar.write("Process total: ", process_total)

# Sudo process count

process_root = db.execute(
    """
SELECT
    COUNT(DISTINCT ROW(fact.pid, dim.started_at)) AS count,
FROM
    gold_fact_process fact
LEFT JOIN
    gold_dim_process pro ON fact.pid = pro.pid
LEFT JOIN
    gold_file_user usr ON pro.uid = usr.uid
LEFT JOIN gold_dim_process dim ON fact.pid = dim.pid
WHERE
    fact.created_at >= ? AND fact.created_at <= ?
AND pro.pid NOT IN ?
AND usr.name = 'root'
AND pro.command NOT IN ?
""",
    [slider_date_min, slider_date_max, hide_pid, hide_command],
).fetchone()[0]

st.sidebar.write("Root Process: ", process_root)

# Running time
end_timer = timer()
st.sidebar.write("Running time: ", round(end_timer - start_timer, 4), " seconds")
