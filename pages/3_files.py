from datetime import timedelta
from timeit import default_timer as timer

import streamlit as st

from pages import add_command_red_list, add_pid_red_list, add_user_red_list, connection

start_timer = timer()
con = connection()

st.set_page_config(
    page_title="Files",
    page_icon="📄",
    layout="wide",
)
st.header("Regular Files", divider=True)

# DATE SLIDE BAR

# Time selection

(min_date, max_date) = con.execute(
    """
SELECT
  MIN(started_at) AS min_date_file,
  MAX(inserted_at) AS max_date_file
FROM
  gold_dim_file_reg
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

hide_user = add_user_red_list(con, st.sidebar)
hide_pid = add_pid_red_list(con, st.sidebar)
hide_command = add_command_red_list(con, st.sidebar)

# Open files Count

st.subheader("File Activity", divider=True)
open_files_chart_row = st.columns(2)

files_count = con.execute(
    """
SELECT
  COUNT(DISTINCT dim.name) AS count,
  TO_TIMESTAMP(FLOOR(EXTRACT('epoch' FROM created_at))) AT TIME ZONE 'UTC' AS time,
FROM
  gold_fact_file_reg fact
  LEFT JOIN gold_dim_process pro ON fact.pid = pro.pid
  LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
  LEFT JOIN gold_dim_file_reg dim ON fact.pid = dim.pid AND fact.fd = dim.fd AND fact.node = dim.node
WHERE
  fact.created_at >= ?
  AND fact.created_at <= ?
  AND pro.pid NOT IN ?
  AND usr.name NOT IN ?
  AND pro.command NOT IN ?
GROUP BY
  time
ORDER BY
  time
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

st.text("Open files total")
st.line_chart(data=files_count, x="time", y="count", x_label="date", y_label="count")


# File by command

st.subheader("By Command Analysis", divider=True)


file_by_command_count = con.execute(
    """
SELECT
  command,
  COUNT(DISTINCT file_name) AS count
FROM
(
    SELECT
      fact.pid,
      fact.fd,
      fact.node,
      pro.command,
      usr.name AS user_name,
      dim.name AS file_name,
      MIN(size) AS min_size,
      MAX(size) AS max_size
   FROM
      gold_fact_file_reg fact
      LEFT JOIN gold_dim_process pro ON fact.pid = pro.pid
      LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
      LEFT JOIN gold_dim_file_reg dim ON fact.pid = dim.pid AND fact.fd = dim.fd AND fact.node = dim.node
   WHERE
      fact.created_at >= ?
      AND fact.created_at <= ?
      AND pro.pid NOT IN ?
      AND usr.name NOT IN ?
      AND pro.command NOT IN ?
   GROUP BY
      fact.pid,
      fact.fd,
      fact.node,
      pro.command,
      user_name,
      file_name
  )
GROUP BY
 command
ORDER BY
 count DESC
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

st.text("Command with most open files")
st.bar_chart(
    file_by_command_count,
    x="command",
    y="count",
    x_label="command",
    y_label="count",
    color="command",
)

modification_by_commands = con.execute(
    """
SELECT
  TO_TIMESTAMP(FLOOR(EXTRACT('epoch' FROM created_at))) AT TIME ZONE 'UTC' AS time,
  command,
  SUM(
    (size::BIGINT - previous_size::BIGINT)
  ) / (1024 * 1024) AS write_mo,
FROM
(
    SELECT
      pro.command,
      fact.created_at,
      size,
      LAG(size, 1, 0) OVER (
        PARTITION BY fact.pid,
            fact.fd,
            fact.node
        ORDER BY
            fact.created_at
       ) AS previous_size,
    ROW_NUMBER() OVER (
        PARTITION BY fact.pid,
           fact.fd,
           fact.node
       ORDER BY
            fact.created_at
      ) AS row_num
   FROM
      gold_fact_file_reg fact
      LEFT JOIN gold_dim_process pro ON fact.pid = pro.pid
      LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
   WHERE
      fact.created_at >= ?
      AND fact.created_at <= ?
      AND pro.pid NOT IN ?
      AND usr.name NOT IN ?
      AND pro.command NOT IN ?
  )
WHERE
  SIZE <> previous_size
  AND row_num > 1
GROUP BY
 time,
 command
ORDER BY
 time
  """,
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()


st.text("Modification Size (Mo) by command")
st.area_chart(
    modification_by_commands,
    x="time",
    y="write_mo",
    color="command",
    stack="center",
    x_label="date",
    y_label="size",
)

# File by user

st.subheader("By User Analysis", divider=True)

file_by_user_count = con.execute(
    """
SELECT
  user_name,
  COUNT(DISTINCT file_name) AS count
FROM
(
    SELECT
      fact.pid,
      fact.fd,
      fact.node,
      pro.command,
      dim.name AS file_name,
      usr.name AS user_name,
      MIN(size) AS min_size,
      MAX(size) AS max_size
   FROM
      gold_fact_file_reg fact
      LEFT JOIN gold_dim_process pro ON fact.pid = pro.pid
      LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
      LEFT JOIN gold_dim_file_reg dim ON fact.pid = dim.pid
      AND fact.fd = dim.fd
      AND fact.node = dim.node
   WHERE
      fact.created_at >= ?
      AND fact.created_at <= ?
      AND pro.pid NOT IN ?
      AND usr.name NOT IN ?
      AND pro.command NOT IN ?
   GROUP BY
      fact.pid,
      fact.fd,
      fact.node,
      pro.command,
      user_name,
      file_name
  )
GROUP BY
 user_name
ORDER BY
 count DESC
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

st.text("User with most open files")
st.bar_chart(
    file_by_user_count,
    x="user_name",
    y="count",
    x_label="user",
    y_label="count",
    color="user_name",
)

modification_by_users = con.execute(
    """
SELECT
  TO_TIMESTAMP(FLOOR(EXTRACT('epoch' FROM created_at))) AT TIME ZONE 'UTC' AS time,
  user,
  SUM(
    SIZE::BIGINT - previous_size::BIGINT
  ) / (1024 * 1024) AS write_mo,
FROM
(
    SELECT
        usr.name AS user,
        fact.created_at,
        size,
        LAG(size, 1, 0) OVER (
        PARTITION BY fact.pid,
            fact.fd,
            fact.node
            ORDER BY
             fact.created_at
        ) AS previous_size,
        ROW_NUMBER() OVER (
        PARTITION BY fact.pid,
            fact.fd,
            fact.node
            ORDER BY
             fact.created_at
        ) AS row_num
   FROM
        gold_fact_file_reg fact
        LEFT JOIN gold_dim_process pro ON fact.pid = pro.pid
        LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
   WHERE
      fact.created_at >= ?
      AND fact.created_at <= ?
      AND pro.pid NOT IN ?
      AND usr.name NOT IN ?
      AND pro.command NOT IN ?
  )
WHERE
  SIZE <> previous_size
  AND row_num > 1
GROUP BY
 time,
 user
ORDER BY
 time
  """,
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()


st.text("Modification Size (Mo) by user")
st.area_chart(
    modification_by_users,
    x="time",
    y="write_mo",
    color="user",
    stack="center",
    x_label="date",
    y_label="size",
)


# Analysis by file name

st.subheader("By File Analysis", divider=True)
by_file_row = st.columns(3)

most_open_files = con.execute(
    """
SELECT
  name,
  COUNT(*) AS count
FROM
(
    SELECT
      *
   FROM
      gold_dim_file_reg file
      LEFT JOIN gold_dim_process pro ON file.pid = pro.pid
      LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
   WHERE
      file.started_at >= ?
      AND file.inserted_at <= ?
      AND pro.pid NOT IN ?
      AND usr.name NOT IN ?
      AND pro.command NOT IN ?
  )
GROUP BY
 name
ORDER BY
 count DESC
    """,
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

with by_file_row[0]:
    st.text("Most opened files")
    st.dataframe(most_open_files, hide_index=True, column_order=["count", "name"])


most_open_files_by_cmd = con.execute(
    """
SELECT
  name,
  COUNT(DISTINCT command) AS count
FROM
(
   SELECT
      *
   FROM
      gold_dim_file_reg file
      LEFT JOIN gold_dim_process pro ON file.pid = pro.pid
      LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
   WHERE
      file.started_at >= ?
      AND file.inserted_at <= ?
      AND pro.pid NOT IN ?
      AND usr.name NOT IN ?
      AND pro.command NOT IN ?
  )
GROUP BY
 name
ORDER BY
 count DESC
    """,
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

with by_file_row[1]:
    st.text("Most opened files by different command")
    st.dataframe(most_open_files_by_cmd, hide_index=True, column_order=["count", "name"])

most_modified_files = con.execute(
    """
SELECT
  name,
  ROUND(
    SUM(max_size - min_size) / (1024 * 1024),
    2
  ) AS write_mo
FROM
(
    SELECT
      fact.pid,
     fact.fd,
     fact.node,
     file.name,
     MIN(size) AS min_size,
     MAX(size) AS max_size
   FROM
      gold_fact_file_reg fact
     LEFT JOIN gold_dim_process pro ON fact.pid = pro.pid
     LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
     LEFT JOIN gold_dim_file_reg file ON fact.pid = file.pid
     AND fact.fd = file.fd
     AND fact.node = file.node
   WHERE
      fact.created_at >= ?
     AND fact.created_at <= ?
     AND pro.pid NOT IN ?
     AND usr.name NOT IN ?
     AND pro.command NOT IN ?
   GROUP BY
      fact.pid,
     fact.fd,
     fact.node,
     file.name,
     )
WHERE
  max_size <> min_size
GROUP BY
 name
ORDER BY
 write_mo DESC
    """,
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).df()

with by_file_row[2]:
    st.text("Most modified files")
    st.dataframe(
        most_modified_files.rename(columns={"write_mo": "Size (Mo)"}),
        hide_index=True,
        column_order=["Size (Mo)", "name"],
    )


# Statistics

st.sidebar.header("Statistics", divider=True)

# Open nodes

open_nodes = con.execute(
    """
SELECT
  COUNT(*) AS count
FROM
 gold_dim_file_reg file
 LEFT JOIN gold_dim_process pro ON file.pid = pro.pid
 LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
WHERE
  file.started_at >= ?
 AND file.inserted_at <= ?
 AND pro.pid NOT IN ?
 AND usr.name NOT IN ?
 AND pro.command NOT IN ?
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).fetchone()[0]
st.sidebar.write("Opened nodes: ", open_nodes)

# Open files

open_files = con.execute(
    """
SELECT
  COUNT(DISTINCT file.name) AS count
FROM
 gold_dim_file_reg file
 LEFT JOIN gold_dim_process pro ON file.pid = pro.pid
 LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
WHERE
  file.started_at >= ?
 AND file.inserted_at <= ?
 AND pro.pid NOT IN ?
 AND usr.name NOT IN ?
 AND pro.command NOT IN ?
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).fetchone()[0]
st.sidebar.write("Opened files: ", open_files)

# Modified files

modified_files = con.execute(
    """
SELECT
  COUNT(*) AS count
FROM
(
    SELECT
      MIN(size) AS min_size,
     MAX(size) AS max_size
   FROM
      gold_fact_file_reg fact
     LEFT JOIN gold_dim_process pro ON fact.pid = pro.pid
     LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
     LEFT JOIN gold_dim_file_reg file ON fact.pid = file.pid
     AND fact.fd = file.fd
     AND fact.node = file.node
   WHERE
      fact.created_at >= ?
     AND fact.created_at <= ?
     AND pro.pid NOT IN ?
     AND usr.name NOT IN ?
     AND pro.command NOT IN ?
   GROUP BY file.name
     )
WHERE
  max_size <> min_size
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).fetchone()[0]
st.sidebar.write("Modified files: ", modified_files)

# Modification size

modification_size = con.execute(
    """
SELECT
  ROUND(SUM(max_size - min_size) / (1024 * 1024), 3) AS write_mo
FROM
(
   SELECT
      MIN(size) AS min_size,
      MAX(size) AS max_size
   FROM
      gold_fact_file_reg fact
     LEFT JOIN gold_dim_process pro ON fact.pid = pro.pid
     LEFT JOIN gold_file_user usr ON pro.uid = usr.uid
     LEFT JOIN gold_dim_file_reg file ON fact.pid = file.pid
     AND fact.fd = file.fd
     AND fact.node = file.node
   WHERE
      fact.created_at >= ?
     AND fact.created_at <= ?
     AND pro.pid NOT IN ?
     AND usr.name NOT IN ?
     AND pro.command NOT IN ?
   GROUP BY
      fact.pid,
     fact.fd,
     fact.node,
     file.name,
     )
WHERE
  max_size <> min_size
""",
    [slider_date_min, slider_date_max, hide_pid, hide_user, hide_command],
).fetchone()[0]
st.sidebar.write("Modification size: ", modification_size, " Mo")

# Running time

end_timer = timer()
st.sidebar.write("Running time: ", round(end_timer - start_timer, 4), " seconds")
