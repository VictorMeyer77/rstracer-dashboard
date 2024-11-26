from datetime import timedelta
from timeit import default_timer as timer

import streamlit as st

from pages import connection

start_timer = timer()
con = connection()

st.set_page_config(
    page_title="Network Activity",
    page_icon="🛜",
    layout="wide",
)
st.header("Network Activity", divider=True)

# DATE SLIDE BAR

(min_date, max_date) = con.execute(
    """
    SELECT
        MIN(created_at) AS min_date_packet,
        MAX(created_at) AS max_date_packet
    FROM gold_fact_network_packet
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

# I/O network packet bytes

st.subheader("Packet size by command", divider=True)

packet_process = con.execute(
    """
SELECT
    TO_TIMESTAMP(FLOOR(EXTRACT('epoch' FROM packet.created_at))) AT TIME ZONE 'UTC' AS time,
    COALESCE(pro.command, pro.full_command, 'Unknown') AS command,
    ROUND(SUM(length) / (1024 * 1024), 3) AS size
FROM gold_fact_network_packet packet
LEFT JOIN gold_fact_process_network net_pro ON net_pro.packet_id = packet._id
LEFT JOIN gold_dim_process pro ON net_pro.pid = pro.pid
WHERE packet.created_at >= ? AND packet.created_at <= ?
GROUP BY time, COALESCE(pro.command, pro.full_command, 'Unknown')
ORDER BY time
""",
    [slider_date_min, slider_date_max],
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

# Protocols by size

st.subheader("Protocols repartition by size", divider=True)
protocols_size_row = st.columns(4)

# Interfaces

interface_by_size = con.execute(
    """
SELECT
    interface,
    ROUND(SUM(length) / (1024 * 1024), 3) AS size
FROM gold_fact_network_packet
WHERE created_at >= ? AND created_at <= ?
GROUP BY interface
""",
    [slider_date_min, slider_date_max],
).df()
with protocols_size_row[0]:
    st.bar_chart(
        interface_by_size, x="interface", y="size", x_label="interface", y_label="size (Mo)", color="interface"
    )

# Network

network_by_size = con.execute(
    """
SELECT
    COALESCE (network, 'unknown') AS network,
    ROUND(SUM(length) / (1024 * 1024), 3) AS size
FROM gold_fact_network_packet
WHERE created_at >= ? AND created_at <= ?
GROUP BY network
    """,
    [slider_date_min, slider_date_max],
).df()
with protocols_size_row[1]:
    st.bar_chart(network_by_size, x="network", y="size", x_label="network", y_label="size (Mo)", color="network")

# Transport

transport_by_size = con.execute(
    """
SELECT
    COALESCE (transport, 'unknown') AS transport,
    ROUND(SUM(length) / (1024 * 1024), 3) AS size
FROM gold_fact_network_packet
WHERE created_at >= ? AND created_at <= ?
AND network IS NOT NULL
GROUP BY transport
    """,
    [slider_date_min, slider_date_max],
).df()
with protocols_size_row[2]:
    st.bar_chart(
        transport_by_size, x="transport", y="size", x_label="transport", y_label="size (Mo)", color="transport"
    )

# Transport

application_by_size = con.execute(
    """
SELECT
    COALESCE (application, 'unknown') AS application,
    ROUND(SUM(length) / (1024 * 1024), 3) AS size
FROM gold_fact_network_packet
WHERE created_at >= ? AND created_at <= ?
AND transport IS NOT NULL
GROUP BY application
""",
    [slider_date_min, slider_date_max],
).df()
with protocols_size_row[3]:
    st.bar_chart(
        application_by_size,
        x="application",
        y="size",
        x_label="application",
        y_label="size (Mo)",
        color="application",
    )

# Foreign IP

st.subheader("Foreign IP", divider=True)
foreign_ip_column = st.columns(2, gap="large")

foreign_ip_traffic = con.execute(
    """
WITH fact_ip_host AS
(
    SELECT
     fact.*,
     host1.host AS source_host,
     host2.host AS destination_host
    FROM gold_fact_network_ip fact
    LEFT JOIN gold_dim_network_host host1 ON fact.source_address = host1.address
    LEFT JOIN gold_dim_network_host host2 ON fact.destination_address = host2.address
),
ip AS
(
    SELECT
        fact.created_at,
        CASE
            WHEN source_host.address IS NOT NULL THEN fact.source_host
            WHEN destination_host.address IS NOT NULL THEN fact.destination_host
        END AS address,
        CASE
            WHEN source_host.address IS NOT NULL THEN 0
            WHEN destination_host.address IS NOT NULL THEN 1
        END AS send,
        pack.length,
    FROM fact_ip_host fact
    LEFT JOIN (SELECT address FROM gold_dim_network_foreign_ip) source_host
    ON fact.source_address = source_host.address
    LEFT JOIN (SELECT address FROM gold_dim_network_foreign_ip) destination_host
    ON fact.destination_address = destination_host.address
    LEFT JOIN gold_fact_network_packet pack ON fact._id = pack._id
    WHERE NOT (source_host.address IS NULL AND destination_host.address IS NULL)
    AND NOT (source_host.address IS NOT NULL AND destination_host.address IS NOT NULL)
    AND fact.created_at >= ? AND fact.created_at <= ?
)
SELECT
    address,
    COUNT(*) AS count,
    ROUND(SUM(length) / (1024 * 1024), 3) AS size,
    AVG(send) AS send,
    TO_TIMESTAMP(AVG(EPOCH(created_at))) AS avg_date
FROM ip
GROUP BY address
ORDER BY size DESC
""",
    [slider_date_min, slider_date_max],
).df()

with foreign_ip_column[0]:
    st.scatter_chart(foreign_ip_traffic, x="avg_date", y="count", color="send", size="size", x_label="date")

with foreign_ip_column[1]:
    st.dataframe(
        foreign_ip_traffic.drop(["avg_date", "send"], axis=1).rename(columns={"size": "size (Mo)"}), hide_index=True
    )

st.text(
    """Each dot represents a unique foreign IP address. Date shows the average timestamp for packets sent or received.
Count indicates the total number of packets exchanged with the IP. Dot size reflects the packet size in megabytes (MB).

Dot color blue reflects the local host received more packets from this IP than sent (send=0).
Dot color white reflects the local host sent more packets to this IP than received (send=1)."""
)

# Local IP

st.subheader("Local IP", divider=True)
local_ip_column = st.columns(2, gap="large")

local_ip_traffic = con.execute(
    """
WITH fact_ip_host AS
(
    SELECT
     fact.*,
     host1.host AS source_host,
     host2.host AS destination_host
    FROM gold_fact_network_ip fact
    LEFT JOIN gold_dim_network_host host1 ON fact.source_address = host1.address
    LEFT JOIN gold_dim_network_host host2 ON fact.destination_address = host2.address
),
interface_host AS
(
    SELECT host.host
    FROM gold_dim_network_interface int
    LEFT JOIN gold_dim_network_host host ON host.address = int.address
),
ip AS
(
    SELECT
        fact.created_at,
        CASE
            WHEN source_int.host IS NOT NULL THEN fact.source_host
            WHEN destination_int.host IS NOT NULL THEN fact.destination_host
        END AS address,
        CASE
            WHEN source_int.host IS NULL THEN 0
            WHEN destination_int.host IS NULL THEN 1
        END AS send,
        pack.length,
    FROM fact_ip_host fact
    LEFT JOIN interface_host source_int ON fact.source_host = source_int.host
    LEFT JOIN interface_host destination_int ON fact.destination_host = destination_int.host
    LEFT JOIN gold_fact_network_packet pack ON fact._id = pack._id
    WHERE NOT (source_int.host IS NULL AND destination_int.host IS NULL)
    AND NOT (source_int.host IS NOT NULL AND destination_int.host IS NOT NULL)
    AND fact.created_at >= ? AND fact.created_at <= ?
)
SELECT
    address,
    COUNT(*) AS count,
    ROUND(SUM(length) / (1024 * 1024), 3) AS size,
    AVG(send) AS send,
    TO_TIMESTAMP(AVG(EPOCH(created_at))) AS avg_date
FROM ip
GROUP BY address
ORDER BY size DESC
""",
    [slider_date_min, slider_date_max],
).df()

with local_ip_column[0]:
    st.scatter_chart(
        local_ip_traffic,
        x="avg_date",
        y="count",
        color="send",
        size="size",
    )

with local_ip_column[1]:
    st.dataframe(
        local_ip_traffic.drop(["avg_date", "send"], axis=1).rename(columns={"size": "size (Mo)"}), hide_index=True
    )

st.text(
    """Each dot represents a unique local IP address. Date shows the average timestamp for packets sent or received.
Count indicates the total number of packets exchanged with the IP. Dot size reflects the packet size in megabytes (MB).

Dot color blue reflects this local IP received more packets than sent (send=0).
Dot color white reflects this local IP sent more packets than received (send=1)."""
)

# Local Port

st.subheader("Local Port", divider=True)
local_port_column = st.columns(2, gap="large")

local_port_traffic = con.execute(
    """
WITH fact_ip_host AS
(
    SELECT
     fact.*,
     host1.host AS source_host,
     host2.host AS destination_host
    FROM gold_fact_network_ip fact
    LEFT JOIN gold_dim_network_host host1 ON fact.source_address = host1.address
    LEFT JOIN gold_dim_network_host host2 ON fact.destination_address = host2.address
),
interface_host AS
(
    SELECT host.host
    FROM gold_dim_network_interface int
    LEFT JOIN gold_dim_network_host host ON host.address = int.address
),
ip AS
(
    SELECT fact.created_at
        ,CASE
            WHEN source_int.host IS NULL THEN fact.destination_port
            WHEN destination_int.host IS NULL THEN fact.source_port
            END AS port
        ,CASE
            WHEN source_int.host IS NULL THEN 0
            WHEN destination_int.host IS NULL THEN 1
            END AS send
        ,pack.length
    FROM fact_ip_host fact
    LEFT JOIN interface_host source_int ON fact.source_host = source_int.host
    LEFT JOIN interface_host destination_int ON fact.destination_host = destination_int.host
    LEFT JOIN gold_fact_network_packet pack ON fact._id = pack._id
    WHERE NOT (source_int.host IS NULL AND destination_int.host IS NULL)
        AND NOT (source_int.host IS NOT NULL AND destination_int.host IS NOT NULL)
        AND fact.created_at >= ? AND fact.created_at <= ?
    )
SELECT ip.port
    ,COALESCE(dim.command, 'Unknown') AS command
    ,COUNT(*) AS count
    ,ROUND(SUM(ip.length) / (1024 * 1024), 3) AS size
    ,AVG(ip.send) AS send
    ,TO_TIMESTAMP(AVG(EPOCH(ip.created_at))) AS avg_date
FROM ip
LEFT JOIN gold_dim_network_open_port dim ON ip.port = dim.port
    AND ip.created_at >= dim.started_at
    AND ip.created_at <= dim.inserted_at
GROUP BY ip.port, COALESCE(dim.command, 'Unknown')
ORDER BY size DESC
""",
    [slider_date_min, slider_date_max],
).df()

with local_port_column[0]:
    st.scatter_chart(
        local_port_traffic,
        x="avg_date",
        y="count",
        color="send",
        size="size",
    )
with local_port_column[1]:
    st.dataframe(
        local_port_traffic.drop(["avg_date", "send"], axis=1).rename(columns={"size": "size (Mo)"}),
        hide_index=True,
    )

st.text(
    """Each dot represents a unique local IP address. Date shows the average timestamp for packets sent or received.
Count indicates the total number of packets exchanged with the IP. Dot size reflects the packet size in megabytes (MB).

Dot color blue reflects this local port received more packets than sent (send=0).
Dot color white reflects this local port sent more packets than received (send=1)."""
)

# Statistics

st.sidebar.header("Statistics", divider=True)

# Packet count

packet_count = con.execute(
    """
SELECT
    COUNT(*) AS count
FROM gold_fact_network_packet
WHERE created_at >= ? AND created_at <= ?
""",
    [slider_date_min, slider_date_max],
).fetchone()[0]
st.sidebar.write("Total packet: ", packet_count)

# Packet size (Mo)

packet_size = con.execute(
    """
SELECT
    ROUND(SUM(length) / (1024 * 1024), 3) AS size
FROM gold_fact_network_packet
WHERE created_at >= ? AND created_at <= ?
""",
    [slider_date_min, slider_date_max],
).fetchone()[0]
st.sidebar.write("Total size: ", packet_size, " Mo")

# Listening port

listening_port = con.execute(
    """
SELECT
    COUNT(DISTINCT source_port) AS count
FROM gold_dim_network_socket
WHERE inserted_at >= ? AND inserted_at <= ?
AND source_port IS NOT NULL
""",
    [slider_date_min, slider_date_max],
).fetchone()[0]
st.sidebar.write("Listening port: ", listening_port)

# Running time

end_timer = timer()
st.sidebar.write("Running time: ", round(end_timer - start_timer, 4), " seconds")
