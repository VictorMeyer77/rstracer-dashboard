from timeit import default_timer as timer

import streamlit as st

from pages import connection

start_timer = timer()
con = connection()


st.set_page_config(
    page_title="Debug",
    page_icon="🚧",
    layout="wide",
)

st.header("Control database consistency", divider=True)

st.subheader("Technical Statistics", divider=True)

table_max_count = con.execute(
    """
SELECT
  name,
  max_count,
  SPLIT(name, '_')[1] AS color,
FROM
  gold_tech_table_count
ORDER BY
  _id
"""
).df()

st.text("Highest row count for each table")
st.bar_chart(table_max_count, x="name", y="max_count", x_label="table name", y_label="highest count", color="color")

chrono_row = st.columns(3)

bronze_ingest_chrono = con.execute(
    """
SELECT
    CASE
        WHEN name = 'process_list' THEN 'process'
        WHEN name = 'open_files' THEN 'files'
        WHEN name = 'network_packet' THEN 'network'
    END AS object,
    brz_min_ingest,
    brz_max_ingest,
    svr_min_ingest,
    svr_max_ingest,
    min_ingest,
    max_ingest,
FROM
  gold_tech_chrono
"""
).df()

with chrono_row[0]:
    st.text("Bronze ingestion in seconds")
    st.dataframe(
        bronze_ingest_chrono[["object", "brz_min_ingest", "brz_max_ingest"]].rename(
            columns={"brz_min_ingest": "fastest", "brz_max_ingest": "slowest"}
        ),
        hide_index=True,
    )

with chrono_row[1]:
    st.text("Silver ingestion in seconds")
    st.dataframe(
        bronze_ingest_chrono[["object", "svr_min_ingest", "svr_max_ingest"]].rename(
            columns={"svr_min_ingest": "fastest", "svr_max_ingest": "slowest"}
        ),
        hide_index=True,
    )

with chrono_row[2]:
    st.text("Bronze & Silver ingestion in seconds")
    st.dataframe(
        bronze_ingest_chrono[["object", "min_ingest", "max_ingest"]].rename(
            columns={"min_ingest": "fastest", "max_ingest": "slowest"}
        ),
        hide_index=True,
    )

st.subheader("Process Data Quality", divider=True)

process_without_open_file = con.execute(
    """
SELECT
    COUNT(*) AS count,
    full_command,
FROM gold_dim_process
WHERE command IS NULL
GROUP BY full_command
ORDER BY count DESC
"""
).df()

st.text("Process without associated open file")
st.dataframe(process_without_open_file.rename(columns={"full_command": "full command"}), hide_index=True)

st.subheader("Network Data Quality", divider=True)

network_consistency_column = st.columns(2)

foreign_ip_packet_without_process = con.execute(
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
foreign_ip_without_process AS
(
    SELECT ip.*
    FROM fact_ip_host ip
    LEFT JOIN gold_fact_process_network pro_net ON ip._id = pro_net.packet_id
    WHERE pro_net.send IS NULL
    AND NOT ip.source_host IN (SELECT host FROM interface_host)
    AND ip.destination_host IN (SELECT host FROM interface_host)
)
SELECT
    foreign_packet.address,
    foreign_packet.port AS port,
    COUNT(*) AS count,
    ROUND(SUM(fact_packet.length) / (1024 * 1024), 3) AS size,
FROM
(
    SELECT
        _id,
        destination_host AS address,
        source_port AS port,
    FROM foreign_ip_without_process
    WHERE source_host IN (SELECT host FROM interface_host)
    UNION ALL
    SELECT
        _id,
        source_host AS address,
        destination_port AS port,
    FROM foreign_ip_without_process
    WHERE destination_host IN (SELECT host FROM interface_host)
) foreign_packet
LEFT JOIN gold_fact_network_packet fact_packet ON fact_packet._id = foreign_packet._id
GROUP BY
    foreign_packet.address,
    foreign_packet.port
ORDER BY
    address,
    port
"""
).df()

st.text("IP packet sent to/received from foreign IP without associated process")
st.dataframe(foreign_ip_packet_without_process.rename(columns={"size": "size (Mo)"}), hide_index=True)

gold_fact_network_ip_count = con.execute(
    """
SELECT
    COUNT(DISTINCT _id) AS count
FROM gold_fact_network_ip
"""
).fetchone()[0]

gold_fact_process_network_count = con.execute(
    """
SELECT
    COUNT(DISTINCT packet_id) AS count
FROM gold_fact_process_network
"""
).fetchone()[0]


st.write(
    gold_fact_network_ip_count,
    " ip packet, ",
    gold_fact_process_network_count,
    "ip packet with associated process, ",
    round((1 - (gold_fact_process_network_count / gold_fact_network_ip_count)) * 100, 2),
    "% of packet with unknown process.",
)

# Running time

st.sidebar.header("Statistics", divider=True)
end_timer = timer()
st.sidebar.write("Running time: ", round(end_timer - start_timer, 4), " seconds")
