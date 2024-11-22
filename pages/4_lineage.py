import tempfile
from timeit import default_timer as timer

import graphviz
import streamlit as st
from PIL import Image

from pages import connection

BACKGROUND_COLOR = "#282A36"
PROCESS_COLOR = "#50FA7B"
FILE_COLOR = "#57C7FF"
SOCKET_COLOR = "#FF6E6E"
EDGE_COLOR = "#82EFFF"
FOREIGN_HOST_COLOR = "#FFFF85"

MAX_DISTINCT_COMMAND_BY_CHILD = 5

start_timer = timer()
con = connection()

st.set_page_config(
    page_title="Zoom",
    page_icon="📄",
    layout="wide",
)
st.header("Dive in your command history", divider=True)

# Process selection

st.sidebar.header("Parameters", divider=True)

commands = con.execute(
    """
    SELECT
        DISTINCT command
    FROM gold_dim_process
    WHERE command IS NOT NULL
    ORDER BY command
"""
).df()
command: str = st.sidebar.selectbox("Choose the command", commands)

pids = con.execute(
    """
    SELECT
        DISTINCT pid
    FROM gold_dim_process
    WHERE command = ?
    ORDER BY pid
""",
    [command],
).df()
pid: str = st.sidebar.selectbox("Choose the pid", pids)

show_only_modified_files = st.sidebar.checkbox("Show only modified files", value=True)

# Forensic

# Object


class Process:

    def __init__(self, process_tuple):
        self.id = str(process_tuple[0])
        self.pid = process_tuple[1]
        self.ppid = process_tuple[2]
        self.user = process_tuple[3]
        self.full_command = process_tuple[4]
        self.started_at = process_tuple[5]
        self.inserted_at = process_tuple[6]

    def __str__(self):
        return (
            f"Process(id={self.id}, pid={self.pid}, ppid={self.ppid}, user='{self.user}', "
            f"full_command='{self.full_command}', started_at={self.started_at}, "
            f"inserted_at={self.inserted_at})"
        )

    def add_node(self, graph):
        command = self.full_command if len(self.full_command) < 100 else self.full_command[:100] + "..."
        label = f"""pid: {self.pid}
user: {self.user}
command: {command}
{self.started_at} -> {self.inserted_at}"""
        graph.node(str(self.id), label, shape="rectangle", color=PROCESS_COLOR, style="filled")


class File:

    def __init__(self, file_tuple):
        self.id = str(hash(str(file_tuple[0])))
        self.name = file_tuple[0]
        self.modified = file_tuple[1]

    def __str__(self):
        return f"File(id={self.id}, name='{self.name}', modified={self.modified}"

    def add_node(self, graph):
        label = f"{self.name}"
        graph.node(str(self.id), label, shape="rectangle", color=FILE_COLOR, style="filled", fillcolor=FILE_COLOR)


class Socket:

    def __init__(self, socket_tuple):
        self.id = str(hash(str(socket_tuple[0]) + str(socket_tuple[1])))
        self.port = socket_tuple[0]
        self.addresses = socket_tuple[1]

    def __str__(self):
        return f"Socket(id={self.id}, port={self.port}, addresses={self.addresses}"

    def add_node(self, graph):
        label = f"port: {self.port}\naddresses: {self.addresses}"
        graph.node(str(self.id), label, shape="rectangle", color=SOCKET_COLOR, style="filled", fillcolor=SOCKET_COLOR)


class ForeignHost:

    def __init__(self, foreign_host_tuple):
        self.id = str(hash(str(foreign_host_tuple[0])))
        self.ip = foreign_host_tuple[0]

    def __str__(self):
        return f"ForeignHost(id={self.id}, ip={self.ip}"

    def add_node(self, graph):
        label = f"ip: {self.ip}"
        graph.node(
            str(self.id),
            label,
            shape="rectangle",
            color=FOREIGN_HOST_COLOR,
            style="filled",
            fillcolor=FOREIGN_HOST_COLOR,
        )


# Database function


def get_process_by_pid(pid):
    process = con.execute(
        """
    SELECT
        HASH(pro.pid, started_at) AS _id,
        pro.pid,
        pro.ppid,
        usr.name AS user,
        pro.full_command,
        pro.started_at,
        pro.inserted_at,
    FROM gold_dim_process pro
    LEFT JOIN gold_file_user usr ON usr.uid = pro.uid
    WHERE pro.pid = ?""",
        [str(pid)],
    ).fetchone()
    return Process(process) if process is not None else None


def get_processes_by_ppid(ppid):
    process_buffer = []
    processes = con.execute(
        """
    SELECT
        HASH(pro.pid, started_at) AS _id,
        pro.pid,
        pro.ppid,
        usr.name AS user,
        pro.full_command,
        pro.started_at,
        pro.inserted_at,
    FROM gold_dim_process pro
    LEFT JOIN gold_file_user usr ON usr.uid = pro.uid
    WHERE pro.ppid = ?
    ORDER BY pro.started_at ASC""",
        [str(ppid)],
    ).df()
    for row in processes.itertuples(index=False, name=None):
        process_buffer.append(Process(row))
    return process_buffer


def get_open_files_by_process(pid):
    files_buffer = []
    files = con.execute(
        """
        SELECT
            name,
            CASE
                WHEN min_size <> max_size THEN TRUE
                ELSE FALSE
            END AS modified,
        FROM
        (
            SELECT
                dim.name AS name,
                MIN(fact.size) AS min_size,
                MAX(fact.size) AS max_size,
            FROM gold_fact_file_reg fact
            LEFT JOIN gold_dim_file_reg dim ON fact.pid = dim.pid AND fact.fd = dim.fd AND fact.node = dim.node
            WHERE fact.pid = ?
            GROUP BY dim.name
        )
    """,
        [str(pid)],
    ).df()
    for row in files.itertuples(index=False, name=None):
        files_buffer.append(File(row))
    return files_buffer


def get_open_sockets_by_process(pid):
    socket_buffer = []
    socket = con.execute(
        """
        SELECT
            port,
            LIST(address)
        FROM
        (
            SELECT DISTINCT
                COALESCE(source_port::TEXT, '*') AS port,
                COALESCE(HOST(source_address::INET), '*') AS address
            FROM gold_dim_network_socket
            WHERE pid = ?
            AND source_address != '::1'::INET
        )
        GROUP BY port
    """,
        [str(pid)],
    ).df()
    for row in socket.itertuples(index=False, name=None):
        socket_buffer.append(Socket(row))
    return socket_buffer


def get_foreign_host_by_port(port, pid):
    foreign_host_buffer = []
    foreign_host = con.execute(
        """
WITH ip_traffic AS
(
       SELECT _id,
              source_address AS address,
              source_port    AS port,
              destination_address AS foreign_address,
              created_at,
              inserted_at
       FROM   gold_fact_network_ip
       UNION ALL
       SELECT _id,
              destination_address AS address,
              destination_port    AS port,
              source_address AS foreign_address,
              created_at,
              inserted_at
       FROM   gold_fact_network_ip
)
SELECT
    HOST(ip_traffic.foreign_address::INET) AS foreign_address,
FROM
    ip_traffic
INNER JOIN gold_dim_network_socket soc
ON         soc.source_port = ip_traffic.port
AND        ip_traffic.created_at >= soc.started_at
AND        ip_traffic.created_at <= soc.inserted_at
WHERE      ip_traffic.address IN
           (
                  SELECT address
                  FROM   gold_dim_network_local_ip
           )
AND        ip_traffic.port = ?
AND        soc.pid = ?
GROUP BY   ip_traffic.foreign_address
    """,
        [str(port), str(pid)],
    ).df()
    for row in foreign_host.itertuples(index=False, name=None):
        foreign_host_buffer.append(ForeignHost(row))
    return foreign_host_buffer


# Graph function


def add_ancestor(process, graph):
    parent = get_process_by_pid(process.ppid)
    if parent is not None:
        parent.add_node(graph)
        graph.edge(parent.id, process.id, color=EDGE_COLOR)
        add_ancestor(parent, graph)


def add_descendant(node_id, pid, graph, process_node_buffer):
    last_process_id = ""
    children = get_processes_by_ppid(pid)
    cut_commands = []
    for child in children:
        if (
            len([p for p in process_node_buffer if p.full_command == child.full_command and p.ppid == child.ppid])
            > MAX_DISTINCT_COMMAND_BY_CHILD
        ):
            cut_commands.append(child.full_command)
        else:
            child.add_node(graph)
            graph.edge(node_id, child.id, color=EDGE_COLOR)
            if last_process_id != "":
                graph.edge(last_process_id, child.id, color=BACKGROUND_COLOR)
            last_process_id = child.id
            add_open_file(child.id, child.pid, graph)
            add_open_socket(child.id, child.pid, graph)
            add_descendant(child.id, child.pid, graph, process_node_buffer)
        process_node_buffer.append(child)
    for command in set(cut_commands):
        occurence = len([c for c in cut_commands if c == command]) + MAX_DISTINCT_COMMAND_BY_CHILD
        st.sidebar.warning(
            f"Command '{command}' with PPID {pid} was launched {occurence} times. Showing only the first 5."
        )


def add_open_file(node_id, pid, graph):
    last_file_id = ""
    open_files = get_open_files_by_process(pid)
    for file in open_files:
        if (not show_only_modified_files) or file.modified:
            file.add_node(graph)
            graph.edge(node_id, file.id, color=EDGE_COLOR)
            if last_file_id != "":
                graph.edge(last_file_id, file.id, color=BACKGROUND_COLOR)
            last_file_id = file.id


def add_open_socket(node_id, pid, graph):
    last_socket_id = ""
    open_sockets = get_open_sockets_by_process(pid)
    for socket in open_sockets:
        socket.add_node(graph)
        graph.edge(node_id, socket.id, color=EDGE_COLOR)
        if last_socket_id != "":
            graph.edge(last_socket_id, socket.id, color=BACKGROUND_COLOR)
        last_socket_id = socket.id
        for foreign_host_node in get_foreign_host_by_port(socket.port, pid):
            if foreign_host_node.id not in graph.source:
                foreign_host_node.add_node(graph)
                graph.edge(socket.id, foreign_host_node.id, color=EDGE_COLOR, dir="both")


graph = graphviz.Digraph(format="png")
graph.attr(bgcolor=BACKGROUND_COLOR)

process_node_buffer: list[Process] = []
process = get_process_by_pid(pid)
process.add_node(graph)
add_open_file(process.id, process.pid, graph)
add_open_socket(process.id, pid, graph)
add_ancestor(process, graph)
add_descendant(process.id, process.pid, graph, process_node_buffer)

st.graphviz_chart(graph)
save_and_open = st.button("Open in explorer 🔎")
if save_and_open:
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        graph.render(tmp_file.name, format="png", cleanup=True)
        img = Image.open(tmp_file.name + ".png")
        img.show()

end_timer = timer()

st.sidebar.header("Statistics", divider=True)
st.sidebar.write("Running time: ", round(end_timer - start_timer, 4), " seconds")
