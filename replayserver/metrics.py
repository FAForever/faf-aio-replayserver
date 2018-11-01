from prometheus_client import Gauge, Counter

active_connections = Gauge(
    "replayserver_active_connections_count",
    "Count of currently active connections.")
served_connections = Counter(
    "replayserver_served_connections_total",
    "How many connections we served to completion, including failures.")

running_replays = Gauge(
    "replayserver_running_replays_count",
    "Count of currently running replays.")
finished_replays = Counter(
    "replayserver_finished_replays_total",
    "Number of replays ran to completion.")
saved_replays = Counter(
    "replayserver_saved_replay_files_total",
    "Total replays successfully saved to disk.")
