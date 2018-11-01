from prometheus_client import Gauge

active_connections = Gauge(
    "replayserver_active_connection_count",
    "Count of currently active connections.")

running_replays = Gauge(
    "replayserver_running_replay_count",
    "Count of currently running replays.")
