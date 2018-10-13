import os


docker_faf_db_config = {
    'host': os.environ.get("FAF_STACK_DB_IP", "172.19.0.2"),
    'port': 3306,
    'user': 'root',
    'password': 'banana',
    'db': 'faf',
}
