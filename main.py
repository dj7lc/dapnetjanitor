import configparser
import logging
import time

import CheckCalls
import CheckNews
import CheckNodes
import CheckRubrics
import CheckSubscribers
import couchdb
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(
    format="%(asctime)s [%(levelname)-7s] %(name)-25s %(message)s",
    handlers=[
        logging.FileHandler(filename="janitor.log", encoding="utf-8", mode="a+"),
        logging.StreamHandler(),
    ],
    level=logging.DEBUG,
)

logging.getLogger("pika").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

version = "20250408"


def db_check():
    CheckCalls.check_calls(couchdb_server, delete=False)
    CheckNews.check_news(couchdb_server, delete=False)
    CheckNodes.check_nodes(couchdb_server, delete=False)
    CheckRubrics.check_rubrics(couchdb_server, delete=False)
    CheckSubscribers.check_subscribers(couchdb_server, delete=False)


if __name__ == "__main__":
    lines = [
        "Program : DAPNET JANITOR",
        f"Version : {version}",
        "Info    : Performs database sanity checks and removes expired data",
    ]
    max_length = max(len(line) for line in lines) + (1 if max(len(line) for line in lines) % 2 == 0 else 0)
    border = "* " * (max_length // 2 + 3)
    log.info(border)
    for line in lines:
        log.info(f"* {line.ljust(max_length)} *")
    log.info(border)
    log.info("Reading configuration")
    config = configparser.ConfigParser()
    config.read("janitor.ini")

    loglevel = config.get("PROGRAM", "loglevel").lower()
    if loglevel == "debug":
        log.info("Loglevel is DEBUG")
        log.setLevel(logging.DEBUG)
    elif loglevel == "info":
        log.info("Loglevel is INFO")
        log.setLevel(logging.INFO)
    elif loglevel == "warning":
        log.info("Loglevel is WARNING")
        log.setLevel(logging.WARNING)
    elif loglevel == "error":
        log.info("Loglevel is ERROR")
        log.setLevel(logging.ERROR)

    node_name = config.get("NODE", "name")

    log.info(f"Node name is {node_name}")

    couchdb_host = config.get("COUCHDB", "host")
    couchdb_port = config.get("COUCHDB", "port")
    couchdb_username = config.get("COUCHDB", "username")
    couchdb_password = config.get("COUCHDB", "password")

    log.info(f"Connecting to couchdb {couchdb_host}:{couchdb_port}")

    couchdb_connect_string = f"http://{couchdb_username}:{couchdb_password}@{couchdb_host}:{couchdb_port}/"
    couchdb_server = couchdb.Server(couchdb_connect_string)

    db_calls = couchdb_server["calls"]
    db_news = couchdb_server["news"]
    db_nodes = couchdb_server["nodes"]
    db_nodes_telemetry = couchdb_server["nodes_telemetry"]
    db_rubrics = couchdb_server["rubrics"]
    db_subscribers = couchdb_server["subscribers"]
    db_transmitters = couchdb_server["transmitters"]
    db_transmitters_telemetry = couchdb_server["transmitters_telemetry"]
    db_users = couchdb_server["users"]

    log.info("Database connection established")

    db_check()

    log.info("Initialise scheduler for cyclic database checks")
    scheduler = BackgroundScheduler()
    scheduler.add_job(db_check, trigger="cron", hour="01,07,13,19")
    log.info("Start scheduler")
    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        log.info("Stop scheduler")
        scheduler.shutdown()
