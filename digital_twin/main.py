from connections import MqttConnection
from process import Processing
import threading
import collections
import signal
from flask import Flask, jsonify, request, Response
import yaml
import logging
import time
import requests
import os
import redis
import events

# for live migration
migration_done = None

# conf_path = "/app/dt/configs/config.yaml"
conf_path = os.environ.get("CONFIG_PATH", "./config.yaml")
confs = yaml.safe_load(open(conf_path))
dt_name = confs["name"]
flask_port = int(confs["flask"]["port"])
process_conf = confs["process"]

connection_conf = confs["connections"]
connection_buffer_conf = connection_conf["buffer"]["size"]
connection_mqtt_conf = connection_conf["mqtt"]
connection_mongo_conf = connection_conf["mongodb"]
connection_use_mongo = connection_mongo_conf["use"]
connection_mongo_url = None
connection_mongo_db = None
connection_mongo_collection = None

if connection_use_mongo:
    connection_mongo_url = connection_mongo_conf["url"]
    connection_mongo_db = connection_mongo_conf["database"]
    connection_mongo_collection = connection_mongo_conf["collection"]

state_max_size_mb = confs.get("state").get("max_size_mb", 10)
state_max_entry_size_kb = confs.get("state").get("max_entry_size_kb", 10)
state_backend = confs.get("state").get("backend", "local")

redis_confs = connection_conf["redis"]
redis_host = redis_confs["host"]
redis_port = redis_confs["port"]
redis_db = int(redis_confs["db"])
if state_backend == "redis":
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
else:
    redis_client = None

processing_buffer_size = round(state_max_size_mb / (state_max_entry_size_kb / 1024.0))

logger_conf = confs["logger"]
level_conf = logger_conf["level"]
shell_level = level_conf["shell"]
file_level = level_conf["file"]
mongo_level = level_conf["mongo"]

odte_conf = confs["odte"]
odte_expected_msg_sec = odte_conf["expected_msg_sec"]


def setup_logging():
    fmt = logging.Formatter(
        fmt="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(getattr(logging, shell_level.upper(), logging.INFO))
    root.addHandler(sh)

    log_process = logging.getLogger("process")
    log_connections = logging.getLogger("connections")
    log_main = logging.getLogger("__main__")

    log_process.propagate = True
    log_connections.propagate = True
    log_main.propagate = True

    logging.getLogger("pymongo").setLevel(
        getattr(logging, mongo_level.upper(), logging.INFO)
    )


setup_logging()
logger = logging.getLogger(__name__)


def graceful_shutdown(signum, frame):
    processing.stop()
    try:
        processing_t.join()
    except:
        pass

    mqtt_connection.stop()
    try:
        mqtt_t.join()
    except:
        pass

    logger.info(f"Processing stopped at: {time.time()}")
    exit(0)


signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)


app = Flask(__name__)

processing_buffer = collections.deque(maxlen=processing_buffer_size)
connection_buffer = collections.deque(maxlen=connection_buffer_conf)
messages_buffer = collections.deque(maxlen=connection_buffer_conf)

mqtt_connection = MqttConnection(
    connection_buffer=connection_buffer,
    mqtt_conf=connection_mqtt_conf,
    use_mongo=connection_use_mongo,
    mongo_url=connection_mongo_url,
    mongo_db=connection_mongo_db,
    mongo_collection=connection_mongo_collection,
    messages_buffer=messages_buffer,
)
processing = Processing(
    connection_buffer=connection_buffer,
    processing_buffer=processing_buffer,
    state_max_size_mb=state_max_size_mb,
    state_max_entry_size_kb=state_max_entry_size_kb,
    use_mongo=connection_use_mongo,
    mongo_url=connection_mongo_url,
    mongo_db=connection_mongo_db,
    mongo_collection=connection_mongo_collection,
    messages_buffer=messages_buffer,
    odte_expected_msg_sec=odte_expected_msg_sec,
    redis_client=redis_client,
)

# check if migrated dt
# live replication
source_dt_url_delta = os.environ.get("SOURCE_DT_URL_DELTA", "")
source_dt_url_disconnect = os.environ.get("SOURCE_DT_URL_DISCONNECT", "")

if source_dt_url_delta:
    migration_done = False
    restore_start_timestamp = time.time()
    # start requesting delta
    logger.info(f"DT is migrated. Starting live migration.")
    max_rounds = int(confs["migration"]["live"]["rounds"])
    events.emit("precopy_start")
    for i in range(max_rounds - 1):
        t0 = time.time()
        resp = requests.get(source_dt_url_delta)
        obj = resp.json()
        t1 = time.time()
        processing.process_delta(obj)
        events.emit(
            "precopy_round",
            round=i,
            bytes=len(resp.content),
            entries=len(obj["processing_buffer"]),
            fetch_s=t1 - t0,
            apply_s=time.time() - t1,
        )
        time.sleep(1)

    # call disconnect on old dt
    logger.debug("Disconnecting source DT.")
    resp = requests.post(source_dt_url_disconnect)
    if resp.status_code != 200:
        logger.error("Error in disconnecting the source DT")
        exit(1)
    mqtt_disconnect_request_ts = time.time()
    events.emit("source_disconnected")

    # call last delta
    logger.debug("Calling last delta.")
    t0 = time.time()
    resp = requests.get(source_dt_url_delta)
    obj = resp.json()
    t1 = time.time()
    processing.process_delta(obj)
    events.emit(
        "precopy_round",
        round="final",
        bytes=len(resp.content),
        entries=len(obj["processing_buffer"]),
        fetch_s=t1 - t0,
        apply_s=time.time() - t1,
    )
    events.emit("precopy_end")

    restore_end_timestamp = time.time()
    live_migration_total_time = restore_end_timestamp - restore_start_timestamp
    logger.info(
        f"Restoring total time: {live_migration_total_time}. Started at: {restore_start_timestamp}, ended at: {restore_end_timestamp}."
    )

startup_mqtt_connection = int(os.environ.get("STARTUP_MQTT_CONNECTION", 1))


@app.route("/rebuild", methods=["POST"])
def rebuild():
    global migration_done
    migration_done = False
    events.emit("rebuild_start")
    rebuild_start_time = time.time()
    try:
        n = processing.rebuild()
    except:
        return jsonify({"message": "Error in rebuild process."}), 500

    rebuild_total_time = time.time() - rebuild_start_time
    events.emit("rebuild_end", docs=n)
    migration_done = True
    return jsonify({"message": f"Rebuild success. Total time: {rebuild_total_time}"})


@app.route("/state", methods=["GET"])
def get_state():
    dump_start_timestamp = time.time()
    state = processing.serialize_state()
    dump_end_timestamp = time.time()
    logger.info(
        f"Dumping total time: {dump_end_timestamp - dump_start_timestamp}. Started at {dump_start_timestamp}, ended at {dump_end_timestamp}."
    )
    return jsonify({"state": state})


@app.route("/state", methods=["POST"])
def set_state():
    restore_start_timestamp = time.time()
    serialized_state = request.get_json()["state"]
    result = processing.deserialize_state(serialized_state)
    restore_end_timestamp = time.time()
    logger.info(
        f"Restoring total time: {restore_end_timestamp - restore_start_timestamp}. Started at: {restore_start_timestamp}, ended at: {restore_end_timestamp}.\n"
    )
    if result == 0:
        return jsonify({"status": "success"})
    else:
        return jsonify({"status": "error"})


@app.route("/disconnect", methods=["POST"])
def disconnect():
    disconnection_time = time.time()
    logger.info(f"Disconnecting from mqtt. Time: {disconnection_time}")
    try:
        mqtt_connection.stop()
        mqtt_t.join()
    except:
        return jsonify({"status": "error"})

    events.emit("mqtt_disconnected", last_seq=processing.last_applied_pt_seq)
    return jsonify({"status": "success"})


@app.route("/reconnect", methods=["POST"])
def reconnect():
    global mqtt_t
    try:
        mqtt_connection.mqtt_loop_run = True
        mqtt_t = threading.Thread(target=mqtt_connection.run)
        mqtt_t.start()
        reconnection_time = time.time()
        logger.info(f"Reconnecting to mqtt. Time: {reconnection_time}")
    except:
        return jsonify({"status": "error"})
    events.emit("mqtt_connected")
    return jsonify({"status": "success"})


@app.route("/healthd")
def health_check():
    return jsonify({"status": "healthy"}), 200


@app.route("/odte")
def odte():
    body = f"odte {float(processing.get_odte())}"
    return Response(body, mimetype="text/plain; version=0.0.4; charset=utf-8")


@app.route("/delta")
def get_delta():
    delta = processing.get_delta()
    return jsonify(delta)


@app.route("/delta", methods=["POST"])
def process_delta():
    different_items = request.get_json()
    try:
        processing.process_delta(different_items)
        return jsonify({"message": "Success."}), 201
    except Exception:
        return jsonify({"message": "Exception in processing delta."}), 500


@app.route("/migration_status", methods=["GET"])
def get_migration_status():
    global migration_done
    return jsonify({"status": migration_done})


if __name__ == "__main__":
    logger.debug("Started main.")
    events.emit("dt_start")

    if startup_mqtt_connection != 0:
        mqtt_t = threading.Thread(target=mqtt_connection.run)
        mqtt_t.start()
        mqtt_connected_ts = time.time()
        logger.info(f"Connected to mqtt. Time: {mqtt_connected_ts}")
        migration_done = True

    processing_t = threading.Thread(target=processing.run)
    processing_t.start()
    processing_start_ts = time.time()

    logger.info(f"Processing started at: {processing_start_ts}")

    app.run(host="0.0.0.0", port=flask_port)
