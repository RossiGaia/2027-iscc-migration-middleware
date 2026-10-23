import paho.mqtt.client as mqtt
import logging
import time
import json
import random
import string
import pymongo
import threading
import sys
import queue
import events

logger = logging.getLogger(__name__)


class MqttConnection:

    def __init__(
        self,
        *,
        connection_buffer,
        mqtt_conf,
        use_mongo=False,
        mongo_url=None,
        mongo_db=None,
        mongo_collection=None,
        messages_buffer,
    ):
        self.mqtt_loop_run = True
        self.mqtt_client = None

        self.connection_buffer = connection_buffer
        self.messages_buffer = messages_buffer

        self.mqtt_broker_url = mqtt_conf["broker_url"]
        self.mqtt_port = mqtt_conf["port"]
        self.mqtt_topics = mqtt_conf["topics"]
        self.use_mongo = use_mongo
        self.mongo_url = mongo_url
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.client_id = mqtt_conf["client_id"]
        if self.use_mongo:
            self.mongo_queue = queue.Queue()
            self.mongo_t = None
            try:
                self.mongo_client = pymongo.MongoClient(self.mongo_url)
            except:
                logger.error("Could not connect to mongo.")
                sys.exit(1)

    def new_client(self):
        self.mqtt_client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
            clean_session=False,
        )
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    def mongo_writer(self):
        assert self.connection_buffer.maxlen is not None
        assert self.mongo_client is not None
        assert self.mongo_db is not None
        assert self.mongo_collection is not None
        coll = self.mongo_client[self.mongo_db][self.mongo_collection]
        while self.mqtt_loop_run or not self.mongo_queue.empty():
            try:
                data = self.mongo_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            coll.insert_one(data)
        return

    def on_connect(self, client, userdata, flags, reason_code, properties):
        assert self.mqtt_client is not None
        logger.info(f"connected to {self.mqtt_broker_url} at port {self.mqtt_port}.")
        events.emit("mqtt_connected")
        for topic in self.mqtt_topics:
            logger.debug(f"subscribing to {topic}.")
            self.mqtt_client.subscribe(topic, qos=1)

    def on_message(self, client, userdata, msg):
        data = {
            "topic": msg.topic,
            "payload": json.loads(msg.payload.decode("UTF-8")),
            "recv_timestamp": time.time(),
            "key": "".join(
                random.choices(string.ascii_uppercase + string.digits, k=10)
            ),
        }
        logger.debug(f"Received message with seq_id: {data['payload']['seq_id']}")
        if len(self.connection_buffer) == self.connection_buffer.maxlen:
            logger.warning(
                f"connection_buffer full, dropping oldest. seq={data['payload']['seq_id']}"
            )
        self.connection_buffer.append(data)
        self.messages_buffer.append(data)
        if self.use_mongo:
            self.mongo_queue.put(data)

    def run(self):
        if self.use_mongo:
            self.mongo_t = threading.Thread(target=self.mongo_writer, daemon=True)
            self.mongo_t.start()
        self.new_client()
        assert self.mqtt_client is not None
        logger.info(f"connecting to {self.mqtt_broker_url} at {self.mqtt_port}.")
        self.mqtt_client.connect(self.mqtt_broker_url, self.mqtt_port)
        self.mqtt_client.loop_start()

        while self.mqtt_loop_run:
            time.sleep(0.05)

        logger.debug("mqtt disconnection.")
        try:
            self.mqtt_client.disconnect()
        finally:
            self.mqtt_client.loop_stop()

        self.mqtt_client = None

        if self.use_mongo:
            assert self.mongo_t is not None
            self.mongo_t.join()

        return

    def stop(self):
        self.mqtt_loop_run = False
        logger.info(f"stop requested. mqtt_loop_run = {self.mqtt_loop_run}")
