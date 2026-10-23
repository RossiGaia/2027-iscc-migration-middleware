from dataclasses import dataclass, asdict
import time
import json
import collections
import threading
import queue
from typing import Any
import pymongo
import sys
import logging
import itertools
import events
import redis

logger = logging.getLogger(__name__)


@dataclass
class VirtualizedConveyorPlant:

    name: str = "CNV1"

    load: float = 0.0
    angular_acceleration: float = 0.0
    angular_speed: float = 0.0
    motor_vibration: float = 0.0
    belt_tension: float = 0.0
    ambient_temperature: float = 0.0
    motor_temperature: float = 0.0
    belt_friction: float = 0.0

    wear: float = 5.0


@dataclass()
class ProcessingConfig:

    def __init__(self, odte_expected_msg_sec=1) -> None:
        self.odte_expected_msg_sec = odte_expected_msg_sec

    pulley_radius_m: float = 0.15
    rolling_window: int = 50

    # anomaly thresholds
    vib_high: float = 3.5
    tension_high: float = 280.0
    friction_high: float = 0.06
    temp_rise_overheat: float = 35.0
    speed_drop: float = 1.0
    speed_drop_load_min: float = 0.1

    # ODTE params
    odte_window_length_sec: int = 10
    odte_desired_timeliness_sec: float = 1.0
    # odte_expected_msg_sec: int = 1
    odte_refresh_s: float = 0.5


class Processing:
    def __init__(
        self,
        *,
        connection_buffer,
        processing_buffer,
        state_max_size_mb=10,
        state_max_entry_size_kb=10,
        use_mongo=False,
        mongo_url=None,
        mongo_db=None,
        mongo_collection=None,
        odte_expected_msg_sec=1,
        messages_buffer,
        redis_client=None
    ):
        self.connection_buffer = connection_buffer
        self.processing_buffer = processing_buffer
        self.observations = collections.deque(maxlen=100)
        self.running = True
        self.conveyor_params = VirtualizedConveyorPlant()
        self.state_max_size_mb = state_max_size_mb
        self.odte = None
        self.lock = threading.Lock()
        self.transmitted_state = {}
        self.last_transmitted_processing_seq_no = -1
        self.use_mongo = use_mongo
        self.mongo_url = mongo_url
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.odte_t = threading.Thread(target=self.odte_computation, daemon=True)
        self.messages_buffer = messages_buffer

        self.redis_client = redis_client
        self.recent_rows = []

        self.cfg = ProcessingConfig(odte_expected_msg_sec)
        self.state_max_entry_size_kb = state_max_entry_size_kb

        self.last_applied_pt_seq = -1

        if self.use_mongo:
            try:
                self.mongo_client = pymongo.MongoClient(self.mongo_url)
            except:
                logger.error("Could not connect to mongo.")
                sys.exit(1)

    def _load_from_redis(self):
        p = self.redis_client.pipeline()
        p.lrange("buf", -self.cfg.rolling_window, -1)
        p.get("params")
        p.get("last_seq")
        rows, params, seq = p.execute()
        self.recent_rows = [json.loads(x) for x in rows]
        if params:
            self.conveyor_params = VirtualizedConveyorPlant(**json.loads(params))
        self.last_applied_pt_seq = int(seq) if seq is not None else -1

    def _save_to_redis(self, snap):
        p = self.redis_client.pipeline()
        p.rpush("buf", json.dumps(snap))
        p.ltrim("buf", -self.processing_buffer.maxlen, -1)
        p.set("params", json.dumps(asdict(self.conveyor_params)))
        p.set("last_seq", self.last_applied_pt_seq)
        p.execute()

    def run(self):
        self.odte_t.start()

        while self.running:

            t0 = time.time()
            with self.lock:
                if not self.connection_buffer:
                    raw_msg = None
                else:
                    raw_msg = self.connection_buffer.popleft()

            if raw_msg is None:
                time.sleep(0.001)
                continue

            if self.redis_client:
                self._load_from_redis()
            
            seq = raw_msg["payload"]["seq_id"]
            if seq <= self.last_applied_pt_seq:
                continue
            self.last_applied_pt_seq = seq

            snap = self._build_snap(raw_msg)
            self._add_derived_metrics(snap)
            self._add_rolling_features(snap)
            self._add_health_and_anomalies(snap)
            self._maybe_pad(snap)

            processing_time = time.time() - t0
            snap["processing_time_s"] = processing_time

            
            if self.redis_client:
                self._save_to_redis(snap)
            else:
                with self.lock:
                    self.processing_buffer.append(snap)
            with self.lock:
                self._record_observation(snap, processing_time)

            events.emit(
                "update",
                seq=seq,
                recv_ts=raw_msg["recv_timestamp"],
                apply_ts=time.time(),
                origin=raw_msg.get("origin", "live"),
            )
            logger.debug(f"time to elaborate the message: {processing_time}")

    def _build_snap(self, raw_msg: dict) -> dict:
        payload = raw_msg["payload"]
        status = payload.get("status", {})

        with self.lock:
            pt = self.conveyor_params
            pt.name = status.get("name", pt.name)
            pt.load = status.get("load", pt.load)
            pt.angular_acceleration = status.get(
                "angular_acceleration", pt.angular_acceleration
            )
            pt.angular_speed = status.get("angular_speed", pt.angular_speed)
            pt.motor_vibration = status.get("motor_vibration", pt.motor_vibration)
            pt.belt_tension = status.get("belt_tension", pt.belt_tension)
            pt.ambient_temperature = status.get(
                "ambient_temperature", pt.ambient_temperature
            )
            pt.motor_temperature = status.get("motor_temperature", pt.motor_temperature)
            pt.belt_friction = status.get("belt_friction", pt.belt_friction)
            pt.wear = status.get("wear", pt.wear)

            snap = asdict(pt)

        snap["recv_timestamp"] = raw_msg["recv_timestamp"]
        snap["creation_timestamp"] = payload["creation_timestamp"]
        snap["key"] = raw_msg["key"]
        snap["seq_id"] = raw_msg["payload"]["seq_id"]
        return snap

    def _add_derived_metrics(self, snap: dict) -> None:
        R = self.cfg.pulley_radius_m
        approx_torque = snap["belt_tension"] * R
        approx_power_w = approx_torque * snap["angular_speed"]

        snap["approx_torque_Nm"] = round(approx_torque, 3)
        snap["approx_power_W"] = round(approx_power_w, 2)
        snap["temp_rise_C"] = round(
            snap["motor_temperature"] - snap["ambient_temperature"], 3
        )

    def _add_rolling_features(self, snap: dict) -> None:
        for key in (
            "angular_speed",
            "motor_vibration",
            "belt_tension",
            "belt_friction",
            "motor_temperature",
        ):
            stats = self._rolling_stats(key, window=self.cfg.rolling_window)
            snap[f"{key}_mean"] = (
                None if stats["mean"] is None else round(stats["mean"], 4)
            )
            snap[f"{key}_slope_per_s"] = (
                None if stats["slope_per_s"] is None else round(stats["slope_per_s"], 6)
            )

    def _add_health_and_anomalies(self, snap: dict) -> None:
        snap["health"] = self._health_score(snap)

        snap["anomaly"] = {
            "vibration_high": snap.get("motor_vibration") is not None
            and snap["motor_vibration"] > self.cfg.vib_high,
            "tension_high": snap.get("belt_tension") is not None
            and snap["belt_tension"] > self.cfg.tension_high,
            "friction_high": snap.get("belt_friction") is not None
            and snap["belt_friction"] > self.cfg.friction_high,
            "temp_overheat": snap.get("temp_rise_C") is not None
            and snap["temp_rise_C"] > self.cfg.temp_rise_overheat,
            "speed_drop": (
                snap.get("angular_speed") is not None
                and snap["angular_speed"] < self.cfg.speed_drop
                and snap.get("load", 0.0) > self.cfg.speed_drop_load_min
            ),
        }

    def _maybe_pad(self, snap: dict) -> None:
        if not self.state_max_entry_size_kb:
            return
        snap_size = len(json.dumps(snap).encode("utf-8"))
        snap["padding"] = self._generate_padding(
            self.state_max_entry_size_kb, snap_size
        )

    def _record_observation(self, snap: dict, processing_time: float) -> None:
        self.observations.append(
            {
                "key": snap["key"],
                "received_timestamp": snap["recv_timestamp"],
                "creation_timestamp": snap["creation_timestamp"],
                "execution_time_s": processing_time,
                "obs_value": (snap["recv_timestamp"] - snap["creation_timestamp"])
                + processing_time,
            }
        )

    def _generate_padding(self, max_size, current_size):
        padding_length = round(max_size * 1024) - current_size
        logger.debug(f"Padding size: {padding_length}")
        if padding_length > 0:
            return "0" * padding_length
        else:
            return None

    def _compute_timeliness(self, desired_timeliness_sec: float):
        with self.lock:
            obs_list = list(self.observations)

        if len(obs_list) == 0:
            return 0.0

        count = 0
        for obs_obj in obs_list:
            if obs_obj["obs_value"] <= desired_timeliness_sec:
                count += 1

        percentile = float(count / len(obs_list))

        return percentile

    def _compute_reliability(self, window_length_sec: int, expected_msg_sec: int):
        end_window_time = time.time()
        start_window_time = time.time() - window_length_sec

        with self.lock:
            msg_list = list(self.messages_buffer)

        count = 0
        for msg in msg_list:
            if (
                msg["payload"]["creation_timestamp"] >= start_window_time
                and msg["payload"]["creation_timestamp"] <= end_window_time
            ):
                count += 1

        expected_msg_tot = window_length_sec * expected_msg_sec

        return float(count / expected_msg_tot)

    def _compute_availability(self):
        return 1.0

    def _compute_odte_phytodig(
        self, window_length_sec, desired_timeliness_sec, expected_msg_sec
    ):
        timeliness = self._compute_timeliness(desired_timeliness_sec)
        reliability = self._compute_reliability(window_length_sec, expected_msg_sec)
        availability = self._compute_availability()

        logger.debug(
            f"Availability: {availability}\tReliability: {reliability}\tTimeliness: {timeliness}"
        )

        return timeliness * reliability * availability

    def stop(self):
        self.running = False

    def _safe_float(self, value, default=0.0):
        """Coerce to float safely."""
        try:
            if value is None:
                return float(default)
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def _rolling_stats(self, key: str, window: int = 50):
        if self.redis_client:
            rows = self.recent_rows
        else:
            with self.lock:
                rows = list(itertools.islice(reversed(self.processing_buffer), window))[::-1]
        if not rows:
            return {"mean": None, "slope_per_s": None}

        pairs = []
        for r in rows:
            y = r.get(key)
            t = r.get("recv_timestamp")
            if y is None or t is None:
                continue
            pairs.append((t, y))

        if not pairs:
            return {"mean": None, "slope_per_s": None}
        if len(pairs) == 1:
            return {"mean": float(pairs[0][1]), "slope_per_s": None}

        ts, ys = zip(*pairs)
        mean = sum(ys) / len(ys)

        t0 = ts[0]
        xt = [t - t0 for t in ts]
        xbar = sum(xt) / len(xt)
        ybar = mean
        num = sum((xt[i] - xbar) * (ys[i] - ybar) for i in range(len(ys)))
        den = sum((xt[i] - xbar) ** 2 for i in range(len(ys))) or 1.0
        return {"mean": mean, "slope_per_s": num / den}

    def _health_score(self, row):
        """
        Heuristic 0..100 (higher is healthier).
        Penalize high vibration, high friction, high motor temp above ambient, high wear, excessive tension.
        """
        vib = self._safe_float(row.get("motor_vibration"), 0.0)  # mm/s
        mu = self._safe_float(row.get("belt_friction"), 0.02)  # -
        tmo = self._safe_float(row.get("motor_temperature"), 0.0)  # °C
        amb = self._safe_float(row.get("ambient_temperature"), 20.0)
        wear = self._safe_float(row.get("wear"), 0.0)  # 0..100
        tens = self._safe_float(row.get("belt_tension"), 200.0)  # N

        vib_risk = min(vib / 4.5, 1.0)  # >4.5 mm/s is concerning
        mu_risk = min(max((mu - 0.03) / 0.02, 0.0), 1.0)  # 0.03..0.05 window
        temp_risk = min(max((tmo - amb - 25.0) / 35.0, 0.0), 1.0)  # > amb+25 °C
        wear_risk = min(wear / 100.0, 1.0)
        tens_risk = min(max((tens - 260.0) / 100.0, 0.0), 1.0)  # >260 N

        risk = (
            0.30 * vib_risk
            + 0.20 * mu_risk
            + 0.25 * temp_risk
            + 0.15 * wear_risk
            + 0.10 * tens_risk
        )
        health = max(0.0, 100.0 * (1.0 - risk))
        return round(health, 1)

    def serialize_state(self):
        start_time = time.time()
        with self.lock:
            state = {
                "connection_buffer": list(self.connection_buffer),
                "processing_buffer": list(self.processing_buffer),
                "conveyor_params": asdict(self.conveyor_params),
                "state_max_size_mb": self.state_max_size_mb,
                "connection_buffer_maxlen": self.connection_buffer.maxlen,
                "processing_buffer_maxlen": self.processing_buffer.maxlen,
                "last_applied_pt_seq": self.last_applied_pt_seq,
            }
        serialization_time = time.time() - start_time
        logger.info(f"Serialization took {serialization_time} seconds.")
        return state

    def deserialize_state(self, serialized_state):
        start_time = time.time()
        with self.lock:
            try:
                processing_buffer_maxlen = serialized_state["processing_buffer_maxlen"]
                self.last_applied_pt_seq = serialized_state["last_applied_pt_seq"]
                self.connection_buffer.extend(serialized_state["connection_buffer"])
                self.processing_buffer = collections.deque(
                    serialized_state["processing_buffer"],
                    maxlen=processing_buffer_maxlen,
                )
                self.conveyor_params = VirtualizedConveyorPlant(
                    **serialized_state["conveyor_params"]
                )
                self.state_max_size_mb = serialized_state["state_max_size_mb"]
            except Exception as e:
                logger.error(f"Error in deserialization. {e}")
                logger.error(
                    f"Connection buffer -> {serialized_state['connection_buffer']}"
                )
                return -1
        deserialization_time = time.time() - start_time
        logger.info(f"Deserialization took {deserialization_time} seconds.")
        return 0

    def rebuild(self):
        t0 = time.time()
        logger.debug(f"Use mongo is {self.use_mongo}")
        if not self.use_mongo:
            return

        coll = self.mongo_client[self.mongo_db][self.mongo_collection]

        cursor = (
            coll.find({})
            .sort("payload.seq_id", -1)
            .limit(self.processing_buffer.maxlen)
        )
        docs = list(cursor)[::-1]
        # logger.debug(f"Documents retrived:\n{events}")

        for doc in docs:
            if "payload" not in doc:
                continue

            message = {
                "payload": doc["payload"],
                "recv_timestamp": doc.get("recv_timestamp", time.time()),
                "key": doc.get("key", str(doc.get("_id"))),
                "origin": "replay"
            }

            while len(self.connection_buffer) >= self.connection_buffer.maxlen - 1:
                time.sleep(0.1)

            with self.lock:
                self.connection_buffer.append(message)

        mongo_docs_retrival_time = time.time() - t0
        logger.info(f"Time to retrieve mongo docs {mongo_docs_retrival_time}.")

    def odte_computation(self):
        while self.running:
            new_odte = self._compute_odte_phytodig(
                self.cfg.odte_window_length_sec,
                self.cfg.odte_desired_timeliness_sec,
                self.cfg.odte_expected_msg_sec,
            )
            with self.lock:
                self.odte = new_odte

            time.sleep(self.cfg.odte_refresh_s)

    def get_odte(self):
        with self.lock:
            return self.odte

    def get_delta(self):
        current_state = self.serialize_state()
        current_conveyor_params = current_state["conveyor_params"]
        if self.transmitted_state == {}:
            transmitted_conveyor_params = {}
        else:
            transmitted_conveyor_params = self.transmitted_state.get(
                "conveyor_params", {}
            )

        conveyor_params_diff = {}
        for key, value in current_conveyor_params.items():
            if key in transmitted_conveyor_params:
                transmitted_value = transmitted_conveyor_params[key]
                if value != transmitted_value:
                    conveyor_params_diff[key] = value
            else:
                conveyor_params_diff[key] = value

        # processing buffer
        current_proc_buffer = current_state["processing_buffer"]
        new_proc_buffer = []
        max_seq_seen = self.last_transmitted_processing_seq_no

        for entry in current_proc_buffer:
            seq = entry["seq_id"]
            if seq > self.last_transmitted_processing_seq_no:
                new_proc_buffer.append(entry)
                if seq > max_seq_seen:
                    max_seq_seen = seq

        # connection buffer
        current_conn_buffer = current_state["connection_buffer"]

        different_items = {
            "connection_buffer": current_conn_buffer,
            "processing_buffer": new_proc_buffer,
            "conveyor_params": conveyor_params_diff,
        }

        prev_state_max_size_mb = (
            None
            if self.transmitted_state == {}
            else self.transmitted_state.get("state_max_size_mb")
        )
        if prev_state_max_size_mb != current_state["state_max_size_mb"]:
            different_items["state_max_size_mb"] = current_state["state_max_size_mb"]

        prev_last_applied_pt_seq = (
            None
            if self.transmitted_state == {}
            else self.transmitted_state.get("last_applied_pt_seq")
        )

        if prev_last_applied_pt_seq != current_state["last_applied_pt_seq"]:
            different_items["last_applied_pt_seq"] = current_state[
                "last_applied_pt_seq"
            ]

        with self.lock:
            self.transmitted_state = current_state
            self.last_transmitted_processing_seq_no = max_seq_seen

        return different_items

    def process_delta(self, different_items):
        logger.debug("Started proccess_delta.")
        with self.lock:
            logger.debug("Lock acquired.")
            new_conveyor_params = different_items["conveyor_params"]
            for key, value in new_conveyor_params.items():
                current_value = getattr(self.conveyor_params, key)
                if current_value != value:
                    setattr(self.conveyor_params, key, value)

            if "last_applied_pt_seq" in different_items:
                self.last_applied_pt_seq = different_items["last_applied_pt_seq"]

            new_conn_buffer = different_items["connection_buffer"]
            self.connection_buffer.extend(new_conn_buffer)

            new_proc_buffer = different_items["processing_buffer"]
            self.processing_buffer.extend(new_proc_buffer)