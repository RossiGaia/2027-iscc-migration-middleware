# If ω (speed) suddenly dropped to ~0 while load is >0 → motor stall/failure.
# If T (tension) grows very high → possible belt overload or jam.
# If μ (friction) creeps upward steadily → maintenance issue (belt wear, misalignment).
# If vib spikes → imbalance or mechanical defect.
# If Tmot rises too much above ambient → overheating risk.
# If wear gets high → nearing end of equipment lifetime.

from dataclasses import dataclass
import yaml
import logging
import paho.mqtt.client as mqtt
import time
import json
import random
import threading
from flask import Flask
import signal


def graceful_shutdown(signum, frame):
    cnv1.stop()
    mqtt_t.join()
    cnv1_t.join()
    exit(0)


signal.signal(signal.SIGINT, graceful_shutdown)

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


config_path = "config.yaml"
configs = yaml.safe_load(open(config_path))
sensors_config = configs["sensors"]
mqtt_config = sensors_config["mqtt"]
flask_port = int(configs["web"].get("port", 5001))

seq_id = 0

@dataclass
class ConveyorParams:

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


@dataclass
class ConveyorConnectionParams:
    sensors_updates_per_second: float = sensors_config["updates_per_second"]
    mqtt_broker_url: str = mqtt_config["broker_url"]
    mqtt_port: int = mqtt_config["port"]
    mqtt_topic: str = mqtt_config["topic"]


class ConveyorPlant:
    def __init__(self):
        self.conveyor_params = ConveyorParams()
        self.mqtt_connection = ConveyorConnectionParams()
        self.running = True
        self.mqtt_client = None
        self._lock = threading.Lock()

    def run(self):
        logger.debug("ConveyorPlant: simulation loop starting.")
        period = 1.0 / self.mqtt_connection.sensors_updates_per_second
        if period <= 0:
            period = 0.1  # sane default

        # --- Tunable constants (lightweight, not unit-accurate) ---
        MAX_SPEED = 15.0  # rad/s
        SPEED_RESPONSE_TAU = 1.5  # s, larger = slower response
        BASE_TENSION = 200.0  # N
        TENSION_PER_LOAD = 300.0  # N at full load
        TENSION_WEAR_LOSS = 2.0  # N per wear unit
        BASE_FRICTION = 0.02  # dimensionless
        FRICTION_PER_TENSION = 0.00005  # per N
        FRICTION_PER_WEAR = 0.001  # per wear unit
        BASE_VIBRATION = 0.2  # mm/s
        VIB_PER_FRICTION = 6.0
        VIB_PER_TENSION_IMBAL = 0.002
        AMBIENT_MEAN = 22.0  # °C
        AMBIENT_NOISE = 0.3  # °C (random jitter)
        MOTOR_THERM_GAIN = 1.2  # °C per (unit of load+speed+friction)
        MOTOR_COOLING_TAU = 120.0  # s to relax toward ambient+heating
        WEAR_RATE = 1.2e-4  # wear units per (friction*speed*second)
        LOAD_RWALK_STEP = 0.02  # random walk step per tick (0..1 scale)
        LOAD_CLAMP_MIN, LOAD_CLAMP_MAX = 0.0, 1.0

        last_time = time.time()

        while self.running:
            with self._lock:
                now = time.time()
                dt = max(now - last_time, 1e-3)
                last_time = now

                p = self.conveyor_params  # shorthand

                # --- 1) Evolve load (0..1) as a small random walk (or replace with your own signal) ---
                p.load += random.uniform(-LOAD_RWALK_STEP, LOAD_RWALK_STEP)
                p.load = max(LOAD_CLAMP_MIN, min(LOAD_CLAMP_MAX, p.load))

                # --- 2) Target speed decreases with load & wear; add tiny noise for realism ---
                wear_factor = max(
                    0.5, 1.0 - p.wear * 0.01
                )  # more wear => less effective speed
                target_speed = MAX_SPEED * (0.6 + 0.4 * (1.0 - p.load)) * wear_factor
                target_speed += random.uniform(-0.2, 0.2)
                target_speed = max(0.0, target_speed)

                # --- 3) First-order response to target speed -> acceleration, then integrate speed ---
                # a ≈ (target - current)/tau with a little noise
                p.angular_acceleration = (
                    target_speed - p.angular_speed
                ) / SPEED_RESPONSE_TAU
                p.angular_acceleration += random.uniform(-0.05, 0.05)
                p.angular_speed += p.angular_acceleration * dt
                p.angular_speed = max(0.0, min(MAX_SPEED * 1.2, p.angular_speed))

                # --- 4) Belt tension rises with load, drops with wear ---
                p.belt_tension = (
                    BASE_TENSION
                    + TENSION_PER_LOAD * p.load
                    - TENSION_WEAR_LOSS * p.wear
                )
                p.belt_tension = max(50.0, p.belt_tension)

                # --- 5) Friction grows with tension and wear ---
                p.belt_friction = (
                    BASE_FRICTION
                    + FRICTION_PER_TENSION * p.belt_tension
                    + FRICTION_PER_WEAR * p.wear
                )
                p.belt_friction = max(0.0, min(0.5, p.belt_friction))

                # --- 6) Vibration driven by friction and small imbalance from tension ---
                tension_imbalance = abs(
                    p.belt_tension - (BASE_TENSION + TENSION_PER_LOAD * 0.5)
                )
                p.motor_vibration = (
                    BASE_VIBRATION
                    + VIB_PER_FRICTION * p.belt_friction
                    + VIB_PER_TENSION_IMBAL * tension_imbalance
                )
                p.motor_vibration += random.uniform(-0.05, 0.05)
                p.motor_vibration = max(0.0, p.motor_vibration)

                # --- 7) Temperatures: ambient with jitter; motor warms with speed/load/friction ---
                p.ambient_temperature = AMBIENT_MEAN + random.uniform(
                    -AMBIENT_NOISE, AMBIENT_NOISE
                )
                motor_heat_input = MOTOR_THERM_GAIN * (
                    0.6 * p.load
                    + 0.3 * (p.angular_speed / MAX_SPEED)
                    + 0.1 * p.belt_friction
                )
                motor_target_temp = p.ambient_temperature + 20.0 * motor_heat_input
                # first-order thermal response
                if MOTOR_COOLING_TAU > 1e-6:
                    p.motor_temperature += (motor_target_temp - p.motor_temperature) * (
                        dt / MOTOR_COOLING_TAU
                    )
                else:
                    p.motor_temperature = motor_target_temp
                p.motor_temperature += random.uniform(-0.05, 0.05)

                # --- 8) Wear accumulates with friction * speed ---
                p.wear += WEAR_RATE * (p.belt_friction * max(p.angular_speed, 0.0)) * dt
                p.wear = max(0.0, min(100.0, p.wear))

                # Debug log for tracing
                logger.debug(
                    f"[{p.name}] load={p.load:.2f} ω={p.angular_speed:.2f} rad/s "
                    f"a={p.angular_acceleration:.2f} rad/s² T={p.belt_tension:.1f} N "
                    f"μ={p.belt_friction:.3f} vib={p.motor_vibration:.2f} mm/s "
                    f"Tamb={p.ambient_temperature:.1f}°C Tmot={p.motor_temperature:.1f}°C "
                    f"wear={p.wear:.2f}"
                )

            # Sleep until next tick
            time.sleep(period)

        logger.debug("ConveyorPlant: simulation loop stopping.")

    def stop(self):
        self.running = False

    def on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        logger.debug(
            f"mqtt connected to {self.mqtt_connection.mqtt_broker_url} at port {self.mqtt_connection.mqtt_port}."
        )

    def mqtt_t(self):
        global seq_id

        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt_client.connect(
            self.mqtt_connection.mqtt_broker_url, self.mqtt_connection.mqtt_port
        )

        self.mqtt_client.loop_start()

        while self.running:
            with self._lock:
                data = {
                    "seq_id": seq_id,
                    "status": {
                        "name": self.conveyor_params.name,
                        "load": self.conveyor_params.load,
                        "angular_acceleration": self.conveyor_params.angular_acceleration,
                        "angular_speed": self.conveyor_params.angular_speed,
                        "motor_vibration": self.conveyor_params.motor_vibration,
                        "belt_tension": self.conveyor_params.belt_tension,
                        "ambient_temperature": self.conveyor_params.ambient_temperature,
                        "motor_temperature": self.conveyor_params.motor_temperature,
                        "belt_friction": self.conveyor_params.belt_friction,
                        "wear": self.conveyor_params.wear,
                    },
                    "creation_timestamp": time.time(),
                }
            payload = json.dumps(data)
            self.mqtt_client.publish(self.mqtt_connection.mqtt_topic, payload, qos=1)
            seq_id += 1
            logger.debug(f"Seq_id: {seq_id}")
            time.sleep(1.0 / self.mqtt_connection.sensors_updates_per_second)

        self.mqtt_client.loop_stop()


cnv1 = ConveyorPlant()

cnv1_t = threading.Thread(target=cnv1.run)
cnv1_t.start()

mqtt_t = threading.Thread(target=cnv1.mqtt_t)
mqtt_t.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=flask_port)
