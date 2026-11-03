"""Per-node counters agent.

Endpoints (all POST, parameters in the query string):
  /snapshot?run_id=R&label=L&uids=U1,U2   one line with node CPU, host NIC bytes, per-pod CPU
  /mem/start?run_id=R&uids=U1,U2          start sampling memory every 100 ms
  /mem/stop                               stop sampling, write the peaks
"""

import glob
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

CG = "/host/sys/fs/cgroup"
NODE = os.environ.get("NODE_NAME", "node")
IFACE = os.environ.get("IFACE", "ens3")
PORT = int(os.environ.get("PORT", "9200"))
OUT = os.path.join(os.environ.get("EVENTS_DIR", "/events"), f"counters-{NODE}.jsonl")

_lock = threading.Lock()
_sampler: dict = {"stop": None, "thread": None}


def emit(d: dict) -> None:
    d["ts"] = time.time_ns()
    d["node"] = NODE
    with _lock, open(OUT, "a") as f:
        f.write(json.dumps(d) + "\n")


def read_kv(path: str) -> dict:
    out = {}
    try:
        with open(path) as fh:
            for line in fh:
                k, v = line.split()[:2]
                out[k] = int(v)
    except FileNotFoundError:
        pass
    return out


def read_int(path: str) -> int | None:
    try:
        with open(path) as fh:
            return int(fh.read().strip())
    except (FileNotFoundError, ValueError):
        return None


def pod_cgroup(uid: str) -> str | None:
    u = uid.replace("-", "_")
    for pattern in (
        f"{CG}/kubepods.slice/*pod{u}.slice",
        f"{CG}/kubepods.slice/*/*pod{u}.slice",
    ):
        m = glob.glob(pattern)
        if m:
            return m[0]
    return None


def nic_bytes(iface: str) -> tuple[int | None, int | None]:
    with open("/proc/net/dev") as fh:
        for line in fh:
            if line.strip().startswith(iface + ":"):
                f = line.split(":", 1)[1].split()
                return int(f[0]), int(f[8])
    return None, None


def snapshot(q: dict) -> None:
    rx, tx = nic_bytes(IFACE)
    pods = {}
    for uid in filter(None, q.get("uids", "").split(",")):
        p = pod_cgroup(uid)
        pods[uid] = None if p is None else read_kv(f"{p}/cpu.stat").get("usage_usec")
    emit({
        "event": "counters",
        "run_id": q.get("run_id"),
        "label": q.get("label"),
        "node_cpu_usec": read_kv(f"{CG}/cpu.stat").get("usage_usec"),
        "rx_bytes": rx,
        "tx_bytes": tx,
        "pod_cpu_usec": pods,
    })


def mem_loop(run_id: str | None, uids: list[str], stop: threading.Event) -> None:
    # Pods are rescanned on every tick, so a destination pod created after
    # /mem/start is still sampled. Keys are pod UIDs (dashes, as in Kubernetes).
    peaks: dict[str, int] = {}
    while not stop.is_set():
        paths = {"kubepods": f"{CG}/kubepods.slice/memory.current"}
        for d in glob.glob(f"{CG}/kubepods.slice/*-pod*.slice") + glob.glob(f"{CG}/kubepods.slice/*/*-pod*.slice"):
            uid = d.rsplit("pod", 1)[1].removesuffix(".slice").replace("_", "-")
            paths[uid] = f"{d}/memory.current"
        for k, path in paths.items():
            v = read_int(path)
            if v is not None and v > peaks.get(k, 0):
                peaks[k] = v
        stop.wait(0.1)
    emit({"event": "mem_peak", "run_id": run_id, "peaks_bytes": peaks})


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if url.path == "/snapshot":
            snapshot(q)
        elif url.path == "/mem/start":
            if _sampler["stop"] is None:
                stop = threading.Event()
                uids = list(filter(None, q.get("uids", "").split(",")))
                t = threading.Thread(target=mem_loop, args=(q.get("run_id"), uids, stop), daemon=True)
                _sampler.update(stop=stop, thread=t)
                t.start()
        elif url.path == "/mem/stop":
            if _sampler["stop"] is not None:
                _sampler["stop"].set()
                _sampler["thread"].join()
                _sampler.update(stop=None, thread=None)
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.end_headers()


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()