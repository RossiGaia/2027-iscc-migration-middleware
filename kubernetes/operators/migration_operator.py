"""Migration middleware for stateful DTs (ISCC).

One operator for the three strategies. On each migration it
  1. measures the inputs (DT /profile, link.json),
  2. estimates recovery and total migration time for each strategy,
  3. logs the decision it would take,
  4. executes spec.strategy if forced by the runner, otherwise its own decision.

Environment:
  NODE_HOST         IP of any cluster node, used to reach DT NodePorts
  AGENT_PORT        port of the counters agent (default 9200)
  EVENTS_FILE       where the operator writes its JSONL events
  LINK_FILE         JSON with the current link: {"bw_mbps": .., "rtt_ms": ..}
  CALIB_FILE        JSON with per-tier calibration constants
  RECOVERY_BOUND_S  maximum acceptable predicted recovery time
"""

import copy
import json
import math
import os
import time

import kopf
import requests
from kubernetes import client
from kubernetes.utils import create_from_dict

from k8s_utils import delete_from_dict

NODE_HOST = os.environ["NODE_HOST"]
AGENT_PORT = int(os.environ.get("AGENT_PORT", "9200"))
EVENTS_FILE = os.environ.get("EVENTS_FILE", "operator.jsonl")
LINK_FILE = os.environ.get("LINK_FILE", "link.json")
CALIB_FILE = os.environ.get("CALIB_FILE", "calibration.json")
RECOVERY_BOUND_S = float(os.environ.get("RECOVERY_BOUND_S", "30"))

POLL_S = 0.5
TIMEOUT_S = 1800

_f = open(EVENTS_FILE, "a", buffering=1)


# ---------------------------------------------------------------- events

def emit(event: str, run_id: str, **fields) -> None:
    _f.write(json.dumps({"event": event, "run_id": run_id, "ts": time.time_ns(), **fields}) + "\n")


def agent(ip: str, path: str, run_id: str, **params) -> None:
    try:
        requests.post(f"http://{ip}:{AGENT_PORT}{path}", params={"run_id": run_id, **params}, timeout=2)
    except requests.RequestException as e:
        emit("agent_error", run_id, ip=ip, path=path, error=str(e))


# ---------------------------------------------------------------- kubernetes helpers

def core() -> client.CoreV1Api:
    return client.CoreV1Api()


def deployment_of(spec: dict, affinity: str) -> dict:
    return next(d for d in spec["deployments"] if d["affinity"] == affinity)


def app_and_ns(configs: list) -> tuple[str, str]:
    d = next(c for c in configs if c["kind"] == "Deployment")
    return d["metadata"]["labels"]["app"], d["metadata"].get("namespace", "default")


def service_of(configs: list) -> dict:
    return next(c for c in configs if c["kind"] == "Service")


def node_ip(zone: str) -> str:
    node = core().list_node(label_selector=f"zone={zone}").items[0]
    return next(a.address for a in node.status.addresses if a.type == "InternalIP")


def pod_of(app: str, ns: str):
    pods = core().list_namespaced_pod(ns, label_selector=f"app={app}").items
    return pods[0] if pods else None


def wait_ready(app: str, ns: str):
    deadline = time.time() + TIMEOUT_S
    while time.time() < deadline:
        p = pod_of(app, ns)
        if p and any(c.type == "Ready" and c.status == "True" for c in (p.status.conditions or [])):
            return p
        time.sleep(POLL_S)
    raise kopf.PermanentError(f"{app} not ready after {TIMEOUT_S}s")


def wait_gone(app: str, ns: str) -> None:
    deadline = time.time() + TIMEOUT_S
    while time.time() < deadline:
        if pod_of(app, ns) is None:
            return
        time.sleep(POLL_S)
    raise kopf.PermanentError(f"{app} not terminated after {TIMEOUT_S}s")


def create(configs: list, affinity: str, name: str, env: dict) -> None:
    api = client.ApiClient()
    for c in configs:
        if c["kind"] == "Deployment":
            podspec = c["spec"]["template"]["spec"]
            podspec["nodeSelector"] = {"zone": affinity}
            ctr = podspec["containers"][0]
            ctr.setdefault("env", []).extend({"name": k, "value": str(v)} for k, v in env.items())
        kopf.label(c, {"related-to": name})
        kopf.adopt(c)
        create_from_dict(api, c)


def delete(configs: list) -> None:
    api = client.ApiClient()
    for c in configs:
        delete_from_dict(api, c)


def nodeport_url(configs: list, ns: str) -> str:
    svc = core().read_namespaced_service(service_of(configs)["metadata"]["name"], ns)
    return f"http://{NODE_HOST}:{svc.spec.ports[0].node_port}"


def cluster_url(configs: list, ns: str) -> str:
    svc = service_of(configs)
    port = svc["spec"]["ports"][0]["port"]
    return f"http://{svc['metadata']['name']}.{ns}.svc.cluster.local:{port}"


# ---------------------------------------------------------------- HTTP to the DTs

def call(method: str, url: str, req_timeout: float = 10) -> requests.Response:
    deadline = time.time() + TIMEOUT_S
    while time.time() < deadline:
        try:
            r = requests.request(method, url, timeout=req_timeout)
            if r.status_code < 300:
                return r
        except requests.RequestException:
            pass
        time.sleep(POLL_S)
    raise kopf.PermanentError(f"{method} {url} failed")


def wait_json(url: str, key: str, value) -> None:
    deadline = time.time() + TIMEOUT_S
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=5).json().get(key) == value:
                return
        except (requests.RequestException, ValueError):
            pass
        time.sleep(POLL_S)
    raise kopf.PermanentError(f"{url}: {key} never became {value}")


# ---------------------------------------------------------------- inputs, estimates, decision

def measure(src_url: str, src: str, dst: str) -> dict:
    prof = call("GET", f"{src_url}/profile").json()
    link = json.load(open(LINK_FILE))
    return {
        **prof,  # entries, entry_bytes, rate, p
        "bw_bps": link["bw_mbps"] * 1e6 / 8,
        "rtt_s": link["rtt_ms"] / 1000,
        "src": src,
        "dst": dst,
    }


def catch_up(d: float, rate: float, p: float) -> float:
    """Time to drain the backlog of a pause of length d."""
    load = rate * p
    return math.inf if load >= 1 else d * load / (1 - load)


def estimate(x: dict) -> dict:
    c = json.load(open(CALIB_FILE))
    dst = c[x["dst"]]
    K, v, lam, B, rtt = x["entries"], x["entry_bytes"], x["rate"], x["bw_bps"], x["rtt_s"]
    S = K * v
    p = dst["p"]
    est = {}

    local = x["dst"] == "cloud"          # Redis and MongoDB run on the cloud node
    link_rtt = 0.0 if local else rtt

    # pre-copy
    b_eff = B - lam * v
    if b_eff <= 0:
        est["replicate"] = {"feasible": False}
    else:
        final_dirty = min(lam * v * c["precopy_wait_s"], S)
        d = final_dirty / B + 2 * rtt
        t = (dst["t_start"] + max(S / b_eff, S / dst["ser_bps"])
             + c["precopy_rounds"] * c["precopy_wait_s"] + d)
        cu = catch_up(d, lam, p)
        est["replicate"] = {"feasible": math.isfinite(cu), "pause": d,
                            "recovery": d + cu, "total": t + cu}

    # cold start
    fetch = 0.0 if local else K * c["event_bytes"] / B
    d = c["t_delete"] + dst["t_start"] + fetch + K * dst["r"]
    cu = catch_up(d, lam, p)
    est["rebuild"] = {"feasible": math.isfinite(cu), "pause": d,
                      "recovery": d + cu, "total": d + cu}

    # sharing
    src_local = x["src"] == "cloud"
    p_src = c["p_redis_local"] + (0 if src_local else
            c["redis_round_trips"] * rtt + c["rolling_window"] * v / B)
    share_src_ok = lam * p_src < 1
    read = 0.0 if local else c["rolling_window"] * v / B
    p_r = c["p_redis_local"] + c["redis_round_trips"] * link_rtt + read
    d = c["handover_s"] + 2 * rtt
    cu = catch_up(d, lam, p_r)
    est["share"] = {"feasible": math.isfinite(cu) and share_src_ok, "pause": d,
                    "recovery": d + cu, "total": dst["t_start"] + d + cu}

    return est


def choose(est: dict) -> str:
    ok = {s: e for s, e in est.items() if e["feasible"] and e["recovery"] <= RECOVERY_BOUND_S}
    if ok:
        return min(ok, key=lambda s: ok[s]["total"])
    feasible = {s: e for s, e in est.items() if e["feasible"]}
    if feasible:
        return min(feasible, key=lambda s: feasible[s]["recovery"])
    return "rebuild"


# ---------------------------------------------------------------- strategies

def replicate(ctx: dict) -> None:
    src_dns = ctx["src_cluster_url"]
    create(ctx["dst_cfg"], ctx["dst"], ctx["name"], {
        "RUN_ID": ctx["run_id"],
        "SOURCE_DT_URL_DELTA": f"{src_dns}/delta",
        "SOURCE_DT_URL_DISCONNECT": f"{src_dns}/disconnect",
    })
    emit("dst_created", ctx["run_id"])
    p = wait_ready(ctx["dst_app"], ctx["ns"])  # Ready only after pre-copy, Flask starts after it
    emit("dst_ready", ctx["run_id"], uid=p.metadata.uid)
    emit("handover_done", ctx["run_id"])
    ctx["delete_source"]()


def rebuild(ctx: dict) -> None:
    ctx["delete_source"]()
    create(ctx["dst_cfg"], ctx["dst"], ctx["name"], {"RUN_ID": ctx["run_id"], "STARTUP_MQTT_CONNECTION": 0})
    emit("dst_created", ctx["run_id"])
    p = wait_ready(ctx["dst_app"], ctx["ns"])
    emit("dst_ready", ctx["run_id"], uid=p.metadata.uid)
    dst_url = nodeport_url(ctx["dst_cfg"], ctx["ns"])
    call("POST", f"{dst_url}/rebuild", req_timeout=TIMEOUT_S)
    emit("rebuild_queued", ctx["run_id"])
    call("POST", f"{dst_url}/reconnect")
    emit("handover_done", ctx["run_id"])


def share(ctx: dict) -> None:
    create(ctx["dst_cfg"], ctx["dst"], ctx["name"], {"RUN_ID": ctx["run_id"], "STARTUP_MQTT_CONNECTION": 0})
    emit("dst_created", ctx["run_id"])
    p = wait_ready(ctx["dst_app"], ctx["ns"])
    emit("dst_ready", ctx["run_id"], uid=p.metadata.uid)
    dst_url = nodeport_url(ctx["dst_cfg"], ctx["ns"])
    call("POST", f"{ctx['src_url']}/disconnect")
    wait_json(f"{ctx['src_url']}/pending", "pending", 0)  # source has written everything to Redis
    emit("src_drained", ctx["run_id"])
    call("POST", f"{dst_url}/reconnect")
    emit("handover_done", ctx["run_id"])
    ctx["delete_source"]()


STRATEGIES = {"replicate": replicate, "rebuild": rebuild, "share": share}


# ---------------------------------------------------------------- handlers

@kopf.on.create("cyberphysicalapplications")
def create_fn(spec, name, patch, **_):
    spec = copy.deepcopy(dict(spec))
    aff = spec["requirements"]["preferredAffinity"]
    create(deployment_of(spec, aff)["configs"], aff, name, {"RUN_ID": spec.get("runId", "")})
    patch.metadata.annotations["current-affinity"] = aff


@kopf.on.field("cyberphysicalapplications", field="spec.migrate", old=False, new=True)
def migrate_fn(spec, meta, name, patch, **_):
    spec = copy.deepcopy(dict(spec))
    run_id = spec.get("runId", "")
    src = meta["annotations"]["current-affinity"]
    dst = next(d["affinity"] for d in spec["deployments"] if d["affinity"] != src)
    src_cfg = deployment_of(spec, src)["configs"]
    dst_cfg = deployment_of(spec, dst)["configs"]
    src_app, ns = app_and_ns(src_cfg)
    dst_app, _ = app_and_ns(dst_cfg)
    src_ip, dst_ip = node_ip(src), node_ip(dst)
    src_url = nodeport_url(src_cfg, ns)

    emit("trigger", run_id, src=src, dst=dst)
    for ip in (src_ip, dst_ip):
        agent(ip, "/snapshot", run_id, label="trigger")
        agent(ip, "/mem/start", run_id)

    t0 = time.perf_counter()
    inputs = measure(src_url, src, dst)
    est = estimate(inputs)
    decision = choose(est)
    decision_us = (time.perf_counter() - t0) * 1e6
    emit("inputs", run_id, **inputs)
    emit("estimates", run_id, estimates=est)
    emit("decision", run_id, chosen=decision, decision_us=decision_us)

    forced = spec.get("strategy", "auto")
    strategy = decision if forced == "auto" else forced
    emit("strategy", run_id, executed=strategy, forced=forced != "auto")

    src_uid = pod_of(src_app, ns).metadata.uid

    def delete_source():
        agent(src_ip, "/snapshot", run_id, label="pre_delete", uids=src_uid)
        delete(src_cfg)
        wait_gone(src_app, ns)
        emit("src_deleted", run_id)

    STRATEGIES[strategy]({
        "run_id": run_id, "name": name, "ns": ns,
        "src": src, "dst": dst, "src_cfg": src_cfg, "dst_cfg": dst_cfg,
        "src_app": src_app, "dst_app": dst_app,
        "src_url": src_url, "src_cluster_url": cluster_url(src_cfg, ns),
        "delete_source": delete_source,
    })

    dst_uid = pod_of(dst_app, ns).metadata.uid
    for ip in (src_ip, dst_ip):
        agent(ip, "/snapshot", run_id, label="end", uids=dst_uid)
        agent(ip, "/mem/stop", run_id)
    emit("migration_ops_end", run_id)
    patch.metadata.annotations["current-affinity"] = dst