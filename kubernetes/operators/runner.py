"""Experiment runner for the ISCC migration middleware.

Runs the full grid (state size x bandwidth x update rate x direction x strategy x repetition)
in a shuffled order. It is resumable: finished runs are listed in runs/done.txt and skipped.

Each run:
  health check -> set link (tc) -> reset -> config -> CPA -> preload -> PT -> warm-up
  -> trigger -> wait for the operator -> wait for recovery -> stop PT -> collect files
"""

import itertools
import json
import random
import shlex
import subprocess
import sys
import time
from pathlib import Path

import paho.mqtt.client as mqtt
import requests
import yaml

CFG = yaml.safe_load(open(sys.argv[1] if len(sys.argv) > 1 else "runner.yaml"))
RUNS = Path(CFG["results_dir"])
RUNS.mkdir(parents=True, exist_ok=True)
DONE = RUNS / "done.txt"
FAILED = RUNS / "failed.txt"
SKIPPED = RUNS / "skipped.txt"


class Skip(Exception):
    pass


# ---------------------------------------------------------------- shell helpers


def sh(cmd: str, check: bool = True, stdin: str | None = None) -> str:
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, input=stdin)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd}\n{r.stderr}")
    return r.stdout


def ssh(ip: str, cmd: str, check: bool = True) -> str:
    return sh(f"ssh -o BatchMode=yes {CFG['ssh_user']}@{ip} {shlex.quote(cmd)}", check)


def kubectl(args: str, check: bool = True, stdin: str | None = None) -> str:
    return sh(f"kubectl {args}", check, stdin)


def post_retry(
    url: str, params: dict | None = None, timeout: float = 1800, wait_s: float = 60
) -> requests.Response:
    deadline = time.time() + wait_s
    while True:
        try:
            r = requests.post(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.ConnectionError:
            if time.time() > deadline:
                raise
            time.sleep(1)


# ---------------------------------------------------------------- grid


def grid() -> list[dict]:
    g = CFG["grid"]
    runs = []
    for size, bw, rate, direction, strategy, rep in itertools.product(
        g["state_mb"],
        g["bw_mbps"],
        g["rate"],
        g["directions"],
        g["strategies"],
        range(g["reps"]),
    ):
        src, dst = direction.split("->")
        run_id = f"s{size}_b{bw}_r{rate}_{src}-{dst}_{strategy}_{rep}"
        runs.append(
            dict(
                run_id=run_id,
                state_mb=size,
                bw_mbps=bw,
                rate=rate,
                src=src,
                dst=dst,
                strategy=strategy,
                rep=rep,
            )
        )
    random.Random(CFG["seed"]).shuffle(runs)
    return runs


# ---------------------------------------------------------------- health check


def chrony_offset_ms(ip: str) -> float:
    out = ssh(ip, "chronyc tracking")
    line = next(l for l in out.splitlines() if l.startswith("System time"))
    return float(line.split(":")[1].split()[0]) * 1000


def health() -> dict:
    nodes = kubectl("get nodes --no-headers")
    if any(" Ready " not in l for l in nodes.strip().splitlines()):
        raise RuntimeError(f"node not ready:\n{nodes}")
    for svc in CFG["services"]:
        kubectl(f"wait --for=condition=Ready pod -l app={svc} --timeout=60s")
    offsets = {name: chrony_offset_ms(ip) for name, ip in CFG["nodes"].items()}
    if max(abs(o) for o in offsets.values()) > CFG["max_clock_offset_ms"]:
        raise RuntimeError(f"clock offset too large: {offsets}")
    return offsets


# ---------------------------------------------------------------- link emulation


def set_link(bw_mbps: float, rtt_ms: float) -> None:
    """Shape only the traffic between the two workers: half the RTT on each side."""
    edge, cloud = CFG["nodes"]["edge"], CFG["nodes"]["cloud"]
    iface = CFG["iface"]
    for ip, other in ((edge, cloud), (cloud, edge)):
        ssh(ip, f"sudo tc qdisc del dev {iface} root", check=False)
        ssh(
            ip,
            f"sudo tc qdisc add dev {iface} root handle 1: prio bands 3 "
            f"priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0",
        )
        limit = max(10, round(bw_mbps * 1e6 * 0.05 / (8 * 1500)))
        ssh(
            ip,
            f"sudo tc qdisc add dev {iface} parent 1:3 handle 30: "
            f"netem delay {rtt_ms / 2}ms rate {bw_mbps}mbit limit {limit}",
        )
        ssh(
            ip,
            f"sudo tc filter add dev {iface} parent 1: protocol ip prio 1 "
            f"u32 match ip dst {other}/32 flowid 1:3",
        )
    json.dump({"bw_mbps": bw_mbps, "rtt_ms": rtt_ms}, open(CFG["link_file"], "w"))


# ---------------------------------------------------------------- reset


def pods_of(app: str) -> list[str]:
    return kubectl(f"get pods -l app={app} -o name", check=False).split()


def reset() -> None:
    for zone in ("edge", "cloud"):
        ssh(
            CFG["nodes"][zone], "sudo find /events -name '*.jsonl' -delete", check=False
        )
    kubectl(f"delete cpa {CFG['cpa_name']} --ignore-not-found --wait=true")
    deadline = time.time() + 300
    while any(pods_of(a) for a in CFG["dt_apps"].values()):
        if time.time() > deadline:
            raise RuntimeError("DT pods did not terminate")
        time.sleep(1)
    kubectl("exec deploy/redis -- redis-cli FLUSHALL")
    m = CFG["mongo"]
    kubectl(
        f"exec deploy/mongo -- mongosh -u {m['user']} -p {m['password']} --quiet "
        f"--eval 'db.getSiblingDB(\"{m['db']}\").{m['collection']}.drop()'"
    )
    kubectl(
        f"exec deploy/mongo -- mongosh -u {m['user']} -p {m['password']} --quiet "
        f"--eval 'db.getSiblingDB(\"{m['db']}\").{m['collection']}"
        f'.createIndex({{"payload.seq_id": 1}})\''
    )
    clear_mqtt_session()


def clear_mqtt_session() -> None:
    """Connect once with a clean session, so the broker drops the DT's queue from the last run."""
    c = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=CFG["mqtt"]["client_id"],
        clean_session=True,
    )
    c.connect(CFG["mqtt"]["host"], CFG["mqtt"]["port"])
    c.loop_start()
    time.sleep(0.5)
    c.disconnect()
    c.loop_stop()


# ---------------------------------------------------------------- config and CPA


def apply_dt_config(run: dict) -> None:
    conf = yaml.safe_load(open(CFG["dt_config_template"]))
    conf["state"]["max_size_mb"] = run["state_mb"]
    conf["state"]["backend"] = "redis" if run["strategy"] == "share" else "local"
    conf["connections"]["mongodb"]["use"] = True
    conf["odte"]["expected_msg_sec"] = run["rate"]
    cm = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "dt-confs", "namespace": "default"},
        "data": {"config.yaml": yaml.safe_dump(conf)},
    }
    kubectl("apply -f -", stdin=yaml.safe_dump(cm))


def create_cpa(run: dict) -> None:
    cpa = yaml.safe_load(open(CFG["cpa_template"]))
    cpa["metadata"]["name"] = CFG["cpa_name"]
    cpa["spec"]["migrate"] = False
    cpa["spec"]["runId"] = run["run_id"]
    cpa["spec"]["strategy"] = run["strategy"]
    cpa["spec"]["requirements"]["preferredAffinity"] = run["src"]
    kubectl("apply -f -", stdin=yaml.safe_dump(cpa))


def wait_dt_ready(zone: str) -> str:
    app = CFG["dt_apps"][zone]
    deadline = time.time() + 300
    while not pods_of(app):
        if time.time() > deadline:
            raise RuntimeError(f"no pod for {app}")
        time.sleep(1)
    kubectl(f"wait --for=condition=Ready pod -l app={app} --timeout=300s")
    port = kubectl(
        f"get svc {CFG['dt_services'][zone]} -o jsonpath='{{.spec.ports[0].nodePort}}'"
    )
    return f"http://{CFG['node_host']}:{port}"


# ---------------------------------------------------------------- PT


def start_pt(run: dict, start_seq: int, out_dir: Path) -> subprocess.Popen:
    cmd = CFG["pt_command"].format(
        rate=run["rate"],
        start_seq=start_seq,
        run_id=run["run_id"],
        events=out_dir / "pt.jsonl",
    )
    return subprocess.Popen(
        shlex.split(cmd),
        stdout=subprocess.DEVNULL,
        stderr=open(out_dir / "pt.err", "w"),
    )


# ---------------------------------------------------------------- waiting


def wait_operator_end(run_id: str, timeout: float) -> None:
    deadline = time.time() + timeout
    path = Path(CFG["operator_events"])
    while time.time() < deadline:
        if path.exists():
            for line in open(path):
                e = json.loads(line)
                if e.get("run_id") == run_id and e["event"] == "migration_ops_end":
                    return
        time.sleep(1)
    raise RuntimeError("operator did not finish")


def wait_recovered(url: str, timeout: float) -> bool:
    """Run control only: the paper's recovery time is computed from the logs afterwards."""
    deadline = time.time() + timeout
    ok_since = None
    while time.time() < deadline:
        try:
            odte = float(requests.get(f"{url}/odte", timeout=2).text.split()[1])
        except (requests.RequestException, ValueError, IndexError):
            odte = 0.0
        if odte >= CFG["odte_threshold"]:
            ok_since = ok_since or time.time()
            if time.time() - ok_since >= CFG["stable_s"]:
                return True
        else:
            ok_since = None
        time.sleep(1)
    return False


def baseline_ok(url: str) -> float:
    try:
        return float(requests.get(f"{url}/odte", timeout=2).text.split()[1])
    except (requests.RequestException, ValueError, IndexError):
        return 0.0


# ---------------------------------------------------------------- collection


def collect(run: dict, out: Path) -> None:
    for zone, ip in CFG["nodes"].items():
        if zone not in ("edge", "cloud"):
            continue
        d = out / zone
        d.mkdir(exist_ok=True)
        sh(
            f"scp -q -o BatchMode=yes '{CFG['ssh_user']}@{ip}:/events/*.jsonl' {d}/",
            check=False,
        )
        ssh(ip, "sudo find /events -name '*.jsonl' -delete", check=False)
    with open(out / "operator.jsonl", "w") as f:
        for line in open(CFG["operator_events"]):
            if json.loads(line).get("run_id") == run["run_id"]:
                f.write(line)


# ---------------------------------------------------------------- one run


def run_one(run: dict) -> None:
    out = RUNS / run["run_id"]
    out.mkdir(exist_ok=True)
    meta = {**run, "started": time.time()}

    meta["clock_offsets_ms"] = health()
    set_link(run["bw_mbps"], CFG["rtt_ms"])
    reset()
    apply_dt_config(run)
    create_cpa(run)
    src_url = wait_dt_ready(run["src"])

    k = round(run["state_mb"] * 1024 / CFG["entry_kb"])
    post_retry(f"{src_url}/preload", params={"n": k})

    pt = start_pt(run, start_seq=k, out_dir=out)
    try:
        for zone in ("edge", "cloud"):
            requests.post(
                f"http://{CFG['nodes'][zone]}:9200/snapshot",
                params={"run_id": run["run_id"], "label": "baseline"},
                timeout=2,
            )
        time.sleep(CFG["warmup_s"])
        meta["baseline_odte"] = baseline_ok(src_url)
        if meta["baseline_odte"] < CFG["odte_threshold"]:
            collect(run, out)
            json.dump(meta, open(out / "run.json", "w"), indent=2)
            raise Skip(f"baseline ODTE {meta['baseline_odte']:.2f}")
        kubectl(
            f"patch cpa {CFG['cpa_name']} --type merge -p '{{\"spec\":{{\"migrate\":true}}}}'"
        )
        meta["triggered"] = time.time()
        wait_operator_end(run["run_id"], timeout=1800)
        dst_url = wait_dt_ready(run["dst"])
        meta["recovered_online"] = wait_recovered(
            dst_url, timeout=CFG["recovery_timeout_s"]
        )
        time.sleep(CFG["tail_s"])
    finally:
        pt.terminate()
        pt.wait(timeout=10)

    collect(run, out)
    meta["finished"] = time.time()
    json.dump(meta, open(out / "run.json", "w"), indent=2)


# ---------------------------------------------------------------- main loop


def main() -> None:
    done = set(DONE.read_text().split()) if DONE.exists() else set()
    todo = [r for r in grid() if r["run_id"] not in done]
    print(f"{len(done)} done, {len(todo)} to go")

    for attempt in range(CFG["max_attempts"]):
        failed = []
        for i, run in enumerate(todo, 1):
            print(f"[{attempt}:{i}/{len(todo)}] {run['run_id']}", flush=True)
            try:
                run_one(run)
                with open(DONE, "a") as f:
                    f.write(run["run_id"] + "\n")
            except Skip as e:
                print(f"  skipped: {e}", flush=True)
                with open(SKIPPED, "a") as f:
                    f.write(f"{run['run_id']}\t{e}\n")
                with open(DONE, "a") as f:
                    f.write(run["run_id"] + "\n")
            except Exception as e:
                print(f"  failed: {e}", flush=True)
                with open(FAILED, "a") as f:
                    f.write(f"{run['run_id']}\t{attempt}\t{e!r}\n")
                failed.append(run)
        if not failed:
            break
        todo = failed


if __name__ == "__main__":
    main()
