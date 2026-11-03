"""Per-run metrics for the ISCC pilot / campaign.

Usage: python3 analyze.py <results_dir> [out.csv]

Definitions (all relative to the operator's trigger):
  total_s     trigger -> max(migration_ops_end, recovery end)
  ops_s       trigger -> migration_ops_end (orchestration only)
  pause_s     largest gap between first-application times of consecutive PT seqs
              created after the trigger, minus the update period 1/rate
  recovery_s  trigger -> apply time of the last post-trigger update whose latency
              (apply - creation) exceeds DEADLINE; 0 if none
  lost        PT seqs created after trigger (and >= TAIL before PT stop) never applied
  dup         live applications of the same seq beyond the first
  net_MB      cross-worker traffic: sum over both workers of tx delta (trigger -> end)
  cpu_core_s  sum over both workers of node CPU delta (trigger -> end)
  mem_dst_MB  peak memory of the destination DT pod
"""
import csv, glob, json, os, sys
from collections import defaultdict

DEADLINE = 1.0
TAIL = 5.0


def jl(path):
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return out


def analyze(run_dir):
    meta = json.load(open(os.path.join(run_dir, "run.json")))
    rid = meta["run_id"]
    # keep only operator events from this run, not from an older run with the same id
    op = [e for e in jl(os.path.join(run_dir, "operator.jsonl"))
          if e.get("run_id") == rid and e["ts"] / 1e9 >= meta["started"]]
    ev = {}
    for e in op:
        ev.setdefault(e["event"], e)
    if "trigger" not in ev or "migration_ops_end" not in ev:
        return None
    t0 = ev["trigger"]["ts"] / 1e9
    t_ops = ev["migration_ops_end"]["ts"] / 1e9
    dst_uid = ev.get("dst_ready", {}).get("uid")

    pt = [e for e in jl(os.path.join(run_dir, "pt.jsonl")) if e.get("run_id") == rid]
    pt_after = {e["seq"]: e["creation_ts"] for e in pt if e["creation_ts"] >= t0}
    pt_end = max((e["creation_ts"] for e in pt), default=t0)

    first_apply = {}
    lat = {}
    napply = defaultdict(int)
    dt_files = glob.glob(os.path.join(run_dir, "*", "dt-*.jsonl"))
    for f in dt_files:
        for e in jl(f):
            if e.get("event") != "update" or e.get("run_id") != rid:
                continue
            s = e["seq"]
            if e.get("origin") == "live":
                napply[s] += 1
            if s not in first_apply or e["apply_ts"] < first_apply[s]:
                first_apply[s] = e["apply_ts"]
                lat[s] = e["apply_ts"] - e["creation_ts"]

    seqs = sorted(s for s in pt_after if s in first_apply)
    applies = sorted(first_apply[s] for s in seqs)
    # include last pre-trigger application so a pause starting at the trigger counts
    pre = [first_apply[s] for s in first_apply if first_apply[s] < t0]
    if pre:
        applies = [max(pre)] + applies
    gap = max((b - a for a, b in zip(applies, applies[1:])), default=float("nan"))
    pause = max(0.0, gap - 1.0 / meta["rate"])  # excess over the normal update period

    late = [first_apply[s] for s in seqs if lat[s] > DEADLINE]
    rec_end = max(late) if late else t0
    recovery = rec_end - t0
    total = max(t_ops, rec_end) - t0

    lost = sum(1 for s, c in pt_after.items() if c <= pt_end - TAIL and s not in first_apply)
    dup = sum(n - 1 for n in napply.values() if n > 1)

    # counters
    net = cpu = 0.0
    mem_dst = None
    for f in glob.glob(os.path.join(run_dir, "*", "counters-*.jsonl")):
        rows = [e for e in jl(f) if e.get("run_id") == rid]
        c = {e["label"]: e for e in rows if e["event"] == "counters"}
        if "trigger" in c and "end" in c:
            net += (c["end"]["tx_bytes"] - c["trigger"]["tx_bytes"]) / 1e6
            cpu += (c["end"]["node_cpu_usec"] - c["trigger"]["node_cpu_usec"]) / 1e6
        for e in rows:
            if e["event"] == "mem_peak" and dst_uid and dst_uid in e["peaks_bytes"]:
                mem_dst = e["peaks_bytes"][dst_uid] / 2**20

    est = ev.get("estimates", {}).get("estimates", {})
    strat = meta["strategy"]
    se = est.get(strat, {})
    return {
        "run_id": rid, "state_mb": meta["state_mb"], "bw_mbps": meta["bw_mbps"],
        "rate": meta["rate"], "dir": f'{meta["src"]}->{meta["dst"]}', "strategy": strat,
        "total_s": round(total, 3), "ops_s": round(t_ops - t0, 3),
        "pause_s": round(pause, 3), "recovery_s": round(recovery, 3),
        "lost": lost, "dup": dup,
        "net_MB": round(net, 1), "cpu_core_s": round(cpu, 1),
        "mem_dst_MB": None if mem_dst is None else round(mem_dst, 1),
        "est_total_s": None if "total" not in se else round(se["total"], 2),
        "est_recovery_s": None if "recovery" not in se else round(se["recovery"], 2),
        "decision": ev.get("decision", {}).get("chosen"),
        "recovered_online": meta.get("recovered_online"),
        "run_wall_s": round(meta["finished"] - meta["started"], 1),
    }


def main():
    root = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else "metrics.csv"
    rows = []
    for d in sorted(glob.glob(os.path.join(root, "*"))):
        if os.path.isfile(os.path.join(d, "run.json")):
            r = analyze(d)
            if r:
                rows.append(r)
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"{len(rows)} runs -> {out}")


if __name__ == "__main__":
    main()