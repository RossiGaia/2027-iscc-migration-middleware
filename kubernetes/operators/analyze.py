"""Per-run metrics for the ISCC campaign.

Usage: python3 analyze.py <results_dir> [out.csv] [--delta SECONDS]

Age of information (AoI). At time t, the AoI of the DT is t minus the creation
time of the most recent update that any DT instance has applied. It grows
linearly between applications and drops when a fresher update is applied.
In normal operation it reaches about 1 / rate before each update, so the DT is
stale when its AoI exceeds the threshold 1 / rate + delta.

Definitions (times in the CSV are in seconds):
  pause_s       downtime: the longest interval between the first applications of
                two consecutive updates (by any instance), minus the normal
                interval between two updates (1 / rate)
  recovery_s    stale time: total time after the trigger in which the AoI of the
                DT exceeds the threshold
  peak_aoi_s    highest AoI after the trigger
  stale_end_s   time from the trigger until the AoI exceeds the threshold for the
                last time (0 if it never does)
  total_s       time from the trigger until both the migration operations have
                ended and the DT is no longer stale (max of ops_s and stale_end_s)
  ops_s         time from the trigger until the migration operations have ended
  lost          updates created after the trigger that no instance ever applies
                (the last TAIL_S seconds of the run are excluded)
  dup           live applications of an update beyond the first one
  margin_s      time the DT is observed after stale_end_s; near 0 means the run
                ended before the DT caught up
  net_MB        traffic sent by both workers between the trigger and the end
  cpu_core_s    CPU time used by both workers between the trigger and the end
  mem_dst_MB    peak memory of the target DT pod
"""
import argparse
import csv
import glob
import json
import os
from collections import defaultdict

TAIL_S = 5.0  # updates created in the last seconds of the run may still be in flight


def read_jsonl(path):
    events = []
    with open(path) as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return events


def age_of_information(applications, freshest_at_trigger, trigger, end, threshold):
    """Walk the AoI from the trigger to the end of the observation.

    applications: list of (apply_time, creation_time), sorted by apply_time.
    Returns (stale_time, peak_aoi, stale_end), with stale_end relative to the trigger.
    """
    stale_time = 0.0
    peak_aoi = 0.0
    stale_end = None
    freshest_creation = freshest_at_trigger   # creation time of the freshest applied update
    interval_start = trigger
    boundaries = [(t, c) for t, c in applications if trigger < t <= end] + [(end, None)]
    for apply_time, creation in boundaries:
        # between interval_start and apply_time the AoI is t - freshest_creation
        becomes_stale = freshest_creation + threshold
        if apply_time > becomes_stale:
            stale_time += apply_time - max(interval_start, becomes_stale)
            stale_end = apply_time - trigger
        peak_aoi = max(peak_aoi, apply_time - freshest_creation)
        if creation is not None:
            freshest_creation = max(freshest_creation, creation)
        interval_start = apply_time
    return stale_time, peak_aoi, (stale_end or 0.0)


def analyze(run_dir, delta):
    run = json.load(open(os.path.join(run_dir, "run.json")))
    run_id = run["run_id"]

    # --- operator events (ignore events of an older run with the same id)
    operator_events = {}
    for event in read_jsonl(os.path.join(run_dir, "operator.jsonl")):
        if event.get("run_id") == run_id and event["ts"] / 1e9 >= run["started"]:
            operator_events.setdefault(event["event"], event)
    if "trigger" not in operator_events or "migration_ops_end" not in operator_events:
        return None
    trigger = operator_events["trigger"]["ts"] / 1e9
    ops_end = operator_events["migration_ops_end"]["ts"] / 1e9
    target_pod_uid = operator_events.get("dst_ready", {}).get("uid")

    # --- updates created by the PT
    pt_events = [e for e in read_jsonl(os.path.join(run_dir, "pt.jsonl")) if e.get("run_id") == run_id]
    creation_of = {e["seq"]: e["creation_ts"] for e in pt_events}
    pt_end = max(creation_of.values(), default=trigger)
    seqs_after_trigger = sorted(s for s, c in creation_of.items() if trigger <= c <= pt_end - TAIL_S)

    # --- applications by the DT instances (preloaded entries are not PT updates)
    first_apply = {}               # seq -> earliest application by any instance
    applications = []              # (apply_time, creation_time) of every application
    live_count = defaultdict(int)  # seq -> number of live applications
    for path in glob.glob(os.path.join(run_dir, "*", "dt-*.jsonl")):
        for event in read_jsonl(path):
            if event.get("event") != "update" or event.get("run_id") != run_id:
                continue
            if event.get("origin") == "preload":
                continue
            seq, applied, created = event["seq"], event["apply_ts"], event["creation_ts"]
            applications.append((applied, created))
            if seq not in first_apply or applied < first_apply[seq]:
                first_apply[seq] = applied
            if event.get("origin", "live") == "live":
                live_count[seq] += 1
    applications.sort()

    # --- downtime: longest gap between first applications, minus the update period
    apply_times = sorted(first_apply[s] for s in seqs_after_trigger if s in first_apply)
    before_trigger = [t for t in first_apply.values() if t < trigger]
    if before_trigger:
        apply_times = [max(before_trigger)] + apply_times  # a gap starting at the trigger counts
    longest_gap = max((b - a for a, b in zip(apply_times, apply_times[1:])), default=float("nan"))
    downtime = max(0.0, longest_gap - 1.0 / run["rate"])

    # --- age of information, observed until the last update we expect to see applied
    applied_after_trigger = [first_apply[s] for s in seqs_after_trigger if s in first_apply]
    observation_end = max(applied_after_trigger, default=ops_end)
    freshest_at_trigger = max((c for t, c in applications if t <= trigger), default=trigger)
    threshold = 1.0 / run["rate"] + delta
    stale_time, peak_aoi, stale_end = age_of_information(
        applications, freshest_at_trigger, trigger, observation_end, threshold)

    total = max(ops_end - trigger, stale_end)
    margin = observation_end - trigger - stale_end
    lost = sum(1 for s in seqs_after_trigger if s not in first_apply)
    duplicates = sum(n - 1 for n in live_count.values() if n > 1)

    # --- resource counters
    network_mb = cpu_core_s = 0.0
    target_memory_mb = None
    for path in glob.glob(os.path.join(run_dir, "*", "counters-*.jsonl")):
        rows = [e for e in read_jsonl(path) if e.get("run_id") == run_id]
        snapshots = {e["label"]: e for e in rows if e["event"] == "counters"}
        if "trigger" in snapshots and "end" in snapshots:
            network_mb += (snapshots["end"]["tx_bytes"] - snapshots["trigger"]["tx_bytes"]) / 1e6
            cpu_core_s += (snapshots["end"]["node_cpu_usec"] - snapshots["trigger"]["node_cpu_usec"]) / 1e6
        for e in rows:
            if e["event"] == "mem_peak" and target_pod_uid and target_pod_uid in e["peaks_bytes"]:
                target_memory_mb = e["peaks_bytes"][target_pod_uid] / 2**20

    estimates = operator_events.get("estimates", {}).get("estimates", {}).get(run["strategy"], {})

    def rounded(value, digits=3):
        return None if value is None else round(value, digits)

    return {
        "run_id": run_id, "state_mb": run["state_mb"], "bw_mbps": run["bw_mbps"],
        "rate": run["rate"], "dir": f"{run['src']}->{run['dst']}", "strategy": run["strategy"],
        "total_s": rounded(total), "ops_s": rounded(ops_end - trigger),
        "pause_s": rounded(downtime), "recovery_s": rounded(stale_time),
        "peak_aoi_s": rounded(peak_aoi), "stale_end_s": rounded(stale_end),
        "lost": lost, "dup": duplicates, "margin_s": rounded(margin),
        "net_MB": round(network_mb, 1), "cpu_core_s": round(cpu_core_s, 1),
        "mem_dst_MB": rounded(target_memory_mb, 1),
        "est_total_s": rounded(estimates.get("total"), 2),
        "est_recovery_s": rounded(estimates.get("recovery"), 2),
        "decision": operator_events.get("decision", {}).get("chosen"),
        "recovered_online": run.get("recovered_online"),
        "run_wall_s": round(run["finished"] - run["started"], 1),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir")
    parser.add_argument("out_csv", nargs="?", default="metrics.csv")
    parser.add_argument("--delta", type=float, default=1.0,
                        help="tolerance in seconds added to 1/rate in the AoI threshold")
    args = parser.parse_args()
    rows = []
    for run_dir in sorted(glob.glob(os.path.join(args.results_dir, "*"))):
        if os.path.isfile(os.path.join(run_dir, "run.json")):
            row = analyze(run_dir, args.delta)
            if row:
                rows.append(row)
    with open(args.out_csv, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} runs, delta = {args.delta} s -> {args.out_csv}")


if __name__ == "__main__":
    main()