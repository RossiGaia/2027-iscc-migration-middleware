import json, sys
run = sys.argv[1]
seqs = sorted(e["seq"] for f in sys.argv[2:] for l in open(f)
                if '"update"' in l and (e := json.loads(l))["run_id"] == run)
print("updates:", len(seqs), "dupes:", len(seqs) - len(set(seqs)),
        "gaps:", [b for a, b in zip(seqs, seqs[1:]) if b != a + 1][:10])