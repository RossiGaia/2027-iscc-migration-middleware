import glob, json, os, sys, statistics as st
root=sys.argv[1]
def jl(p):
    return [json.loads(l) for l in open(p) if l.strip()]
print(f"{'run':42s} pre_p95lat  pre_rate  src_p_ms dst_p_ms  t_start  replay_n replay_s  r0_MBps  srcdel_s")
for d in sorted(glob.glob(root+'/*/')):
    if not os.path.exists(d+'run.json'): continue
    m=json.load(open(d+'run.json')); rid=m['run_id']
    op={}
    for e in jl(d+'operator.jsonl'):
        op.setdefault(e['event'],e)
    t0=op['trigger']['ts']/1e9
    pods={}
    for f in glob.glob(d+'*/dt-*.jsonl'):
        evs=[e for e in jl(f) if e.get('run_id')==rid]
        if evs: pods[f]=evs
    # source = pod with updates before trigger
    src=dst=None
    for f,evs in pods.items():
        ps=[e for e in evs if e['event']=='process_start']
        if ps and ps[0]['ts']/1e9 < t0: src=evs
        else: dst=evs
    def lat(evs,a,b):
        return [e['apply_ts']-e['creation_ts'] for e in evs if e['event']=='update' and e.get('origin')=='live' and a<=e['apply_ts']<b]
    pre=lat(src,t0-30,t0) if src else []
    p95=sorted(pre)[int(0.95*len(pre))-1] if pre else float('nan')
    rate=len(pre)/30
    def pt(evs):
        x=[e['processing_time_s'] for e in evs if e['event']=='update' and e.get('origin')=='live']
        return st.median(x)*1000 if x else float('nan')
    tstart=float('nan')
    if dst and 'dst_created' in op:
        ps=[e for e in dst if e['event']=='process_start']
        if ps: tstart=ps[0]['ts']/1e9-op['dst_created']['ts']/1e9
    rep=[e['apply_ts'] for e in (dst or []) if e['event']=='update' and e.get('origin')=='replay']
    reps=(max(rep)-min(rep)) if len(rep)>1 else float('nan')
    r0=[e for e in (dst or []) if e['event']=='precopy_round' and e['round']==0]
    r0b=r0[0]['bytes']/r0[0]['fetch_s']/1e6 if r0 else float('nan')
    sd=op['src_deleted']['ts']/1e9-t0 if 'src_deleted' in op else float('nan')
    print(f"{rid:42s} {p95:9.3f} {rate:8.1f} {pt(src or []):9.2f} {pt(dst or []):8.2f} {tstart:8.2f} {len(rep):8d} {reps:8.2f} {r0b:8.2f} {sd:8.2f}")