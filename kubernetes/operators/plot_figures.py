"""Figures for the ISCC paper. Usage: python3 plot_figures.py metrics-campaign.csv calibration-campaign.txt figures/"""
import sys, os
import numpy as np, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

MC, CAL, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(OUT, exist_ok=True)
X = 10.0          # recovery bound (s)
LOST_TOL = 2      # updates lost at shutdown tolerated

INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
COL = {"replicate": "#2a78d6", "rebuild": "#eb6834", "share": "#1baf7a"}
TINT = {"replicate": "#d4e4f7", "rebuild": "#fbdccf", "share": "#d0efe3"}
MARK = {"replicate": "o", "rebuild": "s", "share": "^"}
LS = {"replicate": "-", "rebuild": "--", "share": ":"}
NAME = {"replicate": "Pre-copy", "rebuild": "Rebuild", "share": "Sharing"}
ORDER = ["replicate", "rebuild", "share"]

plt.rcParams.update({
    "font.family": "serif", "font.size": 8, "axes.labelsize": 8, "legend.fontsize": 7,
    "xtick.labelsize": 7, "ytick.labelsize": 7, "axes.edgecolor": MUTED, "axes.labelcolor": INK2,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.6, "axes.spines.top": False, "axes.spines.right": False,
    "pdf.fonttype": 42, "lines.linewidth": 1.4,
})

d = pd.read_csv(MC)
c = pd.read_csv(CAL, sep=r"\s+").rename(columns={"run": "run_id"})
d = d.merge(c[["run_id", "pre_p95lat"]], on="run_id")
key = ["state_mb", "bw_mbps", "rate", "dir"]
g = d.groupby(key + ["strategy"]).agg(
    total=("total_s", "median"), rec=("recovery_s", "median"), lost=("lost", "median"),
    pre=("pre_p95lat", "median"), est=("est_total_s", "median")).reset_index()
g["valid"] = (g.pre < 1.0) & (g.lost <= LOST_TOL)
g["ok"] = g.valid & (g.rec <= X)

# ---------------------------------------------------------------- oracle and policies
def oracle(s):
    ok = s[s.ok]
    if len(ok):
        return ok.loc[ok.total.idxmin(), "strategy"]
    v = s[s.valid]
    return v.loc[v.rec.idxmin(), "strategy"] if len(v) else None

orc = g.groupby(key).apply(oracle, include_groups=False).rename("oracle").reset_index()
dec = d.groupby(key).decision.agg(lambda x: x.mode()[0]).reset_index()
sc = orc.merge(dec, on=key)
T = g.set_index(key + ["strategy"])

def cost(k, s):
    r = T.loc[k + (s,)]
    return r.total if r.ok else np.inf

pol = {"Middleware": [], "Always pre-copy": [], "Always rebuild": [], "Always sharing": [], "Random": []}
wts = {p: [] for p in pol}
for _, r in sc.iterrows():
    k = tuple(r[key])
    o = T.loc[k + (r.oracle,)].total
    for p, s in [("Middleware", r.decision), ("Always pre-copy", "replicate"),
                 ("Always rebuild", "rebuild"), ("Always sharing", "share")]:
        pol[p].append(cost(k, s) - o); wts[p].append(1.0)
    for s in ORDER:
        pol["Random"].append(cost(k, s) - o); wts["Random"].append(1 / 3)

# ---------------------------------------------------------------- Fig. 1: crossover
fig, axes = plt.subplots(2, 3, figsize=(7.16, 3.4), sharex=True)
sub = g[(g.bw_mbps == 100) & (g.dir == "cloud->edge")]
for j, rate in enumerate([1, 10, 50]):
    for i, (col, lab) in enumerate([("total", "Total migration time (s)"), ("rec", "Recovery time (s)")]):
        ax = axes[i, j]
        for s in ORDER:
            q = sub[(sub.rate == rate) & (sub.strategy == s)].sort_values("state_mb")
            y = q[col]
            ax.plot(q.state_mb, y, color=COL[s], ls=LS[s], zorder=2)
            v, nv = q[q.valid], q[~q.valid]
            ax.scatter(v.state_mb, v[col], marker=MARK[s], s=22, color=COL[s],
                       edgecolor="white", linewidth=1, zorder=3)
            ax.scatter(nv.state_mb, nv[col], marker="x", s=26, color=COL[s], zorder=3)
        if i == 0:
            ax.set_yscale("log")
            ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
            ax.yaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
            ax.set_yticks([3, 5, 10, 20, 50])
        else:
            ax.set_ylim(-1, 32)
        if i == 1:
            ax.axhline(X, color=INK2, lw=0.8, ls=(0, (4, 3)))
            ax.set_xlabel("State size (MB)")
        else:
            ax.set_title(f"{rate} update/s" if rate == 1 else f"{rate} updates/s", color=INK, fontsize=8)
        if j == 0:
            ax.set_ylabel(lab)
        ax.set_xticks([10, 100, 250])
handles = [Line2D([], [], color=COL[s], ls=LS[s], marker=MARK[s], markeredgecolor="white", label=NAME[s]) for s in ORDER]
handles += [Line2D([], [], color=MUTED, marker="x", ls="none", label="Updates lost"),
            Line2D([], [], color=INK2, ls=(0, (4, 3)), lw=0.8, label=f"Recovery bound ({X:.0f} s)")]
fig.legend(handles=handles, loc="upper center", ncol=5, frameon=False, bbox_to_anchor=(0.5, 1.02))
fig.tight_layout(rect=(0, 0, 1, 0.94))
fig.savefig(f"{OUT}/fig_crossover.pdf"); plt.close(fig)

# ---------------------------------------------------------------- Fig. 2: winners map
rows = [(r, dr) for r in [1, 10, 50] for dr in ["cloud->edge", "edge->cloud"]]
cols = [(s, b) for s in [10, 100, 250] for b in [25, 100, 500]]
fig, ax = plt.subplots(figsize=(3.5, 2.3))
abbr = {"replicate": "P", "rebuild": "R", "share": "S"}
for i, (rate, dr) in enumerate(rows):
    for j, (st, bw) in enumerate(cols):
        r = sc[(sc.state_mb == st) & (sc.bw_mbps == bw) & (sc.rate == rate) & (sc.dir == dr)].iloc[0]
        ax.add_patch(plt.Rectangle((j + 0.04, i + 0.04), 0.92, 0.92, color=TINT[r.oracle], lw=0))
        wrong = r.decision != r.oracle
        if wrong:
            ax.add_patch(plt.Rectangle((j + 0.06, i + 0.06), 0.88, 0.88, fill=False, ec=INK, lw=1.1))
        ax.text(j + 0.5, i + 0.52, abbr[r.decision], ha="center", va="center", fontsize=7.5,
                color=INK, fontweight="bold" if wrong else "normal")
ax.set_xlim(0, 9); ax.set_ylim(6, 0); ax.grid(False)
ax.set_xticks([j + 0.5 for j in range(9)], [str(b) for _, b in cols])
ax.set_yticks([i + 0.5 for i in range(6)], [f"{r} {'C→E' if dr=='cloud->edge' else 'E→C'}" for r, dr in rows])
for j in (3, 6):
    ax.axvline(j, color="white", lw=2.5)
for i in (2, 4):
    ax.axhline(i, color="white", lw=2.5)
for j, st in enumerate([10, 100, 250]):
    ax.text(3 * j + 1.5, -0.35, f"{st} MB", ha="center", va="bottom", fontsize=7, color=INK2)
ax.set_xlabel("Bandwidth (Mbps)")
ax.set_ylabel("Updates/s, direction")
ax.tick_params(length=0)
for sp in ax.spines.values():
    sp.set_visible(False)
leg = [Patch(color=TINT[s], label=f"Oracle: {NAME[s]}") for s in ORDER]
leg += [Patch(fill=False, ec=INK, lw=1.1, label="Middleware differs")]
ax.legend(handles=leg, loc="upper center", bbox_to_anchor=(0.45, -0.22), ncol=2, frameon=False)
fig.tight_layout()
fig.savefig(f"{OUT}/fig_winners.pdf"); plt.close(fig)

# ---------------------------------------------------------------- Fig. 3: predicted vs measured
fig, ax = plt.subplots(figsize=(3.5, 2.6))
q = g[np.isfinite(g.est) & g.valid]  # only runs where the strategy actually worked
for s in ORDER:
    z = q[q.strategy == s]
    ax.scatter(z.total, z.est, marker=MARK[s], s=18, color=COL[s], edgecolor="white", linewidth=0.8, label=NAME[s])
lim = [1, 400]
ax.plot(lim, lim, color=INK2, lw=0.8, ls=(0, (4, 3)), label="Predicted = measured")
ax.set_xscale("log"); ax.set_yscale("log"); ax.set_xlim(lim); ax.set_ylim(lim)
ax.set_xlabel("Measured total migration time (s)")
ax.set_ylabel("Predicted total migration time (s)")
ax.legend(frameon=False, loc="upper left")
fig.tight_layout()
fig.savefig(f"{OUT}/fig_predicted.pdf"); plt.close(fig)

# ---------------------------------------------------------------- Fig. 4: regret CDF
fig, ax = plt.subplots(figsize=(3.5, 2.4))
style = {"Middleware": (INK, "-", 2.0), "Always pre-copy": (COL["replicate"], "-", 1.2),
         "Always rebuild": (COL["rebuild"], "--", 1.2), "Always sharing": (COL["share"], ":", 1.4),
         "Random": (MUTED, "-.", 1.2)}
xs = np.linspace(0, 40, 801)
for p, v in pol.items():
    v, w = np.array(v), np.array(wts[p])
    y = [(w[v <= x]).sum() / w.sum() for x in xs]
    colr, ls, lw = style[p]
    ax.plot(xs, y, color=colr, ls=ls, lw=lw, label=p)
ax.set_xlim(0, 40); ax.set_ylim(0, 1.02)
ax.set_xlabel("Extra migration time vs. oracle (s)")
ax.set_ylabel("Fraction of scenarios")
ax.legend(frameon=False, loc="lower right")
fig.tight_layout()
fig.savefig(f"{OUT}/fig_regret.pdf"); plt.close(fig)

# ---------------------------------------------------------------- Table: policy comparison
rows = []
for p, v in pol.items():
    v, w = np.array(v), np.array(wts[p])
    fin = np.isfinite(v)
    rows.append({"policy": p,
                 "within_bound_pct": round(100 * w[fin].sum() / w.sum(), 1),
                 "zero_regret_pct": round(100 * w[v <= 0.5].sum() / w.sum(), 1),
                 "median_extra_s": round(float(np.median(v[fin])), 2),
                 "max_extra_s": round(float(v[fin].max()), 2)})
pd.DataFrame(rows).to_csv(f"{OUT}/table_policies.csv", index=False)
print(pd.DataFrame(rows).to_string(index=False))