"""
Author:   Claude (for Mohan)
Created:  25/08/2026
Modified: 25/08/2026
Purpose:  Splits the Scoring & Ranking "Fund Utilization & Loan Activity" category
          into two: "Fund Utilization" (the original 4 F01/F03/F05-based metrics,
          renamed only - values untouched) and a new "Loan Portfolio" category (4
          metrics from the CLF-meeting loan data: Repayment Rate, Active Lending
          Turnover, Arrears Rate, Total XIRR). Each is percentile-scored against
          every OTHER CLF that actually has Loans data (546 of 1667) - not against
          the full state population. Funding Source Diversity (Fund Disbursement-
          based) was tried in this category and dropped: Fund Disbursement is
          statewide while Loans isn't, so a CLF/district with zero loan-scrape
          coverage could still show a nonzero "Loan Portfolio" score from that one
          metric alone - misleading given the category's name. It isn't scored
          anywhere for now; it's still visible as plain data on the Fund
          Disbursement subtab.

          Also computes a "Data Coverage" category - now a real, scored 7th
          category (previously shipped as an informational-only panel, then
          promoted to a scored category per explicit direction): one metric,
          "Data Sources Available" (count out of 11 - see DATA_SOURCES below),
          percentile-ranked against ALL 1667 CLFs (unlike the loan metrics, every
          CLF has a well-defined count here, even if it's 0, so there's no
          restricted peer population to worry about). The underlying per-source
          checklist (data_availability block) is still attached to every CLF's
          json too, for the human-readable breakdown the tracker shows alongside
          this category's score.

          Every category key is injected into EVERY CLF's json (metrics/score
          None where a CLF has no data), not just the ones with data -
          build_district_state_data.py's cat_scores_for() indexes categories by
          name without a .get() fallback, so every CLF needs the same set of
          category keys present. Categories are explicitly re-ordered into
          CATEGORY_ORDER on every CLF (Financial Health, Fund Utilization, Loan
          Portfolio, VRF Fund Health, Governance & Compliance, Welfare and
          Livelihood, Data Coverage) - dict insertion order drives display order
          throughout the JS, and naive pop()/assign() renaming does NOT preserve
          original position, so this is done as an explicit final rebuild rather
          than relying on incidental ordering.

          Run AFTER build_loan_tab_data.py (needs "loans"/"fund_disbursement"
          already injected into data/clfs/*.json) and BEFORE
          build_district_state_data.py (district/state aggregation reads the
          renamed/new category names, so they must exist in every CLF json first).

          Recomputes overall_score/overall_district_score for every CLF/quarter
          (mean across whichever categories that CLF has - now potentially 7, not
          5), and does a full statewide re-rank of overall_state_rank/
          overall_district_rank across all 1667 CLFs per quarter - a CLF's score
          changing shifts every OTHER CLF's rank too (crossing above/below them),
          not just its own, so a partial re-rank restricted to the CLFs that
          changed would be wrong.

          Also fixes overview.status_tier's "Neither" label to "Neither Model nor
          Registered" (a bare "Neither" reads as ambiguous on its own) while
          already touching every CLF json - see build_tracker_data.py for the
          durable source-side fix, applied here so it's live without a full rebuild.

Input:  data/clfs/*.json (read + rewritten)
Output: data/clfs/*.json (categories renamed/added/reordered, overall_score+ranks
        recomputed, data_availability block added, status_tier label fixed)
"""
import json
import time
from pathlib import Path

import pandas as pd

T0 = time.time()
def elapsed(): return f"{time.time()-T0:.1f}s"

BASE = "/Users/mohanrajagopal/Dropbox/Bihar Gates Team/3_Jeevika/6_LokOS_Analysis"
CLF_DIR = Path(f"{BASE}/3_Output/CLF Tracker/Scale-Up/data/clfs")

OLD_CAT = "Fund Utilization & Loan Activity"
NEW_CAT = "Fund Utilization"
LOAN_CAT = "Loan Portfolio"
DATA_CAT = "Data Coverage"

CATEGORY_ORDER = ["Financial Health", "Fund Utilization", "Loan Portfolio",
                   "VRF Fund Health", "Governance & Compliance", "Welfare and Livelihood",
                   "Data Coverage"]

METRIC_DEFS = [
    ("Repayment Rate", "repayment_rate", True),
    ("Active Lending Turnover", "active_lending_turnover", True),
    ("Arrears Rate", "arrears_rate", False),
    ("Total XIRR", "total_xirr", True),
]

DATA_SOURCES = [
    ("profile", "LokOS Profile Reports"),
    ("audit", "Odoo CLF Audit Reports"),
    ("f01", "Financial: Balance Sheet (F01)"),
    ("f03", "Financial: Receipts & Payments (F03)"),
    ("f05", "Financial: Credit Disbursement (F05)"),
    ("vrf", "VRF Monitoring & Management Portal"),
    ("vprp_ent", "VPRP: Entitlements"),
    ("vprp_pgsrd", "VPRP: PGSRD"),
    ("vprp_sdp", "VPRP: SDP"),
    ("fund_disbursement", "CLF-Meeting: Fund Disbursement"),
    ("loans", "CLF-Meeting: Loans"),
]


def inclusive_pctl_val(series, value):
    s = series.dropna()
    return round((s <= value).mean() * 100) if pd.notna(value) and len(s) else None


def rank_of_val(series, value, higher_is_better=True):
    s = series.dropna()
    if value is None or (isinstance(value, float) and pd.isna(value)) or len(s) == 0:
        return None
    better = (s > value).sum() if higher_is_better else (s < value).sum()
    return int(better) + 1


def cat_score(metrics, key="state_pctl"):
    valid = [m[1][key] for m in metrics if m[1] and m[1].get(key) is not None]
    return round(sum(valid) / len(valid)) if valid else None


def cat_scores_both(metrics):
    return {"score": cat_score(metrics, "state_pctl"), "district_score": cat_score(metrics, "district_pctl")}


def score_metric(pop_df, col, value, district, higher_is_better=True):
    # pop_df is the full 1667-row population, not pre-filtered per metric (unlike
    # build_tracker_data.py's own score_metric(), which receives an already-scoped
    # population per metric) - n_district/n_state must count only rows where `col`
    # is actually populated, not every row in pop_df, or they'd overstate the real
    # peer-group size (e.g. claiming "of 1,667 CLFs" for a metric only 546 have).
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    dist_pop = pop_df[pop_df["district"] == district]
    dp = inclusive_pctl_val(dist_pop[col], value)
    sp = inclusive_pctl_val(pop_df[col], value)
    dr = rank_of_val(dist_pop[col], value, higher_is_better)
    sr = rank_of_val(pop_df[col], value, higher_is_better)
    if not higher_is_better:
        dp = 100 - dp if dp is not None else None
        sp = 100 - sp if sp is not None else None
    return {"district_pctl": dp, "state_pctl": sp, "district_rank": dr, "state_rank": sr,
            "n_district": int(dist_pop[col].notna().sum()), "n_state": int(pop_df[col].notna().sum())}


def _rank_pop(pop_df, higher_is_better=True):
    # min-rank (best=1); tied peers share the best rank - matches build_tracker_data.py's
    # own _rank_pop() convention for CLF ranking.
    ranks = pop_df["value"].rank(method="min", ascending=not higher_is_better)
    dist_ranks = pop_df.groupby("district")["value"].rank(method="min", ascending=not higher_is_better)
    n_state = len(pop_df)
    dist_n = pop_df.groupby("district")["value"].transform("size")
    out = {}
    for mis, r, dr, dn in zip(pop_df["mis_id"], ranks, dist_ranks, dist_n):
        out[int(mis)] = {"state_rank": int(r), "district_rank": int(dr), "n_state": n_state, "n_district": int(dn)}
    return out


print(f"[{elapsed()}] Loading all CLF json...")
paths = list(CLF_DIR.glob("*.json"))
clfs = {int(p.stem): json.loads(p.read_text()) for p in paths}
print(f"[{elapsed()}] Loaded {len(clfs)} CLFs")

# ---- Step 0: fix status_tier's "Neither" label (bare "Neither" reads as
# ambiguous without saying neither WHAT) ----
for mis, d in clfs.items():
    if d.get("overview", {}).get("status_tier") == "Neither":
        d["overview"]["status_tier"] = "Neither Model nor Registered"

# ---- Step 1: rename the OLD category key everywhere (score/metrics untouched) ----
for mis, d in clfs.items():
    for label, bq in d.get("scoring", {}).get("by_quarter", {}).items():
        cats = bq.get("categories", {})
        if OLD_CAT in cats:
            cats[NEW_CAT] = cats.pop(OLD_CAT)

# ---- Step 2: raw metric population for Loan Portfolio, keyed only on Loans data
# (not Fund Disbursement - see module docstring for why Funding Source Diversity
# was dropped from this category). ----
rows = []
for mis, d in clfs.items():
    district = d["overview"]["district"]
    loans = d.get("loans", {})
    kpi = loans.get("kpi", {}) if loans.get("found") else {}
    total_arrears = kpi.get("total_arrears")
    total_current_demand = kpi.get("total_current_demand")
    arrears_rate = (100 * total_arrears / total_current_demand) if (total_current_demand and total_current_demand > 0) else None
    rows.append({
        "mis_id": mis, "district": district,
        "repayment_rate": kpi.get("repayment_rate"),
        "active_lending_turnover": kpi.get("active_lending_turnover"),
        "arrears_rate": arrears_rate,
        "total_xirr": kpi.get("total_xirr"),
    })
pop = pd.DataFrame(rows).set_index("mis_id", drop=False)

# ---- Step 3: inject Loan Portfolio into every quarter's categories dict for
# EVERY CLF (static/current-standing, not quarter-varying - same convention as
# VRF/Governance/Welfare, repeated identically across quarters). Metrics/score
# are None where a CLF has no Loans data - the key itself always exists. ----
n_with_any_metric = 0
for mis, d in clfs.items():
    row = pop.loc[mis]
    district = row["district"]
    metrics = []
    for label, col, higher_better in METRIC_DEFS:
        val = row[col]
        m = score_metric(pop, col, val, district, higher_better) if pd.notna(val) else None
        metrics.append((label, m))
    if any(m is not None for _, m in metrics):
        n_with_any_metric += 1
    cat_dict = {"metrics": metrics, **cat_scores_both(metrics),
                "state_rank": None, "district_rank": None, "n_state": None, "n_district": None}
    for label, bq in d["scoring"]["by_quarter"].items():
        bq["categories"][LOAN_CAT] = cat_dict

print(f"[{elapsed()}] {n_with_any_metric} of {len(clfs)} CLFs got at least one Loan Portfolio metric.")

# ---- Step 4: data_availability checklist + the "Data Sources Available" raw
# count that feeds the Data Coverage category below. Profile is trivially True
# for every CLF here (a CLF json only exists because its profile data matched
# in the first place) - kept in the list anyway for a complete, honest picture
# of every source this tracker actually draws from. ----
print(f"[{elapsed()}] Computing data_availability...")
for mis, d in clfs.items():
    quarters = d.get("financial", {}).get("quarters", [])
    any_f03 = any((q.get("total_receipts") or 0) > 0 or (q.get("total_payments") or 0) > 0 for q in quarters)
    any_f05 = any((q.get("cum_disbursed") or 0) > 0 or (q.get("cum_requested") or 0) > 0 for q in quarters)
    vprp_years = d.get("vprp", {}).get("years", {})
    any_ent = any((y.get("n_demands") or 0) > 0 for y in vprp_years.values())
    any_pgsrd = any((y.get("n_pgsrd") or 0) > 0 for y in vprp_years.values())
    any_sdp = any((y.get("n_sdp") or 0) > 0 for y in vprp_years.values())
    flags = {
        "profile": True,
        "audit": bool(d.get("audit", {}).get("found")),
        "f01": bool(d.get("financial", {}).get("f01", {}).get("found")),
        "f03": any_f03,
        "f05": any_f05,
        "vrf": bool(d.get("vrf", {}).get("found")),
        "vprp_ent": any_ent,
        "vprp_pgsrd": any_pgsrd,
        "vprp_sdp": any_sdp,
        "fund_disbursement": bool(d.get("fund_disbursement", {}).get("found")),
        "loans": bool(d.get("loans", {}).get("found")),
    }
    sources = [{"key": key, "label": label, "available": flags[key]} for key, label in DATA_SOURCES]
    d["data_availability"] = {
        "sources": sources,
        "n_available": sum(1 for s in sources if s["available"]),
        "n_total": len(sources),
    }

# ---- Step 5: inject Data Coverage - one metric ("Data Sources Available"),
# percentile-ranked against ALL 1667 CLFs (every CLF has a well-defined count,
# so unlike Loan Portfolio there's no restricted peer population here). ----
da_rows = [{"mis_id": mis, "district": d["overview"]["district"], "n_available": d["data_availability"]["n_available"]}
           for mis, d in clfs.items()]
da_pop = pd.DataFrame(da_rows).set_index("mis_id", drop=False)
for mis, d in clfs.items():
    row = da_pop.loc[mis]
    m = score_metric(da_pop, "n_available", row["n_available"], row["district"], higher_is_better=True)
    metrics = [("Data Sources Available", m)]
    cat_dict = {"metrics": metrics, **cat_scores_both(metrics),
                "state_rank": None, "district_rank": None, "n_state": None, "n_district": None}
    for label, bq in d["scoring"]["by_quarter"].items():
        bq["categories"][DATA_CAT] = cat_dict

# ---- Step 6: explicit final category-order rebuild - dict insertion order
# drives display order throughout the JS, and the pop()/assign() rename in
# Step 1 does NOT preserve original position, so this can't be left implicit. ----
for mis, d in clfs.items():
    for label, bq in d["scoring"]["by_quarter"].items():
        cats = bq["categories"]
        bq["categories"] = {name: cats[name] for name in CATEGORY_ORDER if name in cats}

quarters_seen = set()
for d in clfs.values():
    quarters_seen.update(d["scoring"]["by_quarter"].keys())

# ---- Step 7: category-level rank (state_rank/district_rank/n_state/n_district)
# for the 2 new categories - the original 5 already carry this, baked in by
# build_tracker_data.py's own RANK_LOOKUP two-pass system. Without this, the
# Category Summary view (which reads the category's own rank fields, not its
# metrics') would show "Not Found" for Loan Portfolio/Data Coverage even though
# By Category correctly shows a rank for the metric(s) inside them - same
# _rank_pop() pattern as the Overall Score re-rank below, just applied to each
# category's own `score` instead. ----
print(f"[{elapsed()}] Ranking new categories statewide...")
for cat_name in [LOAN_CAT, DATA_CAT]:
    for label in quarters_seen:
        rows = [(mis, d["overview"]["district"], d["scoring"]["by_quarter"][label]["categories"][cat_name]["score"])
                for mis, d in clfs.items()
                if label in d["scoring"]["by_quarter"]
                and cat_name in d["scoring"]["by_quarter"][label]["categories"]
                and d["scoring"]["by_quarter"][label]["categories"][cat_name]["score"] is not None]
        cat_pop = pd.DataFrame(rows, columns=["mis_id", "district", "value"])
        ranks = _rank_pop(cat_pop)
        for mis, d in clfs.items():
            bq = d["scoring"]["by_quarter"].get(label)
            if not bq or cat_name not in bq["categories"]:
                continue
            r = ranks.get(mis)
            cd = bq["categories"][cat_name]
            cd["state_rank"] = r["state_rank"] if r else None
            cd["district_rank"] = r["district_rank"] if r else None
            cd["n_state"] = r["n_state"] if r else None
            cd["n_district"] = r["n_district"] if r else None

# ---- Step 8: recompute overall_score/overall_district_score for every CLF/quarter
# (mean of every non-None category score - now potentially 7, not 5) ----
for mis, d in clfs.items():
    for label, bq in d["scoring"]["by_quarter"].items():
        cats = bq["categories"]
        state_scores = [c["score"] for c in cats.values() if c["score"] is not None]
        dist_scores = [c["district_score"] for c in cats.values() if c["district_score"] is not None]
        bq["overall_score"] = round(sum(state_scores) / len(state_scores)) if state_scores else None
        bq["overall_district_score"] = round(sum(dist_scores) / len(dist_scores)) if dist_scores else None

# ---- Step 9: full statewide re-rank of overall_score, per quarter, over ALL CLFs -
# not just the ones whose score changed (their rank shift can bump every other CLF
# up or down a place too). ----
print(f"[{elapsed()}] Re-ranking Overall Score statewide...")
for label in quarters_seen:
    rows = [(mis, d["overview"]["district"], d["scoring"]["by_quarter"][label]["overall_score"])
            for mis, d in clfs.items()
            if label in d["scoring"]["by_quarter"] and d["scoring"]["by_quarter"][label]["overall_score"] is not None]
    overall_pop = pd.DataFrame(rows, columns=["mis_id", "district", "value"])
    ranks = _rank_pop(overall_pop)
    for mis, d in clfs.items():
        bq = d["scoring"]["by_quarter"].get(label)
        if not bq:
            continue
        r = ranks.get(mis)
        bq["overall_state_rank"] = r["state_rank"] if r else None
        bq["overall_district_rank"] = r["district_rank"] if r else None
        bq["overall_n_state"] = r["n_state"] if r else None
        bq["overall_n_district"] = r["n_district"] if r else None

print(f"[{elapsed()}] Writing updated CLF json...")
for mis, d in clfs.items():
    (CLF_DIR / f"{mis}.json").write_text(json.dumps(d, ensure_ascii=False))
print(f"[{elapsed()}] Done.")
