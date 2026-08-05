"""
Author:   Claude (for Mohan)
Created:  04/08/2026
Purpose:  Aggregate per-CLF JSON (data/clfs/*.json, already built by
          build_tracker_data.py) up to District and State level, producing
          data/districts/{slug}.json (one per district, all 38) and
          data/state.json - the data backing the District and State Tracker
          views in the statewide shell (make_shell.py). Ports the
          aggregation logic proven out in the single-district prototype
          (../build_district_tracker_prototype.py), generalized to run over
          every district and the full state, and upgrades the prototype's
          illustrative "vs. state" standing (fake numbers, since a single-
          district build has no other districts to rank against) to REAL
          cross-district ranks, via the same two-pass pattern
          build_tracker_data.py already uses for CLF ranking: every
          district's aggregate is computed once (pass 1), then each
          district's rank against every other district is looked up from
          that in-memory population (pass 2) before writing the final files
          - no need to recompute anything, unlike the CLF-level pass 1/2
          split, since all district aggregates already fit in memory.

          Run AFTER build_tracker_data.py (needs data/clfs/*.json to exist).

Input:  data/clfs/*.json, data/shared.json (read + extended with
        district/state CONTEXT and ERR_MSG copy, then rewritten)
Output: data/districts/{slug}.json (38 files), data/state.json,
        data/shared.json (updated in place)
"""

import json
import glob
import re
import os
import time

BASE = "/Users/mohanrajagopal/Dropbox/Bihar Gates Team/3_Jeevika/6_LokOS_Analysis"
DATA_DIR = f"{BASE}/3_Output/CLF Tracker/Scale-Up/data"
os.makedirs(f"{DATA_DIR}/districts", exist_ok=True)

T0 = time.time()
def elapsed(): return f"{time.time()-T0:.1f}s"

CATS = ["Fund Utilization & Loan Activity", "Financial Health", "VRF Fund Health", "Governance & Compliance", "Welfare and Livelihood"]

def slugify(name):
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

def wavg(pairs):
    num, den = 0, 0
    for v, w in pairs:
        if v is not None and w:
            num += v * w
            den += w
    return round(num / den, 1) if den else None

def sum_arrays(list_of_lists):
    if not list_of_lists:
        return []
    n = len(list_of_lists[0])
    return [sum(a[i] for a in list_of_lists) for i in range(n)]

def rank_simple(values):
    """values: {name: value or None}. min-rank convention (ties share the
    best rank), higher value = better rank, matching build_tracker_data.py's
    own _rank_pop() convention for CLF ranking."""
    items = sorted(((k, v) for k, v in values.items() if v is not None), key=lambda kv: -kv[1])
    n = len(items)
    out = {}
    i = 0
    while i < len(items):
        j = i
        while j + 1 < len(items) and items[j + 1][1] == items[i][1]:
            j += 1
        rank = i + 1
        for k in range(i, j + 1):
            out[items[k][0]] = rank
        i = j + 1
    return out, n

# ============================================================================
# 1. Load all per-CLF JSON, group by district.
# ============================================================================
print(f"[{elapsed()}] Loading all per-CLF JSON...")
ALL_CLFS = [json.load(open(f)) for f in glob.glob(f"{DATA_DIR}/clfs/*.json")]
print(f"[{elapsed()}] Loaded {len(ALL_CLFS)} CLFs")

BY_DISTRICT = {}
for c in ALL_CLFS:
    BY_DISTRICT.setdefault(c["overview"]["district"], []).append(c)
print(f"[{elapsed()}] Grouped into {len(BY_DISTRICT)} districts")

LATEST_Q = ALL_CLFS[0]["scoring"]["quarters"][-1]

def cat_scores_for(c):
    bq = c["scoring"]["by_quarter"][LATEST_Q]
    return {cat: bq["categories"][cat]["score"] for cat in CATS}

# ============================================================================
# 2. Aggregation - one CLF list (a district's CLFs, or every CLF statewide)
# in, one aggregate dict out. Sums stay sums; ratios/percentages are
# recomputed from summed numerators/denominators (never averaged directly);
# audit/scoring "scores" are plain averages across CLFs (not summed - a
# raw/max-sum approach was tried and explicitly rejected in favour of this).
# ============================================================================
def aggregate_group(CLFS, name, clf_ranking_mode="full"):
    n_clfs = len(CLFS)

    # ---- Overview ----
    n_vo = sum(c["overview"]["n_vo"] for c in CLFS)
    n_shg = sum(c["overview"]["n_shg"] for c in CLFS)
    n_members = sum(c["overview"]["n_members"] for c in CLFS)
    n_active = sum(c["overview"]["n_active"] for c in CLFS)
    status_dist = {}
    for c in CLFS:
        o = c["overview"]
        key = o["status_tier"] if o["status_tier_found"] else "Not Found"
        status_dist[key] = status_dist.get(key, 0) + 1

    coverage_agg, education_agg = {}, {}
    for key in CLFS[0]["overview"]["coverage"]:
        pairs = [(c["overview"]["coverage"][key][0], c["overview"]["n_members"]) for c in CLFS]
        n_total_key = sum(c["overview"]["coverage"][key][1] for c in CLFS)
        coverage_agg[key] = [wavg(pairs), n_total_key]
    for key in CLFS[0]["overview"]["education"]:
        pairs = [(c["overview"]["education"][key][0], c["overview"]["n_members"]) for c in CLFS]
        n_total_key = sum(c["overview"]["education"][key][1] for c in CLFS)
        education_agg[key] = [wavg(pairs), n_total_key]

    livelihood_agg = {}
    for key in CLFS[0]["overview"]["livelihood_split"]:
        livelihood_agg[key] = wavg([(c["overview"]["livelihood_split"][key], c["overview"]["n_members"]) for c in CLFS])

    cadre_agg = {}
    for c in CLFS:
        for role, n in c["overview"]["cadre_roster"].items():
            cadre_agg[role] = cadre_agg.get(role, 0) + n
    cadre_agg = dict(sorted(cadre_agg.items(), key=lambda kv: -kv[1]))

    subcom_agg = {}
    for c in CLFS:
        for role, n in (c["overview"].get("subcom") or {}).items():
            subcom_agg[role] = subcom_agg.get(role, 0) + (n or 0)

    ec_counts = [c["overview"]["ec_count"] for c in CLFS if c["overview"].get("ec_count") is not None]
    total_ec = sum(ec_counts)
    avg_ec = round(total_ec / len(ec_counts), 1) if ec_counts else None

    spa_found_clfs = [c for c in CLFS if c["overview"].get("spa_found")]
    spa_agg = {}
    if spa_found_clfs:
        spa_keys = list(spa_found_clfs[0]["overview"]["spa"].keys())
        for k in spa_keys:
            n_true = sum(1 for c in spa_found_clfs if c["overview"]["spa"].get(k))
            spa_agg[k] = round(n_true / len(spa_found_clfs) * 100, 1)

    overview_agg = {
        "name": name, "n_clfs": n_clfs,
        "n_vo": n_vo, "n_shg": n_shg, "n_members": n_members, "n_active": n_active,
        "pct_active": round(n_active / n_members * 100, 1) if n_members else None,
        "status_distribution": status_dist,
        "coverage": coverage_agg, "education": education_agg,
        "pct_has_livelihood": wavg([(c["overview"]["pct_has_livelihood"], c["overview"]["n_members"]) for c in CLFS]),
        "pct_multi_livelihood": wavg([(c["overview"]["pct_multi_livelihood"], c["overview"]["n_members"]) for c in CLFS]),
        "livelihood_split": livelihood_agg,
        "cadre_roster": cadre_agg,
        "subcom_agg": subcom_agg, "total_ec": total_ec, "avg_ec": avg_ec,
        "spa_agg": spa_agg, "spa_n_clfs_found": len(spa_found_clfs),
    }

    # ---- Audit (averages across CLFs, not sums) ----
    audit_found = [c for c in CLFS if c["audit"]["found"]]
    audit_scored = [c for c in audit_found if c["audit"]["total_score"] is not None]
    grade_dist = {}
    for c in audit_found:
        grade_dist[c["audit"]["grade"]] = grade_dist.get(c["audit"]["grade"], 0) + 1
    top_grade = max(grade_dist.items(), key=lambda kv: kv[1])[0] if grade_dist else None

    cat_pcts, cat_raws, cat_maxes = {}, {}, {}
    cat_order = [cb["label"] for cb in audit_found[0]["audit"]["category_breakdown"]] if audit_found else []
    for c in audit_found:
        for cb in c["audit"]["category_breakdown"]:
            if cb["pct"] is not None:
                cat_pcts.setdefault(cb["label"], []).append(cb["pct"])
            if cb["raw"] is not None:
                cat_raws.setdefault(cb["label"], []).append(cb["raw"])
            if cb["max"] is not None:
                cat_maxes[cb["label"]] = cb["max"]
    cat_breakdown_agg = []
    for label in cat_order:
        pcts, raws = cat_pcts.get(label, []), cat_raws.get(label, [])
        cat_breakdown_agg.append({
            "label": label,
            "pct": round(sum(pcts) / len(pcts)) if pcts else None,
            "raw": round(sum(raws) / len(raws), 1) if raws else None,
            "max": cat_maxes.get(label),
        })

    item_pcts, item_raws, item_maxes, item_order = {}, {}, {}, []
    for c in audit_found:
        for it in c["audit"]["item_scores"]:
            key = (it["category"], it["label"])
            if key not in item_pcts:
                item_pcts[key], item_raws[key] = [], []
                item_order.append(key)
            if it["pct"] is not None:
                item_pcts[key].append(it["pct"])
            if it["raw"] is not None:
                item_raws[key].append(it["raw"])
            if it["max"] is not None:
                item_maxes[key] = it["max"]
    item_scores_agg = []
    for cat, label in item_order:
        pcts, raws = item_pcts[(cat, label)], item_raws[(cat, label)]
        item_scores_agg.append({
            "label": label, "category": cat,
            "pct": round(sum(pcts) / len(pcts)) if pcts else None,
            "raw": round(sum(raws) / len(raws), 1) if raws else None,
            "max": item_maxes.get((cat, label)),
        })

    irreg_cat_counts = {}
    for c in audit_found:
        if c["audit"]["issue_flagged"]:
            for cat in c["audit"].get("irreg_categories", []):
                irreg_cat_counts[cat] = irreg_cat_counts.get(cat, 0) + 1

    gaps = [c["audit"]["cash_vs_physical_gap"] for c in audit_found if c["audit"].get("cash_vs_physical_gap") is not None]
    gap_total_abs = sum(abs(g) for g in gaps) if gaps else None
    n_cash_higher = sum(1 for c in audit_found if c["audit"].get("cash_vs_physical_gap") and c["audit"].get("cash_book_higher"))
    n_physical_higher = sum(1 for c in audit_found if c["audit"].get("cash_vs_physical_gap") and not c["audit"].get("cash_book_higher"))
    n_exact_match = sum(1 for c in audit_found if (c["audit"].get("cash_vs_physical_gap") or 0) == 0)

    audit_agg = {
        "n_found": len(audit_found), "n_total": n_clfs,
        "avg_score": round(sum(c["audit"]["total_score"] for c in audit_scored) / len(audit_scored), 1) if audit_scored else None,
        "top_grade": top_grade,
        "grade_distribution": grade_dist,
        "n_flagged": sum(1 for c in audit_found if c["audit"]["issue_flagged"]),
        "irregularity_categories": dict(sorted(irreg_cat_counts.items(), key=lambda kv: -kv[1])),
        "category_breakdown": cat_breakdown_agg,
        "item_scores": item_scores_agg,
        "gap_total_abs": gap_total_abs, "n_cash_higher": n_cash_higher,
        "n_physical_higher": n_physical_higher, "n_exact_match": n_exact_match,
        "state_rank": None, "n_state": None,  # filled in by rank_districts() below
    }

    # ---- Financial (shaped identically to a real CLF's own `financial` so
    # the shell's renderFinancial/renderFinSummary/renderFinStatements can
    # run UNCHANGED against it via a pseudo-CLF wrapper) ----
    f01_found = [c for c in CLFS if c["financial"]["f01"]["found"]]
    total_assets = sum(c["financial"]["f01"]["total_assets"] for c in f01_found)
    total_liab = sum(c["financial"]["f01"]["liabilities"]["Total Equity & Liabilities"] for c in f01_found)
    total_cash = sum(c["financial"]["f01"]["assets"]["Cash Balance"] + c["financial"]["f01"]["assets"]["Bank Balance"] for c in f01_found)
    total_fixed = sum(c["financial"]["f01"]["assets"]["Fixed Assets"] for c in f01_found)
    total_onlend = sum(
        c["financial"]["f01"]["assets"]["Loan to SHGs/VOs"] + c["financial"]["f01"]["assets"]["Loan to Non-Nodal CLFs"]
        + c["financial"]["f01"]["assets"]["Advances to SHGs/VOs"] + c["financial"]["f01"]["assets"]["Advances to Others"]
        for c in f01_found)
    total_surplus = sum(c["financial"]["f01"]["liabilities"]["Surplus / Deficit"] for c in f01_found)

    assets_sum, liab_sum = {}, {}
    for c in f01_found:
        for k, v in c["financial"]["f01"]["assets"].items():
            if k != "Total Assets":
                assets_sum[k] = assets_sum.get(k, 0) + v
        for k, v in c["financial"]["f01"]["liabilities"].items():
            if k != "Total Equity & Liabilities":
                liab_sum[k] = liab_sum.get(k, 0) + v
    assets_sum["Total Assets"] = total_assets
    liab_sum["Total Equity & Liabilities"] = total_liab

    from collections import Counter
    periods = [c["financial"]["f01"]["period"] for c in f01_found if c["financial"]["f01"].get("period")]
    common_period = Counter(periods).most_common(1)[0][0] if periods else None

    f01_agg = {
        "found": len(f01_found) > 0, "period": common_period,
        "total_assets": total_assets,
        "balance_gap_rs": total_assets - total_liab,
        "liquidity_ratio": round(total_cash / total_assets * 100, 1) if total_assets else None,
        "deployment_ratio": round(total_onlend / (total_assets - total_fixed) * 100, 1) if (total_assets - total_fixed) else None,
        "surplus_pct": round(total_surplus / total_assets * 100, 1) if total_assets else None,
        "assets": assets_sum, "liabilities": liab_sum,
    }

    quarterly_agg = []
    for qi, q0 in enumerate(CLFS[0]["financial"]["quarters"]):
        receipts_full_sum, payments_full_sum = {}, {}
        for c in CLFS:
            q = c["financial"]["quarters"][qi]
            if q.get("receipts_full"):
                for k, v in q["receipts_full"].items():
                    receipts_full_sum[k] = receipts_full_sum.get(k, 0) + v
            if q.get("payments_full"):
                for k, v in q["payments_full"].items():
                    payments_full_sum[k] = payments_full_sum.get(k, 0) + v

        total_receipts = receipts_full_sum.get("Total Receipts", 0)
        total_payments = payments_full_sum.get("Total Payments", 0)
        net_cash_flow = sum(c["financial"]["quarters"][qi]["net_cash_flow"] for c in CLFS)
        cum_disbursed = sum(c["financial"]["quarters"][qi]["cum_disbursed"] for c in CLFS)
        cum_requested = sum(c["financial"]["quarters"][qi]["cum_requested"] for c in CLFS)
        new_demand_amt = sum(c["financial"]["quarters"][qi]["new_demand_amt"] for c in CLFS)
        qtr_pending = sum(c["financial"]["quarters"][qi]["qtr_pending"] for c in CLFS)
        new_disb_total = new_demand_amt - qtr_pending
        avail_balance = sum(c["financial"]["quarters"][qi]["avail_balance"] for c in CLFS)
        amount_pending = sum(c["financial"]["quarters"][qi]["amount_pending"] for c in CLFS)
        n_requesting = sum(c["financial"]["quarters"][qi]["n_requesting"] for c in CLFS)
        n_vo_total_fin = sum(c["financial"]["n_vo"] for c in CLFS)
        expenses = payments_full_sum.get("Group Expense", 0) + payments_full_sum.get("Bank Expense", 0)
        interest_income = receipts_full_sum.get("Interest Received From CBOs", 0)
        is_real = total_receipts > 0 or total_payments > 0 or cum_disbursed > 0
        n_clfs_real = sum(1 for c in CLFS if c["financial"]["quarters"][qi]["is_real"])

        quarterly_agg.append({
            "label": q0["label"],
            "opening_balance": receipts_full_sum.get("Opening Balance", 0),
            "total_receipts": total_receipts, "total_payments": total_payments,
            "closing_balance": payments_full_sum.get("Closing Balance", 0),
            "net_cash_flow": net_cash_flow,
            "receipts_full": receipts_full_sum or None, "payments_full": payments_full_sum or None,
            "operating_expense_ratio": round(expenses / total_receipts * 100, 1) if total_receipts else None,
            "interest_income_share": round(interest_income / total_receipts * 100, 1) if total_receipts else None,
            "pct_disbursed": round(cum_disbursed / cum_requested * 100, 1) if cum_requested else None,
            "amount_pending": amount_pending,
            "new_demand_amt": new_demand_amt,
            "qtr_pending": qtr_pending,
            "this_qtr_disb_rate": round(new_disb_total / new_demand_amt * 100, 1) if new_demand_amt else None,
            "capacity_ratio": round(avail_balance / qtr_pending, 2) if qtr_pending > 0 else None,
            "avail_balance": avail_balance,
            "n_requesting": n_requesting, "pct_vos_requesting": round(n_requesting / n_vo_total_fin * 100, 1) if n_vo_total_fin else None,
            "cum_disbursed": cum_disbursed, "cum_requested": cum_requested,
            "is_real": is_real, "n_clfs_real": n_clfs_real,
        })

    financial_agg = {
        "n_f01_found": len(f01_found), "n_total": n_clfs,
        "f01": f01_agg, "quarters": quarterly_agg,
    }

    # ---- VRF: KPI Snapshot + Forecasts only (VO/Bookkeeper-grain tabs, and
    # their district-level replacements, were dropped entirely per explicit
    # instruction). Forecasts are the elementwise sum of each CLF's own
    # already-computed forecast. ----
    vrf_found_clfs = [c for c in CLFS if c["vrf"]["found"]]
    all_vos = []
    for c in vrf_found_clfs:
        all_vos.extend(c["vrf"]["vo_table"])

    forecast_agg = None
    if vrf_found_clfs:
        forecast_agg = {
            "has_idle_eligible": any(c["vrf"]["forecast"]["has_idle_eligible"] for c in vrf_found_clfs),
            "n_eligible": sum(c["vrf"]["forecast"]["n_eligible"] for c in vrf_found_clfs),
            "n_vo": sum(c["vrf"]["forecast"]["n_vo"] for c in vrf_found_clfs),
            "savings_now": sum(c["vrf"]["forecast"]["savings_now"] for c in vrf_found_clfs),
            "savings_end": sum(c["vrf"]["forecast"]["savings_end"] for c in vrf_found_clfs),
            "savings_monthly": sum_arrays([c["vrf"]["forecast"]["savings_monthly"] for c in vrf_found_clfs]),
        }
        for sk in ["s1", "s2", "s3"]:
            forecast_agg[sk] = {
                "interest_now": sum(c["vrf"]["forecast"][sk]["interest_now"] for c in vrf_found_clfs),
                "interest_end": sum(c["vrf"]["forecast"][sk]["interest_end"] for c in vrf_found_clfs),
                "corpus_now": sum(c["vrf"]["forecast"][sk]["corpus_now"] for c in vrf_found_clfs),
                "corpus_end": sum(c["vrf"]["forecast"][sk]["corpus_end"] for c in vrf_found_clfs),
                "interest_monthly": sum_arrays([c["vrf"]["forecast"][sk]["interest_monthly"] for c in vrf_found_clfs]),
                "corpus_monthly": sum_arrays([c["vrf"]["forecast"][sk]["corpus_monthly"] for c in vrf_found_clfs]),
            }

    vrf_agg = {
        "found": len(all_vos) > 0, "n_clfs_with_vrf": len(vrf_found_clfs), "n_total": n_clfs,
        "n_vo": len(all_vos), "received_vo": sum(c["vrf"]["received_vo"] for c in vrf_found_clfs),
        "n_members": sum(v["totalshgmembers"] for v in all_vos),
        "total_received": sum(c["vrf"]["total_received"] for c in vrf_found_clfs),
        "total_savings": sum(c["vrf"]["total_savings"] for c in vrf_found_clfs),
        "total_interest": sum(c["vrf"]["total_interest"] for c in vrf_found_clfs),
        "total_corpus": sum(c["vrf"]["total_corpus"] for c in vrf_found_clfs),
        "coverage_gap": sum(c["vrf"]["coverage_gap"] for c in vrf_found_clfs),
        "expected_savings": sum(c["vrf"]["expected_savings"] for c in vrf_found_clfs),
        "fsf_eligible": sum(v["fsf_eligibile"] or 0 for v in all_vos),
        "incomplete_coverage": sum(v["incomplete_vrf_coverage"] or 0 for v in all_vos),
        "idle_vo": sum(v["zero_interest_vo"] or 0 for v in all_vos),
        "sdf_monthly": sum(c["vrf"]["sdf_monthly"] for c in vrf_found_clfs),
        "sdf_annual": sum(c["vrf"]["sdf_annual"] for c in vrf_found_clfs),
        "forecast": forecast_agg,
    }

    # ---- VPRP: Entitlements + PGSRD + SDP, shaped identically to a real
    # CLF's own vprp.years[yr] so the shell's renderVprpEnt/Pgsrd/Sdp run
    # UNCHANGED against it via the pseudo-CLF wrapper. ----
    vprp_years_agg = {}
    for yr in [2023, 2024, 2025]:
        yr_key = str(yr)
        total_requesting = sum(c["vprp"]["years"].get(yr_key, {}).get("n_demands", 0) for c in CLFS)
        accessed_est = sum(
            round((c["vprp"]["years"].get(yr_key, {}).get("n_demands", 0) or 0) * (c["vprp"]["years"].get(yr_key, {}).get("pct_nrega_accessed") or 0) / 100)
            for c in CLFS)

        scheme_demand, scheme_n_vo, scheme_raw_map = {}, {}, {}
        for c in CLFS:
            for s in c["vprp"]["years"].get(yr_key, {}).get("by_scheme", []) or []:
                scheme_demand[s["scheme"]] = scheme_demand.get(s["scheme"], 0) + s["demanded"]
                scheme_n_vo[s["scheme"]] = scheme_n_vo.get(s["scheme"], 0) + (s.get("n_vo") or 0)
                scheme_raw_map[s["scheme"]] = s["raw_scheme"]
        by_scheme_agg = [{"scheme": k, "raw_scheme": scheme_raw_map[k], "demanded": v, "n_vo": scheme_n_vo.get(k) or None}
                          for k, v in sorted(scheme_demand.items(), key=lambda kv: -kv[1])]

        other_schemes, has_nrega = set(), False
        for c in CLFS:
            for s in c["vprp"]["years"].get(yr_key, {}).get("by_scheme", []) or []:
                if s["raw_scheme"] == "mgnregs-job-card":
                    has_nrega = True
                else:
                    other_schemes.add(s["raw_scheme"])
        n_schemes_total = len(other_schemes) + (1 if has_nrega else 0)
        # VOs are strictly nested under one CLF each, so summing each CLF's own
        # VO-requesting count across the district/state is an exact count, not
        # an approximation - unlike departments below, which recur across CLFs.
        n_vo_requesting_ent = sum(c["vprp"]["years"].get(yr_key, {}).get("n_vo_requesting_ent") or 0 for c in CLFS)
        n_vo_requesting_pgsrd = sum(c["vprp"]["years"].get(yr_key, {}).get("n_vo_requesting_pgsrd") or 0 for c in CLFS)
        n_vo_sdp = sum(c["vprp"]["years"].get(yr_key, {}).get("n_vo") or 0 for c in CLFS)

        state_breakdown = {}
        for c in CLFS:
            for k, v in (c["vprp"]["years"].get(yr_key, {}).get("state_scheme_breakdown") or {}).items():
                demanded = v.get("demanded", 0) if isinstance(v, dict) else v
                n_vo = v.get("n_vo", 0) if isinstance(v, dict) else 0
                entry = state_breakdown.setdefault(k, {"demanded": 0, "n_vo": 0})
                entry["demanded"] += demanded
                entry["n_vo"] += n_vo
        if state_breakdown:
            for entry in state_breakdown.values():
                entry["n_vo"] = entry["n_vo"] or None
            state_breakdown = dict(sorted(state_breakdown.items(), key=lambda kv: -kv[1]["demanded"]))
        else:
            state_breakdown = None

        n_pgsrd_total = sum(c["vprp"]["years"].get(yr_key, {}).get("n_pgsrd", 0) for c in CLFS)
        pgsrd_type_counts = {}
        for c in CLFS:
            yd = c["vprp"]["years"].get(yr_key, {})
            tsplit, n_pg = yd.get("pgsrd_type_split"), yd.get("n_pgsrd", 0)
            if tsplit and n_pg:
                for k, pct in tsplit.items():
                    pgsrd_type_counts[k] = pgsrd_type_counts.get(k, 0) + round(pct / 100 * n_pg)
        pgsrd_type_split_agg = None
        if pgsrd_type_counts:
            tot = sum(pgsrd_type_counts.values())
            pgsrd_type_split_agg = {k: round(v / tot * 100, 1) for k, v in pgsrd_type_counts.items()} if tot else None

        pgsrd_items_agg = {}
        for c in CLFS:
            for it in c["vprp"]["years"].get(yr_key, {}).get("pgsrd_items") or []:
                key = (it["item_demanded"], it["pgsrd_type"])
                e = pgsrd_items_agg.setdefault(key, {"n": 0, "units": 0, "n_vo": 0})
                e["n"] += it["n"]; e["units"] += it["units"]; e["n_vo"] += (it.get("n_vo") or 0)
        pgsrd_items_list = sorted(
            [{"item_demanded": k[0], "pgsrd_type": k[1], "n": v["n"], "units": v["units"], "n_vo": v["n_vo"] or None} for k, v in pgsrd_items_agg.items()],
            key=lambda r: -r["n"])[:30]  # cap for state-level payload size

        sdg_theme_agg, gpdp_area_agg = {}, {}
        for c in CLFS:
            yd = c["vprp"]["years"].get(yr_key, {})
            for k, v in (yd.get("sdg_theme") or {}).items():
                sdg_theme_agg[k] = sdg_theme_agg.get(k, 0) + v
            for k, v in (yd.get("gpdp_area") or {}).items():
                gpdp_area_agg[k] = gpdp_area_agg.get(k, 0) + v

        n_sdp_total = sum(c["vprp"]["years"].get(yr_key, {}).get("n_sdp", 0) for c in CLFS)
        sdp_sector_counts = {}
        for c in CLFS:
            yd = c["vprp"]["years"].get(yr_key, {})
            ssec, n_sd = yd.get("sdp_sector"), yd.get("n_sdp", 0)
            if ssec and n_sd:
                for k, pct in ssec.items():
                    sdp_sector_counts[k] = sdp_sector_counts.get(k, 0) + round(pct / 100 * n_sd)
        sdp_sector_agg = None
        if sdp_sector_counts:
            tot = sum(sdp_sector_counts.values())
            sdp_sector_agg = {k: round(v / tot * 100, 1) for k, v in sdp_sector_counts.items()} if tot else None

        sdp_issues_agg = {}
        for c in CLFS:
            for it in c["vprp"]["years"].get(yr_key, {}).get("sdp_issues") or []:
                e = sdp_issues_agg.setdefault(it["social_issue"], {"n": 0, "affected": 0, "has_affected": False, "n_vo": 0})
                e["n"] += it["n"]
                e["n_vo"] += (it.get("n_vo") or 0)
                if it.get("affected") is not None:
                    e["affected"] += it["affected"]; e["has_affected"] = True
        sdp_issues_list = sorted(
            [{"social_issue": k, "n": v["n"], "affected": (v["affected"] if v["has_affected"] else None), "n_vo": v["n_vo"] or None} for k, v in sdp_issues_agg.items()],
            key=lambda r: -r["n"])[:30]

        departments_agg = {}
        for c in CLFS:
            for k, v in (c["vprp"]["years"].get(yr_key, {}).get("departments") or {}).items():
                departments_agg[k] = departments_agg.get(k, 0) + v

        vprp_years_agg[yr_key] = {
            "n_demands": total_requesting,
            "pct_nrega_accessed": round(accessed_est / total_requesting * 100, 1) if total_requesting else None,
            "n_other_schemes": len(other_schemes), "n_schemes_total": n_schemes_total,
            "n_vo_requesting_ent": n_vo_requesting_ent or None,
            "by_scheme": by_scheme_agg, "state_scheme_breakdown": state_breakdown,
            "n_pgsrd": n_pgsrd_total, "pgsrd_type_split": pgsrd_type_split_agg,
            "n_vo_requesting_pgsrd": n_vo_requesting_pgsrd or None,
            "pgsrd_items": pgsrd_items_list, "sdg_theme": sdg_theme_agg or None, "gpdp_area": gpdp_area_agg or None,
            "n_sdp": n_sdp_total, "sdp_sector": sdp_sector_agg, "n_vo": n_vo_sdp or None,
            "sdp_issues": sdp_issues_list, "departments": departments_agg or None,
            # departments_agg is a union of each CLF's own top-6 departments, not
            # every CLF's full list (only n_departments, not the full breakdown,
            # is stored per-CLF) - in practice the department vocabulary is a
            # small fixed set of ~10-20 named entities, so a union across dozens
            # of CLFs per district reliably captures the true distinct count.
            "n_departments": len(departments_agg) or None,
        }

    # ---- Scoring: Overall = avg of each CLF's own Overall Score (equal
    # weight per CLF); category scores + individual metrics (avg state
    # percentile) likewise averaged, never summed. Real cross-district ranks
    # for overall/category/metric are filled in by rank_districts() below -
    # left None here (state has none - it's the top level). ----
    overall_scores = [c["scoring"]["by_quarter"][LATEST_Q]["overall_score"] for c in CLFS]
    valid_overall = [s for s in overall_scores if s is not None]
    cat_score_avgs = {}
    for cat in CATS:
        vals = [cat_scores_for(c)[cat] for c in CLFS if cat_scores_for(c)[cat] is not None]
        cat_score_avgs[cat] = round(sum(vals) / len(vals)) if vals else None

    category_metrics_agg = {}
    for cat in CATS:
        metric_vals, metric_order = {}, []
        for c in CLFS:
            cat_obj = c["scoring"]["by_quarter"][LATEST_Q]["categories"].get(cat)
            if not cat_obj:
                continue
            for label, m in cat_obj.get("metrics", []):
                if label not in metric_vals:
                    metric_vals[label] = []
                    metric_order.append(label)
                if m and m.get("state_pctl") is not None:
                    metric_vals[label].append(m["state_pctl"])
        category_metrics_agg[cat] = [
            {"label": label, "avg_state_pctl": round(sum(metric_vals[label]) / len(metric_vals[label])) if metric_vals[label] else None,
             "rank": None, "n": None, "raw_value": None, "raw_fmt": None, "raw_descriptor": None}
            for label in metric_order
        ]

    # ---- Raw average value per metric, for the state-level "By Category"
    # display: averaging a percentile against its OWN reference population is
    # close to a mathematical tautology at state scope (it converges to ~50
    # regardless of the underlying data, since percentiles are by
    # construction spread ~uniformly 0-100 across whatever population
    # produced them) - confirmed empirically, most state-level avg_state_pctl
    # values above landed within 1-2 points of 50. The raw average sidesteps
    # that; each metric mapped to the EXACT source column
    # build_tracker_data.py itself scores that metric from (see score_metric/
    # score_from_pop calls, ~line 807-819 there). A few metrics
    # (Platform Approval Status, Insurance Coverage, Aadhaar KYC Coverage)
    # live only in a raw member-level population inside build_tracker_data.py
    # that never gets surfaced into the per-CLF JSON - raw_value stays None
    # for those; the UI falls back to the best/worst-district callout alone.
    def _member_weighted_vrf_metric(label):
        pairs = []
        for c in vrf_found_clfs:
            m = next((mm for mm in c["vrf"]["metrics"] if mm["label"] == label), None)
            if m and m.get("value") is not None:
                pairs.append((m["value"], c["overview"]["n_members"]))
        return wavg(pairs)

    latest_q_fin = financial_agg["quarters"][-1] if financial_agg["quarters"] else None
    avg_subcom_filled = wavg([(sum(1 for v in c["overview"]["subcom"].values() if v and v > 0), 1) for c in CLFS])
    n_subcom_total = len(CLFS[0]["overview"]["subcom"]) if CLFS and CLFS[0]["overview"].get("subcom") else 6
    spa_counts = [sum(1 for v in c["overview"]["spa"].values() if v) for c in CLFS if c["overview"].get("spa_found")]
    avg_spa_engaged = round(sum(spa_counts) / len(spa_counts), 1) if spa_counts else None
    n_spa_total = len(CLFS[0]["overview"]["spa"]) if CLFS and CLFS[0]["overview"].get("spa_found") else 7
    cadre_div_vals = [c["overview"].get("n_distinct_cadre_types") for c in CLFS if c["overview"].get("n_distinct_cadre_types") is not None]
    avg_cadre_diversity = round(sum(cadre_div_vals) / len(cadre_div_vals), 1) if cadre_div_vals else None
    # CLF Status / Platform Approval Status are %-of-CLFs measures (each CLF
    # either is or isn't Model & Registered / platform-approved); Insurance
    # and Aadhaar Coverage are %-of-MEMBERS measures (member-weighted, same
    # convention as pct_active) - kept explicit in each metric's descriptor
    # below rather than left as a bare, ambiguous percentage.
    pct_model_registered = round(sum(1 for c in CLFS if c["overview"]["status_tier_found"] and c["overview"]["status_tier"] == "Model & Registered") / n_clfs * 100, 1) if n_clfs else None
    pct_platform_approved = round(sum(1 for c in CLFS if c["overview"].get("approval_status") == "Approved by BM") / n_clfs * 100, 1) if n_clfs else None
    pct_insurance = wavg([(c["overview"].get("pct_insurance"), c["overview"]["n_members"]) for c in CLFS])
    pct_aadhaar = wavg([(c["overview"].get("pct_aadhaar"), c["overview"]["n_members"]) for c in CLFS])
    demand_per_member = round(latest_q_fin["new_demand_amt"] / n_members, 1) if (latest_q_fin and n_members) else None
    bookkeeping_accuracy = round(100 - abs(f01_agg["balance_gap_rs"] / f01_agg["total_assets"] * 100), 1) if f01_agg["total_assets"] else None

    # (value, format_code, short descriptor appended after the formatted
    # number - e.g. "80.6% fund deployment ratio", "42.3% of CLFs are Model
    # & Registered"). Empty descriptor means the format code is already
    # self-explanatory (e.g. "2.2 of 6" for Subcommittee Completeness).
    METRIC_RAW = {
        ("Fund Utilization & Loan Activity", "Fund Deployment"): (f01_agg["deployment_ratio"], "pct", "fund deployment ratio"),
        ("Fund Utilization & Loan Activity", "Interest Income Share"): (latest_q_fin["interest_income_share"] if latest_q_fin else None, "pct", "of receipts"),
        ("Fund Utilization & Loan Activity", "This Quarter's Disbursement Rate"): (latest_q_fin["this_qtr_disb_rate"] if latest_q_fin else None, "pct", "of loan demand disbursed"),
        ("Fund Utilization & Loan Activity", "Loan Amount Demanded This Quarter"): (demand_per_member, "rs_per_member", ""),
        ("Financial Health", "Surplus / Deficit"): (f01_agg["surplus_pct"], "pct_signed", "of assets"),
        ("Financial Health", "Net Cash Flow"): (latest_q_fin["net_cash_flow"] if latest_q_fin else None, "rs", ""),
        ("Financial Health", "Bookkeeping Accuracy"): (bookkeeping_accuracy, "pct", "accuracy"),
        ("VRF Fund Health", "Savings Discipline"): (_member_weighted_vrf_metric("Savings Discipline"), "pct", "of promised savings collected"),
        ("VRF Fund Health", "Savings Realisation"): (_member_weighted_vrf_metric("Savings Realisation"), "multiplier", ""),
        ("VRF Fund Health", "Corpus Multiplier"): (_member_weighted_vrf_metric("Corpus Multiplier"), "multiplier", ""),
        ("VRF Fund Health", "Interest Yield"): (_member_weighted_vrf_metric("Interest Yield"), "pct_of_1", "yield"),
        ("VRF Fund Health", "Coverage Completion"): (_member_weighted_vrf_metric("Coverage Completion"), "pct_of_1", "of VOs have full coverage"),
        ("Governance & Compliance", "CLF Status"): (pct_model_registered, "pct", "of CLFs are Model & Registered"),
        ("Governance & Compliance", "Platform Approval Status"): (pct_platform_approved, "pct", "of CLFs are platform-approved"),
        ("Governance & Compliance", "Subcommittee Completeness"): (avg_subcom_filled, f"of_{n_subcom_total}", ""),
        ("Governance & Compliance", "Active Membership"): (overview_agg["pct_active"], "pct", "of members are active"),
        ("Governance & Compliance", "Cadre Diversity"): (avg_cadre_diversity, "types", ""),
        ("Welfare and Livelihood", "Insurance Coverage"): (pct_insurance, "pct", "of members have insurance"),
        ("Welfare and Livelihood", "Aadhaar KYC Coverage"): (pct_aadhaar, "pct", "of members Aadhaar KYC verified"),
        ("Welfare and Livelihood", "Livelihoods Diversification"): (overview_agg["pct_multi_livelihood"], "pct", "of members have multiple livelihoods"),
        ("Welfare and Livelihood", "Special Project Activities"): (avg_spa_engaged, f"of_{n_spa_total}", ""),
    }
    for cat in CATS:
        for m in category_metrics_agg[cat]:
            raw = METRIC_RAW.get((cat, m["label"]))
            if raw:
                m["raw_value"], m["raw_fmt"], m["raw_descriptor"] = raw

    clf_rankings_full = []
    for c in CLFS:
        bq = c["scoring"]["by_quarter"][LATEST_Q]
        clf_rankings_full.append({
            "mis_id": c["overview"]["mis_id"], "name": c["overview"]["clf_name_lokos"].title(),
            "block": c["overview"]["block"], "district": c["overview"]["district"],
            "categories": cat_scores_for(c),
            "overall_score": bq["overall_score"], "district_rank": bq.get("overall_district_rank"),
            "n_district": bq.get("overall_n_district"),
            "state_rank": bq.get("overall_state_rank"), "n_state": bq.get("overall_n_state"),
        })

    if clf_ranking_mode == "top_bottom_20":
        # "tier" is a fixed property of each row (top-20 vs bottom-20 by
        # overall state rank), stamped once here - NOT re-derived from
        # wherever a row happens to be sitting after the table gets sorted by
        # a different column in the browser, which would make the colour
        # meaningless (a bottom-20 CLF with one strong sub-score could sort
        # near the top of the visible list and wrongly render green).
        scored = [r for r in clf_rankings_full if r["overall_score"] is not None]
        scored.sort(key=lambda r: (r["state_rank"] is None, r["state_rank"]))
        if len(scored) > 40:
            top20, bottom20 = scored[:20], scored[-20:]
        else:
            top20, bottom20 = scored, []
        for r in top20:
            r["tier"] = "top"
        for r in bottom20:
            r["tier"] = "bottom"
        clf_rankings = top20 + bottom20
    else:
        clf_rankings_full.sort(key=lambda r: (r["district_rank"] is None, r["district_rank"]))
        clf_rankings = clf_rankings_full

    scoring_agg = {
        "overall_score": round(sum(valid_overall) / len(valid_overall)) if valid_overall else None,
        "n_clfs_scored": len(valid_overall), "n_total": n_clfs,
        "category_scores": cat_score_avgs,
        "category_metrics": category_metrics_agg,
        "clf_rankings": clf_rankings, "clf_ranking_mode": clf_ranking_mode,
        "overall_state_rank": None, "n_districts": None,
        "category_ranks": {cat: {"rank": None, "n": None} for cat in CATS},
    }

    pseudo_clf = {"financial": financial_agg, "vrf": vrf_agg, "vprp": {"years": vprp_years_agg}}

    return {
        "name": name, "overview": overview_agg, "audit": audit_agg,
        "financial": financial_agg, "vrf": vrf_agg, "vprp": {"years": vprp_years_agg},
        "scoring": scoring_agg, "pseudo_clf": pseudo_clf,
    }

# ============================================================================
# 3. Build every district's aggregate once (pass 1), then rank districts
# against each other and inject real ranks (pass 2) - no recomputation, the
# whole population already sits in memory.
# ============================================================================
print(f"[{elapsed()}] Aggregating {len(BY_DISTRICT)} districts...")
district_aggs = {}
for dname, clfs in sorted(BY_DISTRICT.items()):
    district_aggs[dname] = aggregate_group(clfs, dname, clf_ranking_mode="full")
print(f"[{elapsed()}] Pass 1 complete.")

print(f"[{elapsed()}] Ranking districts against each other...")
audit_ranks, n_audit = rank_simple({d: a["audit"]["avg_score"] for d, a in district_aggs.items()})
overall_ranks, n_overall = rank_simple({d: a["scoring"]["overall_score"] for d, a in district_aggs.items()})
cat_ranks = {}
for cat in CATS:
    cat_ranks[cat] = rank_simple({d: a["scoring"]["category_scores"][cat] for d, a in district_aggs.items()})

metric_labels_by_cat = {cat: [m["label"] for m in next(iter(district_aggs.values()))["scoring"]["category_metrics"][cat]] for cat in CATS}
metric_ranks = {}
for cat in CATS:
    for label in metric_labels_by_cat[cat]:
        pop = {}
        for d, a in district_aggs.items():
            m = next((mm for mm in a["scoring"]["category_metrics"][cat] if mm["label"] == label), None)
            pop[d] = m["avg_state_pctl"] if m else None
        metric_ranks[(cat, label)] = rank_simple(pop)

n_districts = len(district_aggs)
for dname, agg in district_aggs.items():
    agg["audit"]["state_rank"] = audit_ranks.get(dname)
    agg["audit"]["n_state"] = n_audit
    agg["scoring"]["overall_state_rank"] = overall_ranks.get(dname)
    agg["scoring"]["n_districts"] = n_overall
    for cat in CATS:
        ranks, n = cat_ranks[cat]
        agg["scoring"]["category_ranks"][cat] = {"rank": ranks.get(dname), "n": n}
    for cat in CATS:
        for m in agg["scoring"]["category_metrics"][cat]:
            ranks, n = metric_ranks[(cat, m["label"])]
            m["rank"] = ranks.get(dname)
            m["n"] = n

# best/worst district per metric (for the state-level "By Category" callout,
# reusing ranks already computed above rather than a fresh pass).
metric_best_worst = {}
for cat in CATS:
    for label in metric_labels_by_cat[cat]:
        ranks, n = metric_ranks[(cat, label)]
        if not ranks:
            metric_best_worst[(cat, label)] = None
            continue
        best = min(ranks.items(), key=lambda kv: kv[1])[0]
        worst = max(ranks.items(), key=lambda kv: kv[1])[0]
        metric_best_worst[(cat, label)] = {
            "best": {"name": best, "slug": slugify(best)},
            "worst": {"name": worst, "slug": slugify(worst)},
        }

# ranked list of every district (for the state's "District Performance"
# sub-tab - same 9-column shape as CLF Rankings, one level up).
district_rankings = [
    {
        "name": dname, "slug": slugify(dname), "n_clfs": agg["overview"]["n_clfs"],
        "overall_score": agg["scoring"]["overall_score"],
        "state_rank": agg["scoring"]["overall_state_rank"], "n_state": agg["scoring"]["n_districts"],
        "categories": agg["scoring"]["category_scores"],
    }
    for dname, agg in district_aggs.items()
]
district_rankings.sort(key=lambda r: (r["state_rank"] is None, r["state_rank"]))
print(f"[{elapsed()}] Pass 2 (ranking) complete.")

for dname, agg in district_aggs.items():
    slug = slugify(dname)
    with open(f"{DATA_DIR}/districts/{slug}.json", "w") as f:
        json.dump(agg, f)
print(f"[{elapsed()}] Wrote {len(district_aggs)} district JSON files.")

# ============================================================================
# 4. State aggregate - same function, over every CLF statewide; no
# cross-group rank above state, so those fields stay None. CLF Rankings is
# top 20 + bottom 20 by state rank (all 1667 CLFs would be far too large a
# table and defeats the point of a "state view").
# ============================================================================
print(f"[{elapsed()}] Aggregating state (all {len(ALL_CLFS)} CLFs)...")
state_agg = aggregate_group(ALL_CLFS, "Bihar", clf_ranking_mode="top_bottom_20")
for cat in CATS:
    for m in state_agg["scoring"]["category_metrics"][cat]:
        bw = metric_best_worst.get((cat, m["label"]))
        m["best_district"] = bw["best"] if bw else None
        m["worst_district"] = bw["worst"] if bw else None
state_agg["scoring"]["district_rankings"] = district_rankings
with open(f"{DATA_DIR}/state.json", "w") as f:
    json.dump(state_agg, f)
print(f"[{elapsed()}] Wrote state.json.")

# ============================================================================
# 5. District/state-worded CONTEXT and ERR_MSG copy, merged into the
# existing shared.json (which already carries the CLF-worded versions
# written by build_tracker_data.py) rather than overwriting the file.
# ============================================================================
DISTRICT_CONTEXT = {
    "overview": "A snapshot of the district's CLFs, aggregated across all of them. <b>Profile</b> covers CLF status distribution and district-wide governance structure (Executive Committee totals, subcommittee membership, summed across CLFs). <b>Members</b> covers social inclusion and welfare coverage, education levels, livelihood diversification, special project activities, and the district's cadre roster - all summed or member-weighted-averaged across every CLF in the district.",
    "audit": "The district's most recent audit results (FY 2025-26, Q4), averaged across every CLF with an audit on file: average grade and score, how this district compares statewide, a breakdown across the 6 scoring categories, the individual line items behind each category, how many CLFs were flagged for a financial irregularity (and what kind), and a cash book vs. physical cash reconciliation check.",
    "financial": "The district's balance sheet, quarterly cash flow, and credit disbursement, summed across every CLF for a selected quarter, with every ratio recomputed from those summed totals (not averaged CLF-by-CLF). <b>Summary</b> gives key ratios (liquidity, fund deployment, surplus/deficit, books balance check) and a breakdown of where capital and cash came from and went to. <b>Statements</b> shows the full balance sheet and receipts &amp; payments statement, summed line-by-line across every CLF.",
    "vrf": "Tracks the district's Vulnerability Reduction Fund, rolled up from every VO across every CLF in the district. <b>KPI Snapshot</b> gives district-wide totals and fund health. <b>Forecasts</b> projects where the fund is headed by 31 March 2027 under three lending-activity scenarios, summed across every CLF's own forecast.",
    "vprp": "Requests and plans raised through VPRP, by year (2023-2025), summed across every CLF in the district. <b>Entitlements</b> tracks government scheme demands (ration cards, pensions, insurance, etc.) and NREGA job card access. <b>PGSRD</b> (Public Goods, Services, and Resource Development) tracks requests for public infrastructure, resources, and services. <b>SDP</b> (Social Development Plan) tracks broader social issues raised and the government departments involved.",
    "scoring": "Combines every other tab into one performance score for the district, against Bihar statewide. <b>Overall</b> gives the district's average Overall Score (equal weight per CLF) plus a category breakdown. <b>By Category</b> shows every individual metric behind the 5 categories, averaged across the district's own CLFs. <b>CLF Rankings</b> lists every CLF in the district side by side, ranked by Overall Score.",
}
DISTRICT_ERR_MSG_TMPL = {
    "audit_scores": "We could not locate Audit Scores for any CLF in {name} district.",
    "financial_irregularities": "We could not locate Financial Irregularities data for any CLF in {name} district.",
    "balance_sheet": "We could not locate Balance Sheet data for {name} district in this quarter.",
    "receipts_payments": "We could not locate Receipts & Payments data for {name} district in this quarter.",
    "transactions": "We could not locate Transactions data for {name} district in this quarter.",
    "vrf": "We could not locate VRF data for any CLF in {name} district.",
    "vprp_ent": "We could not locate Entitlements data for {name} district in {year}.",
    "vprp_pgsrd": "We could not locate PGSRD data for {name} district in {year}.",
    "vprp_sdp": "We could not locate SDP data for {name} district in {year}.",
    "not_found": "Not Found",
}
STATE_CONTEXT = {
    "overview": "A snapshot of every CLF in Bihar, aggregated statewide. <b>Profile</b> covers CLF status distribution and statewide governance structure (Executive Committee totals, subcommittee membership, summed across every CLF). <b>Members</b> covers social inclusion and welfare coverage, education levels, livelihood diversification, special project activities, and the statewide cadre roster.",
    "audit": "The most recent audit results statewide (FY 2025-26, Q4), averaged across every CLF with an audit on file: average grade and score, a breakdown across the 6 scoring categories, the individual line items behind each category, how many CLFs were flagged for a financial irregularity (and what kind), and a cash book vs. physical cash reconciliation check.",
    "financial": "The balance sheet, quarterly cash flow, and credit disbursement of every CLF in Bihar, summed for a selected quarter, with every ratio recomputed from those summed totals (not averaged CLF-by-CLF). <b>Summary</b> gives key ratios and a breakdown of where capital and cash came from and went to. <b>Statements</b> shows the full balance sheet and receipts &amp; payments statement, summed line-by-line across every CLF statewide.",
    "vrf": "Tracks Bihar's Vulnerability Reduction Fund, rolled up from every VO across every CLF in the state. <b>KPI Snapshot</b> gives statewide totals and fund health. <b>Forecasts</b> projects where the fund is headed by 31 March 2027 under three lending-activity scenarios, summed across every CLF's own forecast.",
    "vprp": "Requests and plans raised through VPRP, by year (2023-2025), summed across every CLF in Bihar. <b>Entitlements</b> tracks government scheme demands and NREGA job card access. <b>PGSRD</b> tracks requests for public infrastructure, resources, and services. <b>SDP</b> tracks broader social issues raised and the government departments involved.",
    "scoring": "Combines every other tab into one statewide performance score. <b>Overall</b> gives the average Overall Score across every CLF in Bihar (equal weight per CLF) plus a category breakdown. <b>By Category</b> shows every individual metric behind the 5 categories, averaged statewide. <b>CLF Rankings</b> shows the top 20 and bottom 20 CLFs in Bihar by Overall Score.",
}
STATE_ERR_MSG = {
    "audit_scores": "We could not locate Audit Scores for any CLF statewide.",
    "financial_irregularities": "We could not locate Financial Irregularities data for any CLF statewide.",
    "balance_sheet": "We could not locate Balance Sheet data statewide in this quarter.",
    "receipts_payments": "We could not locate Receipts & Payments data statewide in this quarter.",
    "transactions": "We could not locate Transactions data statewide in this quarter.",
    "vrf": "We could not locate VRF data for any CLF statewide.",
    "vprp_ent": "We could not locate Entitlements data statewide in {year}.",
    "vprp_pgsrd": "We could not locate PGSRD data statewide in {year}.",
    "vprp_sdp": "We could not locate SDP data statewide in {year}.",
    "not_found": "Not Found",
}

# ============================================================================
# 6. District/state-worded hover tooltips (TIPS), for the subset of keys
# actually reachable from District/State views - traced by grepping every
# TIPS[...] reference in make_shell.py and cross-checking which render
# functions district/state actually use (reused-unchanged Financial/VPRP/VRF
# KPI+Forecasts functions, plus this project's own Group Overview code, plus
# the metric-name fallback lookup in scoreRankBar/metricStateBlock for the
# 10 metrics shown in By Category) - the CLF tracker's own TIPS dict has
# ~90 entries, most of which (President, Secretary, VO/Bookkeeper tables,
# etc.) simply never render at district/state scope, so only these ~32 need
# a district-voice and state-voice copy. The 10 By Category metric tips get
# genuinely DIFFERENT wording per level, not just a swapped noun: district's
# "By Category" still shows a real percentile bar (scoreRankBar), so its
# copy stays percentile-framed; state's shows a raw value + best/worst
# district instead (metricStateBlock, since a percentile-of-the-whole-state-
# against-itself is close to tautological - see build_district_state_data.py's
# own category_metrics comment above), so its copy must describe that
# framing instead, not claim to be a percentile it no longer is.
# ============================================================================
DISTRICT_TIPS = {
    "Assets Composition": "How this district's summed total assets break down across every balance-sheet line item - cash, bank balance, loans out to SHGs/VOs, fixed assets, and the rest.",
    "Liabilities Composition": "How this district's summed total liabilities and equity break down across every balance-sheet line item - government grants, member capital, institutional loans, savings held on behalf of VOs/SHGs, and the rest.",
    "Where Money Came In From": "What this quarter's receipts were made up of, summed across every CLF in the district - savings deposits, interest earned, loan repayments, and other income.",
    "Where Money Went Out To": "What this quarter's payments were made up of, summed across every CLF in the district - loans and advances to member CBOs, running costs, and other expenses.",
    "Balance Sheet": "The district's full balance sheet, summed line-by-line across every CLF with one on file.",
    "Receipts & Payments Statement": "The district's full receipts and payments statement for the selected quarter, summed line-by-line across every CLF.",
    "Savings as Promised": "How much of the savings VOs were supposed to contribute has actually been collected, across this district.",
    "What the Fund Is Made Of": "How this district's VRF fund breaks down between the original government grant, member savings, and interest earned, summed across its CLFs.",
    "Grant Not Fully Received": "How many VOs in this district are still owed part of their VRF grant.",
    "Fund Sitting Idle": "How many VOs in this district that have received VRF funds earned no interest from lending them out last year.",
    "Fund Projections": "Where this district's savings, interest, and total fund size are headed by 31 March 2027, summed across every CLF's own forecast.",
    "Social Development Fund": "How much this district's CLFs should be collecting from their VOs each month, and each year, for the Social Development Fund, combined.",
    "Month-by-Month Projection": "A month-by-month look at how this district's savings, interest, and total fund are projected to grow.",
    "Demand & Access by Scheme": "Which government schemes this district's members are demanding most in the selected year. There's no fulfilment tracking for individual schemes in this data - only the NREGA job card's own accessed status is available, shown separately above.",
    "Subcommittee Membership": "How many members sit on each of the six functional subcommittees, summed across every CLF in the district.",
    "Fund Deployment": "This district's average fund deployment ratio, converted to a percentile against other districts.",
    "Bookkeeping Accuracy": "How closely this district's summed Assets match its summed Liabilities+Equity, converted to a percentile - a closer match ranks higher.",
    "VRF Fund Health": "This district's VRF fund-management metrics, each converted to a percentile against other districts.",
    "Subcommittee Completeness": "How many subcommittees are staffed on average across this district's CLFs, converted to a percentile.",
    "Active Membership": "The share of this district's members currently marked active, converted to a percentile.",
    "Cadre Diversity": "How many different specialised cadre roles this district's CLFs have staffed on average, converted to a percentile.",
    "Insurance Coverage": "The share of this district's members with insurance, converted to a percentile.",
    "Aadhaar KYC Coverage": "The share of this district's members with Aadhaar KYC verified, converted to a percentile.",
    "Livelihoods Diversification": "The share of this district's members with more than one livelihood activity, converted to a percentile.",
    "Overall Score": "This district's overall performance score, combining fund management, financial health, governance, and inclusion into one number, and how it compares to other districts across Bihar.",
}
STATE_TIPS = {
    "Assets Composition": "How Bihar's summed total assets break down across every balance-sheet line item - cash, bank balance, loans out to SHGs/VOs, fixed assets, and the rest.",
    "Liabilities Composition": "How Bihar's summed total liabilities and equity break down across every balance-sheet line item - government grants, member capital, institutional loans, savings held on behalf of VOs/SHGs, and the rest.",
    "Where Money Came In From": "What this quarter's receipts were made up of, summed across every CLF in Bihar - savings deposits, interest earned, loan repayments, and other income.",
    "Where Money Went Out To": "What this quarter's payments were made up of, summed across every CLF in Bihar - loans and advances to member CBOs, running costs, and other expenses.",
    "Balance Sheet": "Bihar's full balance sheet, summed line-by-line across every CLF with one on file.",
    "Receipts & Payments Statement": "Bihar's full receipts and payments statement for the selected quarter, summed line-by-line across every CLF.",
    "Savings as Promised": "How much of the savings VOs were supposed to contribute has actually been collected, across Bihar.",
    "What the Fund Is Made Of": "How Bihar's VRF fund breaks down between the original government grant, member savings, and interest earned, summed across every CLF.",
    "Grant Not Fully Received": "How many VOs in Bihar are still owed part of their VRF grant.",
    "Fund Sitting Idle": "How many VOs in Bihar that have received VRF funds earned no interest from lending them out last year.",
    "Fund Projections": "Where Bihar's savings, interest, and total fund size are headed by 31 March 2027, summed across every CLF's own forecast.",
    "Social Development Fund": "How much CLFs across Bihar should be collecting from their VOs each month, and each year, for the Social Development Fund, combined.",
    "Month-by-Month Projection": "A month-by-month look at how Bihar's savings, interest, and total fund are projected to grow.",
    "Demand & Access by Scheme": "Which government schemes Bihar's members are demanding most in the selected year. There's no fulfilment tracking for individual schemes in this data - only the NREGA job card's own accessed status is available, shown separately above.",
    "Subcommittee Membership": "How many members sit on each of the six functional subcommittees, summed across every CLF in Bihar.",
    "Fund Deployment": "Bihar's actual average fund deployment ratio (not a percentile - see the note above), plus which district is doing best and worst on it.",
    "Bookkeeping Accuracy": "How closely Bihar's summed Assets match its summed Liabilities+Equity, shown as the actual accuracy figure, plus which district is doing best and worst.",
    "VRF Fund Health": "Bihar's VRF fund-management metrics, shown as actual values, plus which district is doing best and worst on each.",
    "Subcommittee Completeness": "How many subcommittees are staffed on average across Bihar's CLFs, plus which district is doing best and worst.",
    "Active Membership": "The share of members currently marked active across Bihar, plus which district is doing best and worst.",
    "Cadre Diversity": "How many different specialised cadre roles are staffed on average across Bihar's CLFs, plus which district is doing best and worst.",
    "Insurance Coverage": "The share of members with insurance across Bihar, plus which district is doing best and worst.",
    "Aadhaar KYC Coverage": "The share of members with Aadhaar KYC verified across Bihar, plus which district is doing best and worst.",
    "Livelihoods Diversification": "The share of members with more than one livelihood activity across Bihar, plus which district is doing best and worst.",
    "Overall Score": "Bihar's overall performance score, combining fund management, financial health, governance, and inclusion into one number, averaged equally across every CLF in the state.",
}

shared = json.load(open(f"{DATA_DIR}/shared.json"))
shared["district_context"] = DISTRICT_CONTEXT
shared["district_err_msg_tmpl"] = DISTRICT_ERR_MSG_TMPL
shared["state_context"] = STATE_CONTEXT
shared["state_err_msg"] = STATE_ERR_MSG
shared["district_tips"] = DISTRICT_TIPS
shared["state_tips"] = STATE_TIPS
shared["district_slugs"] = {dname: slugify(dname) for dname in district_aggs}
with open(f"{DATA_DIR}/shared.json", "w") as f:
    json.dump(shared, f)
print(f"[{elapsed()}] Updated shared.json with district/state context+err_msg+tips.")

print(f"[{elapsed()}] Done.")
