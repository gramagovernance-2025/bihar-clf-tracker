"""
Author:   Claude (for Mohan)
Created:  01/08/2026
Purpose:  Statewide scale-up of the single-CLF comprehensive tracker prototype
          (build_comprehensive_clf_tracker.py). Same architecture pattern as
          the VRF tracker's own Scale-Up (5_VRF_Analysis/3_Output/CLF Tracker/
          Scale-Up/): a Python pipeline computes every domain's data for
          EVERY CLF once and writes it out as JSON-per-unit, and a separate
          thin fetch-based HTML shell (make_shell.py -> index.html) fetches
          the right CLF's JSON at runtime instead of baking one CLF's data
          into the page. Chosen for the same reason VRF's team chose it: a
          statewide file with every CLF's data inlined would run to tens of
          MB and load slowly on the field audience's likely low-end devices.

          This script is a refactor, not a rewrite: every formula, column
          name, and edge-case guard is copied verbatim from the single-CLF
          prototype (which stays untouched as the frozen reference/prototype
          file) - only the *structure* changes, splitting each domain's code
          into (a) one-time setup that doesn't depend on which CLF is being
          built (raw file loading, peer-population computation, percentile/
          rank columns), and (b) a per-CLF function called once per CLF in
          the master list.

          Three real inefficiencies in the single-CLF version's per-CLF path
          are fixed here, since they'd be paid ~1,687 times instead of once:
            1. F01/F03/F05 raw CSVs were re-read from disk per CLF (one read
               per district file, per CLF, i.e. the same district's file read
               ~44 times on average) purely to find that ONE CLF's own row.
               Now read once (all districts, all years/quarters) and every
               CLF's own row is just a filter against the already-loaded
               dataframe - the exact same dataframe already needed for the
               Scoring peer population, so this also removes a full second
               copy of the same data from memory.
            2. `audits_clf_transactions_merged.dta` was read from disk 3
               separate times (once full for Overview/Audit's `audit_all`,
               once column-subset for the audit-score population `a`, once
               column-subset again for Scoring's `audit_spa`) - now read once
               and every downstream use slices the same in-memory dataframe.
            3. VRF's `clf_agg_all` peer-population aggregation (a groupby+
               apply across ~60k VO rows) now runs exactly once, not once per
               CLF - this was already correctly placed outside the "target
               CLF" logic in the prototype, just confirming it stays that way
               here.

Input Data:  2_Data/Processed/{clf_id_crosswalk, f0x_vprp_clf_crosswalk}.dta
             2_Data/Cleaned/{audits_clf_transactions_merged, lokos_groups_clean,
                lokos_members_clf_collapsed, clf_vprp_entitlements,
                clf_pgsrd_requests, clf_sdp}.dta
             2_Data/Raw Files/{F01 Balance Sheets, F03 Receipts & Payments,
                F05 Transactions}/*_CLF_level.csv
             5_VRF_Analysis/2_Data/2_Clean_Data/vo_vrf_final.dta

Output Data: 3_Output/CLF Tracker/Scale-Up/data/clfs/{mis_id}.json (one per CLF)
             3_Output/CLF Tracker/Scale-Up/data/manifest.json (district/block/
                CLF hierarchy + counts, for the shell's search/finder)
             3_Output/CLF Tracker/Scale-Up/data/shared.json (TIPS/ERR_MSG/
                FOOTNOTES - identical for every CLF, so loaded once by the
                shell rather than duplicated into every one of the ~1,687
                per-CLF files)
"""

import json
import glob
import re
import os
import time
import numpy as np
import pandas as pd

pd.set_option("mode.chained_assignment", None)

BASE = "/Users/mohanrajagopal/Dropbox/Bihar Gates Team/3_Jeevika/6_LokOS_Analysis"
CLEANED = f"{BASE}/2_Data/Cleaned"
RAWF = f"{BASE}/2_Data/Raw Files"
PROCESSED = f"{BASE}/2_Data/Processed"
VRF_CLEAN = "/Users/mohanrajagopal/Dropbox/Bihar Gates Team/3_Jeevika/5_VRF_Analysis/2_Data/2_Clean_Data"
OUT_DIR = f"{BASE}/3_Output/CLF Tracker/Scale-Up"
DATA_DIR = f"{OUT_DIR}/data"
os.makedirs(f"{DATA_DIR}/clfs", exist_ok=True)

T0 = time.time()
def elapsed(): return f"{time.time()-T0:.1f}s"

# error-message copy, verbatim from CLF_Tracker_Content_Script_edited.docx's
# "Error Messages" section - single source of truth for both Python (data
# flags) and JS (display) so the wording never drifts between the two.
ERR_MSG = {
    "audit_scores": "We could not locate Audit Scores for your CLF.",
    "financial_irregularities": "We could not locate Financial Irregularities data for your CLF.",
    "balance_sheet": "We could not locate Balance Sheet data for your CLF in this quarter.",
    "receipts_payments": "We could not locate Receipts & Payments data for your CLF in this quarter.",
    "transactions": "We could not locate Transactions data for your CLF in this quarter.",
    "vrf": "We could not locate VRF data for your CLF.",
    "vprp_ent": "We could not locate Entitlements data for your CLF in {year}.",
    "vprp_pgsrd": "We could not locate PGSRD data for your CLF in {year}.",
    "vprp_sdp": "We could not locate SDP data for your CLF in {year}.",
    "not_found": "Not Found",
}

# ============================================================================
# ONE-TIME SETUP - everything below runs exactly once, regardless of how many
# CLFs get built. Nothing in this section may reference a specific CLF.
# ============================================================================
print(f"[{elapsed()}] Loading crosswalks...")
master = pd.read_stata(f"{PROCESSED}/clf_id_crosswalk.dta", convert_categoricals=True)
xwalk = pd.read_stata(f"{PROCESSED}/f0x_vprp_clf_crosswalk.dta")
xwalk_by_mis = xwalk.set_index("mis_id")

# ---- Overview + Audit sources (one single read of the audit-merged file,
# reused for Overview's clf_audit (Q4 lookup), the Q4 category-score
# population, AND Scoring's SPA/status population - was 3 separate reads) ----
print(f"[{elapsed()}] Loading groups/audit/members...")
g = pd.read_stata(f"{CLEANED}/lokos_groups_clean.dta", convert_categoricals=True)
audit_all = pd.read_stata(f"{CLEANED}/audits_clf_transactions_merged.dta", convert_categoricals=True)
mem = pd.read_stata(f"{CLEANED}/lokos_members_clf_collapsed.dta", convert_categoricals=False)

def cat_scores(df):
    df = df.copy()
    df['acc'] = df[['accounting_books_updated','cash_bank_balance','outstanding_loan_verification','mis_quality','fixed_assets_verification']].sum(axis=1, min_count=1)
    df['rp'] = df[['receipts','payments']].sum(axis=1, min_count=1)
    df['loan'] = df[['loan_demand_review','loan_repayment_score']].sum(axis=1, min_count=1)
    df['grp'] = df[['shg_performance','vo_performance','clf_performance']].sum(axis=1, min_count=1)
    return df

aq4 = cat_scores(audit_all[audit_all['quarter'] == 4].copy())
aq4['acc_pct'] = aq4['acc']/19*100
aq4['rp_pct'] = aq4['rp']/20*100
aq4['loan_pct'] = aq4['loan']/18*100
aq4['grp_pct'] = aq4['grp']/23*100
aq4['cc_pct'] = aq4['community_coordinator_management']/10*100
aq4['oss_pct'] = aq4['operational_self_sufficiency']/10*100

# ---- Financial-irregularity classifier, ported verbatim from the audit
# dashboard (3_Output/Dashboard/build_dashboard_q4.py:117-162) - most non-
# blank text in these two fields is boilerplate "no issue found" (including a
# comma-joined "False,False,..." checkbox export artifact), not a real issue
# description, so a non-blank check alone drastically over-flags CLFs. ----
CLEAN_EXACT = {
    "NO", "NA", "N/A", "NIL", "NONE", "NOT APPLICABLE",
    "NOT OBSERVED DURING THE AUDIT", "NO ISSUE FOUND",
    "THERE IS NO SUCH IRREGULARITIES",
}
CHECKBOX_RE = re.compile(r"^(TRUE|FALSE)(,(TRUE|FALSE))*,?$", re.IGNORECASE)
HINDI_NEG_RE = re.compile(r"(नहीं|नही)\s*(पाया गया|है)")
ENG_NEG_RE = re.compile(r"no\s+any\s+financial\s+irregularit|no\s+financial\s+irregularit", re.IGNORECASE)

def classify_clean(series):
    s = series.fillna("").astype(str).str.strip()
    s_norm = s.str.rstrip(",.").str.strip()
    upper = s_norm.str.upper()
    clean = (s == "") | upper.isin(CLEAN_EXACT)
    clean |= upper.str.match(CHECKBOX_RE)
    clean |= s.str.contains("गबन", regex=False) & s.str.contains(HINDI_NEG_RE)
    clean |= s.str.contains(ENG_NEG_RE)
    return clean

aq4['clean_a'] = classify_clean(aq4['financial_irregularity_audit'])
aq4['clean_r'] = classify_clean(aq4['financial_irregularities_report'])
aq4['issue_flagged_any'] = ~aq4['clean_a'] | ~aq4['clean_r']
aq4['combined_irreg_text'] = aq4['financial_irregularity_audit'].fillna("") + " " + aq4['financial_irregularities_report'].fillna("")

# non-exclusive keyword tags - a flagged row can match more than one category
IRREG_CATS = {
    "bank_recon":  ("Bank Reconciliation (BRS)", r"BRS|बैंक\s*समाधान|बैंक\s*समाधन|bank reconcil"),
    "cash_register": ("Cash Book / Register Maintenance", r"रोकड़|cash\s*book|कैश\s*बुक|register|रजिस्टर|पंजी|बही(?!खाता)|संधारण|\bBOR\b"),
    "voucher": ("Payment Vouchers", r"विपत्र|voucher|बिल(?!ियन)|बिपत्र"),
    "loan_icf": ("Loan / ICF", r"ऋण|loan|ICF|repayment|वापसी|मांग पंजी|demand register|समान्य ऋण|सामान्य ऋण"),
    "mis_entry": ("MIS / Data Entry", r"\bMIS\b|प्रविष्टि|entry|एंट्री|प्राप्ति.{0,10}भुगतान"),
    "staffing": ("Staffing (MBK/Cadre)", r"MBK|मास्टर बुककीपर|कैडर|CADRE|carder|बुककीपर|bookkeeper|नियुक्ति|appointment|\bCF\b"),
    "verification": ("Verification", r"सत्यापन|verification|भौतिक सत्यापन|physical verif"),
    "prior_unresolved": ("Prior Objection Unresolved", r"गत\s*अंकेक्षण|पिछल[ेी]\s*अंकेक्षण|पूर्व.{0,6}आपत्ति|previous.{0,10}(objection|quarter)|पछल.{0,6}आपत्ति|अनुपालन नहीं"),
}
for _key, (_, _pat) in IRREG_CATS.items():
    aq4[f'iss_{_key}'] = aq4['issue_flagged_any'] & aq4['combined_irreg_text'].str.contains(_pat, case=False, regex=True, na=False)

CATS = [("Accounting", "acc_pct", 19, "acc"), ("Receipts & Payments", "rp_pct", 20, "rp"),
        ("Loan", "loan_pct", 18, "loan"), ("Group Performance", "grp_pct", 23, "grp"),
        ("Cadre Management", "cc_pct", 10, "community_coordinator_management"),
        ("Operational Self-Sufficiency", "oss_pct", 10, "operational_self_sufficiency")]
item_defs = [
    ("Books Updated", "accounting_books_updated", 5, "Accounting"), ("Cash/Bank Verification", "cash_bank_balance", 4, "Accounting"),
    ("Loan Verification", "outstanding_loan_verification", 4, "Accounting"), ("MIS Quality", "mis_quality", 2, "Accounting"),
    ("Fixed Assets Verification", "fixed_assets_verification", 4, "Accounting"), ("Receipts", "receipts", 7, "Receipts & Payments"),
    ("Payments", "payments", 13, "Receipts & Payments"), ("Loan Demand Review", "loan_demand_review", 8, "Loan"),
    ("Loan Repayment", "loan_repayment_score", 10, "Loan"), ("SHG Performance", "shg_performance", 13, "Group Performance"),
    ("VO Performance", "vo_performance", 4, "Group Performance"), ("CLF Performance", "clf_performance", 6, "Group Performance"),
]

def pctl(series, value):
    s = series.dropna()
    return round((s <= value).mean() * 100)

def rank_of(series, value):
    s = series.dropna()
    if pd.isna(value) or len(s) == 0: return None
    return int((s > value).sum()) + 1

# ---- VRF peer population (expensive groupby+apply - runs exactly once) ----
print(f"[{elapsed()}] Loading + aggregating VRF...")
vrf = pd.read_stata(f"{VRF_CLEAN}/vo_vrf_final.dta", convert_categoricals=False)
METRICS = ['savings_discipline_rate','savings_realisation_ratio','corpus_multiplier','interest_yield']

def wavg(grp, col):
    return (grp[col] * grp['totalshgmembers']).sum() / grp['totalshgmembers'].sum()

clf_agg_all = vrf.groupby('clf_id').apply(lambda grp: pd.Series({
    'district_name': grp['district_name'].iloc[0],
    'totalshgmembers': grp['totalshgmembers'].sum(),
    **{m: wavg(grp, m) for m in METRICS},
    'coverage_complete': 1 - wavg(grp, 'incomplete_vrf_coverage'),
}), include_groups=False)

def inclusive_pctl(series):
    # matches the VRF tracker's own documented convention (its CLAUDE.md:
    # "the ranking percentile convention is inclusive (<=, not strict <)").
    s = series.dropna()
    return series.apply(lambda v: (s <= v).mean() * 100 if pd.notna(v) else np.nan)

ALLM = METRICS + ['coverage_complete']
for m in ALLM:
    clf_agg_all[f'{m}_state_pctl'] = inclusive_pctl(clf_agg_all[m])
clf_agg_all['composite'] = clf_agg_all[[f'{m}_state_pctl' for m in ALLM]].mean(axis=1)
clf_agg_all['composite_state_pctl'] = inclusive_pctl(clf_agg_all['composite'])
clf_agg_all['composite_dist_pctl'] = clf_agg_all.groupby('district_name')['composite'].transform(inclusive_pctl)
for m in ALLM:
    clf_agg_all[f'{m}_dist_pctl'] = clf_agg_all.groupby('district_name')[m].transform(inclusive_pctl)
clf_agg_all['composite_state_rank'] = clf_agg_all['composite'].rank(method='min', ascending=False)
clf_agg_all['composite_dist_rank'] = clf_agg_all.groupby('district_name')['composite'].rank(method='min', ascending=False)
for m in ALLM:
    clf_agg_all[f'{m}_state_rank'] = clf_agg_all[m].rank(method='min', ascending=False)
    clf_agg_all[f'{m}_dist_rank'] = clf_agg_all.groupby('district_name')[m].rank(method='min', ascending=False)
metric_labels = {'savings_discipline_rate': 'Savings Discipline', 'savings_realisation_ratio': 'Savings Realisation',
                  'corpus_multiplier': 'Corpus Multiplier', 'interest_yield': 'Interest Yield',
                  'coverage_complete': 'Coverage Completion'}
VRF_MONTHLY_RATE = 0.0075  # official BRLPS VRF rate, confirmed in the VRF tracker's own docs

# ---- Financial: one read of every raw F0x file (all districts, all years/
# quarters) - serves BOTH each CLF's own-row lookup AND (via a filtered copy)
# the Scoring peer population, instead of the prototype's per-district re-read. ----
print(f"[{elapsed()}] Loading F01/F03/F05 (all districts)...")
QUARTERS = [("2025 - 2026", "Q1 (Apr-Jun)", "FY25-26 Q1"), ("2025 - 2026", "Q2 (Jul-Sep)", "FY25-26 Q2"),
            ("2025 - 2026", "Q3 (Oct-Dec)", "FY25-26 Q3"), ("2025 - 2026", "Q4 (Jan-Mar)", "FY25-26 Q4"),
            ("2026 - 2027", "Q1 (Apr-Jun)", "FY26-27 Q1")]

f01_raw = pd.concat([pd.read_csv(f) for f in glob.glob(f"{RAWF}/F01 Balance Sheets/F01_*_CLF_level.csv")], ignore_index=True)
f03_raw = pd.concat([pd.read_csv(f) for f in glob.glob(f"{RAWF}/F03 Receipts & Payments/F03_*_CLF_level.csv")], ignore_index=True)
f05_raw = pd.concat([pd.read_csv(f) for f in glob.glob(f"{RAWF}/F05 Transactions/F05_*_CLF_level.csv")], ignore_index=True)

# --- 6a. Financial-metric peer population: percentiles only need each peer
# CLF's *value*, not its identity, so no ID-matching is needed here at all -
# that's only needed to find OUR OWN CLF's row (done per-CLF below via the
# crosswalk's raw name). ---
f01_m = f01_raw.rename(columns={'district_name': 'district'}).copy()
f01_m['deployment_ratio'] = (f01_m['Loan to SHGs/VOs'] + f01_m['Loan to Non-Nodal CLFs'] + f01_m['Advances to SHGs/ VOs'] + f01_m['Advances to Others']) / (f01_m['Total Assets'] - f01_m['Fixed Assets']) * 100
f01_m['surplus_pct'] = f01_m['Surplus / Deficit'] / f01_m['Total Assets'] * 100
f01_m['balance_accuracy'] = 100 - (f01_m['Total Assets'] - f01_m['Total Equity & Liabilities']).abs() / f01_m['Total Equity & Liabilities'] * 100

# Scoring's F03/F05-derived metrics are quarter-specific (unlike F01, a single
# point-in-time snapshot) - build one peer population per quarter so each
# CLF's percentile is always computed against ITS SELECTED quarter's
# statewide distribution, not always the latest quarter.
xwalk_master = xwalk.merge(master[['mis_id', 'clfcode', 'district', 'block']], on='mis_id', how='inner')
xwalk_master = xwalk_master.merge(mem[['clf_code', 'n_members']], left_on='clfcode', right_on='clf_code', how='left')

f03_m_by_q, f05_m_by_q, demand_pop_by_q = {}, {}, {}
for _fy, _q, _label in QUARTERS:
    f03_q = f03_raw[(f03_raw['financial_year'] == _fy) & (f03_raw['quarter'] == _q) & (f03_raw['Total Receipts'] > 0)].copy()
    f03_q = f03_q.rename(columns={'district_name': 'district'})
    f03_q['interest_income_share'] = f03_q['Interest Received From CBOs'] / f03_q['Total Receipts'] * 100
    # Total Receipts always equals Total Payments by construction (Opening
    # Balance is booked as a receipt, Closing Balance as a payment) - net cash
    # flow is the change in cash position instead.
    f03_q['net_cash_flow'] = f03_q['Closing Balance (₹)'] - f03_q['Opening Balance (₹)']
    f03_m_by_q[_label] = f03_q

    f05_q = f05_raw[(f05_raw['financial_year'] == _fy) & (f05_raw['quarter'] == _q)].copy()
    f05_q = f05_q.rename(columns={'district_name': 'district'})
    f05_q['this_qtr_disb_rate'] = f05_q.apply(lambda r: r['New Loan Disbursed'] / r['newLoanDemand'] * 100 if r['newLoanDemand'] > 0 else np.nan, axis=1)
    f05_m_by_q[_label] = f05_q

    # member-weighted loan demand peer population - unlike the ratio metrics
    # above (which only need each peer's *value*, not its identity), this
    # needs each F05 row's own member count, which lives in a completely
    # different source (lokos_members_clf_collapsed, keyed by clf_code) - so
    # this one population does need real identity matching, via the same
    # crosswalk used for each CLF's own lookup.
    demand_q = xwalk_master.merge(
        f05_q[['CLF Name', 'district', 'newLoanDemand']],
        left_on=['f05_raw_name', 'district'], right_on=['CLF Name', 'district'], how='inner')
    demand_q = demand_q[demand_q['n_members'] > 0].copy()
    demand_q['demand_per_member'] = demand_q['newLoanDemand'] / demand_q['n_members']
    demand_pop_by_q[_label] = demand_q

def inclusive_pctl_val(series, value):
    s = series.dropna()
    return round((s <= value).mean() * 100) if pd.notna(value) and len(s) else None

def rank_of_val(series, value, higher_is_better=True):
    s = series.dropna()
    if value is None or (isinstance(value, float) and np.isnan(value)) or len(s) == 0: return None
    better = (s > value).sum() if higher_is_better else (s < value).sum()
    return int(better) + 1

# ---- Governance & Social peer population (full statewide, ID-based) ----
print(f"[{elapsed()}] Building Governance/Social population...")
mem_pop = mem.copy()
g_pop = g.drop_duplicates(subset=['clf_code'])[['clf_code', 'clf_approvalstatus', 'district', 'block']].rename(columns={'district': 'g_district'})
mem_pop = mem_pop.merge(g_pop, on='clf_code', how='left')
subcom_cols = ['clf_special_subcom_mem_count','clf_monit_subcom_mem_count','clf_bl_subcom_mem_count',
               'clf_sa_subcom_mem_count','clf_asset_subcom_mem_count','clf_lp_subcom_mem_count']
g_subcom = g.drop_duplicates(subset=['clf_code'])[['clf_code'] + subcom_cols].copy()
g_subcom['n_subcom_filled'] = (g_subcom[subcom_cols] > 0).sum(axis=1)
mem_pop = mem_pop.merge(g_subcom[['clf_code', 'n_subcom_filled']], on='clf_code', how='left')

audit_spa_q4 = audit_all[audit_all['quarter'] == 4].copy()
audit_spa_q4['n_spa'] = audit_spa_q4[['sp_chc','sp_dkr','sp_dak','sp_clcdc','sp_mahila_samvad','sp_livestock','sp_other']].sum(axis=1)
audit_spa_q4['status_tier_rank'] = audit_spa_q4['model_clf'].fillna(0) * 2 + audit_spa_q4['registered'].fillna(0)
mem_pop = mem_pop.merge(audit_spa_q4[['clfcode', 'n_spa', 'status_tier_rank']], left_on='clf_code', right_on='clfcode', how='left')
mem_pop['approval_rank'] = mem_pop['clf_approvalstatus'].map({"Approved by BM": 3, "Modified (Autosync)": 2, "Pending with BM": 1, "Rejected by BM": 0}).fillna(0)

# ---- VPRP raw sources (full, unfiltered - each CLF's own year filter happens per-CLF) ----
print(f"[{elapsed()}] Loading VPRP...")
ent_raw = pd.read_stata(f"{CLEANED}/clf_vprp_entitlements.dta", convert_categoricals=True)
pgsrd_raw = pd.read_stata(f"{CLEANED}/clf_pgsrd_requests.dta", convert_categoricals=True)
sdp_raw = pd.read_stata(f"{CLEANED}/clf_sdp.dta", convert_categoricals=True)

roles = ["Bank Correspondent Shakhi (BC Shakhi)","Bank Shakhi","Bima Sakhi","Business Development Service Provider (BDSP)",
         "CLF Book Keeper/ Accountant","Community Auditor","Community Coordinator",
         "Community Mobilizer-Facilitator","Community Trainer","Enterprise Promotion (EP)",
         "Financial Literacy Community Resource Person (CRP)","Gender Mitra","Health Activist-Swasthya Sakhi","Krishi Mitra",
         "Master Book Keeper","Matsya Sakhi","PRP","Pashu Sakhi","Samuh Sakhi","Staff in BLF",
         "Staff in CLF","Staff in CTC","Staff in PE","Staff in PG","Staff in VO","Udyog Mitra",
         "VO Book Keeper/ Accountant","Van Sakhi","Woman Activist","e-Community Resource Person / e-Book Keeper / Tab Didi"]

def safe_int(v, default=0):
    return int(v) if pd.notna(v) else default

def safe_str(v, default="Not reported"):
    return str(v).strip() if pd.notna(v) and str(v).strip() else default

# Overall/category-score ranks need every CLF's own score first - populated
# by a lightweight first pass (section 6b, below the per-CLF function) before
# the real per-CLF loop runs. None here just means "not built yet".
RANK_LOOKUP = {label: None for _, _, label in QUARTERS}

print(f"[{elapsed()}] One-time setup complete.")

# ============================================================================
# PER-CLF EXTRACTION - everything below runs once per CLF in the loop.
# ============================================================================
def build_clf_data(mis_id, clfcode, district, block, audit_name):
    # ---- crosswalk row for this CLF (F01/F03/F05/VPRP raw names, may be missing) ----
    xr = xwalk_by_mis.loc[mis_id] if mis_id in xwalk_by_mis.index else pd.Series(dtype=object)
    f01_raw_name = xr.get("f01_raw_name")
    f03_raw_name = xr.get("f03_raw_name")
    f05_raw_name = xr.get("f05_raw_name")
    vprp_ent_raw_name = xr.get("vprp_ent_raw_name")
    vprp_pgsrd_raw_name = xr.get("vprp_pgsrd_raw_name")
    vprp_sdp_raw_name = xr.get("vprp_sdp_raw_name")

    # ========================================================================
    # 1. OVERVIEW
    # ========================================================================
    clf_g = g[g["clf_code"] == clfcode]
    clf_row = clf_g.iloc[0]
    n_vo = clf_g["vo_code"].nunique()
    n_shg = clf_g["shg_code"].nunique()

    clf_audit = audit_all[audit_all["mis_id"] == mis_id].sort_values("quarter")
    _q4_rows = clf_audit[clf_audit["quarter"] == 4]
    q4 = _q4_rows.iloc[0] if len(_q4_rows) else None
    Q4_FOUND = q4 is not None

    mrow = mem[mem["clf_code"] == clfcode].iloc[0]

    overview = {
        "clf_name_lokos": clf_row["clf_name"],
        "clf_name_audit": audit_name,
        "district": district.title(),
        "block": block.title(),
        "mis_id": int(mis_id),
        "clfcode": int(clfcode),
        "n_vo": int(n_vo),
        "n_shg": int(n_shg),
        "n_members": int(mrow["n_members"]),
        "pct_active": round(mrow["pct_active"], 1),
        "n_active": int(round(mrow["n_members"] * mrow["pct_active"] / 100)),
        "formation_date": clf_row["clf_formation_date"].strftime("%d %b %Y") if pd.notna(clf_row["clf_formation_date"]) else None,
        "registration_date": clf_row["clf_registration_date"].strftime("%d %b %Y") if pd.notna(clf_row["clf_registration_date"]) else None,
        "status_tier": (
            "Model & Registered" if q4["model_clf"] == 1 and q4["registered"] == 1 else
            "Model Only" if q4["model_clf"] == 1 else
            "Registered" if q4["registered"] == 1 else
            "Neither"
        ) if Q4_FOUND else None,
        "status_tier_found": Q4_FOUND,
        "approval_status": clf_row["clf_approvalstatus"],
        "cooption_status": clf_row["clf_cooptionstatus"],
        "nic_code": clf_row["clf_nic_code"],
        "ec_count": safe_int(clf_row["clf_ec_mem_count"]),
        "president": safe_str(clf_row["clf_president"], "Not reported").title(),
        "secretary": safe_str(clf_row["clf_secretary"], "Not reported").title(),
        "subcom": {
            "Special": safe_int(clf_row["clf_special_subcom_mem_count"]),
            "Monitoring": safe_int(clf_row["clf_monit_subcom_mem_count"]),
            "Bank Linkage": safe_int(clf_row["clf_bl_subcom_mem_count"]),
            "Social Action": safe_int(clf_row["clf_sa_subcom_mem_count"]),
            "Asset Verification": safe_int(clf_row["clf_asset_subcom_mem_count"]),
            "Livelihoods Promotion": safe_int(clf_row["clf_lp_subcom_mem_count"]),
        },
        "meeting_frequency": safe_str(clf_row["clf_meeting_frequency"]),
        "savings_frequency": safe_str(clf_row["clf_savings_frequency"]),
        "savings_amount": clf_row["clf_savings_amount"],
        "coverage": {
            "Scheduled Castes / Scheduled Tribes (SC/ST)": (round(mrow["pct_scst"], 1), int(mrow["n_scst"])),
            "Other Backward Classes (OBC)": (round(mrow["pct_obc"], 1), int(mrow["n_obc"])),
            "General Category": (round(mrow["pct_general"], 1), int(mrow["n_general"])),
            "Members with Disabilities": (round(mrow["pct_dis_self"], 1), int(mrow["n_dis_self"])),
            "Members with Disabled Family Members": (round(mrow["pct_dis_family"], 1), int(mrow["n_dis_family"])),
            "Family Head": (round(mrow["pct_family_head"], 1), int(mrow["n_family_head"])),
            "Particularly Vulnerable Tribal Groups (PVTG)": (round(mrow["pct_pvtg"], 1), int(mrow["n_pvtg"])),
        },
        "education": {
            "Can read/write": (round(mrow["pct_can_read"], 1), safe_int(mrow["n_can_read"])),
            "Completed primary": (round(mrow["pct_primary_plus"], 1), safe_int(mrow["n_primary_plus"])),
            "Completed secondary": (round(mrow["pct_secondary_plus"], 1), safe_int(mrow["n_secondary_plus"])),
            "Completed graduation": (round(mrow["pct_grad_plus"], 1), safe_int(mrow["n_grad_plus"])),
        },
        "pct_has_livelihood": round(mrow["pct_has_livelihood"], 1),
        "pct_multi_livelihood": round(mrow["pct_multi_livelihood"], 1),
        # Surfaced for the district/state "By Category" tab's raw-value display
        # (Insurance Coverage / Aadhaar KYC Coverage metrics) - previously only
        # available inside the scoring engine's own transient mem_pop table,
        # never written to the per-CLF JSON. approval_status above already
        # carries the raw label Platform Approval Status needs (no new field).
        "pct_insurance": round(mrow["pct_insurance"], 1) if pd.notna(mrow["pct_insurance"]) else None,
        "pct_aadhaar": round(mrow["pct_aadhaar"], 1) if pd.notna(mrow["pct_aadhaar"]) else None,
        "livelihood_split": {
            "Agriculture": round(mrow["pct_lv_agri"], 1),
            "Livestock": round(mrow["pct_lv_livestk"], 1),
            "Wage Labour": round(mrow["pct_lv_wage"], 1),
            "Trade/Enterprise": round(mrow["pct_lv_trade"], 1),
            "Other": round(mrow["pct_lv_other"], 1),
        },
        "n_cadre_holders": int(mrow["n_cadre_holders"]),
        "n_distinct_cadre_types": int(mrow["n_distinct_cadre_role_types"]),
    }

    cadre_roster_items = sorted(
        [(r, int(mrow[f"cadre_{i+1}"])) for i, r in enumerate(roles) if mrow[f"cadre_{i+1}"] > 0],
        key=lambda kv: kv[1], reverse=True)
    overview["cadre_roster"] = dict(cadre_roster_items)

    if Q4_FOUND:
        spa_flags = {
            "Custom Hiring Centre (CHC)": q4["sp_chc"], "Didi Ki Rasoi (DKR)": q4["sp_dkr"],
            "Didi Adhikar Kendra (DAK)": q4["sp_dak"], "Community Library cum Career Development Centers (CLCDC)": q4["sp_clcdc"],
            "Mahila Samvad": q4["sp_mahila_samvad"], "Livestock": q4["sp_livestock"], "Other": q4["sp_other"],
        }
        overview["spa"] = {k: bool(v) for k, v in spa_flags.items()}
    else:
        overview["spa"] = None
    overview["spa_found"] = Q4_FOUND

    # ========================================================================
    # 2. AUDIT (Q4 only, except grade trend)
    # ========================================================================
    _target_a_rows = aq4[aq4['mis_id'] == mis_id]
    AUDIT_Q4_FOUND = len(_target_a_rows) > 0
    dist_pop_a = aq4[aq4['district'] == district.upper()]

    if AUDIT_Q4_FOUND:
        target_a = _target_a_rows.iloc[0]
        category_breakdown = []
        for label, pctcol, maxv, rawcol in CATS:
            v = target_a[pctcol]
            category_breakdown.append({
                "label": label, "pct": round(v) if pd.notna(v) else 0, "raw": target_a[rawcol], "max": maxv,
                "district_pctl": pctl(dist_pop_a[pctcol], v), "state_pctl": pctl(aq4[pctcol], v),
            })
        item_scores = [{"label": lbl, "pct": round((target_a[col] or 0)/mx*100) if pd.notna(target_a[col]) else 0,
                         "raw": target_a[col] if pd.notna(target_a[col]) else 0, "max": mx, "category": cat} for lbl, col, mx, cat in item_defs]
        issue_flagged = bool(target_a['issue_flagged_any'])
        irreg_categories = [label for key, (label, _) in IRREG_CATS.items() if target_a[f'iss_{key}']] if issue_flagged else []
        # only surface raw text from whichever of the two source fields wasn't
        # itself classified "clean" (boilerplate/checkbox artifact) - the other
        # field, even if non-blank, is not genuine irregularity content.
        irreg_text_parts = []
        if issue_flagged and not target_a['clean_a']:
            t = str(target_a['financial_irregularity_audit']).strip()
            if t: irreg_text_parts.append(t)
        if issue_flagged and not target_a['clean_r']:
            t = str(target_a['financial_irregularities_report']).strip()
            if t: irreg_text_parts.append(t)
        irreg_text = ' '.join(irreg_text_parts) if irreg_text_parts else None
        audit = {
            "found": True,
            "grade": target_a['audit_grade'],
            "total_score": target_a['total_marks_audit'],
            "district_pctl": pctl(dist_pop_a['total_marks_audit'], target_a['total_marks_audit']),
            "state_pctl": pctl(aq4['total_marks_audit'], target_a['total_marks_audit']),
            "district_rank": rank_of(dist_pop_a['total_marks_audit'], target_a['total_marks_audit']),
            "state_rank": rank_of(aq4['total_marks_audit'], target_a['total_marks_audit']),
            "n_district": len(dist_pop_a), "n_state": len(aq4),
            "category_breakdown": category_breakdown,
            "item_scores": item_scores,
            "issue_flagged": issue_flagged,
            "irreg_categories": irreg_categories,
            "irreg_text": irreg_text,
            "cash_vs_physical_gap": round(abs(target_a['tb_cb_cashbook'] - target_a['tb_cb_physical']), 2),
            "cash_book_higher": bool(target_a['tb_cb_cashbook'] > target_a['tb_cb_physical']),
        }
    else:
        audit = {"found": False}

    # ========================================================================
    # 3. VRF
    # ========================================================================
    clf_vrf = vrf[vrf['clf_id'] == mis_id].copy()
    VRF_FOUND = len(clf_vrf) > 0
    target_district_vrf = clf_vrf['district_name'].iloc[0] if VRF_FOUND else None

    if VRF_FOUND:
        vrow = clf_agg_all.loc[mis_id]
        dist_pop_v = clf_agg_all[clf_agg_all['district_name'] == target_district_vrf]
        fsf_elig = int((clf_vrf['fsf_eligibile'] == 1).sum())
        incomplete_cov = int((clf_vrf['incomplete_vrf_coverage'] == 1).sum())
        idle_vo = int((clf_vrf['zero_interest_vo'] == 1).sum())
        received_vo = int((clf_vrf['totalvrfreceived'] > 0).sum())

        vo_table = clf_vrf[['vo_id','vo_name','bookkeeper_id','bookkeeper_name','totalshgmembers','fsf_eligibile',
                             'incomplete_vrf_coverage','zero_interest_vo','savings_discipline_rate',
                             'interest_yield','corpus_multiplier']].sort_values('vo_name').to_dict('records')

        bk = clf_vrf.groupby(['bookkeeper_id', 'bookkeeper_name']).apply(lambda grp: pd.Series({
            'n_vo': grp['vo_id'].nunique(), 'members': grp['totalshgmembers'].sum(),
            'sav_disc': wavg(grp, 'savings_discipline_rate'), 'sav_real': wavg(grp, 'savings_realisation_ratio'),
            'corpus_mult': wavg(grp, 'corpus_multiplier'), 'int_yield': wavg(grp, 'interest_yield'),
            'full_cov': (1 - wavg(grp, 'incomplete_vrf_coverage')) * 100,
        }), include_groups=False).reset_index()
        for m in ['sav_disc', 'sav_real', 'corpus_mult', 'int_yield', 'full_cov']:
            bk[f'{m}_rank'] = inclusive_pctl(bk[m])
        bk['composite'] = bk[[f'{m}_rank' for m in ['sav_disc', 'sav_real', 'corpus_mult', 'int_yield', 'full_cov']]].mean(axis=1)
        bk = bk.sort_values('composite', ascending=False).reset_index(drop=True)
        bk['rank'] = bk.index + 1

        sdf_monthly = clf_vrf['monthlysdfcontribution'].sum()

        elig = clf_vrf[clf_vrf['totalvrfreceived'] > 0].copy()
        elig['years_active'] = elig['vo_age'] / 365.25
        elig['s1_annual'] = elig.apply(lambda r: r['vrfinterestamount'] / r['years_active'] if r['years_active'] > 0 else 0, axis=1)
        active_vos = elig[elig['zero_interest_vo'] == 0]
        median_active_pace = active_vos['s1_annual'].median() if len(active_vos) else 0
        elig['s2_annual'] = elig.apply(lambda r: r['s1_annual'] if r['zero_interest_vo'] == 0 else median_active_pace, axis=1)
        elig['s3_annual'] = elig['totalvrfcorpus'] * VRF_MONTHLY_RATE * 12

        savings_addition = (clf_vrf['totalshgmembers'] * clf_vrf['monthlyvrfsavingamount'] * 12).sum()
        savings_now = float(clf_vrf['vrfsavings'].sum())
        savings_end = savings_now + savings_addition
        corpus_now = float(clf_vrf['totalvrfcorpus'].sum())
        interest_now = float(clf_vrf['vrfinterestamount'].sum())
        has_idle_eligible = bool((elig['zero_interest_vo'] == 1).sum() > 0)

        def month_series(start, end, n=13):
            return [round(start + (end - start) * i / (n - 1), 2) for i in range(n)]

        forecast = {"has_idle_eligible": has_idle_eligible, "n_eligible": len(elig), "n_vo": len(clf_vrf)}
        for s in ['s1', 's2', 's3']:
            add = float(elig[f'{s}_annual'].sum())
            end_interest = interest_now + add
            end_corpus = corpus_now + savings_addition + add
            forecast[s] = {
                "interest_now": round(interest_now), "interest_end": round(end_interest),
                "interest_delta": round(end_interest - interest_now),
                "interest_delta_pct": round((end_interest - interest_now) / interest_now * 100, 1) if interest_now else None,
                "corpus_now": round(corpus_now), "corpus_end": round(end_corpus),
                "corpus_delta": round(end_corpus - corpus_now),
                "corpus_delta_pct": round((end_corpus - corpus_now) / corpus_now * 100, 1) if corpus_now else None,
                "interest_monthly": month_series(interest_now, end_interest),
                "corpus_monthly": month_series(corpus_now, end_corpus),
            }
        forecast["savings_now"] = round(savings_now); forecast["savings_end"] = round(savings_end)
        forecast["savings_delta"] = round(savings_end - savings_now)
        forecast["savings_delta_pct"] = round((savings_end - savings_now) / savings_now * 100, 1) if savings_now else None
        forecast["savings_monthly"] = month_series(savings_now, savings_end)
        forecast["received_monthly"] = month_series(float(clf_vrf['totalvrfreceived'].sum()), float(clf_vrf['totalvrfreceived'].sum()))

        vrf_data = {
            "found": True,
            "n_vo": len(clf_vrf), "fsf_eligible": fsf_elig, "incomplete_coverage": incomplete_cov,
            "idle_vo": idle_vo, "received_vo": received_vo,
            "total_received": int(clf_vrf['totalvrfreceived'].sum()), "total_savings": int(clf_vrf['vrfsavings'].sum()),
            "total_interest": int(clf_vrf['vrfinterestamount'].sum()), "total_corpus": int(clf_vrf['totalvrfcorpus'].sum()),
            "coverage_gap": int(clf_vrf['vrf_coverage_gap'].sum()) if clf_vrf['vrf_coverage_gap'].notna().any() else 0,
            "expected_savings": int(clf_vrf['expected_savings'].sum()) if 'expected_savings' in clf_vrf.columns else None,
            "composite": round(vrow['composite'], 1),
            "composite_dist_pctl": round(vrow['composite_dist_pctl']),
            "composite_state_pctl": round(vrow['composite_state_pctl']),
            "composite_dist_rank": int(vrow['composite_dist_rank']), "composite_state_rank": int(vrow['composite_state_rank']),
            "n_district": len(dist_pop_v), "n_state": len(clf_agg_all),
            "district_name": target_district_vrf.title(),
            "metrics": [{"label": metric_labels[m], "value": round(vrow[m], 3),
                         "district_pctl": round(vrow[f'{m}_dist_pctl']), "state_pctl": round(vrow[f'{m}_state_pctl']),
                         "district_rank": int(vrow[f'{m}_dist_rank']), "state_rank": int(vrow[f'{m}_state_rank']),
                         "n_district": len(dist_pop_v), "n_state": len(clf_agg_all)}
                        for m in ALLM],
            "vo_table": vo_table,
            "bk_ranking": bk.to_dict('records'),
            "sdf_monthly": int(sdf_monthly), "sdf_annual": int(sdf_monthly * 12),
            "forecast": forecast,
        }
    else:
        vrf_data = {
            "found": False,
            "metrics": [{"label": metric_labels[m], "district_pctl": None, "state_pctl": None,
                         "district_rank": None, "state_rank": None, "n_district": None, "n_state": None} for m in ALLM],
        }

    # ========================================================================
    # 4. FINANCIAL RECORDS - F01 (snapshot), F03 + F05 (5-quarter window)
    # ========================================================================
    _r01_rows = f01_raw[f01_raw['CLF Name'] == f01_raw_name] if pd.notna(f01_raw_name) else f01_raw.iloc[0:0]
    F01_FOUND = len(_r01_rows) > 0

    if F01_FOUND:
        r01 = _r01_rows.iloc[0]
        total_assets = r01['Total Assets']; fixed_assets = r01['Fixed Assets']
        cash = r01['Cash Balance'] + r01['Bank Balance']
        onlend = r01['Loan to SHGs/VOs'] + r01['Loan to Non-Nodal CLFs'] + r01['Advances to SHGs/ VOs'] + r01['Advances to Others']
        f01_data = {
            "found": True,
            "period": r01['period'],
            "total_assets": int(total_assets),
            "balance_gap_pct": round((total_assets - r01['Total Equity & Liabilities']) / r01['Total Equity & Liabilities'] * 100, 2),
            "balance_gap_rs": int(round(total_assets - r01['Total Equity & Liabilities'])),
            "liquidity_ratio": round(cash / total_assets * 100, 1),
            "deployment_ratio": round(onlend / (total_assets - fixed_assets) * 100, 1),
            "surplus_pct": round(r01['Surplus / Deficit'] / total_assets * 100, 1),
            "assets": {
                "Cash Balance": int(r01['Cash Balance']), "Bank Balance": int(r01['Bank Balance']),
                "Inventory": int(r01['Inventory']), "Fixed Deposits": int(r01['Fixed Deposits']),
                "Loan to SHGs/VOs": int(r01['Loan to SHGs/VOs']), "Loan to Non-Nodal CLFs": int(r01['Loan to Non-Nodal CLFs']),
                "Advances to SHGs/VOs": int(r01['Advances to SHGs/ VOs']), "Advances to Others": int(r01['Advances to Others']),
                "Working Capital to PE/PG/CHC": int(r01['Working Capital to PE/PG/CHC']),
                "Suspense Amount": int(r01['Suspense Amount.1']), "Investment in Higher Federation": int(r01['Investment In Higher Federation']),
                "Fixed Assets": int(r01['Fixed Assets']), "Total Assets": int(total_assets),
            },
            "liabilities": {
                "Received From SRLM": int(r01['Received From SRLM']), "Loan From Nodal CLF": int(r01['Loan From Nodal CLF']),
                "Advance from SRLM": int(r01['Advance from SRLM']), "Savings & Investment by VO/SHG": int(r01['Savings And Investment By VO/SHG']),
                "Loan from Banks & Other Institutions": int(r01['Loan from Banks & Other Institutions']),
                "Capital Contribution": int(r01['Capital Contribution']), "Suspense Amount": int(r01['Suspense Amount']),
                "Surplus / Deficit": int(r01['Surplus / Deficit']), "Total Equity & Liabilities": int(r01['Total Equity & Liabilities']),
            },
        }
    else:
        f01_data = {"found": False, "deployment_ratio": None, "surplus_pct": None, "balance_gap_pct": None, "balance_gap_rs": None}

    quarterly = []
    for fy, q, label in QUARTERS:
        row3 = f03_raw[(f03_raw['CLF Name'] == f03_raw_name) & (f03_raw['financial_year'] == fy) & (f03_raw['quarter'] == q)]
        row5 = f05_raw[(f05_raw['CLF Name'] == f05_raw_name) & (f05_raw['financial_year'] == fy) & (f05_raw['quarter'] == q)]
        r3 = row3.iloc[0] if len(row3) else None
        r5 = row5.iloc[0] if len(row5) else None
        total_receipts = r3['Total Receipts'] if r3 is not None else 0
        total_payments = r3['Total Payments'] if r3 is not None else 0
        opening_balance = r3['Opening Balance (₹)'] if r3 is not None else 0
        closing_balance = r3['Closing Balance (₹)'] if r3 is not None else 0
        relend = r3['Loans And Advances To CBOs And Others'] if r3 is not None else 0
        expenses = (r3['Group Expense'] + r3['Bank Expense']) if r3 is not None else 0
        interest_income = r3['Interest Received From CBOs'] if r3 is not None else 0
        cum_req = r5['Cumulative Loan Requested'] if r5 is not None else 0
        cum_disb = r5['Cumulative Loan Disbursed'] if r5 is not None else 0
        new_demand = r5['newLoanDemand'] if r5 is not None else 0
        new_disb = r5['New Loan Disbursed'] if r5 is not None else 0
        pending = r5['Remaining Loan Requested'] if r5 is not None else 0
        avail_bal = r5['Available Cash and Bank Balance'] if r5 is not None else 0
        n_requesting = r5['Request from Member CBOs (No. of SHGs/VOs)'] if r5 is not None else 0

        quarterly.append({
            "label": label,
            "opening_balance": int(opening_balance),
            "total_receipts": int(total_receipts), "total_payments": int(total_payments),
            "closing_balance": int(closing_balance),
            "net_cash_flow": int(closing_balance - opening_balance),
            "receipts_full": {
                "Opening Balance": int(opening_balance), "Savings & Deposits From CBOs": int(r3['Savings And Deposits From CBOs']),
                "Funds Receipts": int(r3['Funds Receipts']), "Interest Received From CBOs": int(interest_income),
                "Other Interest Income": int(r3['Other Interest Income']), "Capital Receipts": int(r3['Capital Receipts']),
                "Principal Repayment by CBOs and Others": int(r3['Principal Repayment by CBCs and Others']),
                "Receipts From Sale of Assets": int(r3['Receipts From Sale Of Assets (if any)']), "Other Receipts": int(r3['Other Receipts']),
                "Total Receipts": int(total_receipts),
            } if r3 is not None else None,
            "payments_full": {
                "Capital Payments": int(r3['Capital Payments']), "Principal Repaid": int(r3['Principal Repaid (₹)']),
                "Interest Payments": int(r3['Interest Payments']), "Payments to Purchase Assets": int(r3['Payments Purchase Assets (if any)']),
                "Other Payments": int(r3['Other Payments']), "Loans & Advances to CBOs and Others": int(relend),
                "Group Expense": int(r3['Group Expense']), "Bank Expense": int(r3['Bank Expense']),
                "Closing Balance": int(closing_balance), "Total Payments": int(total_payments),
            } if r3 is not None else None,
            "operating_expense_ratio": round(expenses / total_receipts * 100, 1) if total_receipts else None,
            "interest_income_share": round(interest_income / total_receipts * 100, 1) if total_receipts else None,
            "pct_disbursed": round(cum_disb / cum_req * 100, 1) if cum_req else None,
            "amount_pending": int(pending),
            "new_demand_amt": int(new_demand),
            "qtr_pending": int(new_demand - new_disb),
            "this_qtr_disb_rate": round(new_disb / new_demand * 100, 1) if new_demand else None,
            "capacity_ratio": round(avail_bal / (new_demand - new_disb), 2) if (new_demand - new_disb) > 0 else None,
            "avail_balance": int(avail_bal),
            "n_requesting": int(n_requesting), "pct_vos_requesting": round(n_requesting / n_vo * 100, 1) if n_vo else None,
            "cum_disbursed": int(cum_disb), "cum_requested": int(cum_req),
            "is_real": bool(total_receipts > 0 or total_payments > 0 or cum_disb > 0),
        })

    financial = {"f01": f01_data, "quarters": quarterly, "n_vo": int(n_vo)}

    # ========================================================================
    # 5. VPRP - Entitlements / PGSRD / SDP
    # ========================================================================
    def vprp_year_filter(df, raw_name):
        return df[(df['district'] == district) & (df['block'].str.contains(block, case=False, na=False)) & (df['clf_name'] == raw_name)]

    ent = vprp_year_filter(ent_raw, vprp_ent_raw_name)
    pgsrd = vprp_year_filter(pgsrd_raw, vprp_pgsrd_raw_name)
    sdp = vprp_year_filter(sdp_raw, vprp_sdp_raw_name)

    SCHEME_LABELS = {
        "state-specific": "State-Specific Schemes", "pmayg": "Pradhan Mantri Awaas Yojana – Gramin (PMAY-G)",
        "mgnregs-job-card": "MGNREGS Job Card", "healthcard": "Health Card",
        "ujjwala": "Pradhan Mantri Ujjwala Yojana (PMUY)", "pmjjby": "Pradhan Mantri Jeevan Jyoti Bima Yojana (PMJJBY)",
        "pmsby": "Pradhan Mantri Suraksha Bima Yojana (PMSBY)", "rationcard": "Ration Card",
        "pmsbhgy": "PMSBHGY", "widow-pension": "Widow Pension", "disability-pension": "Disability Pension",
        "rationcard-add": "Ration Card Addition", "old-age-pension": "Old-Age Pension",
    }
    PGSRD_LABELS = {"pgsrd-public-goods": "Public Goods", "pgsrd-resources": "Resources", "pgsrd-services": "Services"}

    vprp = {"years": {}}
    for yr in [2023, 2024, 2025]:
        e_y = ent[ent['year'] == yr]
        p_y = pgsrd[pgsrd['year'] == yr]
        s_y = sdp[sdp['year'] == yr]
        yr_data = {"n_demands": len(e_y), "n_pgsrd": len(p_y), "n_sdp": len(s_y)}
        if len(e_y):
            # nrega_accessed ("Member Family NREGA Job Card Status", per
            # 1_Code/vprp_cleaning.do) is a per-person status that applies to
            # every row regardless of scheme_type - it is NOT whether that
            # row's particular additional scheme was fulfilled (the data has
            # no fulfillment field for individual schemes at all), so it's
            # surfaced once as its own CLF-wide KPI, not per-scheme.
            accessed = int(e_y['nrega_accessed'].sum())
            yr_data["pct_nrega_accessed"] = round(accessed / len(e_y) * 100, 1)
            other_schemes = e_y[e_y['scheme_type'] != 'mgnregs-job-card']
            yr_data["n_other_schemes"] = int(other_schemes['scheme_type'].nunique())
            by_scheme = e_y.groupby('scheme_type').size().sort_values(ascending=False)
            by_scheme = by_scheme[by_scheme > 0]
            yr_data["by_scheme"] = [{"scheme": SCHEME_LABELS.get(s, s), "raw_scheme": s, "demanded": int(v)} for s, v in by_scheme.head(6).items()]
            state_ss = e_y[e_y['scheme_type'] == 'state-specific']
            if len(state_ss):
                ss_counts = state_ss['state_scheme'].value_counts()
                yr_data["state_scheme_breakdown"] = {k: int(v) for k, v in ss_counts[ss_counts > 0].items()}
            cat_counts = e_y['social_category'].value_counts(normalize=True) * 100
            yr_data["social_category"] = {k: round(v, 1) for k, v in cat_counts.items()} if cat_counts.notna().any() else None
        if len(p_y):
            vc = (p_y['pgsrd_type'].value_counts(normalize=True) * 100)
            yr_data["pgsrd_type_split"] = {PGSRD_LABELS.get(k, k): round(v, 1) for k, v in vc[vc > 0].items()}
            items = p_y.groupby(['item_demanded', 'pgsrd_type']).agg(n=('unitsdemanded', 'size'), units=('unitsdemanded', 'sum')).reset_index().sort_values('n', ascending=False)
            items['pgsrd_type'] = items['pgsrd_type'].map(lambda k: PGSRD_LABELS.get(k, k))
            yr_data["pgsrd_items"] = items.head(8).to_dict('records')
            sdg = p_y['sankalp_sdg_theme'].value_counts()
            yr_data["sdg_theme"] = sdg[sdg > 0].head(6).to_dict()
            gpdp = p_y['gpdp_area'].value_counts()
            yr_data["gpdp_area"] = gpdp[gpdp > 0].head(6).to_dict()
        if len(s_y):
            vc = (s_y['sector'].value_counts(normalize=True) * 100)
            yr_data["sdp_sector"] = {k: round(v, 1) for k, v in vc[vc > 0].items()}
            issues = s_y.groupby('social_issue').agg(n=('affected_people_num', 'size'), affected=('affected_people_num', 'median')).reset_index().sort_values('n', ascending=False)
            issues['affected'] = issues['affected'].apply(lambda v: round(v) if pd.notna(v) else None)
            yr_data["sdp_issues"] = issues.head(6).to_dict('records')
            dept_cols = [c for c in s_y.columns if c.startswith('department_')]
            depts = pd.concat([s_y[c] for c in dept_cols]).dropna()
            dc = depts.value_counts()
            yr_data["departments"] = dc[dc > 0].head(6).to_dict()
        vprp["years"][yr] = yr_data

    # ========================================================================
    # 6. SCORING & RANKING
    # ========================================================================
    def score_metric(pop, col, value, higher_is_better=True):
        dist_pop_s = pop[pop['district'] == district]
        v = value
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        dp = inclusive_pctl_val(dist_pop_s[col], v)
        sp = inclusive_pctl_val(pop[col], v)
        dr = rank_of_val(dist_pop_s[col], v, higher_is_better)
        sr = rank_of_val(pop[col], v, higher_is_better)
        if not higher_is_better:
            dp = 100 - dp if dp is not None else None
            sp = 100 - sp if sp is not None else None
        return {"district_pctl": dp, "state_pctl": sp, "district_rank": dr, "state_rank": sr,
                "n_district": len(dist_pop_s), "n_state": len(pop)}

    target_mem_row = mem_pop[mem_pop['clf_code'] == clfcode].iloc[0]

    def score_from_pop(pop, col, value, district_col='g_district', district_val=None):
        dist_pop_s = pop[pop[district_col] == (district_val or district)]
        return {
            "district_pctl": inclusive_pctl_val(dist_pop_s[col], value),
            "state_pctl": inclusive_pctl_val(pop[col], value),
            "district_rank": rank_of_val(dist_pop_s[col], value), "state_rank": rank_of_val(pop[col], value),
            "n_district": len(dist_pop_s), "n_state": len(pop),
        }

    governance_metrics = [
        ("CLF Status", score_from_pop(mem_pop, 'status_tier_rank', target_mem_row['status_tier_rank'])),
        ("Platform Approval Status", score_from_pop(mem_pop, 'approval_rank', target_mem_row['approval_rank'])),
        ("Subcommittee Completeness", score_from_pop(mem_pop, 'n_subcom_filled', target_mem_row['n_subcom_filled'])),
        ("Active Membership", score_from_pop(mem_pop, 'pct_active', target_mem_row['pct_active'])),
        ("Cadre Diversity", score_from_pop(mem_pop, 'n_distinct_cadre_role_types', target_mem_row['n_distinct_cadre_role_types'])),
    ]
    social_metrics = [
        ("Insurance Coverage", score_from_pop(mem_pop, 'pct_insurance', target_mem_row['pct_insurance'])),
        ("Aadhaar KYC Coverage", score_from_pop(mem_pop, 'pct_aadhaar', target_mem_row['pct_aadhaar'])),
        ("Livelihoods Diversification", score_from_pop(mem_pop, 'pct_multi_livelihood', target_mem_row['pct_multi_livelihood'])),
        ("Special Project Activities", score_from_pop(mem_pop, 'n_spa', target_mem_row['n_spa'])),
    ]
    vrf_health_metrics = [(m['label'], {"district_pctl": m['district_pctl'], "state_pctl": m['state_pctl'],
                                         "district_rank": m['district_rank'], "state_rank": m['state_rank'],
                                         "n_district": m['n_district'], "n_state": m['n_state']}) for m in vrf_data['metrics']]

    def cat_score(metrics, key='state_pctl'):
        valid = [m[1][key] for m in metrics if m[1] and m[1].get(key) is not None]
        return round(sum(valid) / len(valid)) if valid else None

    def cat_scores_both(metrics):
        return {"score": cat_score(metrics, 'state_pctl'), "district_score": cat_score(metrics, 'district_pctl')}

    # Fund Utilization & Loan Activity and Financial Health each mix a static
    # (F01 snapshot) metric with quarter-specific (F03/F05) ones, so a full
    # categories+overall-score snapshot is built per quarter - everything else
    # (VRF/Governance/Welfare) doesn't vary by quarter and is simply repeated
    # into every quarter's snapshot so the JS can always render one flat
    # `DATA.scoring.by_quarter[label]` object without special-casing which
    # metrics happen to be static this time.
    scoring_by_quarter = {}
    for _qi, (_fy, _q, _label) in enumerate(QUARTERS):
        q_data = quarterly[_qi]
        demand_per_member_q = q_data['new_demand_amt'] / overview['n_members'] if overview['n_members'] > 0 else None
        fund_util_metrics = [
            ("Fund Deployment", score_metric(f01_m, 'deployment_ratio', f01_data['deployment_ratio'])),
            ("Interest Income Share", score_metric(f03_m_by_q[_label], 'interest_income_share', q_data['interest_income_share'])),
            ("This Quarter's Disbursement Rate", score_metric(f05_m_by_q[_label], 'this_qtr_disb_rate', q_data['this_qtr_disb_rate'])),
            ("Loan Amount Demanded This Quarter", score_metric(demand_pop_by_q[_label], 'demand_per_member', demand_per_member_q)),
        ]
        financial_health_metrics = [
            ("Surplus / Deficit", score_metric(f01_m, 'surplus_pct', f01_data['surplus_pct'])),
            ("Net Cash Flow", score_metric(f03_m_by_q[_label], 'net_cash_flow', q_data['net_cash_flow'])),
            ("Bookkeeping Accuracy", score_metric(f01_m, 'balance_accuracy', 100 - abs(f01_data['balance_gap_pct']) if f01_data['balance_gap_pct'] is not None else None)),
        ]
        categories = {
            "Fund Utilization & Loan Activity": {"metrics": fund_util_metrics, **cat_scores_both(fund_util_metrics)},
            "Financial Health": {"metrics": financial_health_metrics, **cat_scores_both(financial_health_metrics)},
            "VRF Fund Health": {"metrics": vrf_health_metrics, **cat_scores_both(vrf_health_metrics)},
            "Governance & Compliance": {"metrics": governance_metrics, **cat_scores_both(governance_metrics)},
            "Welfare and Livelihood": {"metrics": social_metrics, **cat_scores_both(social_metrics)},
        }
        # Overall Score is an equal weight across the 5 CATEGORIES (each
        # category's own already-computed score), not a flat pool across
        # every individual metric - a category with more metrics no longer
        # gets more say than one with fewer. Missing categories are dropped
        # from both sides of the average, not treated as 0. This is also
        # exactly what "equal weights" means as the default for the
        # customizable weight picker (see scoring_summary.json below).
        cat_state_scores = [c['score'] for c in categories.values() if c['score'] is not None]
        cat_dist_scores = [c['district_score'] for c in categories.values() if c['district_score'] is not None]
        overall_score = round(sum(cat_state_scores) / len(cat_state_scores)) if cat_state_scores else None
        overall_district_score = round(sum(cat_dist_scores) / len(cat_dist_scores)) if cat_dist_scores else None

        # absolute rank ("2nd of 1667 CLFs") for the Overall Score and each
        # category's composite - unlike every other rank in this file, this
        # needs every OTHER CLF's own overall/category score first, so it's
        # looked up from RANK_LOOKUP, a peer population built in a lightweight
        # first pass over all CLFs before the real per-CLF loop runs (see
        # section 6b below) - RANK_LOOKUP stays all-None during that first
        # pass itself, since it's what's building the very population it
        # needs; ranks simply come back None for that pass's (discarded) output.
        rp = RANK_LOOKUP[_label]
        overall_rank = rp['overall'].get(mis_id) if rp else None
        scoring_by_quarter[_label] = {
            "categories": categories, "overall_score": overall_score, "overall_district_score": overall_district_score,
            "overall_state_rank": overall_rank['state_rank'] if overall_rank else None,
            "overall_district_rank": overall_rank['district_rank'] if overall_rank else None,
            "overall_n_state": overall_rank['n_state'] if overall_rank else None,
            "overall_n_district": overall_rank['n_district'] if overall_rank else None,
        }
        for _cat_name, _cat_dict in categories.items():
            cr = rp['categories'][_cat_name].get(mis_id) if rp else None
            _cat_dict['state_rank'] = cr['state_rank'] if cr else None
            _cat_dict['district_rank'] = cr['district_rank'] if cr else None
            _cat_dict['n_state'] = cr['n_state'] if cr else None
            _cat_dict['n_district'] = cr['n_district'] if cr else None

    scoring = {"quarters": [lbl for _, _, lbl in QUARTERS], "default_idx": len(QUARTERS) - 1, "by_quarter": scoring_by_quarter}

    return {
        "overview": overview, "audit": audit, "vrf": vrf_data,
        "financial": financial, "vprp": vprp, "scoring": scoring,
    }

# ============================================================================
# 6b. RANK LOOKUP - a lightweight first pass over every CLF, computed before
# the real per-CLF loop, purely to learn each CLF's own Overall Score and
# category scores so every OTHER CLF can be ranked against them (a rank -
# unlike a percentile - can't be computed one CLF at a time). Calls the exact
# same build_clf_data() as the real pass (single source of truth for the
# scoring math, no risk of a second implementation drifting out of sync) and
# keeps only the tiny `scoring` slice of its output, discarding the rest.
# RANK_LOOKUP is still all-None at this point, so every rank in this pass's
# own (discarded) output comes back None - expected, since this pass IS what
# builds the population those ranks would come from.
# ============================================================================
print(f"[{elapsed()}] Pass 1/2: collecting Overall/category scores for ranking...")
_score_cache = {}
for _i, _row in master.iterrows():
    _mis = _row['mis_id']
    if pd.isna(_mis):
        continue
    _mis = int(_mis)
    try:
        _result = build_clf_data(_mis, _row['clfcode'], _row['district'], _row['block'], _row['clf_name_audit'])
        _score_cache[_mis] = {'district': _row['district'], 'by_quarter': _result['scoring']['by_quarter']}
    except Exception:
        pass
    if (_i + 1) % 400 == 0:
        print(f"[{elapsed()}]   ...{_i+1}/{len(master)} scored")

CATEGORY_NAMES = ["Fund Utilization & Loan Activity", "Financial Health", "VRF Fund Health",
                  "Governance & Compliance", "Welfare and Livelihood"]

def _rank_pop(pop_df, higher_is_better=True):
    # min-rank (best=1); tied peers share the best rank, equivalent to
    # "count strictly better + 1" - matches VRF's own ranking convention.
    ranks = pop_df['value'].rank(method='min', ascending=not higher_is_better)
    dist_ranks = pop_df.groupby('district')['value'].rank(method='min', ascending=not higher_is_better)
    n_state = len(pop_df)
    dist_n = pop_df.groupby('district')['value'].transform('size')
    out = {}
    for mis, r, dr, dn in zip(pop_df['mis_id'], ranks, dist_ranks, dist_n):
        out[int(mis)] = {"state_rank": int(r), "district_rank": int(dr), "n_state": n_state, "n_district": int(dn)}
    return out

for _fy, _q, _label in QUARTERS:
    overall_rows = [(mis, v['district'], v['by_quarter'][_label]['overall_score']) for mis, v in _score_cache.items()
                     if v['by_quarter'][_label]['overall_score'] is not None]
    overall_pop = pd.DataFrame(overall_rows, columns=['mis_id', 'district', 'value'])
    cat_lookups = {}
    for _cat in CATEGORY_NAMES:
        cat_rows = [(mis, v['district'], v['by_quarter'][_label]['categories'][_cat]['score']) for mis, v in _score_cache.items()
                    if v['by_quarter'][_label]['categories'][_cat]['score'] is not None]
        cat_pop = pd.DataFrame(cat_rows, columns=['mis_id', 'district', 'value'])
        cat_lookups[_cat] = _rank_pop(cat_pop)
    RANK_LOOKUP[_label] = {'overall': _rank_pop(overall_pop), 'categories': cat_lookups}

del _score_cache
print(f"[{elapsed()}] Pass 1/2 complete.")

# ============================================================================
# 7. LOOP OVER EVERY CLF, WRITE JSON-PER-UNIT + MANIFEST
# ============================================================================
class NpEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (np.integer,)): return int(o)
        if isinstance(o, (np.floating,)): return None if (np.isnan(o) or np.isinf(o)) else float(o)
        if isinstance(o, (np.bool_,)): return bool(o)
        return super().default(o)

def sanitize_nan(obj):
    if isinstance(obj, dict):
        return {k: sanitize_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_nan(v) for v in obj]
    if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None
    return obj

print(f"[{elapsed()}] Starting per-CLF loop over {len(master)} CLFs...")
n_ok, n_fail = 0, 0
failures = []
manifest_rows = []
summary_rows = []
for i, row in master.iterrows():
    mis_id = row['mis_id']
    if pd.isna(mis_id):
        continue
    mis_id = int(mis_id)
    clfcode = row['clfcode']
    district = row['district']
    block = row['block']
    audit_name = row['clf_name_audit']
    try:
        data = build_clf_data(mis_id, clfcode, district, block, audit_name)
        data = sanitize_nan(data)
        with open(f"{DATA_DIR}/clfs/{mis_id}.json", "w") as f:
            json.dump(data, f, cls=NpEncoder)
        manifest_rows.append({
            "id": mis_id, "n": data["overview"]["clf_name_lokos"].title(),
            "district": district.title(), "block": block.title(),
        })
        # compact per-CLF category-score summary (every quarter) for the
        # customizable-weight picker's live cross-CLF ranking - deliberately
        # excludes everything else (VO tables, forecasts, item-level detail)
        # to stay small enough to fetch once in the browser.
        summary_rows.append({
            "id": mis_id, "district": district.title(),
            "by_quarter": {q: {cat: v["score"] for cat, v in bq["categories"].items()}
                           for q, bq in data["scoring"]["by_quarter"].items()},
        })
        n_ok += 1
    except Exception as e:
        n_fail += 1
        failures.append((mis_id, str(e)))
    if (i + 1) % 200 == 0:
        print(f"[{elapsed()}]   ...{i+1}/{len(master)} processed ({n_ok} ok, {n_fail} failed)")

print(f"[{elapsed()}] Done: {n_ok} CLFs written, {n_fail} failed.")
if failures:
    print("First 20 failures:")
    for mis_id, err in failures[:20]:
        print(f"  {mis_id}: {err}")

# ---- manifest.json: district -> block -> [{id, n}], for the shell's search/finder ----
districts = {}
for r in manifest_rows:
    d = districts.setdefault(r["district"], {})
    blk = d.setdefault(r["block"], [])
    blk.append({"id": r["id"], "n": r["n"]})

manifest = {
    "version": int(time.time()),
    "counts": {"clfs": n_ok, "districts": len(districts)},
    "districts": [
        {"name": dname, "blocks": [{"name": bname, "clfs": clfs} for bname, clfs in sorted(blocks.items())]}
        for dname, blocks in sorted(districts.items())
    ],
}
with open(f"{DATA_DIR}/manifest.json", "w") as f:
    json.dump(manifest, f)
print(f"[{elapsed()}] Wrote manifest.json ({len(districts)} districts, {n_ok} CLFs)")

scoring_summary = {"quarters": [lbl for _, _, lbl in QUARTERS], "clfs": summary_rows}
with open(f"{DATA_DIR}/scoring_summary.json", "w") as f:
    json.dump(scoring_summary, f, cls=NpEncoder)
print(f"[{elapsed()}] Wrote scoring_summary.json ({len(summary_rows)} CLFs)")

# ---- shared.json: TIPS/ERR_MSG/FOOTNOTES - identical for every CLF, loaded
# once by the shell rather than duplicated into every per-CLF file ----
TIPS = {'Number of VOs': 'How many Village Organisations belong to this CLF.', 'Number of SHGs': 'How many Self-Help Groups belong to this CLF, across all its VOs.', 'Total Members': 'How many individual SHG members belong to this CLF.', 'Active Members': "How many of this CLF's members are currently marked active on LokOS, out of the total.", 'Formation Date': 'The date this CLF was formed.', 'Registration Date': 'The date this CLF was formally registered as a cooperative society.', 'CLF Status': "Whether this CLF is both Model and Registered, Model only, Registered only, or neither, based on the audit form.", 'Platform Approval Status': "Whether this CLF's registration on LokOS has been approved, is pending, or was rejected by the Block Manager.", 'Co-option Status': 'Whether this CLF is newly formed, was co-opted from an earlier structure, or was revived after being inactive.', 'Registration Code': "This CLF's official state cooperative society registration number.", 'Executive Committee Size': "How many members sit on this CLF's Executive Committee.", 'President': "The name of this CLF's current President.", 'Secretary': "The name of this CLF's current Secretary.", 'Subcommittee Membership': "How many members sit on each of the CLF's six functional subcommittees.", 'Meeting Frequency': "How often this CLF's Executive Committee is supposed to meet.", 'Savings Schedule': 'How often this CLF collects savings from its member VOs.', 'Member Composition': "The share of this CLF's members who belong to Scheduled Castes/Scheduled Tribes.", 'Education & Literacy': "The share of this CLF's members who can read and write.", 'Members With a Livelihood': "The share of this CLF's members who reported at least one livelihood activity (farm or non-farm) on their LokOS profile.", 'Members With Multiple Livelihoods': 'The share of members who reported more than one livelihood activity.', 'Livelihood Split': "What kinds of livelihood activities this CLF's members are engaged in, combining their primary, secondary, and tertiary activities.", 'Special Project Activities': 'Which optional CLF-run initiatives — like a Custom Hiring Centre, Didi Ki Rasoi, or livestock activities — this CLF has taken up, as reported in its audit.', 'Cadre Roles in This CLF': 'Which specialised community-cadre roles (like Master Book Keeper, Enterprise Promotion, or Krishi Mitra) are staffed in this CLF, and how many members hold each.', 'Audit Grade': "This CLF's official letter grade from its most recent (Q4) audit.", 'Total Audit Score': "This CLF's total audit score out of 100, from its most recent (Q4) audit.", 'Standing vs. Peers': "How this CLF's total audit score compares to every other CLF in its district.", 'Accounting': 'How well this CLF keeps its accounting books, cash/bank records, loan verification, MIS quality, and fixed-asset records — out of 19, and how that compares to peers.', 'Receipts & Payments': 'How well this CLF manages and records its receipts and payments — out of 20, and how that compares to peers.', 'Loan': 'How well this CLF reviews loan demand and manages loan repayment — out of 18, and how that compares to peers.', 'Group Performance': "How well this CLF's SHGs, VOs, and the CLF itself are functioning — out of 23, and how that compares to peers.", 'Cadre Management': 'How well this CLF manages its cadre members — out of 10, and how that compares to peers.', 'Operational Self-Sufficiency': "How much of this CLF's running costs are covered by its own earned income — out of 10, and how that compares to peers.", 'Detailed Scores': 'The individual line items that make up each audit category score, so you can see exactly where points were gained or lost.', 'Irregularity Flag': "Whether this CLF's Q4 audit flagged a financial irregularity.", 'Type of Irregularity': "Which category of issue the auditor's remark falls under - a flagged CLF can match more than one.", 'Cash Book vs. Physical Cash': "The difference between what the CLF's cash book says it should have on hand, and what was physically counted during the audit.", 'Books Balance Check': "Whether this CLF's reported Total Assets match its reported Total Liabilities & Equity.", 'Liquidity Ratio': "The share of this CLF's assets sitting as cash or bank balance, rather than out on loan or invested.", 'Fund Deployment Ratio': "The share of this CLF's financial assets that are actually out on loan to SHGs, VOs, or other CLFs, rather than sitting idle.", 'Assets Composition': "How this CLF's total assets break down across every balance-sheet line item - cash, bank balance, loans out to SHGs/VOs, fixed assets, and the rest.", 'Liabilities Composition': "How this CLF's total liabilities and equity break down across every balance-sheet line item - government grants, member capital, institutional loans, savings held on behalf of VOs/SHGs, and the rest.", 'Surplus / Deficit': "This CLF's reported surplus or deficit, shown as a share of its total assets — a negative number means a deficit.", 'Net Cash Flow': 'The difference between what this CLF received and what it paid out this quarter.', 'Where Money Came In From': "What this quarter's receipts were made up of — savings deposits, interest earned, loan repayments, and other income.", 'Where Money Went Out To': "What this quarter's payments were made up of — loans and advances to member CBOs, running costs, and other expenses.", 'Operating Expense Ratio': 'How much this CLF spent on its own running costs, as a share of everything it received this quarter.', 'Interest Income Share': "How much of this CLF's receipts came from interest earned by lending to members, rather than from deposits or other income.", '% of Loans Disbursed': "Of everything ever requested by this CLF's member SHGs/VOs, how much has actually been disbursed to date.", 'Loan Amount Still Pending': 'How much loan money has been requested by member SHGs/VOs but not yet disbursed.', "This Quarter's Disbursement Rate": 'Of the new loan demand raised this specific quarter, how much was actually disbursed.', 'Capacity to Meet Demand': "Whether this CLF currently has enough cash and bank balance on hand to cover this quarter's still-pending loan demand.", 'Loan Amount Demanded This Quarter': 'How much loan money this CLF\'s member SHGs/VOs newly requested this specific quarter.', "This Quarter's Pending Loans": "Of this quarter's new loan demand, how much hasn't been disbursed yet.", 'Share of VOs Requesting Loans': "The share of this CLF's member VOs/SHGs that actively requested a loan this quarter.", 'Receipts Over Time': "How this CLF's quarterly receipts have moved over the last five quarters.", 'Payments Over Time': "How this CLF's quarterly payments have moved over the last five quarters.", 'Cash Balance Over Time': "How this CLF's quarterly closing cash balance has moved over the last five quarters.", 'Loans Disbursed Over Time': "How this CLF's total loans disbursed has grown over the last five quarters.", 'Disbursement Rate Over Time': "How this CLF's disbursement rate has moved over the last five quarters.", 'Balance Sheet': "This CLF's full balance sheet, exactly as reported.", 'Receipts & Payments Statement': "This CLF's full receipts and payments statement for the selected quarter, exactly as reported.", 'FSF-Eligible VOs': "How many of this CLF's VOs qualify for the Food Security Fund — at least 40% of their members belong to SC/ST groups.", 'VOs With Incomplete Coverage': "How many VOs haven't yet received their full ₹1,50,000 VRF government grant.", 'Idle VOs': 'Of the VOs that have received VRF funds, how many earned no interest from lending last year.', 'Total VRF Received': "The total VRF government grant this CLF's VOs have received to date.", 'Total Savings Collected': "The total member savings collected across this CLF's VOs.", 'Total Interest Collected': "The total interest earned by this CLF's VOs from lending VRF funds to members.", 'Total Fund Size': "The combined size of this CLF's VRF fund — grant, savings, and interest together.", 'Coverage Gap': "The total grant amount still owed to VOs that haven't received their full ₹1,50,000.", 'Savings as Promised': 'How much of the savings members were supposed to contribute has actually been collected.', 'What the Fund Is Made Of': "How this CLF's VRF fund breaks down between the original government grant, member savings, and interest earned.", 'Grant Not Fully Received': 'How many VOs are still owed part of their VRF grant.', 'Fund Sitting Idle': 'How many VOs that have received VRF funds earned no interest from lending them out last year.', 'VO Identity': 'Which VO this row is about, and who its bookkeeper is.', 'VO Fund Status': 'Whether this VO is FSF-eligible, has received its full grant, and is currently lending out its VRF funds.', 'SHG Members': 'How many SHG members belong to this VO.', 'VO Fund Metrics': 'How well this VO is doing on savings discipline, interest earned, and overall fund growth.', 'VO Savings Discipline %': 'How much of the promised monthly savings has actually been collected, out of what should have been saved by now.', 'VO Interest Yield': 'How much of the total fund came from interest earned by lending to members, rather than from savings or the grant.', 'VO Corpus Multiplier': 'How many times bigger the total fund has grown compared to the government grant it started with.', 'Bookkeeper Identity': 'Which bookkeeper this row is about, how many VOs and members they cover, and their overall rank within this CLF.', 'Bookkeeper Fund Metrics': "How this bookkeeper's VOs are doing on savings, fund growth, and coverage, combined into one composite score.", 'Fund Projections': "Where this CLF's savings, interest, and total fund size are headed by 31 March 2027, under three different assumptions about lending activity.", 'Social Development Fund': 'How much this CLF should be collecting from its VOs each month, and each year, for the Social Development Fund.', 'Month-by-Month Projection': "A month-by-month look at how this CLF's savings, interest, and total fund are projected to grow.", 'Total Entitlement Demands': "How many entitlement/scheme demands this CLF's members raised in the selected year.", 'Number of People Requesting': "How many of this CLF's members raised an entitlement demand in the selected year - every person in this dataset has requested an NREGA job card, forming the base population.", '% of Job Cards Accessed': "Of the members who requested an NREGA job card, what share have actually received it.", 'Non-NREGA Schemes Requested': "How many distinct government schemes, other than the NREGA job card itself, this CLF's members requested in the selected year - a count of scheme types touched, not total requests.", 'Demand & Access by Scheme': "Which government schemes this CLF's members are demanding most in the selected year. There's no fulfilment tracking for individual schemes in this data - only the NREGA job card's own accessed status is available, shown separately above.", 'Total PGSRD Requests': "How many public-goods, resource, or service requests this CLF's members raised in the selected year.", 'Type of Request': 'Whether requests are for public goods, resources, or services.', 'Most-Requested Items': 'The specific items requested most often, and the total quantity demanded for each.', 'By Development Theme': 'Which broader development themes these requests fall under.', 'By GPDP Focus Area': 'Which Gram Panchayat Development Plan focus areas these requests relate to.', 'Total Social Issues Raised': "How many social development issues this CLF's members raised in the selected year.", 'By Sector': 'Which broad sector these issues fall under.', 'Most-Raised Issues': 'The specific social issues raised most often, how many times each was raised, and roughly how many people each one affects.', 'Government Departments Involved': 'Which government departments are most often implicated by these issues.', 'Fund Deployment': "This CLF's fund deployment ratio, converted to a percentile against other CLFs.", 'Bookkeeping Accuracy': "How closely this CLF's Assets match its Liabilities+Equity, converted to a percentile — a closer match ranks higher.", 'VRF Fund Health': "This CLF's VRF fund-management metrics, each converted to a percentile against other CLFs.", 'Savings Discipline': "How much of the savings this CLF's VOs were expected to have collected by now has actually come in, member-weighted across VOs and converted to a percentile against other CLFs.", 'Savings Realisation': "How much savings this CLF's VOs have collected relative to the size of the VRF government grant they've received, member-weighted across VOs and converted to a percentile against other CLFs.", 'Corpus Multiplier': "How many times bigger this CLF's VRF fund has grown compared to the government grant it started with, member-weighted across VOs and converted to a percentile against other CLFs.", 'Interest Yield': "How much of this CLF's VRF fund is made up of interest earned by lending to members, rather than the original grant or savings, member-weighted across VOs and converted to a percentile against other CLFs.", 'Coverage Completion': "The share of this CLF's VOs, member-weighted, that have received their full ₹1,50,000 VRF government grant, converted to a percentile against other CLFs.", 'Subcommittee Completeness': "How many of this CLF's six subcommittees are actually staffed, converted to a percentile.", 'Active Membership': "The share of this CLF's members currently marked active, converted to a percentile.", 'Cadre Diversity': 'How many different specialised cadre roles this CLF has staffed, converted to a percentile.', 'Insurance Coverage': "The share of this CLF's members with insurance, converted to a percentile.", 'Aadhaar KYC Coverage': "The share of this CLF's members with Aadhaar KYC verified, converted to a percentile.", 'Livelihoods Diversification': "The share of this CLF's members with more than one livelihood activity, converted to a percentile.", 'Overall Score': "This CLF's overall performance score, combining fund management, financial health, governance, and inclusion into one number, and how it compares to other CLFs in its district and across Bihar."}

FOOTNOTES = {
    "overview": "Sourced from LokOS Profile Reports, plus Odoo CLF Audit Reports for CLF status.",
    "audit": "Sourced from Odoo CLF Audit Reports; transaction data from the Credit Disbursement Report, LokOS.",
    "financial": "Sourced from LokOS: Balance Sheets (F01), Receipts &amp; Payments (F03), and the Credit Disbursement Report (F05).",
    "vrf": "Sourced from the VRF Monitoring and Management Portal, BRLPS.",
    "vprp": "Sourced from Entitlement Reports, VPRP LokOS portal.",
    "scoring": "Combines LokOS Profile Reports, Odoo CLF Audit Reports, LokOS financial reports (Balance Sheets, Receipts &amp; Payments, Credit Disbursement Report), and the VRF Monitoring and Management Portal (BRLPS).",
}

# Brief per-tab explanation shown in a context box at the top of each tab,
# with the FOOTNOTES source line folded into the end of the same box.
CONTEXT = {
    "overview": "A snapshot of the CLF's identity, structure, and membership. <b>Profile</b> covers formation and registration details, CLF status (Model/Registered), platform approval status, governance structure (President, Secretary, Executive Committee, subcommittees), and meeting/savings schedule. <b>Members</b> covers social inclusion and welfare coverage (SC/ST, OBC, disability, etc.), education levels, livelihood diversification, special project activities, and the CLF's cadre roster.",
    "audit": "The CLF's most recent audit results (FY 2025-26, Q4): its overall grade and score, how it compares to other CLFs in its district and statewide, a breakdown across the 6 scoring categories, the individual line items behind each category, whether the auditor flagged a financial irregularity (and what kind), and a cash book vs. physical cash reconciliation check.",
    "financial": "The CLF's balance sheet, quarterly cash flow, and credit disbursement, for a selected quarter. <b>Summary</b> gives key ratios (liquidity, fund deployment, surplus/deficit, books balance check) and a breakdown of where capital and cash came from and went to. <b>Statements</b> shows the full balance sheet and receipts &amp; payments statement, exactly as reported.",
    "vrf": "Tracks the CLF's Vulnerability Reduction Fund, managed at the VO level. <b>KPI Snapshot</b> gives CLF-wide totals and fund health. <b>VO-Level Breakdown</b> lets you inspect every individual VO. <b>Bookkeeper &amp; CLF Rankings</b> compares the CLF's fund management to peers and ranks its bookkeepers. <b>Forecasts</b> projects where the fund is headed by 31 March 2027 under three lending-activity scenarios.",
    "vprp": "Requests and plans raised through VPRP, by year (2023-2025). <b>Entitlements</b> tracks government scheme demands (ration cards, pensions, insurance, etc.) and NREGA job card status. <b>PGSRD</b> (Public Goods, Services, and Resource Development) tracks requests for public infrastructure, resources, and services. <b>SDP</b> (Social Development Plan) tracks broader social issues raised and the government departments involved.",
    "scoring": "Combines every other tab into one performance score, against the CLF's district and Bihar statewide. <b>Overall</b> gives one pooled score plus a category breakdown, and lets you customize the weight given to each category. <b>By Category</b> shows every individual metric behind the 5 categories, with district/state percentiles and ranks.",
}

with open(f"{DATA_DIR}/shared.json", "w") as f:
    json.dump({"tips": TIPS, "err_msg": ERR_MSG, "footnotes": FOOTNOTES, "context": CONTEXT}, f)
print(f"[{elapsed()}] Wrote shared.json (TIPS/ERR_MSG/FOOTNOTES/CONTEXT)")
print(f"[{elapsed()}] ALL DONE.")
