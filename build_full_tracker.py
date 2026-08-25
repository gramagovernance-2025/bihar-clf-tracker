"""
Author:   Claude (for Mohan)
Created:  25/08/2026
Purpose:  Consolidated, single-file build pipeline for the Bihar Comprehensive
          CLF Tracker (3_Output/CLF Tracker/Scale-Up). Replaces 6 previously
          separate scripts - build_tracker_data.py, build_vprp_vo_data.py,
          build_loan_tab_data.py, build_loan_scoring_data.py,
          build_district_state_data.py, make_shell.py - which had accumulated
          as an organic byproduct of being built incrementally across many
          sessions, not for any deliberate architectural reason. Each stage's
          original logic is preserved EXACTLY as it was (each stage's code is
          wrapped in its own function, unmodified internally, rather than
          "cleverly" merged/deduplicated - safer than a rewrite for something
          this large, and every stage's own inline comments/docstring explaining
          ITS reasoning are preserved verbatim below).

          Run as one command: `python3 build_full_tracker.py`. Runs all 6
          stages in the only order that's actually valid (each stage reads
          data/clfs/*.json written by the one before it):

          1. stage_1_build_clf_data()      - base per-CLF json: overview, audit,
                                              financial, VRF, VPRP summary,
                                              governance/social, scoring skeleton
                                              (5 original categories)
          2. stage_2_build_vprp_vo_data()  - adds vo_overview + richer per-VO/
                                              per-GP VPRP breakdown (must run
                                              before stage 3 - it reads
                                              vo_overview to reconcile VO rosters)
          3. build_loan_tab_data_main()    - adds loans + fund_disbursement blocks
                                              (kept as its own top-level
                                              function/module scope, not wrapped
                                              like the others, since
                                              build_district_state_data's stage
                                              needs its xirr()/LENDING_FUND_HEADINGS
                                              as plain importable-from-same-file
                                              names)
          4. stage_4_build_loan_scoring()  - splits Fund Utilization & Loan
                                              Activity into Fund Utilization +
                                              Loan Portfolio, adds Data Coverage
                                              category, recomputes Overall Score
                                              + full statewide re-rank
          5. stage_5_build_district_state()- aggregates every CLF up to District
                                              and State level
          6. stage_6_make_shell()         - generates index.html (the actual
                                              tracker UI/JS)

          A cross-script dependency existed before consolidation:
          build_district_state_data.py did `from build_loan_tab_data import
          xirr, LENDING_FUND_HEADINGS`. Since build_loan_tab_data's code stays
          at true module scope in this file (see point 3 above), that import
          line was removed - xirr/LENDING_FUND_HEADINGS are just already in
          scope as ordinary module-level names, no import needed.
"""

def stage_1_build_clf_data():
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
        "loans": "Loan data has not yet been scraped for this CLF's district. The Loans tab currently covers 15 districts, as the statewide CLF-meeting loan scrape is still in progress.",
        "fund_disbursement": "Fund disbursement data has not yet been scraped for this CLF's district.",
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
                "Neither Model nor Registered"
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
    TIPS = {'Number of VOs': 'How many Village Organisations belong to this CLF.', 'Number of SHGs': 'How many Self-Help Groups belong to this CLF, across all its VOs.', 'Total Members': 'How many individual SHG members belong to this CLF.', 'Active Members': "How many of this CLF's members are currently marked active on LokOS, out of the total.", 'Formation Date': 'The date this CLF was formed.', 'Registration Date': 'The date this CLF was formally registered as a cooperative society.', 'CLF Status': "Whether this CLF is both Model and Registered, Model only, Registered only, or neither, based on the audit form.", 'Platform Approval Status': "Whether this CLF's registration on LokOS has been approved, is pending, or was rejected by the Block Manager.", 'Co-option Status': 'Whether this CLF is newly formed, was co-opted from an earlier structure, or was revived after being inactive.', 'Registration Code': "This CLF's official state cooperative society registration number.", 'Executive Committee Size': "How many members sit on this CLF's Executive Committee.", 'President': "The name of this CLF's current President.", 'Secretary': "The name of this CLF's current Secretary.", 'Subcommittee Membership': "How many members sit on each of the CLF's six functional subcommittees.", 'Meeting Frequency': "How often this CLF's Executive Committee is supposed to meet.", 'Savings Schedule': 'How often this CLF collects savings from its member VOs.', 'Member Composition': "The share of this CLF's members who belong to Scheduled Castes/Scheduled Tribes.", 'Education & Literacy': "The share of this CLF's members who can read and write.", 'Members With a Livelihood': "The share of this CLF's members who reported at least one livelihood activity (farm or non-farm) on their LokOS profile.", 'Members With Multiple Livelihoods': 'The share of members who reported more than one livelihood activity.', 'Livelihood Split': "What kinds of livelihood activities this CLF's members are engaged in, combining their primary, secondary, and tertiary activities.", 'Special Project Activities': 'Which optional CLF-run initiatives — like a Custom Hiring Centre, Didi Ki Rasoi, or livestock activities — this CLF has taken up, as reported in its audit.', 'Cadre Roles in This CLF': 'Which specialised community-cadre roles (like Master Book Keeper, Enterprise Promotion, or Krishi Mitra) are staffed in this CLF, and how many members hold each.', 'Audit Grade': "This CLF's official letter grade from its most recent (Q4) audit.", 'Total Audit Score': "This CLF's total audit score out of 100, from its most recent (Q4) audit.", 'Standing vs. Peers': "How this CLF's total audit score compares to every other CLF in its district.", 'Accounting': 'How well this CLF keeps its accounting books, cash/bank records, loan verification, MIS quality, and fixed-asset records — out of 19, and how that compares to peers.', 'Receipts & Payments': 'How well this CLF manages and records its receipts and payments — out of 20, and how that compares to peers.', 'Loan': 'How well this CLF reviews loan demand and manages loan repayment — out of 18, and how that compares to peers.', 'Group Performance': "How well this CLF's SHGs, VOs, and the CLF itself are functioning — out of 23, and how that compares to peers.", 'Cadre Management': 'How well this CLF manages its cadre members — out of 10, and how that compares to peers.', 'Operational Self-Sufficiency': "How much of this CLF's running costs are covered by its own earned income — out of 10, and how that compares to peers.", 'Detailed Scores': 'The individual line items that make up each audit category score, so you can see exactly where points were gained or lost.', 'Irregularity Flag': "Whether this CLF's Q4 audit flagged a financial irregularity.", 'Type of Irregularity': "Which category of issue the auditor's remark falls under - a flagged CLF can match more than one.", 'Cash Book vs. Physical Cash': "The difference between what the CLF's cash book says it should have on hand, and what was physically counted during the audit.", 'Books Balance Check': "Whether this CLF's reported Total Assets match its reported Total Liabilities & Equity.", 'Liquidity Ratio': "The share of this CLF's assets sitting as cash or bank balance, rather than out on loan or invested.", 'Fund Deployment Ratio': "The share of this CLF's financial assets that are actually out on loan to SHGs, VOs, or other CLFs, rather than sitting idle.", 'Assets Composition': "How this CLF's total assets break down across every balance-sheet line item - cash, bank balance, loans out to SHGs/VOs, fixed assets, and the rest.", 'Liabilities Composition': "How this CLF's total liabilities and equity break down across every balance-sheet line item - government grants, member capital, institutional loans, savings held on behalf of VOs/SHGs, and the rest.", 'Surplus / Deficit': "This CLF's reported surplus or deficit, shown as a share of its total assets — a negative number means a deficit.", 'Net Cash Flow': 'The difference between what this CLF received and what it paid out this quarter.', 'Where Money Came In From': "What this quarter's receipts were made up of — savings deposits, interest earned, loan repayments, and other income.", 'Where Money Went Out To': "What this quarter's payments were made up of — loans and advances to member CBOs, running costs, and other expenses.", 'Operating Expense Ratio': 'How much this CLF spent on its own running costs, as a share of everything it received this quarter.', 'Interest Income Share': "How much of this CLF's receipts came from interest earned by lending to members, rather than from deposits or other income.", '% of Loans Disbursed': "Of everything ever requested by this CLF's member SHGs/VOs, how much has actually been disbursed to date.", 'Loan Amount Still Pending': 'How much loan money has been requested by member SHGs/VOs but not yet disbursed.', "This Quarter's Disbursement Rate": 'Of the new loan demand raised this specific quarter, how much was actually disbursed.', 'Capacity to Meet Demand': "Whether this CLF currently has enough cash and bank balance on hand to cover this quarter's still-pending loan demand.", 'Loan Amount Demanded This Quarter': 'How much loan money this CLF\'s member SHGs/VOs newly requested this specific quarter.', "This Quarter's Pending Loans": "Of this quarter's new loan demand, how much hasn't been disbursed yet.", 'Share of VOs Requesting Loans': "The share of this CLF's member VOs/SHGs that actively requested a loan this quarter.", 'Receipts Over Time': "How this CLF's quarterly receipts have moved over the last five quarters.", 'Payments Over Time': "How this CLF's quarterly payments have moved over the last five quarters.", 'Cash Balance Over Time': "How this CLF's quarterly closing cash balance has moved over the last five quarters.", 'Loans Disbursed Over Time': "How this CLF's total loans disbursed has grown over the last five quarters.", 'Disbursement Rate Over Time': "How this CLF's disbursement rate has moved over the last five quarters.", 'Balance Sheet': "This CLF's full balance sheet, exactly as reported.", 'Receipts & Payments Statement': "This CLF's full receipts and payments statement for the selected quarter, exactly as reported.", 'FSF-Eligible VOs': "How many of this CLF's VOs qualify for the Food Security Fund — at least 40% of their members belong to SC/ST groups.", 'VOs With Incomplete Coverage': "How many VOs haven't yet received their full ₹1,50,000 VRF government grant.", 'Idle VOs': 'Of the VOs that have received VRF funds, how many earned no interest from lending last year.', 'Total VRF Received': "The total VRF government grant this CLF's VOs have received to date.", 'Total Savings Collected': "The total member savings collected across this CLF's VOs.", 'Total Interest Collected': "The total interest earned by this CLF's VOs from lending VRF funds to members.", 'Total Fund Size': "The combined size of this CLF's VRF fund — grant, savings, and interest together.", 'Coverage Gap': "The total grant amount still owed to VOs that haven't received their full ₹1,50,000.", 'Savings as Promised': 'How much of the savings members were supposed to contribute has actually been collected.', 'What the Fund Is Made Of': "How this CLF's VRF fund breaks down between the original government grant, member savings, and interest earned.", 'Grant Not Fully Received': 'How many VOs are still owed part of their VRF grant.', 'Fund Sitting Idle': 'How many VOs that have received VRF funds earned no interest from lending them out last year.', 'VO Identity': 'Which VO this row is about, and who its bookkeeper is.', 'VO Fund Status': 'Whether this VO is FSF-eligible, has received its full grant, and is currently lending out its VRF funds.', 'SHG Members': 'How many SHG members belong to this VO.', 'VO Fund Metrics': 'How well this VO is doing on savings discipline, interest earned, and overall fund growth.', 'VO Savings Discipline %': 'How much of the promised monthly savings has actually been collected, out of what should have been saved by now.', 'VO Interest Yield': 'How much of the total fund came from interest earned by lending to members, rather than from savings or the grant.', 'VO Corpus Multiplier': 'How many times bigger the total fund has grown compared to the government grant it started with.', 'Bookkeeper Identity': 'Which bookkeeper this row is about, how many VOs and members they cover, and their overall rank within this CLF.', 'Bookkeeper Fund Metrics': "How this bookkeeper's VOs are doing on savings, fund growth, and coverage, combined into one composite score.", 'Fund Projections': "Where this CLF's savings, interest, and total fund size are headed by 31 March 2027, under three different assumptions about lending activity.", 'Social Development Fund': 'How much this CLF should be collecting from its VOs each month, and each year, for the Social Development Fund.', 'Month-by-Month Projection': "A month-by-month look at how this CLF's savings, interest, and total fund are projected to grow.", 'Total Entitlement Demands': "How many entitlement/scheme demands this CLF's members raised in the selected year.", 'Number of People Requesting': "How many of this CLF's members raised an entitlement demand in the selected year - every person in this dataset has requested an NREGA job card, forming the base population.", '% of Job Cards Accessed': "Of the members who requested an NREGA job card, what share have actually received it.", 'Non-NREGA Schemes Requested': "How many distinct government schemes, other than the NREGA job card itself, this CLF's members requested in the selected year - a count of scheme types touched, not total requests.", 'Demand & Access by Scheme': "Which government schemes this CLF's members are demanding most in the selected year. There's no fulfilment tracking for individual schemes in this data - only the NREGA job card's own accessed status is available, shown separately above.", 'Total PGSRD Requests': "How many public-goods, resource, or service requests this CLF's members raised in the selected year.", 'Type of Request': 'Whether requests are for public goods, resources, or services.', 'Most-Requested Items': 'The specific items requested most often, and the total quantity demanded for each.', 'By Development Theme': 'Which broader development themes these requests fall under.', 'By GPDP Focus Area': 'Which Gram Panchayat Development Plan focus areas these requests relate to.', 'Total Social Issues Raised': "How many social development issues this CLF's members raised in the selected year.", 'By Sector': 'Which broad sector these issues fall under.', 'Most-Raised Issues': 'The specific social issues raised most often, how many times each was raised, and roughly how many people each one affects.', 'Government Departments Involved': 'Which government departments are most often implicated by these issues.', 'Fund Deployment': "This CLF's fund deployment ratio, converted to a percentile against other CLFs.", 'Bookkeeping Accuracy': "How closely this CLF's Assets match its Liabilities+Equity, converted to a percentile — a closer match ranks higher.", 'VRF Fund Health': "This CLF's VRF fund-management metrics, each converted to a percentile against other CLFs.", 'Savings Discipline': "How much of the savings this CLF's VOs were expected to have collected by now has actually come in, member-weighted across VOs and converted to a percentile against other CLFs.", 'Savings Realisation': "How much savings this CLF's VOs have collected relative to the size of the VRF government grant they've received, member-weighted across VOs and converted to a percentile against other CLFs.", 'Corpus Multiplier': "How many times bigger this CLF's VRF fund has grown compared to the government grant it started with, member-weighted across VOs and converted to a percentile against other CLFs.", 'Interest Yield': "How much of this CLF's VRF fund is made up of interest earned by lending to members, rather than the original grant or savings, member-weighted across VOs and converted to a percentile against other CLFs.", 'Coverage Completion': "The share of this CLF's VOs, member-weighted, that have received their full ₹1,50,000 VRF government grant, converted to a percentile against other CLFs.", 'Subcommittee Completeness': "How many of this CLF's six subcommittees are actually staffed, converted to a percentile.", 'Active Membership': "The share of this CLF's members currently marked active, converted to a percentile.", 'Cadre Diversity': 'How many different specialised cadre roles this CLF has staffed, converted to a percentile.', 'Insurance Coverage': "The share of this CLF's members with insurance, converted to a percentile.", 'Aadhaar KYC Coverage': "The share of this CLF's members with Aadhaar KYC verified, converted to a percentile.", 'Livelihoods Diversification': "The share of this CLF's members with more than one livelihood activity, converted to a percentile.", 'Overall Score': "This CLF's overall performance score, combining fund management, financial health, governance, and inclusion into one number, and how it compares to other CLFs in its district and across Bihar.", 'Arrears Rate': "The share of this CLF's total current loan demand that is currently overdue (in arrears), per LokOS's live snapshot - lower is better, converted to a percentile against other CLFs with loan data.", 'Funding Source Diversity': "How many distinct fund headings (Community Investment Fund, Community Enterprise Fund, PMFME Seed Capital, and other central/state schemes) this CLF has actually received disbursements under, converted to a percentile against other CLFs with Fund Disbursement data."}

    FOOTNOTES = {
        "overview": "Sourced from LokOS Profile Reports, plus Odoo CLF Audit Reports for CLF status.",
        "audit": "Sourced from Odoo CLF Audit Reports; transaction data from the Credit Disbursement Report, LokOS.",
        "financial": "Sourced from LokOS: Balance Sheets (F01), Receipts &amp; Payments (F03), the Credit Disbursement Report (F05), and CLF-Meeting Fund Disbursement records.",
        "loans": "Sourced from LokOS CLF-Meeting records (LoanRequests, LoanRepayments).",
        "vrf": "Sourced from the VRF Monitoring and Management Portal, BRLPS.",
        "vprp": "Sourced from Entitlement Reports, VPRP LokOS portal.",
        "scoring": "Combines LokOS Profile Reports, Odoo CLF Audit Reports, LokOS financial reports (Balance Sheets, Receipts &amp; Payments, Credit Disbursement Report), and the VRF Monitoring and Management Portal (BRLPS).",
    }

    # Brief per-tab explanation shown in a context box at the top of each tab,
    # with the FOOTNOTES source line folded into the end of the same box.
    CONTEXT = {
        "overview": "A snapshot of the CLF's identity, structure, and membership. <b>Profile</b> covers formation and registration details, CLF status (Model/Registered), platform approval status, governance structure (President, Secretary, Executive Committee, subcommittees), and meeting/savings schedule. <b>Members</b> covers social inclusion and welfare coverage (SC/ST, OBC, disability, etc.), education levels, livelihood diversification, special project activities, and the CLF's cadre roster.",
        "audit": "The CLF's most recent audit results (FY 2025-26, Q4): its overall grade and score, how it compares to other CLFs in its district and statewide, a breakdown across the 6 scoring categories, the individual line items behind each category, whether the auditor flagged a financial irregularity (and what kind), and a cash book vs. physical cash reconciliation check.",
        "financial": "The CLF's balance sheet, quarterly cash flow, and credit disbursement, for a selected quarter. <b>Summary</b> gives key ratios (liquidity, fund deployment, surplus/deficit, books balance check) and a breakdown of where capital and cash came from and went to. <b>Statements</b> shows the full balance sheet and receipts &amp; payments statement, exactly as reported. <b>Fund Disbursement</b> shows the CLF's lifetime capital received from the state under every fund heading, not tied to a specific quarter.",
        "loans": "Tracks loans disbursed by the CLF to its member VOs, sourced from LokOS CLF-meeting records. <b>Portfolio Overview</b> gives CLF-wide totals, repayment/return metrics, portfolio composition, and every individual loan. <b>Loan Schedule</b> lets you inspect a from-scratch monthly amortization reconstruction for a specific VO/loan. Loans that have been fully repaid drop their origination details (amount, tenure, interest rate) from the source system once closed, so those loans show up with repayment history only.",
        "vrf": "Tracks the CLF's Vulnerability Reduction Fund, managed at the VO level. <b>KPI Snapshot</b> gives CLF-wide totals and fund health. <b>VO-Level Breakdown</b> lets you inspect every individual VO. <b>Bookkeeper &amp; CLF Rankings</b> compares the CLF's fund management to peers and ranks its bookkeepers. <b>Forecasts</b> projects where the fund is headed by 31 March 2027 under three lending-activity scenarios.",
        "vprp": "Requests and plans raised through VPRP, by year (2023-2025). <b>Entitlements</b> tracks government scheme demands (ration cards, pensions, insurance, etc.) and NREGA job card status. <b>PGSRD</b> (Public Goods, Services, and Resource Development) tracks requests for public infrastructure, resources, and services. <b>SDP</b> (Social Development Plan) tracks broader social issues raised and the government departments involved.",
        "scoring": "Combines every other tab into one performance score, against the CLF's district and Bihar statewide. <b>Overall</b> gives one pooled score plus a category breakdown, and lets you customize the weight given to each category. <b>By Category</b> shows every individual metric behind all 7 categories, with district/state percentiles and ranks.",
    }

    with open(f"{DATA_DIR}/shared.json", "w") as f:
        json.dump({"tips": TIPS, "err_msg": ERR_MSG, "footnotes": FOOTNOTES, "context": CONTEXT}, f)
    print(f"[{elapsed()}] Wrote shared.json (TIPS/ERR_MSG/FOOTNOTES/CONTEXT)")
    print(f"[{elapsed()}] ALL DONE.")


def stage_2_build_vprp_vo_data():
    """
    Author:   Claude (for Mohan)
    Created:  05/08/2026
    Purpose:  Statewide version of the VPRP-by-GP/by-VO + VO Overview data built
              and reviewed in the district tracker prototype
              (3_Output/CLF Tracker/build_vprp_vo_extra_data.py, Saran-only).
              Same source files, same aggregation formulas (VPRP filtering/
              labels straight from build_tracker_data.py's own VPRP section;
              member-collapse formulas from 1_Code/collapse_lokos_members_to_clf.do,
              just grouped by vo_code instead of clf_code), extended to every
              district and every CLF, with three fixes found during prototype
              review:
                1. Entitlements and PGSRD no longer share the same
                   "n_vo_requesting" dict key (PGSRD was silently overwriting
                   Entitlements' own count, since PGSRD is computed second).
                2. State-specific scheme sub-rows also get their own VO
                   count / GP names now, not just the parent scheme row.
                3. Free-text fields (VO/president/secretary/GP/item/issue names)
                   are title-cased here at the source, matching the same
                   convention build_tracker_data.py already uses for CLF name/
                   district/block, instead of being left in raw ALL CAPS.

              Writes results directly into each CLF's already-built
              data/clfs/{mis_id}.json (adds vo_overview, replaces vprp.years
              with the richer version) - run AFTER build_tracker_data.py's own
              rebuild finishes (it would otherwise overwrite these additions).

              Efficiency note: unlike the Saran-only prototype version (which
              could afford to re-filter the full raw dataframe once per CLF),
              this pre-groups the raw VPRP frames by (district, clf_name) once
              up front so each of the 1667 CLFs' own lookup is O(1) instead of
              an O(n) scan - block matching (fuzzy .str.contains, not exact,
              since block-name formatting can differ from the crosswalk) still
              happens per-CLF, but against an already-tiny candidate slice.

    Input:  2_Data/Processed/{clf_id_crosswalk,f0x_vprp_clf_crosswalk}.dta
            2_Data/Cleaned/{clf_vprp_entitlements,clf_pgsrd_requests,clf_sdp,
                            lokos_groups_clean,lokos_members_mapped}.dta
            Scale-Up/data/clfs/*.json (must already exist - run after
            build_tracker_data.py)
    Output: Scale-Up/data/clfs/*.json (updated in place)
    """

    import pandas as pd
    import json
    import time
    import os

    BASE = "/Users/mohanrajagopal/Dropbox/Bihar Gates Team/3_Jeevika/6_LokOS_Analysis"
    CLEANED = f"{BASE}/2_Data/Cleaned"
    PROCESSED = f"{BASE}/2_Data/Processed"
    DATA_DIR = f"{BASE}/3_Output/CLF Tracker/Scale-Up/data"
    CHECKPOINT = f"{BASE}/3_Output/CLF Tracker/Scale-Up/_scratch_all_members.pkl"

    T0 = time.time()
    def elapsed(): return f"{time.time()-T0:.1f}s"

    def tc(s):
        """Title-case, matching the convention build_tracker_data.py already
        uses for clf_name_lokos/district/block - None-safe."""
        if s is None:
            return None
        s = str(s).strip()
        return s.title() if s else s

    # ============================================================================
    # 1. Crosswalks (every district this time, not just Saran)
    # ============================================================================
    print(f"[{elapsed()}] Loading crosswalks...")
    xwalk = pd.read_stata(f"{PROCESSED}/clf_id_crosswalk.dta", convert_categoricals=True)
    vxwalk = pd.read_stata(f"{PROCESSED}/f0x_vprp_clf_crosswalk.dta").set_index("mis_id")
    all_clfs = xwalk.dropna(subset=["mis_id"]).copy()
    all_clfs["mis_id"] = all_clfs["mis_id"].astype(int)
    print(f"[{elapsed()}] {len(all_clfs)} CLFs statewide")

    # ============================================================================
    # 2. VPRP: by-GP / by-VO breakdown + new KPIs, per CLF per year
    # ============================================================================
    print(f"[{elapsed()}] Loading raw VPRP files (statewide)...")
    ent_raw = pd.read_stata(f"{CLEANED}/clf_vprp_entitlements.dta", convert_categoricals=True)
    pgsrd_raw = pd.read_stata(f"{CLEANED}/clf_pgsrd_requests.dta", convert_categoricals=True)
    sdp_raw = pd.read_stata(f"{CLEANED}/clf_sdp.dta", convert_categoricals=True)
    print(f"[{elapsed()}] ent={len(ent_raw)} pgsrd={len(pgsrd_raw)} sdp={len(sdp_raw)} rows")

    print(f"[{elapsed()}] Pre-grouping raw VPRP frames by (district, clf_name) for fast lookup...")
    ent_groups = {k: v for k, v in ent_raw.groupby(["district", "clf_name"], observed=True)}
    pgsrd_groups = {k: v for k, v in pgsrd_raw.groupby(["district", "clf_name"], observed=True)}
    sdp_groups = {k: v for k, v in sdp_raw.groupby(["district", "clf_name"], observed=True)}
    EMPTY_ENT, EMPTY_PGSRD, EMPTY_SDP = ent_raw.iloc[0:0], pgsrd_raw.iloc[0:0], sdp_raw.iloc[0:0]

    SCHEME_LABELS = {
        "state-specific": "State-Specific Schemes", "pmayg": "Pradhan Mantri Awaas Yojana – Gramin (PMAY-G)",
        "mgnregs-job-card": "MGNREGS Job Card", "healthcard": "Health Card",
        "ujjwala": "Pradhan Mantri Ujjwala Yojana (PMUY)", "pmjjby": "Pradhan Mantri Jeevan Jyoti Bima Yojana (PMJJBY)",
        "pmsby": "Pradhan Mantri Suraksha Bima Yojana (PMSBY)", "rationcard": "Ration Card",
        "pmsbhgy": "PMSBHGY", "widow-pension": "Widow Pension", "disability-pension": "Disability Pension",
        "rationcard-add": "Ration Card Addition", "old-age-pension": "Old-Age Pension",
    }
    PGSRD_LABELS = {"pgsrd-public-goods": "Public Goods", "pgsrd-resources": "Resources", "pgsrd-services": "Services"}

    def block_filter(df, block):
        if not len(df):
            return df
        return df[df["block"].str.contains(block, case=False, na=False)]

    def vo_gp_cols(sub):
        return int(sub["vo_name"].nunique()), sorted(tc(g) for g in sub["gp_name"].dropna().unique().tolist())

    def build_yr_data(e_y, p_y, s_y):
        """Same shape as build_tracker_data.py's own yr_data, extended with
        n_vo/gp_names columns on each breakdown row (incl. state-specific
        sub-rows) and new, DOMAIN-SPECIFIC VO-count KPI keys - entitlements and
        PGSRD each get their own key now (n_vo_requesting_ent /
        n_vo_requesting_pgsrd), not a shared one that silently overwrites."""
        yr_data = {"n_demands": len(e_y), "n_pgsrd": len(p_y), "n_sdp": len(s_y)}
        if len(e_y):
            accessed = int(e_y["nrega_accessed"].sum())
            yr_data["pct_nrega_accessed"] = round(accessed / len(e_y) * 100, 1)
            other_schemes = e_y[e_y["scheme_type"] != "mgnregs-job-card"]
            n_other = int(other_schemes["scheme_type"].nunique())
            has_nrega = bool((e_y["scheme_type"] == "mgnregs-job-card").any())
            yr_data["n_other_schemes"] = n_other
            yr_data["n_schemes_total"] = n_other + (1 if has_nrega else 0)
            yr_data["n_vo_requesting_ent"] = int(e_y["vo_name"].nunique())
            by_scheme_counts = e_y.groupby("scheme_type").size().sort_values(ascending=False)
            by_scheme_counts = by_scheme_counts[by_scheme_counts > 0]
            by_scheme = []
            for s, v in by_scheme_counts.head(6).items():
                sub = e_y[e_y["scheme_type"] == s]
                n_vo, gp_names = vo_gp_cols(sub)
                by_scheme.append({"scheme": SCHEME_LABELS.get(s, s), "raw_scheme": s, "demanded": int(v), "n_vo": n_vo, "gp_names": gp_names})
            yr_data["by_scheme"] = by_scheme
            state_ss = e_y[e_y["scheme_type"] == "state-specific"]
            if len(state_ss):
                ss_grouped = state_ss.groupby("state_scheme")
                ss_counts = ss_grouped.size().sort_values(ascending=False)
                ss_counts = ss_counts[ss_counts > 0]
                state_scheme_breakdown = {}
                for scheme_name, count in ss_counts.items():
                    sub = state_ss[state_ss["state_scheme"] == scheme_name]
                    n_vo, gp_names = vo_gp_cols(sub)
                    state_scheme_breakdown[tc(scheme_name)] = {"demanded": int(count), "n_vo": n_vo, "gp_names": gp_names}
                yr_data["state_scheme_breakdown"] = state_scheme_breakdown
            cat_counts = e_y["social_category"].value_counts(normalize=True) * 100
            yr_data["social_category"] = {k: round(v, 1) for k, v in cat_counts.items()} if cat_counts.notna().any() else None
        if len(p_y):
            vc = p_y["pgsrd_type"].value_counts(normalize=True) * 100
            yr_data["pgsrd_type_split"] = {PGSRD_LABELS.get(k, k): round(v, 1) for k, v in vc[vc > 0].items()}
            yr_data["n_vo_requesting_pgsrd"] = int(p_y["vo_name"].nunique())
            items_agg = p_y.groupby(["item_demanded", "pgsrd_type"]).agg(n=("unitsdemanded", "size"), units=("unitsdemanded", "sum")).reset_index().sort_values("n", ascending=False)
            items_list = []
            for _, row in items_agg.head(8).iterrows():
                sub = p_y[(p_y["item_demanded"] == row["item_demanded"]) & (p_y["pgsrd_type"] == row["pgsrd_type"])]
                n_vo, gp_names = vo_gp_cols(sub)
                items_list.append({
                    "item_demanded": tc(row["item_demanded"]), "pgsrd_type": PGSRD_LABELS.get(row["pgsrd_type"], row["pgsrd_type"]),
                    "n": int(row["n"]), "units": float(row["units"]), "n_vo": n_vo, "gp_names": gp_names,
                })
            yr_data["pgsrd_items"] = items_list
            sdg = p_y["sankalp_sdg_theme"].value_counts()
            yr_data["sdg_theme"] = {tc(k): int(v) for k, v in sdg[sdg > 0].head(6).items()}
            gpdp = p_y["gpdp_area"].value_counts()
            yr_data["gpdp_area"] = {tc(k): int(v) for k, v in gpdp[gpdp > 0].head(6).items()}
        if len(s_y):
            vc = s_y["sector"].value_counts(normalize=True) * 100
            yr_data["sdp_sector"] = {k: round(v, 1) for k, v in vc[vc > 0].items()}
            yr_data["n_vo"] = int(s_y["vo_name"].nunique())
            issues_agg = s_y.groupby("social_issue").agg(n=("affected_people_num", "size"), affected=("affected_people_num", "median")).reset_index().sort_values("n", ascending=False)
            issues_list = []
            for _, row in issues_agg.head(6).iterrows():
                sub = s_y[s_y["social_issue"] == row["social_issue"]]
                n_vo, gp_names = vo_gp_cols(sub)
                issues_list.append({
                    "social_issue": tc(row["social_issue"]), "n": int(row["n"]),
                    "affected": (round(row["affected"]) if pd.notna(row["affected"]) else None),
                    "n_vo": n_vo, "gp_names": gp_names,
                })
            yr_data["sdp_issues"] = issues_list
            dept_cols = [c for c in s_y.columns if c.startswith("department_")]
            depts = pd.concat([s_y[c] for c in dept_cols]).dropna()
            dc = depts.value_counts()
            yr_data["departments"] = {tc(k): int(v) for k, v in dc[dc > 0].head(6).items()}
            yr_data["n_departments"] = int((dc > 0).sum())
        return yr_data

    print(f"[{elapsed()}] Building VPRP by-GP/by-VO data for {len(all_clfs)} CLFs...")
    vprp_extra = {}
    n_done = 0
    for _, row in all_clfs.iterrows():
        mis_id = int(row["mis_id"])
        district, block = row["district"], row["block"]
        xr = vxwalk.loc[mis_id] if mis_id in vxwalk.index else pd.Series(dtype=object)

        ent_cand = ent_groups.get((district, xr.get("vprp_ent_raw_name")), EMPTY_ENT)
        pgsrd_cand = pgsrd_groups.get((district, xr.get("vprp_pgsrd_raw_name")), EMPTY_PGSRD)
        sdp_cand = sdp_groups.get((district, xr.get("vprp_sdp_raw_name")), EMPTY_SDP)
        ent = block_filter(ent_cand, block)
        pgsrd = block_filter(pgsrd_cand, block)
        sdp = block_filter(sdp_cand, block)

        years_out = {}
        for yr in [2023, 2024, 2025]:
            e_y = ent[ent["year"] == yr] if len(ent) else ent
            p_y = pgsrd[pgsrd["year"] == yr] if len(pgsrd) else pgsrd
            s_y = sdp[sdp["year"] == yr] if len(sdp) else sdp
            yr_data = build_yr_data(e_y, p_y, s_y)
            all_gps = set(e_y["gp_name"].dropna().unique()) | set(p_y["gp_name"].dropna().unique()) | set(s_y["gp_name"].dropna().unique())
            by_gp = {}
            for gp in sorted(all_gps):
                by_gp[tc(gp)] = build_yr_data(e_y[e_y["gp_name"] == gp], p_y[p_y["gp_name"] == gp], s_y[s_y["gp_name"] == gp])
            yr_data["by_gp"] = by_gp
            yr_data["gp_list"] = sorted(tc(g) for g in all_gps)
            years_out[str(yr)] = yr_data
        vprp_extra[mis_id] = {"years": years_out}
        n_done += 1
        if n_done % 200 == 0:
            print(f"[{elapsed()}]   ...{n_done}/{len(all_clfs)} CLFs")
    print(f"[{elapsed()}] VPRP extra data built for {len(vprp_extra)} CLFs.")

    # ============================================================================
    # 3. VO-level Overview: governance fields (lokos_groups_clean.dta)
    # ============================================================================
    print(f"[{elapsed()}] Loading lokos_groups_clean.dta...")
    g = pd.read_stata(f"{CLEANED}/lokos_groups_clean.dta", convert_categoricals=True)
    gs = g.dropna(subset=["clf_code", "vo_code"]).copy()
    clf_code_to_mis = dict(zip(all_clfs["clfcode"], all_clfs["mis_id"]))

    GOV_COLS = [
        "vo_code", "vo_name", "clf_code", "vo_block", "vo_nic_code", "vo_mappedshgs", "vo_ec_mem_count",
        "vo_signat_count", "vo_approvalstatus", "vo_president", "vo_secretary", "vo_cooptionstatus",
        "vo_meeting_frequency", "vo_savings_frequency", "vo_savings_amount",
        "vo_special_subcom_mem_count", "vo_monit_subcom_mem_count", "vo_bl_subcom_mem_count",
        "vo_sa_subcom_mem_count", "vo_asset_subcom_mem_count", "vo_lp_subcom_mem_count",
        "vo_formation_date", "vo_registration_date", "vo_active", "vo_financial_intermediation",
    ]
    vo_gov = gs.drop_duplicates(subset=["vo_code"])[GOV_COLS].copy()
    vo_gov["mis_id"] = vo_gov["clf_code"].map(clf_code_to_mis)
    vo_gov = vo_gov.dropna(subset=["mis_id"])
    vo_gov["mis_id"] = vo_gov["mis_id"].astype(int)
    print(f"[{elapsed()}] {len(vo_gov)} VOs with governance data statewide.")

    # ============================================================================
    # 4. VO-level Overview: member welfare/education/livelihood/cadre stats,
    # grouped by vo_code instead of clf_code (exact formulas from
    # 1_Code/collapse_lokos_members_to_clf.do). Registration Date dropped from
    # the eventual output entirely (confirmed: only 0.3% of Saran VOs have one,
    # vs 100% for Formation Date - not worth carrying/showing anywhere).
    # ============================================================================
    if os.path.exists(CHECKPOINT):
        print(f"[{elapsed()}] Loading members from checkpoint (skipping the 16.7M-row full read)...")
        ms = pd.read_pickle(CHECKPOINT)
    else:
        print(f"[{elapsed()}] Loading lokos_members_mapped.dta (16.7M rows, statewide)...")
        MEMBER_COLS = [
            "member_code", "vo_code", "clf_code", "member_active", "member_social_category",
            "member_insurance", "member_disability_self", "member_disability_family", "member_family_head",
            "member_aadhaar_kyc", "member_education", "member_pvtg_category", "member_cadre_role",
            "member_primarylivelihoods", "member_secondarylivelihoods", "member_tertiarylivelihoods",
        ]
        ms = pd.read_stata(f"{CLEANED}/lokos_members_mapped.dta", convert_categoricals=True, columns=MEMBER_COLS)
        ms = ms.dropna(subset=["vo_code"]).copy()
        ms.to_pickle(CHECKPOINT)
        print(f"[{elapsed()}] Saved checkpoint to {CHECKPOINT}")
    print(f"[{elapsed()}] {len(ms)} members loaded.")

    ms["i_active"] = ms["member_active"] == 1
    ms["i_sc"] = ms["member_social_category"] == "SC"
    ms["i_st"] = ms["member_social_category"] == "ST"
    ms["i_scst"] = ms["i_sc"] | ms["i_st"]
    ms["i_obc"] = ms["member_social_category"] == "OBC"
    ms["i_general"] = ms["member_social_category"] == "GENERAL"
    ms["i_other_cat"] = ms["member_social_category"].isin(["OTHER", "DNT"])
    ms["social_cat_reported"] = ms["member_social_category"].notna()

    ms["i_insurance"] = ms["member_insurance"] == 1
    ms["insurance_reported"] = ms["member_insurance"].notna()
    ms["i_dis_self"] = ms["member_disability_self"] == 1
    ms["dis_self_reported"] = ms["member_disability_self"].notna()
    ms["i_dis_family"] = ms["member_disability_family"] == 1
    ms["dis_family_reported"] = ms["member_disability_family"].notna()
    ms["i_family_head"] = ms["member_family_head"] == 1
    ms["i_aadhaar"] = ms["member_aadhaar_kyc"] == 1
    ms["i_pvtg"] = ms["member_pvtg_category"].notna()

    ms["edu_reported"] = ms["member_education"].notna()
    ms["i_can_read"] = ms["edu_reported"] & (ms["member_education"] != "ILLITERATE")
    PRIMARY_PLUS = ["CLASS - 5", "CLASS - 7", "CLASS - 8", "CLASS - 10", "CLASS - 12", "DIPLOMA", "BACHELORs DEGREE", "MASTERs DEGREE", "DOCTORATE"]
    SECONDARY_PLUS = ["CLASS - 10", "CLASS - 12", "DIPLOMA", "BACHELORs DEGREE", "MASTERs DEGREE", "DOCTORATE"]
    GRAD_PLUS = ["BACHELORs DEGREE", "MASTERs DEGREE", "DOCTORATE"]
    ms["i_primary_plus"] = ms["member_education"].isin(PRIMARY_PLUS)
    ms["i_secondary_plus"] = ms["member_education"].isin(SECONDARY_PLUS)
    ms["i_grad_plus"] = ms["member_education"].isin(GRAD_PLUS)

    ms["livelihood_reported"] = ms["member_primarylivelihoods"].notna()
    has_primary = ms["member_primarylivelihoods"].notna() & ~ms["member_primarylivelihoods"].isin(["No Primary Livelihood"])
    has_secondary = ms["member_secondarylivelihoods"].notna() & ~ms["member_secondarylivelihoods"].isin(["No Secondary Livelihood"])
    has_tertiary = ms["member_tertiarylivelihoods"].notna() & ~ms["member_tertiarylivelihoods"].isin(["No Tertiary Livelihood"])
    ms["i_has_livelihood"] = has_primary
    ms["i_multi_livelihood"] = has_primary & (has_secondary | has_tertiary)

    AGRI = ["Agriculture Activities", "Organic Agriculture Activities", "Horticulture Activities", "NTFP Collection"]
    LIVESTK = ["Livestock Rearing-Dairy", "Livestock Rearing-Goatery", "Livestock Rearing-Poultry", "Other Livestock Rearing", "Fishery Activities"]
    WAGE = ["Daily Wages-NREGA", "Daily Wages-Others", "Salaried Job-Government", "Salaried Job-Private"]
    TRADE = ["Trading - All Types", "Trading Vegetables", "Services - All Types", "Manufacturing - Food Processing", "Manufacturing - Handicraft", "Manufacturing - Handloom", "Manufacturing - Others"]
    OTHER_LV = ["Community Cadre Services", "Other Livelihoods Activities"]
    BUCKETS = {"agri": AGRI, "livestk": LIVESTK, "wage": WAGE, "trade": TRADE, "other": OTHER_LV}
    for lv in ["primary", "secondary", "tertiary"]:
        col = {"primary": "member_primarylivelihoods", "secondary": "member_secondarylivelihoods", "tertiary": "member_tertiarylivelihoods"}[lv]
        for bname, blist in BUCKETS.items():
            ms[f"{lv}_{bname}"] = ms[col].isin(blist)
    for bname in BUCKETS:
        ms[f"lv_{bname}"] = ms[f"primary_{bname}"] | ms[f"secondary_{bname}"] | ms[f"tertiary_{bname}"]
    vote_cols = [f"{lv}_{b}" for lv in ["primary", "secondary", "tertiary"] for b in BUCKETS]
    ms["lv_bucket_votes"] = ms[vote_cols].sum(axis=1)

    ROLES = ["BC Shakhi", "Bank Shakhi", "Bima Sakhi", "Business Dev. Service Provider (BDSP)", "CLF Book Keeper/ Accountant",
        "Community Auditor", "Community Coordinator", "Community Mobilizer-Facilitator", "Community Trainer",
        "Enterprise Promotion(EP)", "Financial Literacy CRP", "Gender Mitra", "Health Activist-Swasthya Sakhi",
        "Krishi Mitra", "Master Book Keeper", "Matsya Sakhi", "PRP", "Pashu Sakhi", "Samuh Sakhi", "Staff in BLF",
        "Staff in CLF", "Staff in CTC", "Staff in PE", "Staff in PG", "Staff in VO", "Udyog Mitra",
        "VO Book Keeper/ Accountant", "Van Sakhi", "Woman Activist", "eCRP/eBK/Tab Didi"]
    ms["member_cadre_role"] = ms["member_cadre_role"].astype(object).fillna("")
    for role in ROLES:
        ms[f"cadre__{role}"] = ms["member_cadre_role"].str.contains(role, regex=False)
    cadre_cols = [f"cadre__{r}" for r in ROLES]
    ms["has_any_cadre"] = ms[cadre_cols].any(axis=1)

    print(f"[{elapsed()}] Collapsing to VO grain...")
    agg_dict = {
        "member_code": ("member_code", "count"),
        "n_active": ("i_active", "sum"), "n_scst": ("i_scst", "sum"), "n_obc": ("i_obc", "sum"),
        "n_general": ("i_general", "sum"), "n_other_cat": ("i_other_cat", "sum"),
        "n_social_cat_reported": ("social_cat_reported", "sum"),
        "n_insurance": ("i_insurance", "sum"), "n_insurance_reported": ("insurance_reported", "sum"),
        "n_dis_self": ("i_dis_self", "sum"), "n_dis_self_reported": ("dis_self_reported", "sum"),
        "n_dis_family": ("i_dis_family", "sum"), "n_dis_family_reported": ("dis_family_reported", "sum"),
        "n_family_head": ("i_family_head", "sum"), "n_aadhaar": ("i_aadhaar", "sum"), "n_pvtg": ("i_pvtg", "sum"),
        "n_edu_reported": ("edu_reported", "sum"), "n_can_read": ("i_can_read", "sum"),
        "n_primary_plus": ("i_primary_plus", "sum"), "n_secondary_plus": ("i_secondary_plus", "sum"),
        "n_grad_plus": ("i_grad_plus", "sum"), "n_livelihood_reported": ("livelihood_reported", "sum"),
        "n_has_livelihood": ("i_has_livelihood", "sum"), "n_multi_livelihood": ("i_multi_livelihood", "sum"),
        "lv_agri_votes": ("lv_agri", "sum"), "lv_livestk_votes": ("lv_livestk", "sum"),
        "lv_wage_votes": ("lv_wage", "sum"), "lv_trade_votes": ("lv_trade", "sum"), "lv_other_votes": ("lv_other", "sum"),
        "lv_bucket_votes": ("lv_bucket_votes", "sum"), "n_cadre_holders": ("has_any_cadre", "sum"),
    }
    vo_collapsed = ms.groupby("vo_code").agg(**agg_dict).reset_index()
    vo_collapsed = vo_collapsed.rename(columns={"member_code": "n_members"})

    cadre_by_vo = ms.groupby("vo_code")[cadre_cols].sum()
    cadre_by_vo["n_distinct_cadre_role_types"] = (cadre_by_vo > 0).sum(axis=1)
    cadre_roster_by_vo = {}
    for vo_code, row in cadre_by_vo.iterrows():
        roster = {tc(r): int(row[f"cadre__{r}"]) for r in ROLES if row[f"cadre__{r}"] > 0}
        cadre_roster_by_vo[vo_code] = roster
    n_distinct_cadre_by_vo = cadre_by_vo["n_distinct_cadre_role_types"].to_dict()
    del ms

    def pct(num, den):
        return round(num / den * 100, 1) if den else None

    print(f"[{elapsed()}] Merging governance + member data into per-VO records...")
    vo_member_by_code = vo_collapsed.set_index("vo_code").to_dict("index")

    VO_OVERVIEW = {}
    for _, gov in vo_gov.iterrows():
        vo_code = gov["vo_code"]
        mis_id = int(gov["mis_id"])
        mem = vo_member_by_code.get(vo_code, {})
        n_members = mem.get("n_members", 0)
        vo_data = {
            "vo_code": vo_code, "vo_name": tc(gov["vo_name"]), "block": tc(gov["vo_block"]),
            "nic_code": gov["vo_nic_code"], "n_shgs": gov["vo_mappedshgs"], "ec_count": gov["vo_ec_mem_count"],
            "approval_status": gov["vo_approvalstatus"], "president": tc(gov["vo_president"]), "secretary": tc(gov["vo_secretary"]),
            "cooption_status": gov["vo_cooptionstatus"], "meeting_frequency": gov["vo_meeting_frequency"],
            "savings_frequency": gov["vo_savings_frequency"], "savings_amount": gov["vo_savings_amount"],
            "subcom": {
                "Special": gov["vo_special_subcom_mem_count"], "Monitoring": gov["vo_monit_subcom_mem_count"],
                "Bank Linkage": gov["vo_bl_subcom_mem_count"], "Social Action": gov["vo_sa_subcom_mem_count"],
                "Asset Verification": gov["vo_asset_subcom_mem_count"], "Livelihoods Promotion": gov["vo_lp_subcom_mem_count"],
            },
            "formation_date": str(gov["vo_formation_date"])[:10] if pd.notna(gov["vo_formation_date"]) else None,
            "active": bool(gov["vo_active"]) if pd.notna(gov["vo_active"]) else None,
            "financial_intermediation": gov["vo_financial_intermediation"],
            "n_members": int(n_members),
            "pct_active": pct(mem.get("n_active", 0), n_members),
            "coverage": {
                "Scheduled Castes / Scheduled Tribes (SC/ST)": [pct(mem.get("n_scst", 0), n_members), int(mem.get("n_scst", 0) or 0)],
                "Other Backward Classes (OBC)": [pct(mem.get("n_obc", 0), n_members), int(mem.get("n_obc", 0) or 0)],
                "General Category": [pct(mem.get("n_general", 0), n_members), int(mem.get("n_general", 0) or 0)],
                "Members with Disabilities": [pct(mem.get("n_dis_self", 0), mem.get("n_dis_self_reported", 0)), int(mem.get("n_dis_self", 0) or 0)],
                "Members with Disabled Family Members": [pct(mem.get("n_dis_family", 0), mem.get("n_dis_family_reported", 0)), int(mem.get("n_dis_family", 0) or 0)],
                "Family Head": [pct(mem.get("n_family_head", 0), n_members), int(mem.get("n_family_head", 0) or 0)],
                "Particularly Vulnerable Tribal Groups (PVTG)": [pct(mem.get("n_pvtg", 0), n_members), int(mem.get("n_pvtg", 0) or 0)],
            },
            "education": {
                "Can Read/Write": [pct(mem.get("n_can_read", 0), mem.get("n_edu_reported", 0)), int(mem.get("n_can_read", 0) or 0)],
                "Completed Primary School": [pct(mem.get("n_primary_plus", 0), mem.get("n_edu_reported", 0)), int(mem.get("n_primary_plus", 0) or 0)],
                "Completed Secondary School": [pct(mem.get("n_secondary_plus", 0), mem.get("n_edu_reported", 0)), int(mem.get("n_secondary_plus", 0) or 0)],
                "Completed Graduation": [pct(mem.get("n_grad_plus", 0), mem.get("n_edu_reported", 0)), int(mem.get("n_grad_plus", 0) or 0)],
            },
            "pct_has_livelihood": pct(mem.get("n_has_livelihood", 0), mem.get("n_livelihood_reported", 0)),
            "pct_multi_livelihood": pct(mem.get("n_multi_livelihood", 0), mem.get("n_livelihood_reported", 0)),
            "livelihood_split": {
                "Agriculture": pct(mem.get("lv_agri_votes", 0), mem.get("lv_bucket_votes", 0)),
                "Livestock": pct(mem.get("lv_livestk_votes", 0), mem.get("lv_bucket_votes", 0)),
                "Wage Labour": pct(mem.get("lv_wage_votes", 0), mem.get("lv_bucket_votes", 0)),
                "Trade/Enterprise": pct(mem.get("lv_trade_votes", 0), mem.get("lv_bucket_votes", 0)),
                "Other": pct(mem.get("lv_other_votes", 0), mem.get("lv_bucket_votes", 0)),
            },
            "n_cadre_holders": int(mem.get("n_cadre_holders", 0) or 0),
            "n_distinct_cadre_types": int(n_distinct_cadre_by_vo.get(vo_code, 0)),
            "cadre_roster": dict(sorted(cadre_roster_by_vo.get(vo_code, {}).items(), key=lambda kv: -kv[1])),
        }
        VO_OVERVIEW.setdefault(mis_id, []).append(vo_data)

    for mis_id in VO_OVERVIEW:
        VO_OVERVIEW[mis_id].sort(key=lambda v: v["vo_name"])
    print(f"[{elapsed()}] VO Overview data built for {len(VO_OVERVIEW)} CLFs, {sum(len(v) for v in VO_OVERVIEW.values())} VOs total.")

    # ============================================================================
    # 5. Write results into each CLF's already-built data/clfs/{mis_id}.json
    # ============================================================================
    def sanitize(obj):
        if isinstance(obj, dict):
            return {str(k): sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [sanitize(v) for v in obj]
        if isinstance(obj, float) and pd.isna(obj):
            return None
        return obj

    print(f"[{elapsed()}] Writing results into per-CLF JSON files...")
    n_written, n_missing_file = 0, 0
    for mis_id in all_clfs["mis_id"]:
        path = f"{DATA_DIR}/clfs/{mis_id}.json"
        if not os.path.exists(path):
            n_missing_file += 1
            continue
        with open(path) as f:
            data = json.load(f)
        ve = vprp_extra.get(mis_id)
        if ve:
            data["vprp"]["years"] = sanitize(ve["years"])
        vo = VO_OVERVIEW.get(mis_id)
        data["vo_overview"] = sanitize(vo) if vo else []
        with open(path, "w") as f:
            json.dump(data, f)
        n_written += 1
        if n_written % 300 == 0:
            print(f"[{elapsed()}]   ...{n_written} files written")
    print(f"[{elapsed()}] Done: {n_written} CLF files updated, {n_missing_file} CLFs had no existing JSON (skipped).")


"""
Injects a "loans" block into every CLF json that has LokOS CLF-meeting loan
data, sourced from 2_Data/Cleaned/clf_loan_tracker.dta and
2_Data/Processed/loan_repayments_by_month.dta (see
1_Code/clf_loan_cleaning.do). Scope: whatever districts that do-file has been
run for so far (15 as of 2026-08-25 - see clf_loan_cleaning.do's header) -
this script only touches the CLF jsons that actually match a clfcode in the
loan data; every other CLF's json is left untouched, and the tracker's
renderLoans() JS shows the standard "not found" disclaimer for those.

Two things live in the "loans" block:
1. Portfolio Overview data: KPI tiles, fund-source/loan-type/status mix,
   VO loan-count histogram, and the full loan-level table.
2. Loan Schedule data: a from-scratch monthly amortization reconstruction per
   active loan (Fixed Principal uses the ACT/365 formula verified against the
   raw LokOS schedule to the cent; EMI uses the standard annuity formula as an
   approximation - only 3.4% of loans are EMI). Matched against actual
   monthly repayment totals to get Amount Due / Amount Repaid / Status /
   Arrear / Outstanding per month, for the VO -> Loan Number dropdown table.
   Only covers the window where we actually observe repayment transactions
   (the loan_repayments_by_month data's own date range) - showing "Not Paid"
   for months before our repayment log starts would be misleading, since we
   genuinely don't know what happened before then. Loans whose raw remaining
   schedule shows signs of restructuring (schedule_is_monotonic==0) are
   marked unreliable rather than given a reconstructed schedule that has no
   way to know about a mid-life renegotiation.

Also computes pooled portfolio Total XIRR per CLF (see xirr()/
build_xirr_cashflows() below) and injects a "fund_disbursement" block
(2_Data/Cleaned/clf_fund_disbursement.dta) used by the Financial Records tab's
Fund Disbursement subtab.

XIRR scope note: computed over ACTIVE loans only. LokOS drops a loan's
origination metadata (amount, disbursement date) once it fully closes, so a
closed loan's outflow can never be reconstructed - including its repayment
inflows without a matching outflow would inflate the rate artificially. Only
Total XIRR (real disbursement/repayment cash flows + the currently-outstanding
balance credited back as if collected today) is shipped - a "Realized" variant
with no terminal value was tried and dropped (see comment at its computation
site below): it came out near-uniformly around -85% regardless of actual CLF
performance, because an active, mid-tenure loan book has most of its principal
legitimately still outstanding (not overdue, just not due yet), which Realized
XIRR gives zero credit for.
"""
import calendar
import json
import math
import re
from datetime import date
from pathlib import Path

import pandas as pd
from scipy.optimize import brentq

BASE = Path("/Users/mohanrajagopal/Dropbox/Bihar Gates Team/3_Jeevika/6_LokOS_Analysis")
LOAN_DTA = BASE / "2_Data/Cleaned/clf_loan_tracker.dta"
MONTHLY_REPAY_DTA = BASE / "2_Data/Processed/loan_repayments_by_month.dta"
TRANSACTIONS_DTA = BASE / "2_Data/Processed/loan_repayments_transactions.dta"
FUND_DISBURSEMENT_DTA = BASE / "2_Data/Cleaned/clf_fund_disbursement.dta"
CLF_DIR = BASE / "3_Output/CLF Tracker/Scale-Up/data/clfs"

FUND_COLORS_ORDER = [
    "Community Investment Fund (CIF)",
    "Community Enterprise Fund (CEF)",
    "PMFME Seed Capital",
    "Other Funds",
    "Other/Unmapped",
]

# fundsource_label (loan side) -> exact fundname string in clf_fund_disbursement.dta (LokOS's
# own heading text, kept verbatim there for the Fund Disbursement subtab) - only these three
# headings are lending capital; the corpus denominator for Active Lending Turnover sums them.
LENDING_FUND_HEADINGS = {
    "Community Investment Fund (CIF)": "Community Investment Fund (CIF)",
    "Community Enterprise Fund (CEF)": "Community Enterprise Fund (CEF)",
    "PMFME Seed Capital": "PMFME- Seed Capital",
}


def clean_vo_name(name):
    return re.sub(r"\s*\(\d+\)\s*$", "", str(name)).strip()


def to_iso(d):
    if d is None or (isinstance(d, float) and math.isnan(d)) or pd.isna(d):
        return None
    return pd.Timestamp(d).strftime("%Y-%m-%d")


def num_or_none(v):
    if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v):
        return None
    return float(v)


def add_months(d: date, n: int, anchor_day: int) -> date:
    month_index = d.month - 1 + n
    year = d.year + month_index // 12
    month = month_index % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(anchor_day, last_day)
    return date(year, month, day)


def reconstruct_schedule(loanamount, annual_rate_pct, tenuremonths, loantype,
                          disbursementdate, moratorium, window_start, window_end):
    """Theoretical monthly amortization schedule, restricted to [window_start, window_end].
    Fixed Principal: exact ACT/365 formula (verified against raw LokOS data).
    EMI: standard annuity formula, flat monthly rate (approximation)."""
    if pd.isna(loanamount) or pd.isna(tenuremonths) or tenuremonths <= 0 or pd.isna(disbursementdate):
        return []
    tenuremonths = int(tenuremonths)
    moratorium = int(moratorium) if not pd.isna(moratorium) else 0
    anchor_day = min(disbursementdate.day, 30)
    rate = annual_rate_pct if not pd.isna(annual_rate_pct) else 0

    rows = []
    balance = loanamount
    prev_date = disbursementdate

    if loantype == "EMI":
        monthly_rate = rate / 100 / 12
        if monthly_rate > 0:
            emi = loanamount * monthly_rate * (1 + monthly_rate) ** tenuremonths / ((1 + monthly_rate) ** tenuremonths - 1)
        else:
            emi = loanamount / tenuremonths

    for i in range(1, tenuremonths + 1):
        due_date = add_months(disbursementdate, moratorium + i, anchor_day)
        if due_date > window_end:
            break
        if loantype == "EMI":
            interest = balance * monthly_rate
            principal = emi - interest
        else:
            principal = loanamount / tenuremonths
            days = (due_date - prev_date).days
            interest = balance * (rate / 100) * (days / 365)
        installment = principal + interest
        balance_after = max(balance - principal, 0)
        if due_date >= window_start:
            rows.append({
                "due_date": due_date,
                "amount_due": round(installment, 2),
                "outstanding": round(balance_after, 2),
            })
        balance = balance_after
        prev_date = due_date
    return rows


def xnpv(rate, cashflows):
    """cashflows: list of (date, amount). Amount negative = outflow, positive = inflow."""
    d0 = min(d for d, _ in cashflows)
    return sum(amt / (1 + rate) ** ((d - d0).days / 365.0) for d, amt in cashflows)


def xirr(cashflows):
    """Annualized rate (%) that zeroes the XNPV of cashflows, or None if it can't be
    solved (fewer than 2 flows, no mix of inflow/outflow, or no sign change in the
    bracket - e.g. a portfolio too young to have any repayments yet)."""
    if len(cashflows) < 2:
        return None
    amounts = [a for _, a in cashflows]
    if not (any(a < 0 for a in amounts) and any(a > 0 for a in amounts)):
        return None
    try:
        rate = brentq(lambda r: xnpv(r, cashflows), -0.9999, 100, maxiter=200)
    except ValueError:
        return None
    return round(rate * 100, 2)


def build_xirr_cashflows(clf_active_loans, repay_txns_for_clf):
    """clf_active_loans: rows from clf_active (this CLF's active loans only - see module
    docstring for why closed loans are excluded). repay_txns_for_clf: transaction-level
    repayment rows already filtered to this CLF's active loans."""
    cashflows = []
    for _, r in clf_active_loans.iterrows():
        if pd.isna(r["disbursementdate"]) or pd.isna(r["loanamount"]):
            continue
        cashflows.append((r["disbursementdate"].date(), -float(r["loanamount"])))
    for _, r in repay_txns_for_clf.iterrows():
        if pd.isna(r["transactiondate"]) or pd.isna(r["amount"]):
            continue
        cashflows.append((r["transactiondate"].date(), float(r["amount"])))
    return cashflows


def build_loan_tab_data_main():
    df = pd.read_stata(LOAN_DTA)
    df["clfcode_int"] = df["clfcode"].astype(str).str.strip().astype("int64")
    df["vocode_str"] = df["vocode"].astype(str).str.strip()

    monthly = pd.read_stata(MONTHLY_REPAY_DTA)
    monthly["clfcode_int"] = monthly["clfcode"].astype(str).str.strip().astype("int64")
    monthly["vocode_str"] = monthly["vocode"].astype(str).str.strip()

    txns = pd.read_stata(TRANSACTIONS_DTA)
    txns["clfcode_int"] = txns["clfcode"].astype(str).str.strip().astype("int64")
    txns["vocode_str"] = txns["vocode"].astype(str).str.strip()

    fund_disb = pd.read_stata(FUND_DISBURSEMENT_DTA)
    fund_disb["clfcode_int"] = fund_disb["clfcode"].astype(str).str.strip().astype("int64")
    corpus_by_clf = {}
    fund_disb_by_clf = {}
    lending_headings = set(LENDING_FUND_HEADINGS.values())
    for clfcode_int, g in fund_disb.groupby("clfcode_int"):
        corpus_by_clf[clfcode_int] = float(g.loc[g["fundname"].isin(lending_headings), "total_received"].sum())
        fund_disb_by_clf[clfcode_int] = [
            {
                "heading": r["fundname"],
                "total_received": float(r["total_received"]),
                "latest_receipt_date": to_iso(r["latest_txn_date"]),
                "n_batches": int(r["n_batches"]),
            }
            for _, r in g.sort_values("total_received", ascending=False).iterrows()
        ]
    monthly_lookup = {
        (r.clfcode_int, r.vocode_str, int(r.loanno), int(r.repay_year), int(r.repay_month)): r.amount_repaid
        for r in monthly.itertuples()
    }
    window_start = date(int(monthly["repay_year"].min()), int(monthly.loc[monthly["repay_year"] == monthly["repay_year"].min(), "repay_month"].min()), 1)
    max_year = int(monthly["repay_year"].max())
    max_month = int(monthly.loc[monthly["repay_year"] == max_year, "repay_month"].max())
    window_end = date(max_year, max_month, calendar.monthrange(max_year, max_month)[1])

    active = df[df["is_closed_no_metadata"] == 0].copy()

    clf_files = list(CLF_DIR.glob("*.json"))
    clfcode_to_path = {}
    for p in clf_files:
        d = json.loads(p.read_text())
        clfcode_to_path[int(d["overview"]["clfcode"])] = p

    matched, unmatched_codes = 0, set()

    for clfcode_int, clf_loans in df.groupby("clfcode_int"):
        path = clfcode_to_path.get(clfcode_int)
        if path is None:
            unmatched_codes.add(clfcode_int)
            continue

        clf_json = json.loads(path.read_text())
        clf_active = clf_loans[clf_loans["is_closed_no_metadata"] == 0]

        # ---- KPIs ----
        total_disbursed = float(clf_active["loanamount"].sum())
        total_outstanding = float(clf_active["current_outstanding_principal"].sum())
        repayment_rate = (
            round(100 * (total_disbursed - total_outstanding) / total_disbursed, 1)
            if total_disbursed > 0 else None
        )
        n_active_loans = int(len(clf_active))

        corpus = corpus_by_clf.get(clfcode_int)
        active_lending_turnover = round(total_disbursed / corpus, 3) if corpus and corpus > 0 else None

        arrears = clf_active["arrear_amount_total"]
        n_loans_in_arrears = int((arrears > 0).sum())
        total_arrears = float(arrears[arrears > 0].sum())
        total_current_demand = float(clf_active["curr_demand_total"].sum())

        # ---- Pooled portfolio XIRR (active loans only - see module docstring) ----
        active_keys = pd.MultiIndex.from_arrays([clf_active["vocode_str"], clf_active["loanno"].astype(int)])
        clf_txns = txns[txns["clfcode_int"] == clfcode_int].copy()
        clf_txns_keys = pd.MultiIndex.from_arrays([clf_txns["vocode_str"], clf_txns["loanno"].astype(int)])
        clf_txns = clf_txns[clf_txns_keys.isin(active_keys)]
        # Realized XIRR (no terminal value) was tried and dropped: for an active, mid-tenure
        # loan book, most disbursed principal is legitimately still outstanding (not overdue,
        # just not due yet), and Realized XIRR gives that zero credit - it came out at -85%
        # median across all 53 Araria CLFs regardless of actual repayment behaviour, so it was
        # measuring "this book is young" rather than anything about CLF performance. Total XIRR
        # (crediting the outstanding balance back as if collected today) doesn't have this
        # problem and is kept as the only XIRR figure shipped.
        base_cashflows = build_xirr_cashflows(clf_active, clf_txns)
        full_cashflows = base_cashflows + [(date.today(), total_outstanding)] if total_outstanding > 0 else base_cashflows
        total_xirr = xirr(full_cashflows)

        all_clf_vo_codes = {str(v["vo_code"]).strip() for v in clf_json.get("vo_overview", []) if v.get("vo_code") is not None}
        # vo_overview vo_code is stored as a float (e.g. 50000043709.0); loan data's vocode
        # is a 12-digit zero-padded string (e.g. "050000043709") - normalise both to int for matching
        all_clf_vo_ints = set()
        for vc in all_clf_vo_codes:
            try:
                all_clf_vo_ints.add(int(float(vc)))
            except ValueError:
                pass

        vo_loan_counts = clf_loans.groupby("vocode_str").size()
        vo_loan_counts_by_int = {}
        for vocode_str, n in vo_loan_counts.items():
            try:
                vo_loan_counts_by_int[int(vocode_str)] = int(n)
            except ValueError:
                pass

        # Union with the loan data's own VO codes: some VOs that took loans no longer
        # appear in the CLF's current vo_overview roster (member/VO profile scrape and
        # loan scrape don't always agree on VO rosters - same cross-domain ID mismatch
        # documented elsewhere in this project). Without the union, VOs missing from
        # vo_overview would make "VOs Never Borrowed" go negative.
        all_clf_vo_ints |= set(vo_loan_counts_by_int.keys())

        n_vo_total = len(all_clf_vo_ints) if all_clf_vo_ints else None
        if n_vo_total:
            n_vo_never_borrowed = n_vo_total - len(vo_loan_counts_by_int)
            hist = {}
            for vo_int in all_clf_vo_ints:
                n = vo_loan_counts_by_int.get(vo_int, 0)
                bucket = n if n < 5 else 5
                hist[bucket] = hist.get(bucket, 0) + 1
            vo_loan_histogram = [
                {"n_loans": ("5+" if k == 5 else k), "n_vo": v, "pct": round(100 * v / n_vo_total, 1)}
                for k, v in sorted(hist.items())
            ]
        else:
            n_vo_never_borrowed = None
            vo_loan_histogram = []

        # ---- Mixes ----
        fund_source_mix = {}
        for fs in FUND_COLORS_ORDER:
            amt = float(clf_active.loc[clf_active["fundsource_label"] == fs, "loanamount"].sum())
            if amt > 0:
                fund_source_mix[fs] = amt

        loan_type_mix = {k: int(v) for k, v in clf_active["loantype"].value_counts().items()}
        loan_status_mix = {k: int(v) for k, v in clf_loans["loan_status"].value_counts().items()}

        # ---- Loan-level table ----
        loan_table = []
        for _, r in clf_loans.iterrows():
            loan_table.append({
                "vo_name": clean_vo_name(r["vo"]),
                "loan_no": int(r["loanno"]),
                "fund_source": r["fundsource_label"] if isinstance(r["fundsource_label"], str) and r["fundsource_label"] else None,
                "loan_type": r["loantype"] if isinstance(r["loantype"], str) and r["loantype"] else None,
                "loan_amount": num_or_none(r["loanamount"]),
                "current_outstanding": num_or_none(r["current_outstanding_principal"]),
                "cumulative_amount_repaid": num_or_none(r["cumulative_amount_repaid"]),
                "n_repayment_transactions": int(r["n_repayment_transactions"]) if not pd.isna(r["n_repayment_transactions"]) else 0,
                "last_repayment_date": to_iso(r["last_repayment_date"]),
                "status": r["loan_status"],
                "current_demand": num_or_none(r["curr_demand_total"]),
                "arrears": num_or_none(r["arrear_amount_total"]),
            })
        loan_table.sort(key=lambda r: (r["vo_name"], r["loan_no"]))

        # ---- Loan Schedule tab: VO/loan dropdown options + reconstructed monthly schedules ----
        vo_options, loan_options, schedules = [], {}, {}
        for vocode_str, g in clf_active.groupby("vocode_str"):
            vo_name = clean_vo_name(g["vo"].iloc[0])
            vo_options.append({"vo_code": vocode_str, "vo_name": vo_name})
            loan_options[vocode_str] = []
            schedules[vocode_str] = {}
            for _, r in g.iterrows():
                loanno = int(r["loanno"])
                loan_options[vocode_str].append({
                    "loan_no": loanno,
                    "label": f"Loan #{loanno} — {r['fundsource_label'] or 'Fund n/a'} — ₹{r['loanamount']:,.0f}",
                    "current_demand": num_or_none(r["curr_demand_total"]),
                    "arrears": num_or_none(r["arrear_amount_total"]),
                })
                if r["schedule_is_monotonic"] == 0:
                    schedules[vocode_str][str(loanno)] = {"reliable": False, "rows": []}
                    continue
                sched_rows = reconstruct_schedule(
                    r["loanamount"], r["interestrate"], r["tenuremonths"], r["loantype"],
                    r["disbursementdate"].date() if not pd.isna(r["disbursementdate"]) else None,
                    r["moratorium"], window_start, window_end,
                )
                month_rows = []
                for sr in sched_rows:
                    key = (clfcode_int, vocode_str, loanno, sr["due_date"].year, sr["due_date"].month)
                    repaid = monthly_lookup.get(key)
                    due = sr["amount_due"]
                    repaid_val = float(repaid) if repaid is not None else 0.0
                    if repaid_val <= 0:
                        status, arrear = "Not Paid", round(due, 2)
                    elif repaid_val >= due - 1:
                        status, arrear = "Paid", None
                    else:
                        status, arrear = "Partially Paid", round(due - repaid_val, 2)
                    month_rows.append({
                        "month": sr["due_date"].strftime("%b %Y"),
                        "amount_due": due,
                        "amount_repaid": round(repaid_val, 2) if repaid is not None else 0.0,
                        "status": status,
                        "arrear": arrear,
                        "outstanding": sr["outstanding"],
                    })
                schedules[vocode_str][str(loanno)] = {"reliable": True, "rows": month_rows}
            loan_options[vocode_str].sort(key=lambda x: x["loan_no"])
        vo_options.sort(key=lambda x: x["vo_name"])

        loans_block = {
            "found": True,
            "kpi": {
                "n_active_loans": n_active_loans,
                "total_disbursed": total_disbursed,
                "total_outstanding": total_outstanding,
                "repayment_rate": repayment_rate,
                "n_vo_never_borrowed": n_vo_never_borrowed,
                "active_lending_turnover": active_lending_turnover,
                "n_loans_in_arrears": n_loans_in_arrears,
                "total_arrears": total_arrears,
                "total_current_demand": total_current_demand,
                "total_xirr": total_xirr,
            },
            # Raw cash-flow series total_xirr was solved from, kept so build_district_state_data.py
            # can pool this CLF's flows with others' and re-solve xirr() at group scope for a real
            # (not averaged) District/State Total XIRR - averaging per-CLF XIRR percentages would
            # not be mathematically valid the way summing/re-solving the underlying flows is.
            "xirr_cashflows": [[to_iso(d), amt] for d, amt in full_cashflows],
            "fund_source_mix": fund_source_mix,
            "loan_type_mix": loan_type_mix,
            "loan_status_mix": loan_status_mix,
            "vo_loan_histogram": vo_loan_histogram,
            "loan_table": loan_table,
            "vo_options": vo_options,
            "loan_options": loan_options,
            "schedules": schedules,
            "schedule_window": {"start": window_start.strftime("%b %Y"), "end": window_end.strftime("%b %Y")},
        }

        clf_json["loans"] = loans_block

        path.write_text(json.dumps(clf_json, ensure_ascii=False))
        matched += 1

    print(f"Matched {matched} CLFs with loan data.")
    if unmatched_codes:
        print(f"{len(unmatched_codes)} clfcodes in the loan data had no matching CLF json: {sorted(unmatched_codes)[:10]}{'...' if len(unmatched_codes) > 10 else ''}")

    # Fund Disbursement is statewide (38/38 districts), well ahead of the Loans data (15
    # districts) - injected via its own independent pass over fund_disb_by_clf so every CLF
    # with FundDisbursement data gets the Financial Records > Fund Disbursement subtab
    # populated, regardless of whether that CLF also has loan data. Re-reads each CLF's json
    # (picking up the "loans" block just written above, if any) rather than sharing the main
    # loop, since the two source populations no longer coincide.
    fd_matched, fd_unmatched = 0, set()
    for clfcode_int, fund_rows in fund_disb_by_clf.items():
        path = clfcode_to_path.get(clfcode_int)
        if path is None:
            fd_unmatched.add(clfcode_int)
            continue
        clf_json = json.loads(path.read_text())
        clf_json["fund_disbursement"] = {
            "found": bool(fund_rows),
            "headings": fund_rows,
        }
        path.write_text(json.dumps(clf_json, ensure_ascii=False))
        fd_matched += 1

    print(f"Matched {fd_matched} CLFs with Fund Disbursement data.")
    if fd_unmatched:
        print(f"{len(fd_unmatched)} clfcodes in the Fund Disbursement data had no matching CLF json: {sorted(fd_unmatched)[:10]}{'...' if len(fd_unmatched) > 10 else ''}")




def stage_4_build_loan_scoring():
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


def stage_5_build_district_state():
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
    from datetime import datetime


    BASE = "/Users/mohanrajagopal/Dropbox/Bihar Gates Team/3_Jeevika/6_LokOS_Analysis"
    DATA_DIR = f"{BASE}/3_Output/CLF Tracker/Scale-Up/data"
    os.makedirs(f"{DATA_DIR}/districts", exist_ok=True)

    LENDING_HEADINGS_SET = set(LENDING_FUND_HEADINGS.values())

    def _parse_iso(s):
        return datetime.strptime(s, "%Y-%m-%d").date()

    def _lending_corpus(c):
        """Sum of CIF/CEF/PMFME total_received for one CLF's fund_disbursement block - same 3
        headings build_loan_tab_data.py sums for its own Active Lending Turnover denominator."""
        headings = c.get("fund_disbursement", {}).get("headings", [])
        return sum(h["total_received"] for h in headings if h["heading"] in LENDING_HEADINGS_SET)

    T0 = time.time()
    def elapsed(): return f"{time.time()-T0:.1f}s"

    CATS = ["Financial Health", "Fund Utilization", "Loan Portfolio", "VRF Fund Health", "Governance & Compliance", "Welfare and Livelihood", "Data Coverage"]

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

        # ---- Fund Disbursement (statewide-complete, 38/38 districts - independent of
        # Loans' partial coverage, same reasoning build_loan_tab_data.py's own
        # independent injection pass uses). Per-heading sum of total_received/n_batches,
        # max of latest_receipt_date, across every CLF in the group that has it. ----
        fd_found = [c for c in CLFS if c.get("fund_disbursement", {}).get("found")]
        heading_agg = {}
        for c in fd_found:
            for h in c["fund_disbursement"]["headings"]:
                key = h["heading"]
                if key not in heading_agg:
                    heading_agg[key] = {"heading": key, "total_received": 0.0, "n_batches": 0, "latest_receipt_date": None}
                heading_agg[key]["total_received"] += h["total_received"]
                heading_agg[key]["n_batches"] += h["n_batches"]
                if h["latest_receipt_date"] and (heading_agg[key]["latest_receipt_date"] is None or h["latest_receipt_date"] > heading_agg[key]["latest_receipt_date"]):
                    heading_agg[key]["latest_receipt_date"] = h["latest_receipt_date"]
        fund_disbursement_agg = {
            "found": len(fd_found) > 0,
            "headings": sorted(heading_agg.values(), key=lambda h: -h["total_received"]),
        }

        # ---- Loans: Portfolio Overview only - the full "All Loans" table and the
        # per-loan Loan Schedule subtab don't generalize to group scope (same
        # reasoning as VRF's VO-Level Breakdown being CLF-only), so neither is
        # aggregated here. KPI rates are recomputed from summed totals, not
        # averaged CLF-by-CLF (same convention as Financial above). Total XIRR is
        # re-solved from every matched CLF's own pooled cash-flow series
        # (xirr_cashflows, added to the CLF json for exactly this purpose) rather
        # than averaged - averaging per-CLF XIRR percentages isn't mathematically
        # valid the way pooling the underlying flows and re-solving xirr() is. ----
        loan_found = [c for c in CLFS if c.get("loans", {}).get("found")]
        total_disbursed_l = sum(c["loans"]["kpi"]["total_disbursed"] for c in loan_found)
        total_outstanding_l = sum(c["loans"]["kpi"]["total_outstanding"] for c in loan_found)

        fund_source_mix_l, loan_type_mix_l, loan_status_mix_l = {}, {}, {}
        for c in loan_found:
            for k, v in c["loans"]["fund_source_mix"].items():
                fund_source_mix_l[k] = fund_source_mix_l.get(k, 0) + v
            for k, v in c["loans"]["loan_type_mix"].items():
                loan_type_mix_l[k] = loan_type_mix_l.get(k, 0) + v
            for k, v in c["loans"]["loan_status_mix"].items():
                loan_status_mix_l[k] = loan_status_mix_l.get(k, 0) + v

        hist_by_bucket = {}
        for c in loan_found:
            for row in c["loans"]["vo_loan_histogram"]:
                hist_by_bucket[row["n_loans"]] = hist_by_bucket.get(row["n_loans"], 0) + row["n_vo"]
        hist_total_vo = sum(hist_by_bucket.values())
        vo_loan_histogram_l = [
            {"n_loans": k, "n_vo": v, "pct": round(100 * v / hist_total_vo, 1) if hist_total_vo else None}
            for k, v in sorted(hist_by_bucket.items(), key=lambda kv: (kv[0] == "5+", kv[0]))
        ]

        total_current_demand_l = sum(c["loans"]["kpi"].get("total_current_demand") or 0 for c in loan_found)
        corpus_l = sum(_lending_corpus(c) for c in loan_found)
        pooled_cashflows = []
        for c in loan_found:
            for d_iso, amt in c["loans"].get("xirr_cashflows", []):
                pooled_cashflows.append((_parse_iso(d_iso), amt))
        total_xirr_l = xirr(pooled_cashflows) if pooled_cashflows else None

        loans_agg = {
            "found": len(loan_found) > 0, "n_clfs_with_loans": len(loan_found), "n_total": n_clfs,
            "kpi": {
                "n_active_loans": sum(c["loans"]["kpi"]["n_active_loans"] for c in loan_found),
                "total_disbursed": total_disbursed_l,
                "total_outstanding": total_outstanding_l,
                "repayment_rate": round(100 * (total_disbursed_l - total_outstanding_l) / total_disbursed_l, 1) if total_disbursed_l else None,
                "n_vo_never_borrowed": sum(c["loans"]["kpi"]["n_vo_never_borrowed"] for c in loan_found),
                "active_lending_turnover": round(total_disbursed_l / corpus_l, 3) if corpus_l else None,
                "n_loans_in_arrears": sum(c["loans"]["kpi"]["n_loans_in_arrears"] for c in loan_found),
                "total_arrears": sum(c["loans"]["kpi"]["total_arrears"] for c in loan_found),
                "total_current_demand": total_current_demand_l,
                "total_xirr": total_xirr_l,
            },
            "fund_source_mix": fund_source_mix_l,
            "loan_type_mix": loan_type_mix_l,
            "loan_status_mix": loan_status_mix_l,
            "vo_loan_histogram": vo_loan_histogram_l,
        }

        # ---- Data Availability: per-source coverage counts across every CLF in the
        # group - informational only, same reasoning as the CLF-level block this
        # rolls up (build_loan_scoring_data.py). Preserves each CLF's own source
        # order/labels (identical across all CLFs) rather than re-deriving it. ----
        da_source_order = CLFS[0]["data_availability"]["sources"] if CLFS and "data_availability" in CLFS[0] else []
        da_flags_by_clf = [
            {s["key"]: s["available"] for s in c.get("data_availability", {}).get("sources", [])}
            for c in CLFS
        ]
        data_availability_agg = {
            "sources": [
                {"key": s["key"], "label": s["label"],
                 "n_available": sum(1 for flags in da_flags_by_clf if flags.get(s["key"])),
                 "n_total": n_clfs}
                for s in da_source_order
            ],
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
            ("Fund Utilization", "Fund Deployment"): (f01_agg["deployment_ratio"], "pct", "fund deployment ratio"),
            ("Fund Utilization", "Interest Income Share"): (latest_q_fin["interest_income_share"] if latest_q_fin else None, "pct", "of receipts"),
            ("Fund Utilization", "This Quarter's Disbursement Rate"): (latest_q_fin["this_qtr_disb_rate"] if latest_q_fin else None, "pct", "of loan demand disbursed"),
            ("Fund Utilization", "Loan Amount Demanded This Quarter"): (demand_per_member, "rs_per_member", ""),
            ("Loan Portfolio", "Repayment Rate"): (loans_agg["kpi"]["repayment_rate"], "pct", "repaid of disbursed"),
            ("Loan Portfolio", "Active Lending Turnover"): (loans_agg["kpi"]["active_lending_turnover"], "multiplier", "of corpus recycled"),
            ("Loan Portfolio", "Arrears Rate"): (
                round(100 * loans_agg["kpi"]["total_arrears"] / total_current_demand_l, 1) if total_current_demand_l else None,
                "pct", "of current demand overdue"),
            ("Loan Portfolio", "Total XIRR"): (loans_agg["kpi"]["total_xirr"], "pct_signed", "annualized"),
            ("Data Coverage", "Data Sources Available"): (
                (sum(c["data_availability"]["n_available"] for c in CLFS) / n_clfs) if n_clfs else None,
                "of_11", ""),
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

        pseudo_clf = {
            "financial": financial_agg, "vrf": vrf_agg, "vprp": {"years": vprp_years_agg},
            "loans": loans_agg, "fund_disbursement": fund_disbursement_agg,
        }

        return {
            "name": name, "overview": overview_agg, "audit": audit_agg,
            "financial": financial_agg, "vrf": vrf_agg, "vprp": {"years": vprp_years_agg},
            "loans": loans_agg, "fund_disbursement": fund_disbursement_agg,
            "data_availability": data_availability_agg,
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
        "financial": "The district's balance sheet, quarterly cash flow, and credit disbursement, summed across every CLF for a selected quarter, with every ratio recomputed from those summed totals (not averaged CLF-by-CLF). <b>Summary</b> gives key ratios (liquidity, fund deployment, surplus/deficit, books balance check) and a breakdown of where capital and cash came from and went to. <b>Statements</b> shows the full balance sheet and receipts &amp; payments statement, summed line-by-line across every CLF. <b>Fund Disbursement</b> shows lifetime capital received from the state under every fund heading, summed across every CLF in the district.",
        "loans": "Tracks loans disbursed to member VOs, summed across every CLF in the district with loan data. <b>Portfolio Overview</b> gives district-wide totals, repayment/return metrics recomputed from those totals (not averaged CLF-by-CLF), and portfolio composition. The per-loan Loan Schedule and full loan listing don't generalize to district scope and stay CLF-only.",
        "vrf": "Tracks the district's Vulnerability Reduction Fund, rolled up from every VO across every CLF in the district. <b>KPI Snapshot</b> gives district-wide totals and fund health. <b>Forecasts</b> projects where the fund is headed by 31 March 2027 under three lending-activity scenarios, summed across every CLF's own forecast.",
        "vprp": "Requests and plans raised through VPRP, by year (2023-2025), summed across every CLF in the district. <b>Entitlements</b> tracks government scheme demands (ration cards, pensions, insurance, etc.) and NREGA job card access. <b>PGSRD</b> (Public Goods, Services, and Resource Development) tracks requests for public infrastructure, resources, and services. <b>SDP</b> (Social Development Plan) tracks broader social issues raised and the government departments involved.",
        "scoring": "Combines every other tab into one performance score for the district, against Bihar statewide. <b>Overall</b> gives the district's average Overall Score (equal weight per CLF) plus a category breakdown. <b>By Category</b> shows every individual metric behind all 7 categories, averaged across the district's own CLFs. <b>CLF Rankings</b> lists every CLF in the district side by side, ranked by Overall Score.",
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
        "loans": "We could not locate Loan data for any CLF in {name} district. The Loans tab currently covers 15 districts, as the statewide CLF-meeting loan scrape is still in progress.",
        "fund_disbursement": "We could not locate Fund Disbursement data for any CLF in {name} district.",
        "not_found": "Not Found",
    }
    STATE_CONTEXT = {
        "overview": "A snapshot of every CLF in Bihar, aggregated statewide. <b>Profile</b> covers CLF status distribution and statewide governance structure (Executive Committee totals, subcommittee membership, summed across every CLF). <b>Members</b> covers social inclusion and welfare coverage, education levels, livelihood diversification, special project activities, and the statewide cadre roster.",
        "audit": "The most recent audit results statewide (FY 2025-26, Q4), averaged across every CLF with an audit on file: average grade and score, a breakdown across the 6 scoring categories, the individual line items behind each category, how many CLFs were flagged for a financial irregularity (and what kind), and a cash book vs. physical cash reconciliation check.",
        "financial": "The balance sheet, quarterly cash flow, and credit disbursement of every CLF in Bihar, summed for a selected quarter, with every ratio recomputed from those summed totals (not averaged CLF-by-CLF). <b>Summary</b> gives key ratios and a breakdown of where capital and cash came from and went to. <b>Statements</b> shows the full balance sheet and receipts &amp; payments statement, summed line-by-line across every CLF statewide. <b>Fund Disbursement</b> shows lifetime capital received from the state under every fund heading, summed across every CLF in Bihar.",
        "loans": "Tracks loans disbursed to member VOs, summed across every CLF statewide with loan data. <b>Portfolio Overview</b> gives statewide totals, repayment/return metrics recomputed from those totals (not averaged CLF-by-CLF), and portfolio composition. The per-loan Loan Schedule and full loan listing don't generalize to statewide scope and stay CLF-only.",
        "vrf": "Tracks Bihar's Vulnerability Reduction Fund, rolled up from every VO across every CLF in the state. <b>KPI Snapshot</b> gives statewide totals and fund health. <b>Forecasts</b> projects where the fund is headed by 31 March 2027 under three lending-activity scenarios, summed across every CLF's own forecast.",
        "vprp": "Requests and plans raised through VPRP, by year (2023-2025), summed across every CLF in Bihar. <b>Entitlements</b> tracks government scheme demands and NREGA job card access. <b>PGSRD</b> tracks requests for public infrastructure, resources, and services. <b>SDP</b> tracks broader social issues raised and the government departments involved.",
        "scoring": "Combines every other tab into one statewide performance score. <b>Overall</b> gives the average Overall Score across every CLF in Bihar (equal weight per CLF) plus a category breakdown. <b>By Category</b> shows every individual metric behind all 7 categories, averaged statewide. <b>CLF Rankings</b> shows the top 20 and bottom 20 CLFs in Bihar by Overall Score.",
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
        "loans": "We could not locate Loan data statewide. The Loans tab currently covers 15 districts, as the statewide CLF-meeting loan scrape is still in progress.",
        "fund_disbursement": "We could not locate Fund Disbursement data statewide.",
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


def stage_6_make_shell():
    """
    Author:   Claude (for Mohan)
    Created:  01/08/2026
    Purpose:  Builds the statewide tracker's thin HTML/JS shell (index.html),
              pairing with build_tracker_data.py's JSON-per-unit output. This is
              the same architecture the VRF tracker's own Scale-Up uses (see
              5_VRF_Analysis/3_Output/CLF Tracker/Scale-Up/index.html): rather
              than baking one CLF's data into the page (what the single-CLF
              prototype, 3_Output/CLF Tracker/build_comprehensive_clf_tracker.py,
              does), this page ships with NO CLF data at all - it fetches
              data/manifest.json + data/shared.json on load, lets the user find
              a CLF via cascading District->Block->CLF dropdowns or a direct
              MIS-ID search box, then fetches that one CLF's data/clfs/{id}.json
              at runtime and renders it. Every render function's CSS/JS is
              copied verbatim from the single-CLF prototype (which stays as the
              frozen single-CLF reference) - only the top of the JS (how DATA/
              TIPS/ERR_MSG_JS/FOOTNOTES get populated) and the outer page chrome
              (search view + fetch orchestration) are new.

              Because this page relies on fetch() to load its data files, it
              must be served over http:// (e.g. `python3 -m http.server` from
              this folder), not opened directly via file:// - browsers block
              fetch() against file:// origins.

    Input Data:  data/manifest.json, data/shared.json, data/clfs/*.json
                 (all written by build_tracker_data.py, must run first)

    Output Data: index.html (this folder)
    """

    import json

    OUT_PATH = "index.html"

    # ============================================================================
    # CSS - identical to the single-CLF prototype's CSS block, plus a new block
    # at the end for the search/finder landing view (not present in the
    # prototype, since it never needed to find a CLF - there was only ever one).
    # ============================================================================
    CSS = r"""
    :root{
      --ground:#FFFFFF; --panel:#FFFFFF; --panel-alt:#FAF9F4; --ink:#33413A; --ink-soft:#7C8B81;
      --primary:#2F9C74; --primary-soft:#E7F5EF; --gold:#CE9C3C; --gold-soft:#FBF1DA;
      --line:#EAE4D5; --line-strong:#DCD2BB; --low:#C55F49; --low-soft:#FBE8E1;
      --grey:#B7BEB7; --grey-soft:#EFF1EE;
    }
    *{ box-sizing:border-box; }
    html{ -webkit-text-size-adjust:100%; text-size-adjust:100%; }
    body{
      margin:0; background:var(--ground); color:var(--ink);
      font-family:"Noto Sans Devanagari","Nirmala UI",-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
      font-size:15px; line-height:1.55; padding:20px 16px 60px;
    }
    .page{ max-width:1040px; margin:0 auto; }
    .serif{ font-family:"Iowan Old Style","Palatino Linotype",Georgia,"Noto Sans Devanagari",serif; }
    .top-row{ display:flex; align-items:center; justify-content:space-between; gap:10px; flex-wrap:wrap; margin-bottom:18px; }
    .badge{ display:inline-flex; align-items:center; gap:6px; font-size:11px; letter-spacing:.08em; text-transform:uppercase; font-weight:700; color:var(--primary); background:var(--primary-soft); border:1px solid var(--line-strong); border-radius:999px; padding:5px 12px; }
    .badge::before{ content:"\25CF"; font-size:8px; }
    header.masthead{ border-bottom:1px solid var(--line); padding-bottom:18px; margin-bottom:20px; }
    h1{ font-weight:600; font-size:clamp(28px,5vw,38px); margin:0 0 6px 0; text-wrap:balance; letter-spacing:-0.01em; }
    .crumbs{ color:var(--ink-soft); font-size:14px; }
    .crumbs b{ color:var(--ink); font-weight:600; }
    .tabbar{ display:flex; gap:4px; margin-bottom:8px; border-bottom:1px solid var(--line); flex-wrap:wrap; }
    .tabbtn{ appearance:none; border:none; background:none; cursor:pointer; font-family:inherit; font-size:14.5px; font-weight:600; color:var(--ink-soft); padding:12px 6px; margin-right:20px; border-bottom:2px solid transparent; min-height:44px; }
    /* long tab labels wrapping to 2-3 lines looks cluttered on narrow phones -
       scroll horizontally instead, same pattern already used for wide tables. */
    @media (max-width:640px){
      .tabbar{ flex-wrap:nowrap; overflow-x:auto; -webkit-overflow-scrolling:touch; }
      .subtabbar{ flex-wrap:nowrap; overflow-x:auto; -webkit-overflow-scrolling:touch; padding-bottom:4px; }
      .tabbtn, .subtabbtn{ white-space:nowrap; flex:none; }
    }
    .tabbtn:hover{ color:var(--ink); }
    .tabbtn.active{ color:var(--primary); border-bottom-color:var(--primary); }
    .subtabbar{ display:flex; gap:6px; margin:14px 0 22px; flex-wrap:wrap; }
    .subtabbtn{ appearance:none; cursor:pointer; font-family:inherit; font-size:13px; font-weight:600; color:var(--ink-soft); background:var(--panel-alt); border:1px solid var(--line-strong); border-radius:999px; padding:8px 15px; min-height:38px; }
    .subtabbtn.active{ background:var(--primary); border-color:var(--primary); color:#fff; }
    .tabpanel{ display:none; } .tabpanel.active{ display:block; }
    .subpanel{ display:none; } .subpanel.active{ display:block; }
    section{ margin-bottom:26px; }
    .section-head{ display:flex; align-items:baseline; justify-content:space-between; gap:12px; margin-bottom:14px; flex-wrap:wrap; }
    .section-head h2{ font-size:19px; font-weight:600; margin:0; }
    .section-head .hint{ font-size:12.5px; color:var(--ink-soft); }
    .panel{ background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:18px 20px; }
    .tiles{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; }
    .tiles.n1{ grid-template-columns:1fr; }
    .tiles.n2{ grid-template-columns:repeat(2,1fr); } .tiles.n3{ grid-template-columns:repeat(3,1fr); }
    .tiles.n4{ grid-template-columns:repeat(4,1fr); } .tiles.n5{ grid-template-columns:repeat(5,1fr); } .tiles.n6{ grid-template-columns:repeat(6,1fr); }
    @media (max-width:900px){ .tiles.n5,.tiles.n6{ grid-template-columns:repeat(3,1fr); } }
    @media (max-width:700px){ .tiles,.tiles.n2,.tiles.n3,.tiles.n4,.tiles.n5,.tiles.n6{ grid-template-columns:repeat(2,1fr); } }
    @media (max-width:420px){ .tiles,.tiles.n2,.tiles.n3,.tiles.n4,.tiles.n5,.tiles.n6{ grid-template-columns:1fr; } }
    .tile{ border:1px solid var(--line); border-radius:8px; padding:14px 16px; }
    .tile .label{ font-size:11.5px; letter-spacing:.06em; text-transform:uppercase; color:var(--ink-soft); margin-bottom:8px; }
    .tile .label.tip{ cursor:help; text-decoration:underline dotted var(--line-strong); text-underline-offset:3px; }
    .tile .value{ font-size:22px; font-weight:600; font-variant-numeric:tabular-nums; color:var(--primary); }
    .tile.big-value .value{ font-size:38px; line-height:1; }
    .tile.neutral .value{ color:var(--ink); } .tile.info .value{ color:#5B8AA6; }
    .tile.neg .value{ color:var(--low); } .tile.warn .value{ color:var(--gold); }
    .tile .sub{ font-size:12px; color:var(--ink-soft); margin-top:4px; }
    .health-grid{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }
    @media (max-width:680px){ .health-grid{ grid-template-columns:1fr; } }
    .health-card{ border:1px solid var(--line); border-radius:10px; padding:20px; display:flex; flex-direction:column; align-items:center; text-align:center; gap:10px; }
    .health-card h3{ margin:0; font-size:15.5px; font-weight:700; }
    .ring-wrap{ position:relative; width:190px; height:190px; max-width:80vw; max-height:80vw; }
    .ring-wrap .ring-center{ position:absolute; inset:0; display:flex; flex-direction:column; align-items:center; justify-content:center; }
    .ring-center .num{ font-size:34px; font-weight:700; color:var(--primary); font-variant-numeric:tabular-nums; }
    .ring-center .lbl{ font-size:11.5px; color:var(--ink-soft); margin-top:4px; max-width:120px; line-height:1.3; }
    .donut-legend{ display:flex; flex-direction:column; gap:8px; width:100%; max-width:300px; margin-top:4px; }
    .legend-row{ display:flex; align-items:center; gap:8px; font-size:12.5px; }
    .legend-swatch{ width:10px; height:10px; border-radius:3px; flex:none; }
    .legend-row .amt{ margin-left:auto; font-variant-numeric:tabular-nums; color:var(--ink-soft); }
    .desc{ font-size:12.5px; color:var(--ink-soft); line-height:1.5; max-width:320px; }
    .attn-grid{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }
    @media (max-width:680px){ .attn-grid{ grid-template-columns:1fr; } }
    .attn-card{ border:1px solid var(--line); border-radius:10px; padding:18px 20px; }
    .attn-card h3{ margin:0 0 4px; font-size:15.5px; font-weight:700; }
    .attn-headline{ font-size:20px; font-weight:700; color:var(--gold); font-variant-numeric:tabular-nums; margin:4px 0 10px; }
    .table-wrap{ overflow-x:auto; -webkit-overflow-scrolling:touch; }
    table{ width:100%; border-collapse:collapse; }
    thead th{ text-align:left; font-size:11px; letter-spacing:.06em; text-transform:uppercase; color:var(--ink-soft); font-weight:600; padding:0 10px 10px; border-bottom:1px solid var(--line-strong); white-space:nowrap; }
    thead th.num, tbody td.num{ text-align:right; }
    tbody tr{ border-bottom:1px solid var(--line); } tbody tr:last-child{ border-bottom:none; }
    tbody tr:hover{ background:var(--panel-alt); }
    tbody td{ padding:10px 10px; vertical-align:middle; font-variant-numeric:tabular-nums; }
    th.sortable{ cursor:pointer; user-select:none; }
    th.sortable:hover{ color:var(--ink); }
    .sort-arrow{ font-size:9px; margin-left:4px; color:var(--line-strong); display:inline-block; }
    .sort-arrow.active{ color:var(--primary); }
    .rank-cell{ font-family:"Iowan Old Style","Palatino Linotype",Georgia,serif; font-size:17px; font-weight:600; color:var(--ink-soft); width:34px; }
    tr.top .rank-cell{ color:var(--primary); }
    .bk-name{ font-weight:600; white-space:nowrap; } .bk-id{ font-size:11.5px; color:var(--ink-soft); margin-top:2px; }
    .score-cell{ display:flex; align-items:center; gap:10px; justify-content:flex-end; }
    .score-bar{ width:60px; height:6px; border-radius:999px; background:var(--line); position:relative; }
    .score-bar .fill{ position:absolute; left:0; top:0; bottom:0; border-radius:999px; background:var(--primary); }
    .score-num{ width:40px; text-align:right; font-weight:600; }
    tr.low .score-bar .fill{ background:var(--low); } tr.low .score-num{ color:var(--low); }
    .status-tag{ font-size:11.5px; font-weight:700; padding:3px 9px; border-radius:999px; white-space:nowrap; display:inline-block; }
    .status-tag.good{ color:var(--primary); background:var(--primary-soft); }
    .status-tag.flag{ color:var(--low); background:var(--low-soft); }
    .status-tag.na{ color:var(--ink-soft); background:var(--grey-soft); }
    .standing-grid{ display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-bottom:22px; }
    @media (max-width:560px){ .standing-grid{ grid-template-columns:1fr; } }
    .standing-card{ border:1px solid var(--line); border-radius:8px; padding:16px 18px; }
    .standing-card .label{ font-size:11.5px; letter-spacing:.07em; text-transform:uppercase; color:var(--ink-soft); margin-bottom:8px; }
    .standing-card .big{ font-size:38px; font-weight:600; line-height:1; font-variant-numeric:tabular-nums; color:var(--primary); }
    .standing-card .big sup{ font-size:15px; vertical-align:super; }
    .standing-card .big .outof{ font-size:14px; font-weight:400; color:var(--ink-soft); margin-left:9px; vertical-align:middle; }
    .standing-card .sub{ font-size:13px; color:var(--ink-soft); margin-top:4px; }
    .track{ position:relative; height:8px; border-radius:999px; background:linear-gradient(to right,var(--primary-soft),var(--line)); margin-top:14px; }
    .track .fill{ position:absolute; left:0; top:0; bottom:0; border-radius:999px; background:var(--primary); opacity:.35; }
    .track .marker{ position:absolute; top:50%; width:14px; height:14px; background:var(--primary); border-radius:50%; border:2px solid var(--panel); transform:translate(-50%,-50%); box-shadow:0 0 0 1px var(--line-strong); }
    .standing-card.state .big{ color:var(--gold); }
    .standing-card.state .track{ background:linear-gradient(to right,var(--gold-soft),var(--line)); }
    .standing-card.state .track .fill{ background:var(--gold); } .standing-card.state .track .marker{ background:var(--gold); }
    .standing-card .rank-line{ font-size:12px; color:var(--ink-soft); margin-top:10px; }

    /* customizable category-weight picker (modal) */
    .weight-btn{ appearance:none; cursor:pointer; font-family:inherit; font-size:13px; font-weight:600; color:var(--primary); background:var(--primary-soft); border:1px solid var(--line-strong); border-radius:999px; padding:8px 16px; min-height:38px; }
    .weight-btn:hover{ opacity:.9; }
    .weight-modal-backdrop{ display:none; position:fixed; inset:0; background:rgba(30,35,32,0.45); z-index:100; align-items:center; justify-content:center; padding:20px; }
    .weight-modal-backdrop.open{ display:flex; }
    .weight-modal{ background:var(--panel); border-radius:14px; max-width:520px; width:100%; max-height:88vh; overflow-y:auto; padding:26px 28px; box-shadow:0 20px 60px rgba(0,0,0,0.25); }
    .weight-modal-head{ display:flex; align-items:baseline; justify-content:space-between; margin-bottom:4px; }
    .weight-modal-head h3{ font-size:19px; font-weight:600; margin:0; }
    .weight-modal-close{ appearance:none; border:none; background:none; cursor:pointer; font-size:22px; line-height:1; color:var(--ink-soft); padding:2px 6px; }
    .weight-modal-close:hover{ color:var(--ink); }
    .weight-modal-hint{ font-size:12.5px; color:var(--ink-soft); margin:2px 0 20px; }
    .weight-row{ margin-bottom:18px; }
    .weight-row-head{ display:flex; justify-content:space-between; align-items:baseline; margin-bottom:8px; }
    .weight-row-head .wname{ font-size:13.5px; font-weight:600; }
    .weight-row-head .wpct{ font-size:14px; font-weight:700; color:var(--primary); font-variant-numeric:tabular-nums; }
    .weight-slider{ width:100%; accent-color:var(--primary); height:22px; }
    .weight-result{ margin-top:24px; padding:16px 18px; background:var(--panel-alt); border-radius:10px; border:1px solid var(--line); }
    .weight-result-row{ display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; }
    .weight-result-row:last-child{ margin-bottom:0; }
    .weight-result .wlabel{ font-size:12.5px; color:var(--ink-soft); }
    .weight-result .wval{ font-size:20px; font-weight:700; color:var(--primary); font-variant-numeric:tabular-nums; }
    .weight-result .wval.gold{ color:var(--gold); }
    .weight-modal-actions{ display:flex; justify-content:space-between; gap:10px; margin-top:22px; }
    .weight-reset-btn{ appearance:none; cursor:pointer; font-family:inherit; font-size:13px; font-weight:600; color:var(--ink-soft); background:var(--panel-alt); border:1px solid var(--line-strong); border-radius:999px; padding:8px 16px; }
    .weight-done-btn{ appearance:none; cursor:pointer; font-family:inherit; font-size:13px; font-weight:700; color:#fff; background:var(--primary); border:none; border-radius:999px; padding:8px 20px; }
    .donut-row{ display:flex; gap:18px; flex-wrap:wrap; justify-content:center; }
    /* minmax(0,1fr), not plain 1fr - a plain 1fr column can't shrink below its
       content's natural width, so a wide table forces the whole grid (and page)
       to overflow instead of letting the table's own .table-wrap scroll inside
       its column; minmax(0,1fr) removes that floor. Stacks to one column on
       narrow phones so each table gets the full width for its own scroll. */
    .statement-grid{ display:grid; grid-template-columns:minmax(0,1fr) minmax(0,1fr); gap:24px; }
    @media (max-width:680px){ .statement-grid{ grid-template-columns:minmax(0,1fr); } }
    .donut-card{ width:150px; text-align:center; }
    .donut-card .dlabel{ font-size:12.5px; color:var(--ink-soft); margin-top:8px; min-height:2.4em; }
    .bar-list{ display:flex; flex-direction:column; gap:11px; }
    .bar-row{ display:grid; grid-template-columns:200px 1fr 50px; gap:12px; align-items:center; }
    @media (max-width:560px){ .bar-row{ grid-template-columns:1fr; gap:4px; } }
    .metric-pctl-row{ display:grid; grid-template-columns:190px 1fr 1fr; gap:16px; align-items:center; margin-bottom:12px; }
    @media (max-width:560px){ .metric-pctl-row{ grid-template-columns:1fr; gap:6px; margin-bottom:16px; } }
    .bar-row .blabel{ font-size:13px; color:var(--ink); }
    .bar-track{ position:relative; height:9px; border-radius:999px; background:var(--grey-soft); }
    .bar-track .fill{ position:absolute; left:0; top:0; bottom:0; border-radius:999px; }
    .bar-row .bval{ font-size:13px; font-weight:700; text-align:right; font-variant-numeric:tabular-nums; }
    .flag-headline{ font-size:16px; font-weight:600; margin:0 0 16px; padding:13px 16px; border-radius:8px; }
    .flag-headline.yes{ background:var(--low-soft); color:#8a3226; } .flag-headline.no{ background:var(--primary-soft); color:#1f6b4d; }
    .pills{ display:flex; flex-wrap:wrap; gap:7px; }
    .pill{ font-size:12.5px; padding:5px 12px; border-radius:999px; background:var(--panel-alt); border:1px solid var(--line-strong); color:var(--ink-soft); }
    .pill.on{ background:var(--primary); border-color:var(--primary); color:#fff; font-weight:600; }
    .pill-sm{ font-size:11.5px; padding:3px 10px; border-radius:999px; border:1px solid var(--line-strong); background:var(--panel-alt); color:var(--ink-soft); white-space:nowrap; }
    .pill-sm.status-active{ background:var(--primary-soft); border-color:var(--primary); color:#1f6b4d; font-weight:600; }
    .pill-sm.status-closed{ background:var(--grey-soft); border-color:var(--grey); color:var(--ink-soft); font-weight:600; }
    .pill-sm.status-none{ background:var(--gold-soft); border-color:var(--gold); color:#8a6a1f; font-weight:600; }
    .pill-sm.status-negative{ background:var(--low-soft); border-color:var(--low); color:#8a3226; font-weight:600; }
    .selectbar{ display:flex; align-items:center; gap:10px; margin-bottom:18px; flex-wrap:wrap; }
    .selectbar label{ font-size:12.5px; font-weight:600; color:var(--ink-soft); }
    .selectbar select{ min-height:40px; font-family:inherit; font-size:14px; color:var(--ink); background:var(--panel); border:1px solid var(--line-strong); border-radius:8px; padding:8px 12px; }
    .note-inline{ font-size:12.5px; color:var(--ink-soft); background:var(--primary-soft); border-radius:8px; padding:9px 12px; margin-bottom:14px; }
    .context-box{ font-size:13px; line-height:1.55; color:var(--ink-soft); background:var(--panel-alt); border:1px solid var(--line); border-radius:10px; padding:14px 18px; margin-bottom:22px; }
    .context-box b{ color:var(--ink); }
    .context-source{ font-size:11.5px; color:var(--ink-soft); opacity:.8; margin-top:8px; }
    .foot-note{ margin-top:24px; font-size:12.5px; color:var(--ink-soft); border-top:1px solid var(--line); padding-top:14px; }
    .disclaimer{ font-size:13px; color:var(--gold); background:var(--gold-soft); border-radius:8px; padding:10px 14px; margin-bottom:14px; }

    /* Hover-tooltip system (data-tip attribute + event delegation) */
    .tooltip-box{ position:fixed; z-index:999; max-width:260px; background:var(--ink); color:#fff; font-size:12.5px;
      line-height:1.45; padding:8px 11px; border-radius:6px; pointer-events:none; opacity:0; transition:opacity .12s ease;
      transform:translate(-50%,calc(-100% - 10px)); }
    .tooltip-box.visible{ opacity:1; }
    [data-tip]{ cursor:help; }
    .tip, .label.tip, h2.tip, h3.tip, .th-tip{ text-decoration:underline dotted var(--line-strong); text-underline-offset:3px; }

    /* Category-grouping bracket for Audit's Individual Item Scores */
    .cat-group{ display:flex; align-items:stretch; gap:10px; margin-bottom:14px; }
    .cat-group:last-child{ margin-bottom:0; }
    .cat-bracket-label{ flex:none; width:130px; font-size:12px; font-weight:700; color:var(--ink-soft);
      display:flex; align-items:center; justify-content:flex-end; text-align:right; padding-right:2px; }
    .cat-bracket{ flex:none; width:10px; border:2px solid var(--line-strong); border-right:none; border-radius:6px 0 0 6px; }
    .cat-group-items{ flex:1; display:flex; flex-direction:column; justify-content:center; gap:11px; }

    /* Shared highlighted-callout style for the "needs attention" / "bottom-half" sentences */
    .callout-attn{ text-align:center; font-weight:700; margin:14px 0 0; padding:11px 16px; background:var(--low-soft); color:#8a3226; border-radius:8px; }
    .callout-good{ text-align:center; font-weight:700; margin:14px 0 0; padding:11px 16px; background:var(--primary-soft); color:#1f6b4d; border-radius:8px; }
    .avg-row td{ font-style:italic; border-bottom:2px solid var(--ink-soft); }

    /* Forecast table + line charts (ported from the VRF tracker) */
    .forecast-table td, .forecast-table th{ padding:12px 10px; }
    @media (min-width:600px){ .forecast-table td, .forecast-table th{ padding:12px 14px; } }
    .scenario-divider td{ font-weight:700; font-size:13px; padding:9px 10px !important; border-bottom:1px solid var(--line-strong); }
    .scenario-divider.s1 td{ background:var(--grey-soft); color:var(--ink-soft); }
    .scenario-divider.s2 td{ background:var(--gold-soft); color:var(--gold); }
    .scenario-divider.s3 td{ background:var(--primary-soft); color:var(--primary); }
    .delta-pos{ color:var(--primary); font-weight:600; } .delta-neg{ color:var(--low); font-weight:600; }
    .method-note{ font-size:12.5px; color:var(--ink-soft); margin-top:6px; }
    .chart-tabs{ display:flex; gap:6px; margin-bottom:14px; flex-wrap:wrap; }
    .chart-tab{ appearance:none; cursor:pointer; font-family:inherit; font-size:13px; font-weight:600; color:var(--ink-soft);
      background:var(--panel-alt); border:1px solid var(--line-strong); border-radius:999px; padding:9px 16px; min-height:40px; }
    .chart-tab.active{ background:var(--primary); border-color:var(--primary); color:#fff; }
    .chart-box{ overflow-x:auto; -webkit-overflow-scrolling:touch; position:relative; }
    .chart-box svg{ display:block; min-width:480px; width:100%; height:auto; touch-action:pan-y; }
    .chart-caption{ font-size:12.5px; color:var(--ink-soft); margin-top:10px; }
    .chart-tooltip{ position:absolute; pointer-events:none; background:var(--ink); color:#fff; font-size:12px; font-weight:600;
      padding:6px 10px; border-radius:6px; opacity:0; transform:translate(-50%,calc(-100% - 10px)); transition:opacity .1s ease;
      white-space:nowrap; z-index:5; font-variant-numeric:tabular-nums; }
    .chart-tooltip.visible{ opacity:1; }
    .chart-dot{ cursor:pointer; }
    .chart-legend{ display:flex; flex-direction:column; gap:8px; margin-bottom:14px; }
    .chart-legend-item{ display:flex; align-items:flex-start; gap:8px; }
    .chart-legend-swatch{ width:12px; height:12px; border-radius:3px; flex:none; margin-top:3px; }
    .chart-legend-text{ font-size:12.5px; line-height:1.4; }
    .chart-legend-text b{ color:var(--ink); } .chart-legend-text span{ color:var(--ink-soft); }
    .scroll-hint{ font-size:11px; color:var(--ink-soft); margin-top:8px; text-align:center; display:none; }
    @media (max-width:640px){ .scroll-hint.show-mobile{ display:block; } }

    /* pictogram (one square per VO) + legend for VRF's Needs Attention cards */
    .pictogram{ display:flex; flex-wrap:wrap; gap:4px; margin:10px 0 12px; }
    .pictogram .dot{ width:11px; height:11px; border-radius:3px; }
    .dot.good{ background:var(--primary); } .dot.flag{ background:var(--gold); } .dot.na{ background:var(--grey); }
    .pict-legend{ display:flex; flex-wrap:wrap; gap:12px; margin-top:12px; font-size:12px; color:var(--ink-soft); }
    .pict-legend span{ display:inline-flex; align-items:center; gap:6px; }
    .pict-legend .sw{ width:10px; height:10px; border-radius:3px; display:inline-block; }

    /* donut with hoverable slices, centred */
    .donut-wrap{ position:relative; margin:0 auto; }
    .donut-slice{ cursor:pointer; }

    /* ---- new for the statewide shell: search/finder landing view ---- */
    #view-landing{ max-width:640px; margin:60px auto; }
    #view-landing .intro{ text-align:center; margin-bottom:32px; }
    #view-landing .intro h1{ margin-bottom:10px; }
    #view-landing .intro p{ color:var(--ink-soft); font-size:14.5px; }
    .finder-panel{ background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:28px 26px; }
    .nav-label{ font-size:12px; font-weight:700; letter-spacing:.06em; text-transform:uppercase; color:var(--ink-soft); margin-bottom:8px; }
    .nav-dropdown-row{ display:flex; flex-direction:column; gap:14px; margin-bottom:6px; }
    .nav-select{ appearance:none; width:100%; min-height:46px; font-family:inherit; font-size:15px; color:var(--ink);
      background:var(--panel) url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="%237C8B81"><path d="M5.5 7.5l4.5 5 4.5-5z"/></svg>') no-repeat right 14px center;
      background-size:16px; border:1px solid var(--line-strong); border-radius:8px; padding:10px 40px 10px 14px; }
    .nav-select:disabled{ color:var(--grey); background-color:var(--grey-soft); cursor:not-allowed; }
    .nav-divider{ display:flex; align-items:center; gap:12px; margin:24px 0; color:var(--ink-soft); font-size:12px; font-weight:700; letter-spacing:.06em; text-transform:uppercase; }
    .nav-divider::before, .nav-divider::after{ content:""; flex:1; height:1px; background:var(--line); }
    .nav-id-row{ display:flex; gap:10px; }
    /* min-width:0 overrides the flex item's default min-width:auto, which
       otherwise floors its shrink-width at the input's intrinsic content size -
       without it, the row overflows its box on narrow phones instead of the
       input shrinking to make room for the Go button. */
    .nav-id-input{ flex:1; min-width:0; min-height:46px; font-family:inherit; font-size:15px; border:1px solid var(--line-strong); border-radius:8px; padding:10px 14px; color:var(--ink); }
    .nav-go-btn{ appearance:none; cursor:pointer; font-family:inherit; font-size:14px; font-weight:700; color:#fff; background:var(--primary); border:none; border-radius:8px; padding:0 22px; min-height:46px; flex:none; }
    .nav-go-btn:hover{ opacity:.92; }
    .nav-error{ font-size:13px; color:#8a3226; background:var(--low-soft); border-radius:8px; padding:9px 12px; margin-top:12px; }
    .back-link{ display:inline-flex; align-items:center; gap:6px; font-size:13px; font-weight:600; color:var(--primary); cursor:pointer; margin-bottom:14px; background:none; border:none; font-family:inherit; padding:0; }
    .back-link:hover{ text-decoration:underline; }
    .placeholder-icon{ font-size:34px; text-align:center; margin-bottom:14px; }
    .finder-count{ text-align:center; font-size:12.5px; color:var(--ink-soft); margin-top:18px; }
    """

    # ============================================================================
    # JS - the shared helper library, identical to the single-CLF prototype's
    # JS block, except: DATA/TIPS/ERR_MSG_JS/FOOTNOTES change from const-with-
    # baked-in-value to `let`, populated at runtime once shared.json and the
    # selected CLF's JSON have been fetched (see JS_SHELL at the bottom).
    # ============================================================================
    JS = r"""
    let DATA = null;
    let FOOTNOTES = {};
    let TIPS = {};
    let ERR_MSG_JS = {};
    let CONTEXT = {};

    function contextBox(key){
      const c = CONTEXT[key];
      if(!c) return '';
      return `<div class="context-box">${c}<div class="context-source">Data source: ${FOOTNOTES[key]||''}</div></div>`;
    }

    function fmtRs(v){ if(v===null||v===undefined) return '—'; const av=Math.abs(v);
      if(av>=10000000) return '₹'+(v/10000000).toFixed(2)+'Cr'; if(av>=100000) return '₹'+(v/100000).toFixed(2)+'L';
      return '₹'+Math.round(v).toLocaleString('en-IN'); }
    function fmtNum(v){ return v===null||v===undefined ? '—' : Math.round(v).toLocaleString('en-IN'); }
    function fmtF(v,d){ return v===null||v===undefined ? '—' : v.toFixed(d); }
    // VO names carry a trailing formation-date/org-suffix fragment in the source
    // data (e.g. "ABHILASHA  21  01  17 MAHASHAKTI CLF") - keep only what precedes
    // the first digit, same rule the VRF tracker uses for its own VO names.
    function stripVoName(n){
      const m = n.match(/^[^0-9]+/);
      return (m ? m[0] : n).trim();
    }
    function fmtPct(v,d){ return v===null||v===undefined ? '—' : v.toFixed(d===undefined?1:d)+'%'; }
    function grade(pct){ if(pct===null||pct===undefined) return 'var(--grey)'; if(pct>=75) return 'var(--primary)'; if(pct>=50) return 'var(--gold)'; return 'var(--low)'; }
    function ord(n){ if(n===null||n===undefined) return ''; const s=['th','st','nd','rd'], v=Math.round(n)%100; return s[(v-20)%10]||s[v]||s[0]; }
    function withOrd(n){ return n===null||n===undefined ? ERR_MSG_JS.not_found : n+ord(n); }
    function cssVar(n){ return getComputedStyle(document.documentElement).getPropertyValue(n).trim(); }
    // "the X category" / "the X and Y categories" - shared phrasing for attention callouts
    function catListPhrase(names){
      if(names.length===1) return `the ${names[0]} category`;
      return `the ${names.slice(0,-1).join(', ')} and ${names[names.length-1]} categories`;
    }
    // plain Oxford-comma join, no "category/categories" suffix - for lists of
    // individual metric names rather than whole categories
    function listPhrase(names){
      if(names.length===1) return names[0];
      return `${names.slice(0,-1).join(', ')} and ${names[names.length-1]}`;
    }

    function drawRing(svgId, frac, color, trackColor){
      const svg=document.getElementById(svgId); if(!svg) return;
      const r=75,cx=95,cy=95,C=2*Math.PI*r;
      svg.innerHTML = `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${trackColor||'var(--line)'}" stroke-width="18"/>`+
        `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${color}" stroke-width="18" stroke-linecap="round" stroke-dasharray="${(frac*C).toFixed(1)} ${C.toFixed(1)}" transform="rotate(-90 ${cx} ${cy})"/>`;
    }
    // segments: [{frac, color, tip}] - each slice carries its own data-tip so the
    // shared tooltip-delegation system picks it up on hover with no extra wiring.
    // Each slice also gets its %-share printed on the ring itself (white, at the
    // slice's mid-angle), matching the reference donut style the user supplied.
    function drawDonut(svgId, segments){
      const svg=document.getElementById(svgId); if(!svg) return;
      const r=65,cx=95,cy=95,C=2*Math.PI*r,sw=50; let off=0,html='';
      segments.forEach(seg=>{
        const frac = Math.max(seg.frac,0), len=frac*C;
        html+=`<circle class="donut-slice" ${seg.tip?`data-tip="${seg.tip}"`:''} cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${seg.color}" stroke-width="${sw}" stroke-dasharray="${len.toFixed(1)} ${C.toFixed(1)}" stroke-dashoffset="${(-off).toFixed(1)}" transform="rotate(-90 ${cx} ${cy})"/>`;
        // Slices below ~5% don't have enough arc width to hold a legible label
        // without colliding with its neighbours (this is exactly what produced the
        // overlapping "3.9%/0.3%/0.1%" jumble the user flagged) - skip the on-slice
        // text for those; the legend row and hover tooltip still show the value.
        if(frac>=0.05){
          const midAngleDeg = ((off+len/2)/C)*360 - 90;
          const rad = midAngleDeg*Math.PI/180;
          const lx = cx + r*Math.cos(rad), ly = cy + r*Math.sin(rad);
          html += `<text x="${lx.toFixed(1)}" y="${ly.toFixed(1)}" font-size="13" font-weight="600" fill="#fff" text-anchor="middle" dominant-baseline="middle" style="pointer-events:none;">${Math.round(frac*100)}%</text>`;
        }
        off+=len; });
      svg.innerHTML=html;
    }
    // Centred donut + legend block, built from a plain {label: value} object.
    // Filters non-positive entries automatically (so a hidden bug that leaves an
    // unmapped line item can't silently draw an incomplete ring) and hovering
    // each legend swatch AND each ring slice shows label + value via data-tip.
    // Percentages are printed on the slices themselves (see drawDonut) - the
    // legend intentionally keeps only the swatch + category name.
    function donutBlock(svgId, data, colors, opts){
      opts = opts||{};
      const entries = Object.entries(data).filter(([k,v])=>v>0);
      const total = entries.reduce((a,[k,v])=>a+v,0);
      const fmt = opts.fmt || (v=>fmtRs(v));
      const segs = entries.map(([k,v],i)=>({frac: total?v/total:0, color: colors[i%colors.length], tip: `${k}: ${fmt(v)}`}));
      const size = opts.size || 150;
      return {
        html: `<div style="display:flex;gap:20px;align-items:center;flex-wrap:wrap;justify-content:center;">
          <div class="donut-wrap" style="width:${size}px;height:${size}px;"><svg id="${svgId}" viewBox="0 0 190 190" width="${size}" height="${size}"></svg></div>
          <div class="donut-legend">${entries.map(([k,v],i)=>`<div class="legend-row" data-tip="${k}: ${fmt(v)}"><span class="legend-swatch" style="background:${colors[i%colors.length]}"></span><span>${k}</span></div>`).join('')}</div>
        </div>`,
        draw: ()=> drawDonut(svgId, segs),
      };
    }
    function miniRing(pct,color,size=76){
      const r=(size-9)/2,cx=size/2,cy=size/2,C=2*Math.PI*r;
      return `<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}">
        <circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="var(--line)" stroke-width="9"/>
        <circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${color}" stroke-width="9" stroke-dasharray="${C}" stroke-dashoffset="${C*(1-pct/100)}" transform="rotate(-90 ${cx} ${cy})" stroke-linecap="round"/></svg>`;
    }

    function tile(label, value, sub, cls, tip){
      const t = tip || TIPS[label];
      const l = t ? `<div class="label tip" data-tip="${t}">${label}</div>` : `<div class="label">${label}</div>`;
      const v = (value===null||value===undefined) ? '—' : value;
      return `<div class="tile ${cls||''}">${l}<div class="value">${v}</div>${sub?`<div class="sub">${sub}</div>`:''}</div>`;
    }
    // "45th out of 53 CLFs · 51st percentile" - rank leads, percentile follows;
    // falls back to just the percentile when no rank is available (composite/
    // derived scores with no real peer population to rank against).
    // Headline is the RANK ("2nd" + smaller "out of 64 CLFs" on the same line);
    // the percentile moves to its own smaller line below - a rank alone isn't
    // enough context on its own ("2nd" could mean 2nd of 3 or 2nd of 3,000), so
    // pairing it with the percentile keeps both readable at a glance. The
    // progress track still runs on the percentile's 0-100 scale (a rank has no
    // natural 0-100 equivalent), so pctl is still needed even when rank leads.
    function standingCard(kind,pctl,rank,n,rankLine,labelText){
      const pctlKnown = pctl!==null && pctl!==undefined;
      const rankKnown = rank!=null && n!=null;
      const barPct = pctlKnown ? pctl : 0;
      const headline = rankKnown
        ? `${fmtNum(rank)}<sup>${ord(rank)}</sup><span class="outof">out of ${fmtNum(n)} CLFs</span>`
        : (pctlKnown ? `${pctl}<sup>${ord(pctl)}</sup>` : '—');
      const subText = pctlKnown ? `${pctl}${ord(pctl)} percentile` : (rankKnown ? '' : 'percentile');
      return `<div class="standing-card ${kind}"><div class="label">${labelText}</div><div class="big serif">${headline}</div>
        <div class="sub">${subText}</div><div class="track"><div class="fill" style="width:${barPct}%"></div><div class="marker" style="left:${barPct}%"></div></div>
        <div class="rank-line">${rankLine}</div></div>`;
    }
    function pctlRow(label, m, tip){
      const t = tip || TIPS[label];
      const lbl = t ? `<span class="tip" data-tip="${t}">${label}</span>` : label;
      if(!m) return `<div class="bar-row"><div class="blabel">${lbl}</div><div class="bar-track"></div><div class="bval" style="color:var(--ink-soft)">${ERR_MSG_JS.not_found}</div></div>`;
      const dRank = m.district_rank!=null && m.n_district!=null ? `${fmtNum(m.district_rank)}${ord(m.district_rank)} of ${fmtNum(m.n_district)}` : ERR_MSG_JS.not_found;
      const sRank = m.state_rank!=null && m.n_state!=null ? `${fmtNum(m.state_rank)}${ord(m.state_rank)} of ${fmtNum(m.n_state)}` : ERR_MSG_JS.not_found;
      return `<div class="metric-pctl-row">
        <div style="font-size:13.5px;">${lbl}</div>
        <div><div style="font-size:11px;color:var(--ink-soft);display:flex;justify-content:space-between;margin-bottom:4px;"><span style="color:var(--primary);font-weight:700;">DISTRICT</span><span>${dRank}</span></div>
          <div class="bar-track"><div class="fill" style="width:${m.district_pctl||0}%;background:var(--primary)"></div></div></div>
        <div><div style="font-size:11px;color:var(--ink-soft);display:flex;justify-content:space-between;margin-bottom:4px;"><span style="color:var(--gold);font-weight:700;">STATE</span><span>${sRank}</span></div>
          <div class="bar-track"><div class="fill" style="width:${m.state_pctl||0}%;background:var(--gold)"></div></div></div>
      </div>`;
    }
    function gradedBar(label, pct, tip){
      // tip===undefined (not passed at all) means "use the TIPS auto-lookup";
      // tip==='' (explicitly passed empty) means "this caller wants no tooltip
      // here, even if TIPS happens to have a same-named entry" - Category
      // Summary's 5 category names need the latter, since e.g. "VRF Fund
      // Health" collides with an unrelated tip written for the VRF tab.
      const t = tip !== undefined ? tip : TIPS[label];
      const lbl = t ? `<span class="tip" data-tip="${t}">${label}</span>` : label;
      const c = grade(pct);
      return `<div class="bar-row"><div class="blabel"><b>${lbl}</b></div><div class="bar-track"><div class="fill" style="width:${pct}%;background:${c}"></div></div><div class="bval" style="color:${c}">${pct}%</div></div>`;
    }
    // Plain colour-graded percentage, no bar/track - the category name never carries
    // a hover here (category-level headers intentionally have no tooltip; only the
    // individual metrics beneath do).
    function scorePercent(label, pct){
      const c = grade(pct);
      return `<div style="display:flex;justify-content:space-between;align-items:center;padding:2px 0 6px;"><b style="font-size:14px;">${label}</b><span style="color:${c};font-weight:700;font-size:18px;font-variant-numeric:tabular-nums;">${pct}%</span></div>`;
    }
    function tableHtml(cols, rows){
      return `<div class="table-wrap"><table><thead><tr>${cols.map(c=>{
          const t = c.tip || TIPS[c.label];
          // a single class attribute only - duplicating it (one for num, one for
          // th-tip) silently drops the second one in HTML, which is exactly why
          // numeric+tipped columns weren't showing their dotted-underline hover.
          const cls = [c.num?'num':'', t?'th-tip':''].filter(Boolean).join(' ');
          return `<th${cls?` class="${cls}"`:''}${t?` data-tip="${t}"`:''}>${c.label}</th>`;
        }).join('')}</tr></thead>
        <tbody>${rows.map(r=>`<tr>${r.map((v,i)=>`<td${cols[i].num?' class="num"':''}>${v}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
    }

    // Click-to-sort table: cols carry an optional `key` (the field on each item
    // in `data` to sort by - omit `key` for a column that shouldn't be sortable,
    // e.g. a composite chip column). Re-sorts the underlying data array itself
    // (not just the rendered strings), so numeric columns sort numerically
    // rather than as text, then rebuilds display rows via rowBuilder(item).
    function makeSortableTable(containerId, cols, data, rowBuilder, tip, extraRow){
      // extraRow (e.g. the State Average benchmark row) is a raw data object,
      // shaped identically to a normal `data` row - it's merged into the sortable
      // pool and sorts naturally by whichever column is clicked (so you can see
      // where it lands relative to real rows), just rendered with a distinguishing
      // class (avg-row) rather than pinned to a fixed position.
      const state = { col: null, dir: 1 };
      function draw(){
        const fullData = extraRow ? [...data, extraRow] : data;
        const sorted = state.col === null ? fullData : fullData.slice().sort((a, b) => {
          let av = a[cols[state.col].key], bv = b[cols[state.col].key];
          if(typeof av === 'string'){ av = av.toUpperCase(); bv = bv.toUpperCase(); }
          if(av == null) return 1; if(bv == null) return -1;
          if(av < bv) return -1 * state.dir;
          if(av > bv) return 1 * state.dir;
          return 0;
        });
        const theadHtml = `<tr>${cols.map((c,i)=>{
          const t = c.tip || tip || TIPS[c.label];
          const cls = [c.num?'num':'', t?'th-tip':'', c.key?'sortable':''].filter(Boolean).join(' ');
          const arrow = c.key ? ` <span class="sort-arrow${state.col===i?' active':''}">${state.col===i?(state.dir===1?'▲':'▼'):'⇅'}</span>` : '';
          return `<th${cls?` class="${cls}"`:''}${t?` data-tip="${t}"`:''}${c.key?` data-colidx="${i}"`:''}>${c.label}${arrow}</th>`;
        }).join('')}</tr>`;
        const tbodyHtml = sorted.map(item=>{
          const r = rowBuilder(item);
          const rowCls = item===extraRow ? ' class="avg-row"' : '';
          return `<tr${rowCls}>${r.map((v,i)=>`<td${cols[i].num?' class="num"':''}>${v}</td>`).join('')}</tr>`;
        }).join('');
        document.getElementById(containerId).innerHTML = `<div class="table-wrap"><table><thead>${theadHtml}</thead><tbody>${tbodyHtml}</tbody></table></div>`;
        document.querySelectorAll(`#${containerId} th.sortable`).forEach(th=>{
          th.addEventListener('click', ()=>{
            const idx = +th.dataset.colidx;
            if(state.col === idx) state.dir *= -1; else { state.col = idx; state.dir = 1; }
            draw();
          });
        });
      }
      draw();
    }

    // ---- global hover-tooltip delegation (data-tip attribute, any element) ----
    function initTooltips(){
      const box = document.getElementById('tooltip-box');
      if(!box) return;
      function place(e){ box.style.left = e.clientX+'px'; box.style.top = (e.clientY-8)+'px'; }
      document.addEventListener('mouseover', e=>{
        const el = e.target.closest('[data-tip]'); if(!el) return;
        box.textContent = el.getAttribute('data-tip'); box.classList.add('visible'); place(e);
      });
      document.addEventListener('mousemove', e=>{ if(e.target.closest('[data-tip]')) place(e); });
      document.addEventListener('mouseout', e=>{ if(e.target.closest('[data-tip]')) box.classList.remove('visible'); });
    }
    """

    JS_OVERVIEW = r"""
    function renderOverviewProfile(){
      const o = DATA.overview;
      const statusCls = !o.status_tier_found ? 'neutral' : (o.status_tier==='Model & Registered' ? '' : (o.status_tier==='Neither Model nor Registered' ? 'neg' : 'warn'));
      const statusVal = o.status_tier_found ? o.status_tier : 'Not Found';
      return `
      <section><div class="section-head"><h2 class="serif">CLF Snapshot</h2></div>
        <div class="panel"><div class="tiles n4">
          ${tile('Number of VOs', o.n_vo, null, 'info')}${tile('Number of SHGs', o.n_shg, null, 'info')}${tile('Total Members', fmtNum(o.n_members), null, 'info')}
          ${tile('Active Members', fmtNum(o.n_active), o.pct_active+'% of total')}
        </div>
        <div class="tiles n2" style="margin-top:14px;">
          ${tile('Formation Date', o.formation_date, null, 'neutral')}${tile('Registration Date', o.registration_date, null, 'neutral')}
        </div></div></section>
      <section><div class="section-head"><h2 class="serif">CLF Status</h2></div>
        <div class="panel"><div class="tiles n4">
          ${tile('CLF Status', statusVal, null, statusCls)}
          ${tile('Platform Approval Status', o.approval_status, null, 'neutral')}
          ${tile('Co-option Status', o.cooption_status, null, 'neutral')}
          ${tile('Registration Code', o.nic_code, null, 'neutral')}
        </div></div></section>
      <section><div class="section-head"><h2 class="serif">Governance Structure</h2></div>
        <div class="panel">
          <div class="tiles n3" style="margin-bottom:16px;">
            ${tile('President', o.president, null, 'neutral')}${tile('Secretary', o.secretary, null, 'neutral')}${tile('Executive Committee Size', o.ec_count, null, 'info')}
          </div>
          ${tableHtml([{label:'Subcommittee', tip:TIPS['Subcommittee Membership']},{label:'Members',num:true}], Object.entries(o.subcom).map(([k,v])=>[k,v]))}
          <div class="tiles n2" style="margin-top:16px;">
            ${tile('Meeting Frequency', o.meeting_frequency, null, 'neutral')}${tile('Savings Schedule', o.savings_frequency + (o.savings_amount?(' · '+fmtRs(o.savings_amount)):''), null, 'neutral')}
          </div>
        </div></section>`;
    }
    function renderOverviewMembers(){
      const o = DATA.overview;
      const covRows = Object.entries(o.coverage).map(([k,[pct,n]])=>[k, fmtNum(n), pct+'%']);
      const eduRows = Object.entries(o.education).map(([k,[pct,n]])=>[k, fmtNum(n), pct+'%']);
      const lvColors = ['var(--primary)','var(--gold)','#5B8AA6','var(--low)','var(--grey)'];
      const lvDonut = donutBlock('ring-livelihood', o.livelihood_split, lvColors, {size:190, fmt:v=>v+'%'});
      return `
      <section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Member Composition']}">Social Inclusion / Welfare Coverage</h2><span class="hint">% and # shown together</span></div>
        <div class="panel">${tableHtml([{label:'Group'},{label:'#',num:true},{label:'%',num:true}], covRows)}</div></section>
      <section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Education & Literacy']}">Education &amp; Literacy</h2></div>
        <div class="panel">${tableHtml([{label:'Milestone'},{label:'#',num:true},{label:'%',num:true}], eduRows)}</div></section>
      <section><div class="section-head"><h2 class="serif">Livelihoods Diversification</h2></div>
        <div class="panel">
          <div class="tiles n2" style="margin-bottom:18px;">
            ${tile('Members With a Livelihood', o.pct_has_livelihood+'%', 'at least one of primary/secondary/tertiary')}
            ${tile('Members With Multiple Livelihoods', o.pct_multi_livelihood+'%', 'more than one activity')}
          </div>
          ${lvDonut.html}
        </div></section>
      <section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Special Project Activities']}">Special Project Activities</h2></div>
        <div class="panel">${o.spa_found ? `<div class="pills">${Object.entries(o.spa).map(([k,v])=>`<span class="pill ${v?'on':''}">${k}</span>`).join('')}</div>` : `<p class="disclaimer">${ERR_MSG_JS.not_found}</p>`}</div></section>
      <section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Cadre Roles in This CLF']}">Cadre Members</h2><span class="hint">${o.n_cadre_holders} members across ${o.n_distinct_cadre_types} distinct roles, ordered by count</span></div>
        <div class="panel">${tableHtml([{label:'Position'},{label:'Members',num:true}], Object.entries(o.cadre_roster).map(([k,v])=>[k,v]))}</div></section>`;
    }
    // ---- VO Overview: table of every VO in this CLF, click a name to swap the
    // same sub-tab to a single consolidated detail page for that VO (governance
    // + member stats together, no further sub-split), with a Back button
    // returning to the table - all in-page state, no dedicated URL. ----
    let voDetailCode = null;
    function voOverviewCols(){ return [
      {label:'VO', key:'vo_name'}, {label:'Block', key:'block'}, {label:'SHGs', num:true, key:'n_shgs'},
      {label:'Members', num:true, key:'n_members'}, {label:'% Active', num:true, key:'pct_active'},
    ]; }
    function voOverviewRow(v){
      return [
        `<a href="#" class="clf-link vo-detail-link" data-vo="${v.vo_code}">${v.vo_name}</a>`,
        v.block||'—', fmtNum(v.n_shgs), fmtNum(v.n_members), v.pct_active!=null?v.pct_active+'%':'—',
      ];
    }
    function renderVoTable(){
      const vos = DATA.vo_overview || [];
      if(!vos.length) return `<section><div class="panel"><p class="disclaimer">No VO-level data available for this CLF.</p></div></section>`;
      return `<section><div class="section-head"><h2 class="serif">VOs in this CLF</h2><span class="hint">${vos.length} VOs &middot; click a VO to see its full profile</span></div>
        <div class="panel"><div id="vo-overview-table-container"></div></div></section>`;
    }
    function renderVoDetail(voCode){
      const v = (DATA.vo_overview||[]).find(x=>String(x.vo_code)===String(voCode));
      if(!v) return `<p class="disclaimer">VO not found.</p>`;
      const covRows = Object.entries(v.coverage).map(([k,[pct,n]])=>[k, fmtNum(n), pct!=null?pct+'%':'—']);
      const eduRows = Object.entries(v.education).map(([k,[pct,n]])=>[k, fmtNum(n), pct!=null?pct+'%':'—']);
      const subcomRows = Object.entries(v.subcom).map(([k,n])=>[k, fmtNum(n)]);
      const cadreRows = Object.entries(v.cadre_roster).map(([k,n])=>[k, fmtNum(n)]);
      const lvColors = ['var(--primary)','var(--gold)','#5B8AA6','var(--low)','var(--grey)'];
      const lvDonut = donutBlock('ring-vo-livelihood', v.livelihood_split, lvColors, {size:170, fmt:val=>val+'%'});
      return `
      <button id="vo-back-btn" class="back-link">&larr; Back to VO list</button>
      <section><div class="section-head"><h2 class="serif">${v.vo_name}</h2><span class="hint">${v.block||''} Block &middot; LokOS Code ${v.nic_code||'—'}</span></div>
        <div class="panel"><div class="tiles n4">
          ${tile('Number of SHGs', fmtNum(v.n_shgs), null, 'info')}${tile('Total Members', fmtNum(v.n_members), null, 'info')}
          ${tile('Active Members', v.pct_active!=null?v.pct_active+'%':'—')}${tile('Status', v.active===true?'Active':(v.active===false?'Inactive':'—'), null, v.active===false?'neg':'')}
        </div>
        <div class="tiles n1" style="margin-top:14px;">${tile('Formation Date', v.formation_date||'—', null, 'neutral')}</div>
        </div></section>
      <section><div class="section-head"><h2 class="serif">Governance</h2></div>
        <div class="panel">
          <div class="tiles n3" style="margin-bottom:16px;">${tile('President', v.president||'—', null, 'neutral')}${tile('Secretary', v.secretary||'—', null, 'neutral')}${tile('Executive Committee Size', fmtNum(v.ec_count), null, 'info')}</div>
          ${tableHtml([{label:'Subcommittee'},{label:'Members',num:true}], subcomRows)}
          <div class="tiles n2" style="margin-top:16px;">${tile('Meeting Frequency', v.meeting_frequency||'—', null, 'neutral')}${tile('Savings Schedule', (v.savings_frequency||'—')+(v.savings_amount?(' &middot; '+fmtRs(v.savings_amount)):''), null, 'neutral')}</div>
        </div></section>
      <section><div class="section-head"><h2 class="serif">Social Inclusion / Welfare Coverage</h2></div>
        <div class="panel">${tableHtml([{label:'Group'},{label:'#',num:true},{label:'%',num:true}], covRows)}</div></section>
      <section><div class="section-head"><h2 class="serif">Education &amp; Literacy</h2></div>
        <div class="panel">${tableHtml([{label:'Milestone'},{label:'#',num:true},{label:'%',num:true}], eduRows)}</div></section>
      <section><div class="section-head"><h2 class="serif">Livelihoods Diversification</h2></div>
        <div class="panel">
          <div class="tiles n2" style="margin-bottom:18px;">${tile('Members With a Livelihood', v.pct_has_livelihood!=null?v.pct_has_livelihood+'%':'—')}${tile('Members With Multiple Livelihoods', v.pct_multi_livelihood!=null?v.pct_multi_livelihood+'%':'—')}</div>
          ${lvDonut.html}
        </div></section>
      <section><div class="section-head"><h2 class="serif">Cadre Members</h2><span class="hint">${fmtNum(v.n_cadre_holders)} members across ${fmtNum(v.n_distinct_cadre_types)} distinct roles</span></div>
        <div class="panel">${cadreRows.length?tableHtml([{label:'Position'},{label:'Members',num:true}], cadreRows):'<p class="disclaimer">No cadre roles recorded.</p>'}</div></section>`;
    }
    function renderOverviewVODispatch(){
      const panel = document.getElementById('panel-overview');
      if(voDetailCode){
        panel.innerHTML = renderVoDetail(voDetailCode);
        document.getElementById('vo-back-btn').addEventListener('click', ()=>{ voDetailCode=null; renderOverviewVODispatch(); });
        const v = (DATA.vo_overview||[]).find(x=>String(x.vo_code)===String(voDetailCode));
        if(v) donutBlock('ring-vo-livelihood', v.livelihood_split, ['var(--primary)','var(--gold)','#5B8AA6','var(--low)','var(--grey)'], {size:170, fmt:val=>val+'%'}).draw();
      } else {
        panel.innerHTML = renderVoTable();
        if((DATA.vo_overview||[]).length){
          makeSortableTable('vo-overview-table-container', voOverviewCols(), DATA.vo_overview, voOverviewRow);
          document.querySelectorAll('.vo-detail-link').forEach(a=>{
            a.addEventListener('click', e=>{ e.preventDefault(); voDetailCode=a.dataset.vo; renderOverviewVODispatch(); });
          });
        }
      }
    }
    function renderOverview(sub){
      if(sub!=='vo') voDetailCode = null;
      if(sub==='vo'){ renderOverviewVODispatch(); return; }
      document.getElementById('panel-overview').innerHTML =
        contextBox('overview') +
        (sub==='profile' ? renderOverviewProfile() : renderOverviewMembers());
      if(sub==='members'){
        const lvColors = ['var(--primary)','var(--gold)','#5B8AA6','var(--low)','var(--grey)'];
        donutBlock('ring-livelihood', DATA.overview.livelihood_split, lvColors, {size:190, fmt:v=>v+'%'}).draw();
      }
    }
    """

    JS_AUDIT = r"""
    function renderAuditScores(){
      const a = DATA.audit;
      if(!a.found) return `<section><div class="panel"><p class="disclaimer">${ERR_MSG_JS.audit_scores}</p></div></section>`;
      const worst = a.category_breakdown.filter(c=>grade(c.pct)==='var(--low)');
      const attnSentence = worst.length ? `<p class="callout-attn">Your CLF needs to pay most attention to ${catListPhrase(worst.map(c=>c.label))}.</p>` : '';
      // group item_scores by their parent category, in category_breakdown order, so
      // each category's items sit under one labelled bracket rather than a flat list.
      const grouped = a.category_breakdown.map(c=>({cat:c.label, items:a.item_scores.filter(it=>it.category===c.label)})).filter(g=>g.items.length);
      return `
      <section><div class="section-head"><h2 class="serif">Grade &amp; Standing</h2><span class="hint">Q4 only</span></div>
        <div class="panel">
          <div class="tiles n2" style="margin-bottom:18px;">${tile('Grade', a.grade, null, 'big-value')}${tile('Total Audit Score', a.total_score+' / 100', null, 'big-value')}</div>
          <div class="standing-grid">
            ${standingCard('district', a.district_pctl, a.district_rank, a.n_district, 'Score '+a.total_score+' / 100', 'Standing vs. District')}
            ${standingCard('state', a.state_pctl, a.state_rank, a.n_state, 'Score '+a.total_score+' / 100', 'Standing vs. State')}
          </div>
        </div></section>
      <section><div class="section-head"><h2 class="serif">Category Breakdown</h2><span class="hint">normalised to % of max, colour-graded</span></div>
        <div class="panel"><div class="donut-row">${a.category_breakdown.map((c,i)=>`
          <div class="donut-card"><div style="position:relative;width:76px;height:76px;margin:0 auto;">
            <svg id="cat-ring-${i}" viewBox="0 0 190 190" width="76" height="76" style="display:block;"></svg>
            <div style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;"><span style="font-weight:700;font-size:1.05rem;color:${grade(c.pct)}">${c.pct}%</span></div>
          </div><div class="dlabel"><b class="tip" data-tip="${TIPS[c.label]||''}">${c.label}</b><br><span style="font-size:11px;">${c.raw||0}/${c.max}</span></div></div>`).join('')}
        </div>${attnSentence}</div></section>
      <section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Detailed Scores']}">Individual Item Scores</h2></div>
        <div class="panel">${grouped.map(g=>`
          <div class="cat-group">
            <div class="cat-bracket-label">${g.cat}</div><div class="cat-bracket"></div>
            <div class="cat-group-items">${g.items.map(it=>gradedBar(it.label+' ('+(it.raw!==null?it.raw:'—')+'/'+it.max+')', it.pct)).join('')}</div>
          </div>`).join('')}</div></section>`;
    }
    // The raw irregularity text is usually a run-on numbered list ("1. ... 2.
    // ... 3. ..."); split on that pattern into one line per point for
    // readability, falling back to the plain text when there's no such pattern.
    function irregTextHtml(text){
      const points = text.split(/\d+\.\s+/).map(s=>s.trim()).filter(Boolean);
      if(points.length <= 1) return `<p class="desc" style="max-width:none;">${text}</p>`;
      return `<ul style="margin:0;padding-left:20px;">${points.map(p=>`<li style="margin-bottom:6px;font-size:12.5px;color:var(--ink-soft);line-height:1.5;">${p}</li>`).join('')}</ul>`;
    }
    function renderAuditIrreg(){
      const a = DATA.audit;
      if(!a.found) return `<section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Irregularity Flag']}">Financial Irregularities</h2></div>
        <div class="panel"><p class="disclaimer">${ERR_MSG_JS.financial_irregularities}</p></div></section>`;
      const cls = a.issue_flagged?'yes':'no';
      const txt = a.issue_flagged ? 'Your CLF <strong>was flagged</strong> for a financial irregularity this quarter by the auditor.' : 'Your CLF was not flagged for a financial irregularity this quarter by the auditor.';
      const gapDir = a.cash_vs_physical_gap===0 ? 'Cash book matches physical cash exactly' : (a.cash_book_higher ? 'Cash book records more than what was physically counted (physical cash is short)' : 'Physical cash counted is more than the cash book records');
      const gapColor = a.cash_vs_physical_gap===0 ? 'var(--primary)' : 'var(--low)';
      const catsBlock = (a.issue_flagged && a.irreg_categories && a.irreg_categories.length) ?
        `<div class="pills tip" data-tip="${TIPS['Type of Irregularity']}" style="margin-top:14px;">${a.irreg_categories.map(c=>`<span class="pill on">${c}</span>`).join('')}</div>` : '';
      const textBlock = (a.issue_flagged && a.irreg_text) ?
        `<div style="margin-top:14px;padding:12px 14px;background:var(--panel-alt);border-radius:8px;border:1px solid var(--line);">${irregTextHtml(a.irreg_text)}</div>` : '';
      return `<section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Irregularity Flag']}">Financial Irregularities</h2><span class="hint">Q4 only</span></div>
        <div class="panel"><p class="flag-headline ${cls}">${txt}</p>
          ${catsBlock}${textBlock}
          <div class="tiles n1" style="margin-top:14px;"><div class="tile"><div class="label tip" data-tip="${TIPS['Cash Book vs. Physical Cash']}">Cash Book vs. Physical Cash</div><div class="value" style="color:${gapColor};font-size:18px;">${fmtRs(a.cash_vs_physical_gap)} gap</div><div class="sub">${gapDir}</div></div></div>
        </div></section>`;
    }
    function renderAudit(){
      document.getElementById('panel-audit').innerHTML =
        contextBox('audit') +
        renderAuditScores() + renderAuditIrreg();
      if(DATA.audit.found) DATA.audit.category_breakdown.forEach((c,i)=> drawRing('cat-ring-'+i, c.pct/100, grade(c.pct)));
    }
    """

    JS_FINANCIAL = r"""
    let finQtrIdx = 0;
    const RC_COLORS = ['var(--primary)','var(--gold)','#5B8AA6','var(--low)','var(--grey)','#9C7FB0','#B08968','#6B9E78','#C97B63'];
    function renderFinSummary(){
      const f = DATA.financial; const q = f.quarters[finQtrIdx]; const f01 = f.f01;
      const isCurrentSnapshot = (finQtrIdx === f.quarters.length - 1);
      // "Suspense Amount" is an unreconciled accounting placeholder, not a real
      // asset/capital source - relabeled for the donut/legend/hover so it doesn't
      // read as a normal category (the Statements-tab table keeps the plain name).
      const assetsForDonut = f01.found ? Object.fromEntries(Object.entries(f01.assets).filter(([k])=>k!=='Total Assets').map(([k,v])=>[k==='Suspense Amount'?'Suspense Amount (unreconciled)':k, v])) : null;
      const liabForDonut = f01.found ? Object.fromEntries(Object.entries(f01.liabilities).filter(([k])=>k!=='Total Equity & Liabilities').map(([k,v])=>[k==='Suspense Amount'?'Suspense Amount (unreconciled)':k, v])) : null;
      const assetsDonut = (f01.found && isCurrentSnapshot && assetsForDonut) ? donutBlock('ring-assets', assetsForDonut, RC_COLORS, {size:150}) : null;
      const liabDonut = (f01.found && isCurrentSnapshot && liabForDonut) ? donutBlock('ring-liabilities', liabForDonut, RC_COLORS, {size:150}) : null;
      const gapKnown = f01.balance_gap_rs!=null;
      const gapVal = gapKnown ? (f01.balance_gap_rs>=0?'+':'−')+fmtRs(Math.abs(f01.balance_gap_rs)) : '—';
      const f01Block = !f01.found ? `
        <section><div class="section-head"><h2 class="serif">Balance Sheet</h2></div>
          <div class="panel"><p class="disclaimer">${ERR_MSG_JS.balance_sheet}</p></div></section>` :
        isCurrentSnapshot ? `
        <section><div class="section-head"><h2 class="serif">Balance Sheet</h2><span class="hint">as of ${f01.period}</span></div>
          <div class="panel"><div class="tiles n4">
            ${tile('Books Balance Check', gapVal, 'Assets − Liabilities+Equity', gapKnown&&f01.balance_gap_rs!==0?'neg':'')}
            ${tile('Liquidity Ratio', f01.liquidity_ratio!=null?f01.liquidity_ratio+'%':'—')}
            ${tile('Fund Deployment Ratio', f01.deployment_ratio!=null?f01.deployment_ratio+'%':'—')}
            ${tile('Surplus / Deficit', f01.surplus_pct!=null?(f01.surplus_pct>=0?'+':'')+f01.surplus_pct+'%':'—', null, f01.surplus_pct!=null&&f01.surplus_pct<0?'neg':'')}
          </div>
          <div style="display:flex;gap:24px;flex-wrap:wrap;justify-content:center;margin-top:18px;">
            <div><p class="hint" style="text-align:center;margin-bottom:8px;" data-tip="${TIPS['Assets Composition']}">Assets</p>${assetsDonut?assetsDonut.html:''}</div>
            <div><p class="hint" style="text-align:center;margin-bottom:8px;" data-tip="${TIPS['Liabilities Composition']}">Liabilities &amp; Equity</p>${liabDonut?liabDonut.html:''}</div>
          </div>
          </div></section>` :
        `<section><div class="section-head"><h2 class="serif">Balance Sheet</h2></div>
          <div class="panel"><p class="disclaimer">Balance sheet not available for this quarter — only exported as of ${f01.period}.</p></div></section>`;
      // Opening/Closing Balance is carried-over cash position, not real flow this
      // quarter - excluded from the composition donuts (kept in the full
      // statement table on the Statements tab, where it belongs).
      const rc = q.receipts_full ? Object.fromEntries(Object.entries(q.receipts_full).filter(([k])=>k!=='Total Receipts'&&k!=='Opening Balance')) : null;
      const pc = q.payments_full ? Object.fromEntries(Object.entries(q.payments_full).filter(([k])=>k!=='Total Payments'&&k!=='Closing Balance')) : null;
      const rDonut = rc ? donutBlock('ring-receipts', rc, RC_COLORS, {size:150}) : null;
      const pDonut = pc ? donutBlock('ring-payments', pc, RC_COLORS, {size:150}) : null;
      const html = f01Block + `
      <section><div class="section-head"><h2 class="serif">Receipts &amp; Payments</h2><span class="hint">${q.label}</span></div>
        <div class="panel">
          ${q.is_real ? `
          <div class="tiles n3" style="margin-bottom:16px;">${tile('Net Cash Flow', (q.net_cash_flow>=0?'+':'')+fmtRs(q.net_cash_flow))}
            ${tile('Operating Expense Ratio', fmtPct(q.operating_expense_ratio), null, q.operating_expense_ratio===0?'neg':'')}${tile('Interest Income Share', fmtPct(q.interest_income_share))}</div>
          <div style="display:flex;gap:24px;flex-wrap:wrap;justify-content:center;">
            <div><p class="hint" style="text-align:center;margin-bottom:8px;" data-tip="${TIPS['Where Money Came In From']}">Where Money Came In From</p>${rDonut?rDonut.html:''}</div>
            <div><p class="hint" style="text-align:center;margin-bottom:8px;" data-tip="${TIPS['Where Money Went Out To']}">Where Money Went Out To</p>${pDonut?pDonut.html:''}</div>
          </div>` : `<p class="disclaimer">${ERR_MSG_JS.receipts_payments}</p>`}
        </div></section>
      <section><div class="section-head"><h2 class="serif">Transactions</h2><span class="hint">${q.label}</span></div>
        <div class="panel">${q.is_real || q.cum_disbursed>0 ? `<div class="tiles n4">
          ${tile('Loan Amount Demanded This Quarter', fmtRs(q.new_demand_amt), null, q.new_demand_amt===0?'neg':'')}
          ${tile("This Quarter's Disbursement Rate", fmtPct(q.this_qtr_disb_rate), null, (q.this_qtr_disb_rate===0||q.this_qtr_disb_rate==null)?'neg':'')}
          ${tile("This Quarter's Pending Loans", fmtRs(q.qtr_pending), null, q.qtr_pending>0?'neg':'')}
          ${tile('Capacity to Meet Demand', q.capacity_ratio!==null?q.capacity_ratio.toFixed(1)+'×':'—', null, q.capacity_ratio===0?'neg':'')}
        </div>` : `<p class="disclaimer">${ERR_MSG_JS.transactions}</p>`}</div></section>`;
      return { html, assetsDonut, liabDonut, rDonut, pDonut };
    }
    function renderFinStatements(){
      const f = DATA.financial; const q = f.quarters[finQtrIdx]; const f01 = f.f01;
      const isCurrentSnapshot = (finQtrIdx === f.quarters.length - 1);
      const assetRows = f01.found ? Object.entries(f01.assets).map(([k,v])=>k==='Total Assets'?[`<b>${k}</b>`, `<b>${fmtRs(v)}</b>`]:[k, fmtRs(v)]) : null;
      const liabRows = f01.found ? Object.entries(f01.liabilities).map(([k,v])=>k==='Total Equity & Liabilities'?[`<b>${k}</b>`, `<b>${fmtRs(v)}</b>`]:[k, fmtRs(v)]) : null;
      const recvRows = q.receipts_full ? Object.entries(q.receipts_full).map(([k,v])=>k==='Total Receipts'?[`<b>${k}</b>`,`<b>${fmtRs(v)}</b>`]:[k, fmtRs(v)]) : null;
      const payRows = q.payments_full ? Object.entries(q.payments_full).map(([k,v])=>k==='Total Payments'?[`<b>${k}</b>`,`<b>${fmtRs(v)}</b>`]:[k, fmtRs(v)]) : null;
      return `
      <section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Balance Sheet']}">Balance Sheet</h2><span class="hint">${(f01.found && isCurrentSnapshot)?('as of '+f01.period):q.label}</span></div>
        <div class="panel">
          ${!f01.found ? `<p class="disclaimer">${ERR_MSG_JS.balance_sheet}</p>` : isCurrentSnapshot ? `
          <div class="statement-grid">
            <div><h3 style="font-size:13.5px;margin:0 0 8px;">Assets</h3>${tableHtml([{label:'Line Item'},{label:'Amount',num:true}], assetRows)}</div>
            <div><h3 style="font-size:13.5px;margin:0 0 8px;">Liabilities &amp; Equity</h3>${tableHtml([{label:'Line Item'},{label:'Amount',num:true}], liabRows)}</div>
          </div>
          <p class="hint" style="margin-top:14px;">Assets and Liabilities+Equity are reported separately and needn't sum to the same figure line-by-line — the Books Balance Check on the Summary tab compares their two totals directly.</p>` :
            `<p class="disclaimer">Not available for this quarter — only exported as of ${f01.period}.</p>`}
        </div></section>
      <section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Receipts & Payments Statement']}">Receipts &amp; Payments Statement</h2><span class="hint">${q.label}</span></div>
        <div class="panel">
          ${(q.is_real && recvRows && payRows) ? `
          <div class="statement-grid">
            <div><h3 style="font-size:13.5px;margin:0 0 8px;">Receipts</h3>${tableHtml([{label:'Line Item'},{label:'Amount',num:true}], recvRows)}</div>
            <div><h3 style="font-size:13.5px;margin:0 0 8px;">Payments</h3>${tableHtml([{label:'Line Item'},{label:'Amount',num:true}], payRows)}</div>
          </div>` : `<p class="disclaimer">${ERR_MSG_JS.receipts_payments}</p>`}
        </div></section>`;
    }
    function FUND_DISBURSEMENT_COLS(){
      return [
        {label:'Fund Heading', key:'heading'},
        {label:'Total Received', num:true, key:'total_received'},
        {label:'Batches', num:true, key:'n_batches'},
        {label:'Latest Receipt', key:'latest_receipt_date'},
      ];
    }
    function fundDisbursementRow(r){
      return [r.heading, fmtRs(r.total_received), fmtNum(r.n_batches), fmtLoanDate(r.latest_receipt_date)];
    }
    // Not quarter-scoped - this is a lifetime cumulative register of everything the CLF has
    // received under each fund heading, unlike Summary/Statements which are point-in-time or
    // quarterly. Donut collapses everything past the top 5 headings by amount into "Other" so
    // the visual stays legible even for CLFs with a long tail of one-off/rare fund categories -
    // the table below it still lists every heading individually, uncollapsed.
    function renderFinDisbursement(){
      const fd = DATA.fund_disbursement;
      if(!fd || !fd.found || !fd.headings.length){
        return `<section><div class="panel"><p class="disclaimer">${ERR_MSG_JS.fund_disbursement}</p></div></section>`;
      }
      return `<section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Fund Disbursement']||''}">Fund Disbursement</h2><span class="hint">lifetime total received, all batches</span></div>
        <div class="panel">
          <div style="display:flex;gap:32px;flex-wrap:wrap;align-items:flex-start;justify-content:center;">
            <div style="flex:2;min-width:320px;"><div id="fund-disbursement-table-container"></div></div>
            <div style="text-align:center;"><p class="hint" style="margin-bottom:8px;">Composition</p><div id="ring-fund-disbursement-wrap"></div></div>
          </div>
        </div></section>`;
    }
    function renderFinancial(sub){
      if(sub==='disbursement'){
        document.getElementById('panel-financial').innerHTML = contextBox('financial') + renderFinDisbursement();
        const fd = DATA.fund_disbursement;
        if(fd && fd.found && fd.headings.length){
          makeSortableTable('fund-disbursement-table-container', FUND_DISBURSEMENT_COLS(), fd.headings, fundDisbursementRow);
          const sorted = fd.headings.slice().sort((a,b)=>b.total_received-a.total_received);
          const donutData = {};
          sorted.forEach((h,i)=>{
            if(i<5) donutData[h.heading] = h.total_received;
            else donutData['Other'] = (donutData['Other']||0) + h.total_received;
          });
          document.getElementById('ring-fund-disbursement-wrap').innerHTML = '';
          const d = donutBlock('ring-fund-disbursement', donutData, RC_COLORS, {size:170});
          document.getElementById('ring-fund-disbursement-wrap').innerHTML = d.html;
          d.draw();
        }
        return;
      }
      const opts = DATA.financial.quarters.map((q,i)=>`<option value="${i}" ${i===finQtrIdx?'selected':''}>${q.label}</option>`).join('');
      const dropdown = `<div class="selectbar"><label for="qtr-select">Quarter:</label><select id="qtr-select">${opts}</select></div>`;
      let summaryDraws = null;
      let body;
      if(sub==='summary'){ const r = renderFinSummary(); body = r.html; summaryDraws = r; }
      else { body = renderFinStatements(); }
      document.getElementById('panel-financial').innerHTML =
        contextBox('financial') + dropdown + body;
      document.getElementById('qtr-select').addEventListener('change', e=>{ finQtrIdx=+e.target.value; renderFinancial(sub); });
      if(sub==='summary' && summaryDraws){
        if(summaryDraws.assetsDonut) summaryDraws.assetsDonut.draw();
        if(summaryDraws.liabDonut) summaryDraws.liabDonut.draw();
        if(summaryDraws.rDonut) summaryDraws.rDonut.draw();
        if(summaryDraws.pDonut) summaryDraws.pDonut.draw();
      }
    }
    """

    JS_VRF = r"""
    function renderVrfKpi(){
      const v = DATA.vrf;
      const earning = v.received_vo - v.idle_vo, notReceived = v.n_vo - v.received_vo;
      const missedSavings = v.expected_savings ? Math.max(v.expected_savings - v.total_savings, 0) : 0;
      return `
      <section><div class="section-head"><h2 class="serif">CLF Details</h2></div>
        <div class="panel"><div class="tiles n3">
          ${tile('FSF-Eligible VOs', fmtNum(v.fsf_eligible)+' of '+fmtNum(v.n_vo))}
          ${tile('VOs With Incomplete Coverage', fmtNum(v.incomplete_coverage)+' of '+fmtNum(v.n_vo), null, v.incomplete_coverage>0?'neg':'')}
          ${tile('Idle VOs', fmtNum(v.idle_vo)+' of '+fmtNum(v.received_vo), null, v.idle_vo>0?'neg':'')}
        </div></div></section>
      <section><div class="section-head"><h2 class="serif">Vulnerability Reduction Fund Size</h2></div>
        <div class="panel"><div class="tiles n5">
          ${tile('Total VRF Received', fmtRs(v.total_received))}${tile('Total Savings Collected', fmtRs(v.total_savings))}
          ${tile('Total Interest Collected', fmtRs(v.total_interest))}${tile('Total Fund Size', fmtRs(v.total_corpus))}
          ${tile('Coverage Gap', fmtRs(v.coverage_gap), null, v.coverage_gap>0?'neg':'')}
        </div></div></section>
      <section><div class="section-head"><h2 class="serif">Fund Health</h2></div>
        <div class="panel"><div class="health-grid">
          <div class="health-card"><h3 class="tip" data-tip="${TIPS['Savings as Promised']}">Savings as Promised</h3><div class="ring-wrap"><svg id="ring-savings" viewBox="0 0 190 190"></svg>
            <div class="ring-center"><div class="num">${v.expected_savings?Math.round(v.total_savings/v.expected_savings*100):'—'}%</div><div class="lbl">of promised savings collected</div></div></div>
            ${missedSavings>0?`<p class="desc" style="font-weight:600;color:var(--low);">Your CLF has missed out on ${fmtRs(missedSavings)} of savings.</p>`:''}</div>
          <div class="health-card"><h3 class="tip" data-tip="${TIPS['What the Fund Is Made Of']}">What the Fund Is Made Of</h3><div class="ring-wrap"><svg id="ring-composition" viewBox="0 0 190 190"></svg></div>
            <div class="donut-legend">
              <div class="legend-row" data-tip="Government Grant: ${fmtRs(v.total_received)}"><span class="legend-swatch" style="background:var(--gold)"></span><span>Government Grant</span></div>
              <div class="legend-row" data-tip="Savings: ${fmtRs(v.total_savings)}"><span class="legend-swatch" style="background:var(--primary)"></span><span>Savings</span></div>
              <div class="legend-row" data-tip="Interest: ${fmtRs(v.total_interest)}"><span class="legend-swatch" style="background:var(--ink)"></span><span>Interest</span></div>
            </div></div>
        </div></div></section>
      <section><div class="section-head"><h2 class="serif">Needs Attention</h2></div>
        <div class="panel"><div class="attn-grid">
          <div class="attn-card"><h3 class="tip" data-tip="${TIPS['Grant Not Fully Received']}">Grant Not Fully Received</h3><div class="attn-headline">${fmtNum(v.incomplete_coverage)} of ${fmtNum(v.n_vo)} VOs</div>
            <p class="desc" style="max-width:none;">Every VO can receive up to ₹1,50,000 from the government. This shows how many of your VOs haven't received the full amount yet — ${fmtRs(v.coverage_gap)} in total is still owed to them.</p>
            <div class="pictogram" id="pict-coverage"></div>
            <div class="pict-legend">
              <span><span class="sw" style="background:var(--primary)"></span><span>Fully received (${fmtNum(v.n_vo-v.incomplete_coverage)})</span></span>
              <span><span class="sw" style="background:var(--gold)"></span><span>Still owed (${fmtNum(v.incomplete_coverage)})</span></span>
            </div></div>
          <div class="attn-card"><h3 class="tip" data-tip="${TIPS['Fund Sitting Idle']}">Fund Sitting Idle</h3><div class="attn-headline">${fmtNum(v.idle_vo)} of ${fmtNum(v.received_vo)} VOs</div>
            <p class="desc" style="max-width:none;">Of the VOs that have received VRF funds, this shows how many earned no interest from lending last year — meaning that portion of the fund is sitting unused instead of helping members.</p>
            <div class="pictogram" id="pict-interest"></div>
            <div class="pict-legend">
              <span><span class="sw" style="background:var(--primary)"></span><span>Earning interest (${fmtNum(earning)})</span></span>
              <span><span class="sw" style="background:var(--gold)"></span><span>Idle, no interest (${fmtNum(v.idle_vo)})</span></span>
              <span><span class="sw" style="background:var(--grey)"></span><span>Not yet received VRF (${fmtNum(notReceived)})</span></span>
            </div></div>
        </div></div></section>`;
    }
    function drawPictogram(containerId, counts){ // [{n, cls}]
      const el = document.getElementById(containerId); if(!el) return;
      let html = '';
      counts.forEach(c=>{ for(let i=0;i<c.n;i++){ html += `<div class="dot ${c.cls}"></div>`; } });
      el.innerHTML = html;
    }
    function voTableCols(){ return [
      {label:'VO', tip:TIPS['VO Identity'], key:'vo_name'},
      {label:'Bookkeeper', tip:TIPS['VO Identity'], key:'bookkeeper_name'},
      {label:'FSF', tip:TIPS['VO Fund Status'], key:'fsf_eligibile'},
      {label:'Coverage', tip:TIPS['VO Fund Status'], key:'incomplete_vrf_coverage'},
      {label:'Lending', tip:TIPS['VO Fund Status'], key:'zero_interest_vo'},
      {label:'Members',num:true, tip:TIPS['SHG Members'], key:'totalshgmembers'},
      {label:'Sav.Disc%',num:true, tip:TIPS['VO Savings Discipline %'], key:'savings_discipline_rate'},
      {label:'Int.Yield',num:true, tip:TIPS['VO Interest Yield'], key:'interest_yield'},
      {label:'Corpus×',num:true, tip:TIPS['VO Corpus Multiplier'], key:'corpus_multiplier'},
    ]; }
    function voTableRow(r){
      return [stripVoName(r.vo_name), `<div class="bk-name">${r.bookkeeper_name}</div>`,
        r.fsf_eligibile?'<span class="status-tag good">Eligible</span>':'<span class="status-tag na">Not Eligible</span>',
        r.incomplete_vrf_coverage?'<span class="status-tag flag">Partial</span>':'<span class="status-tag good">Full</span>',
        r.zero_interest_vo?'<span class="status-tag flag">Idle</span>':'<span class="status-tag good">Active</span>',
        fmtNum(r.totalshgmembers), fmtF(r.savings_discipline_rate,1), fmtF(r.interest_yield,3), fmtF(r.corpus_multiplier,2)];
    }
    function renderVrfVO(){
      const v = DATA.vrf;
      return `<section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['VO Identity']}">VO-by-VO Detail</h2><span class="hint">all ${v.n_vo} VOs · click a column to sort</span></div>
        <div class="panel"><div id="vo-table-container"></div></div></section>`;
    }
    function bkTableCols(){ return [
      {label:'Rank', tip:TIPS['Bookkeeper Identity'], key:'rank'},
      {label:'Bookkeeper', tip:TIPS['Bookkeeper Identity'], key:'bookkeeper_name'},
      {label:'VOs',num:true, tip:TIPS['Bookkeeper Identity'], key:'n_vo'},
      {label:'Members',num:true, tip:TIPS['Bookkeeper Identity'], key:'members'},
      {label:'Sav.Disc',num:true, tip:TIPS['Bookkeeper Fund Metrics'], key:'sav_disc'},
      {label:'Sav.Real',num:true, tip:TIPS['Bookkeeper Fund Metrics'], key:'sav_real'},
      {label:'Corpus×',num:true, tip:TIPS['Bookkeeper Fund Metrics'], key:'corpus_mult'},
      {label:'Int.Yield',num:true, tip:TIPS['Bookkeeper Fund Metrics'], key:'int_yield'},
      {label:'Full Cov',num:true, tip:TIPS['Bookkeeper Fund Metrics'], key:'full_cov'},
      {label:'Composite', num:true, tip:TIPS['Bookkeeper Fund Metrics'], key:'composite'},
    ]; }
    function bkTableRow(b){
      return [`<span class="rank-cell">${b.rank}</span>`, `<div class="bk-name">${b.bookkeeper_name}</div><div class="bk-id">BK ID ${Math.round(b.bookkeeper_id)}</div>`,
        b.n_vo, fmtNum(b.members), fmtF(b.sav_disc,1), fmtF(b.sav_real,2), fmtF(b.corpus_mult,2), fmtF(b.int_yield,3), b.full_cov!=null?b.full_cov.toFixed(0)+'%':'—',
        `<div class="score-cell"><div class="score-bar"><div class="fill" style="width:${b.composite||0}%"></div></div><span class="score-num">${fmtF(b.composite,1)}</span></div>`];
    }
    function renderVrfBK(){
      const v = DATA.vrf;
      return renderVrfFundStanding() + `<section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Bookkeeper Identity']}">Bookkeeper Ranking</h2><span class="hint">within this CLF · click a column to sort</span></div>
        <div class="panel"><div id="bk-table-container"></div></div></section>`;
    }
    function deltaCell(now, end){
      const d = end-now, pos = d>=0;
      const pct = now ? (d/now*100) : null;
      return `<td class="num ${pos?'delta-pos':'delta-neg'}">${pos?'+':''}${fmtRs(d)}${pct!==null?` (${pos?'+':''}${pct.toFixed(1)}%)`:''}</td>`;
    }
    function renderVrfForecasts(){
      const v = DATA.vrf, f = v.forecast;
      const scenarioRows = (label, s) => `
        <tr><td>Total Interest Collected</td><td class="num">${fmtRs(s.interest_now)}</td><td class="num">${fmtRs(s.interest_end)}</td>${deltaCell(s.interest_now, s.interest_end)}</tr>
        <tr><td>Total Fund Size</td><td class="num">${fmtRs(s.corpus_now)}</td><td class="num">${fmtRs(s.corpus_end)}</td>${deltaCell(s.corpus_now, s.corpus_end)}</tr>`;
      const scenarios = [['s1','Current Trend'],['s2','If Idle VOs Start Lending'],['s3','If Fully Utilized']]
        .filter(([k])=> k!=='s2' || f.has_idle_eligible);
      return `
      <section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Fund Projections']}">Targets for 31 March 2027</h2></div>
        <div class="panel">
          <div class="table-wrap"><table class="forecast-table">
            <thead><tr><th>Metric</th><th class="num">Current</th><th class="num">Projected (31 Mar 2027)</th><th class="num">Δ over 12 months</th></tr></thead>
            <tbody>
              <tr><td>Total Savings Collected</td><td class="num">${fmtRs(f.savings_now)}</td><td class="num">${fmtRs(f.savings_end)}</td>${deltaCell(f.savings_now, f.savings_end)}</tr>
              ${scenarios.map(([k,label])=>`<tr class="scenario-divider ${k}"><td colspan="4">${label}</td></tr>${scenarioRows(label, f[k])}`).join('')}
            </tbody>
          </table></div>
          <div class="method-note"><b>Savings</b> — where the CLF lands if all VOs hit full savings-collection discipline going forward.</div>
          <div class="method-note">Interest forecasts only include VOs that have received some VRF (${f.n_eligible} of ${f.n_vo} here).</div>
          <div class="method-note"><b>Current Trend</b> — extends each VO's own historical $-per-year accrual pace forward one more year; VOs earning nothing today continue earning nothing.</div>
          ${f.has_idle_eligible?`<div class="method-note"><b>If Idle VOs Start Lending</b> — same as Current Trend for VOs already earning interest, but assumes currently idle VOs start lending at the typical (median) pace of other VOs in this CLF.</div>`:''}
          <div class="method-note"><b>If Fully Utilized</b> — every eligible VO lends out its entire corpus all year, at the official 0.75%/month VRF rate.</div>
          <div class="method-note"><b>Fund Total</b> — current fund + savings addition + that scenario's interest addition; the VRF grant itself is held fixed (a one-time payment, not behaviour-driven).</div>
        </div></section>
      <section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Social Development Fund']}">Social Development Fund</h2></div>
        <div class="panel"><div class="tiles n2">${tile('Expected Monthly SDF Contribution', fmtRs(v.sdf_monthly))}${tile('Expected Annual SDF Contribution (×12)', fmtRs(v.sdf_annual))}</div>
          <p class="desc" style="max-width:none;margin-top:14px;">Each VO submits a fixed monthly amount to the CLF for the Social Development Fund (SDF). This is the total the CLF should be collecting from all its VOs each month, and over the year, if every VO pays what it owes.</p>
        </div></section>
      <section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Month-by-Month Projection']}">Month-by-Month</h2><span class="hint">Toggle between metrics · hover a dot for that month's value</span></div>
        <div class="panel">
          <div class="chart-tabs" id="chart-tabs">
            <button class="chart-tab active" data-series="savings">Savings</button>
            <button class="chart-tab" data-series="interest">Interest</button>
            <button class="chart-tab" data-series="corpus">Fund Total</button>
          </div>
          <div class="chart-legend" id="chart-legend" style="display:none;"></div>
          <div class="chart-box" id="line-chart-box"><svg id="line-chart" viewBox="0 0 640 240"></svg><div class="chart-tooltip" id="line-tooltip"></div></div>
          <div class="chart-caption" id="line-caption"></div>
          <div class="scroll-hint show-mobile">← Swipe to see more →</div>
        </div></section>`;
    }
    function renderVrfFundStanding(){
      const v = DATA.vrf;
      return `<section><div class="section-head"><h2 class="serif">Fund Standing</h2><span class="hint">composite of 5 fund-health metrics, member-weighted, percentile-ranked</span></div>
        <div class="panel"><div class="standing-grid">
          ${standingCard('district', v.composite_dist_pctl, v.composite_dist_rank, v.n_district, 'Composite score '+v.composite, 'Within '+v.district_name+' District')}
          ${standingCard('state', v.composite_state_pctl, v.composite_state_rank, v.n_state, 'Composite score '+v.composite, 'Within Bihar (Statewide)')}
        </div>
        ${v.metrics.map(m=>pctlRow(m.label, m)).join('')}
        </div></section>`;
    }

    // ---- month-by-month line chart (ported from the VRF tracker's own forecast engine) ----
    const MONTH_LABELS = ["Now","M1","M2","M3","M4","M5","M6","M7","M8","M9","M10","M11","31 Mar '27"];
    function vrfChartGrid(W,H,padL,padR,padT,innerH){
      let html='';
      for(let g=0; g<=3; g++){ const gy=padT+innerH*g/3; html+=`<line x1="${padL}" y1="${gy}" x2="${W-padR}" y2="${gy}" stroke="${cssVar('--line')}" stroke-width="1"/>`; }
      return html;
    }
    function vrfChartTicks(x, H){
      return [0,3,6,9,12].map(i=>`<text x="${x(i)}" y="${H-10}" font-size="11" fill="${cssVar('--ink-soft')}" text-anchor="middle">${MONTH_LABELS[i]}</text>`).join('');
    }
    function bindVrfDotTooltips(svg, tipFn){
      const box = document.getElementById('line-chart-box'), tooltip = document.getElementById('line-tooltip');
      svg.querySelectorAll('.chart-dot').forEach(dot=>{
        dot.addEventListener('mouseenter', ()=>{
          const dr=dot.getBoundingClientRect(), br=box.getBoundingClientRect();
          tooltip.style.left=(dr.left-br.left+dr.width/2)+'px'; tooltip.style.top=(dr.top-br.top)+'px';
          tooltip.textContent=tipFn(dot); tooltip.classList.add('visible');
        });
        dot.addEventListener('mouseleave', ()=> tooltip.classList.remove('visible'));
      });
    }
    function drawVrfSingleLine(key){
      document.getElementById('chart-legend').style.display='none';
      const svg = document.getElementById('line-chart');
      const data = DATA.vrf.forecast.savings_monthly;
      const W=640,H=240,padL=76,padR=16,padT=26,padB=34, innerW=W-padL-padR, innerH=H-padT-padB;
      const seriesMax = Math.max.apply(null, data), minV = seriesMax*0.80, maxV = seriesMax*1.08;
      const x = i => padL + innerW*(i/(data.length-1));
      const y = v => padT + innerH*(1-(v-minV)/(maxV-minV));
      const pts = data.map((v,i)=>x(i)+','+y(v)).join(' ');
      const areaPts = pts+' '+x(data.length-1)+','+(padT+innerH)+' '+x(0)+','+(padT+innerH);
      const color = cssVar('--primary');
      const dots = data.map((v,i)=>{ const isEnd=(i===0||i===data.length-1);
        return `<circle class="chart-dot" data-i="${i}" cx="${x(i)}" cy="${y(v)}" r="${isEnd?7:5}" fill="${color}" ${isEnd?'':'fill-opacity="0.55"'} stroke="${cssVar('--panel')}" stroke-width="1.5"/>`; }).join('');
      svg.innerHTML = `<defs><linearGradient id="grad-savings" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${color}" stop-opacity="0.28"/><stop offset="100%" stop-color="${color}" stop-opacity="0.02"/></linearGradient></defs>`+
        vrfChartGrid(W,H,padL,padR,padT,innerH)+`<polygon points="${areaPts}" fill="url(#grad-savings)"/>`+
        `<polyline points="${pts}" fill="none" stroke="${color}" stroke-width="2.5"/>`+
        `<text x="${x(0)}" y="${y(data[0])-12}" font-size="12" font-weight="600" fill="${cssVar('--ink')}" text-anchor="start">${fmtRs(data[0])}</text>`+
        `<text x="${x(data.length-1)}" y="${y(data[data.length-1])-12}" font-size="12" font-weight="600" fill="${cssVar('--ink')}" text-anchor="end">${fmtRs(data[data.length-1])}</text>`+
        vrfChartTicks(x,H) + dots;
      document.getElementById('line-caption').textContent = `Total Savings Collected: ${fmtRs(data[0])} now → ${fmtRs(data[data.length-1])} projected by 31 March 2027.`;
      bindVrfDotTooltips(svg, dot=>{ const i=+dot.dataset.i; return MONTH_LABELS[i]+': '+fmtRs(data[i]); });
    }
    function drawVrfScenarioLine(family){
      const svg = document.getElementById('line-chart');
      const f = DATA.vrf.forecast;
      const suffixes = f.has_idle_eligible ? ['s1','s2','s3'] : ['s1','s3'];
      const seriesKey = family==='interest' ? 'interest_monthly' : 'corpus_monthly';
      const scenarioLabel = {s1:'Current Trend', s2:'If Idle VOs Start Lending', s3:'If Fully Utilized'};
      const scenarioColor = {s1: cssVar('--ink-soft'), s2: cssVar('--gold'), s3: cssVar('--primary')};
      let allValues = []; suffixes.forEach(s=> allValues = allValues.concat(f[s][seriesKey]));
      const W=640,H=240,padL=76,padR=92,padT=26,padB=34, innerW=W-padL-padR, innerH=H-padT-padB;
      const minV = 0, maxV = Math.max.apply(null, allValues) * 1.06 || 1;
      const x = i => padL + innerW*(i/12);
      const y = v => padT + innerH*(1-(v-minV)/(maxV-minV));
      let linesHtml=''; const endLabelInfo=[];
      suffixes.forEach(s=>{
        const data = f[s][seriesKey]; const color = scenarioColor[s];
        const pts = data.map((v,i)=>x(i)+','+y(v)).join(' ');
        const dots = data.map((v,i)=>{ const isEnd=(i===0||i===12);
          return `<circle class="chart-dot" data-i="${i}" data-suffix="${s}" cx="${x(i)}" cy="${y(v)}" r="${isEnd?6:4}" fill="${color}" ${isEnd?'':'fill-opacity="0.5"'} stroke="${cssVar('--panel')}" stroke-width="1.5"/>`; }).join('');
        linesHtml += `<polyline points="${pts}" fill="none" stroke="${color}" stroke-width="2.5"/>`+dots;
        endLabelInfo.push({y:y(data[12]), color, text: fmtRs(data[12])});
      });
      endLabelInfo.sort((a,b)=>a.y-b.y);
      for(let i=1;i<endLabelInfo.length;i++){ if(endLabelInfo[i].y-endLabelInfo[i-1].y<16) endLabelInfo[i].y = endLabelInfo[i-1].y+16; }
      const labelX = W-padR+10;
      const labelsHtml = endLabelInfo.map(l=>`<text x="${labelX}" y="${l.y+4}" font-size="11.5" font-weight="700" fill="${l.color}" text-anchor="start">${l.text}</text>`).join('');
      svg.innerHTML = vrfChartGrid(W,H,padL,padR,padT,innerH) + linesHtml + labelsHtml + vrfChartTicks(x,H);
      const legendEl = document.getElementById('chart-legend'); legendEl.style.display='flex';
      legendEl.innerHTML = suffixes.map(s=>`<div class="chart-legend-item"><span class="chart-legend-swatch" style="background:${scenarioColor[s]}"></span><span class="chart-legend-text"><b>${scenarioLabel[s]}</b></span></div>`).join('');
      document.getElementById('line-caption').textContent = '';
      bindVrfDotTooltips(svg, dot=>{ const i=+dot.dataset.i, s=dot.dataset.suffix;
        return scenarioLabel[s]+' · '+MONTH_LABELS[i]+': '+fmtRs(f[s][seriesKey][i]); });
    }
    function drawVrfLineChart(key){ if(key==='interest'||key==='corpus') drawVrfScenarioLine(key); else drawVrfSingleLine(key); }

    function renderVRF(sub){
      if(!DATA.vrf.found){
        document.getElementById('panel-vrf').innerHTML = contextBox('vrf') + `<section><div class="panel"><p class="disclaimer">${ERR_MSG_JS.vrf}</p></div></section>`;
        return;
      }
      const fn = {kpi: renderVrfKpi, vobreak: renderVrfVO, bkrank: renderVrfBK, forecasts: renderVrfForecasts}[sub];
      document.getElementById('panel-vrf').innerHTML = contextBox('vrf') + fn();
      if(sub==='kpi'){
        const v = DATA.vrf;
        const savPct = v.expected_savings ? Math.min(v.total_savings/v.expected_savings,1) : 0;
        drawRing('ring-savings', savPct, cssVar('--primary'), cssVar('--low'));
        const tot = v.total_received+v.total_savings+v.total_interest;
        drawDonut('ring-composition', [{frac:v.total_received/tot,color:cssVar('--gold'),tip:'Government Grant: '+fmtRs(v.total_received)},
          {frac:v.total_savings/tot,color:cssVar('--primary'),tip:'Savings: '+fmtRs(v.total_savings)},
          {frac:v.total_interest/tot,color:cssVar('--ink'),tip:'Interest: '+fmtRs(v.total_interest)}]);
        drawPictogram('pict-coverage', [{n:v.n_vo-v.incomplete_coverage,cls:'good'},{n:v.incomplete_coverage,cls:'flag'}]);
        drawPictogram('pict-interest', [{n:v.received_vo-v.idle_vo,cls:'good'},{n:v.idle_vo,cls:'flag'},{n:v.n_vo-v.received_vo,cls:'na'}]);
      }
      if(sub==='vobreak'){
        makeSortableTable('vo-table-container', voTableCols(), DATA.vrf.vo_table, voTableRow);
      }
      if(sub==='bkrank'){
        makeSortableTable('bk-table-container', bkTableCols(), DATA.vrf.bk_ranking, bkTableRow);
      }
      if(sub==='forecasts'){
        drawVrfLineChart('savings');
        document.querySelectorAll('.chart-tab').forEach(btn=>{
          btn.addEventListener('click', ()=>{
            document.querySelectorAll('.chart-tab').forEach(b=>b.classList.remove('active'));
            btn.classList.add('active'); drawVrfLineChart(btn.dataset.series);
          });
        });
      }
    }
    """

    JS_VPRP = r"""
    let vprpYear = 2025;
    let vprpGp = 'ALL';
    const PGSRD_COLORS = ['var(--primary)','var(--gold)','#5B8AA6'];
    const SDP_COLORS = ['var(--primary)','var(--gold)','#5B8AA6','var(--low)','var(--grey)'];
    function currentVprpYearData(){
      const yearObj = DATA.vprp.years[vprpYear];
      if(!yearObj) return yearObj;
      if(vprpGp !== 'ALL' && yearObj.by_gp && yearObj.by_gp[vprpGp]) return yearObj.by_gp[vprpGp];
      return yearObj;
    }
    function vprpGpNamesCell(gpNames){
      if(!gpNames || !gpNames.length) return '—';
      if(gpNames.length <= 2) return gpNames.join(', ');
      return `<span class="tip" data-tip="${gpNames.join(', ')}">${gpNames.length} GPs</span>`;
    }
    function yearDropdown(){
      const opts = [2023,2024,2025].map(y=>`<option value="${y}" ${y===vprpYear?'selected':''}>${y}</option>`).join('');
      const yearObj = DATA.vprp.years[vprpYear];
      const gpList = (yearObj && yearObj.gp_list) || [];
      const gpBlock = gpList.length ? `<label for="gp-select" style="margin-left:16px;">GP:</label>
        <select id="gp-select"><option value="ALL" ${vprpGp==='ALL'?'selected':''}>All GPs</option>${gpList.map(gp=>`<option value="${gp}" ${gp===vprpGp?'selected':''}>${gp}</option>`).join('')}</select>` : '';
      return `<div class="selectbar"><label for="yr-select">Year:</label><select id="yr-select">${opts}</select>${gpBlock}</div>`;
    }
    function emptyYearNote(domainKey){
      const msg = (ERR_MSG_JS[domainKey] || 'We could not locate data for your CLF in {year}.').replace('{year}', vprpYear);
      return `<p class="disclaimer">${msg}</p>`;
    }
    function renderVprpEnt(){
      const y = currentVprpYearData();
      if(!y || !y.n_demands) return emptyYearNote('vprp_ent');
      const showGp = CURRENT_VIEW === 'clf'; // GP breakdown only makes sense at CLF granularity
      const accessedNote = vprpYear===2025 ? ` — this year is recent, so accessed status may not be fully updated yet` : '';
      const schemeRows = (y.by_scheme||[]).map(s=>{
        const tds = `<td>${s.scheme}</td><td class="num">${fmtNum(s.demanded)}</td><td class="num">${fmtNum(s.n_vo)}</td>${showGp?`<td>${vprpGpNamesCell(s.gp_names)}</td>`:''}`;
        let extra = '';
        if(s.raw_scheme==='state-specific' && y.state_scheme_breakdown){
          // Sibling <tr>s in the SAME table (not a nested <table> in a merged
          // cell) so the sub-rows share the parent's actual column grid - a
          // nested table lays its own columns out independently and doesn't
          // know where "Demanded" sits in the outer table.
          extra = Object.entries(y.state_scheme_breakdown).map(([k,v])=>
            `<tr><td style="padding-left:28px;color:var(--ink-soft);">${k}</td><td class="num">${fmtNum(v.demanded)}</td><td class="num">${fmtNum(v.n_vo)}</td>${showGp?`<td>${vprpGpNamesCell(v.gp_names)}</td>`:''}</tr>`).join('');
        }
        return `<tr>${tds}</tr>${extra}`;
      }).join('');
      return `<section><div class="section-head"><h2 class="serif">Entitlements</h2></div>
        <div class="panel">
          <div class="tiles n3" style="margin-bottom:16px;">
            ${tile('Number of People Requesting', fmtNum(y.n_demands))}
            ${tile('Number of VOs Requesting', fmtNum(y.n_vo_requesting_ent))}
            ${tile('Number of Schemes Requested', fmtNum(y.n_schemes_total), 'distinct scheme types, incl. NREGA')}
          </div>
          ${y.by_scheme ? `<p class="hint tip" style="margin-bottom:8px;" data-tip="${TIPS['Demand & Access by Scheme']}"><b>Demand by Scheme</b>${accessedNote}</p>
            <div class="table-wrap"><table><thead><tr><th>Scheme</th><th class="num">Demanded</th><th class="num">VOs</th>${showGp?'<th>GPs</th>':''}</tr></thead><tbody>${schemeRows}</tbody></table></div>` : ''}
        </div></section>`;
    }
    function renderVprpPgsrd(){
      const y = currentVprpYearData();
      if(!y || !y.n_pgsrd) return emptyYearNote('vprp_pgsrd');
      const showGp = CURRENT_VIEW === 'clf';
      const items = (y.pgsrd_items||[]).map(it=>{
        const row = [it.item_demanded, it.pgsrd_type, fmtNum(it.n), fmtNum(Math.round(it.units)), fmtNum(it.n_vo)];
        if(showGp) row.push(vprpGpNamesCell(it.gp_names));
        return row;
      });
      const itemCols = [{label:'Item'},{label:'Type'},{label:'# Requests',num:true},{label:'Total Units',num:true},{label:'VOs',num:true}];
      if(showGp) itemCols.push({label:'GPs'});
      const typeDonut = y.pgsrd_type_split ? donutBlock('ring-pgsrd-type', y.pgsrd_type_split, PGSRD_COLORS, {size:150, fmt:v=>v+'%'}) : null;
      return `<section><div class="section-head"><h2 class="serif">Public Goods, Services, and Resource Development</h2></div>
        <div class="panel">
          <div class="tiles n2" style="margin-bottom:16px;">${tile('Total PGSRD Requests', fmtNum(y.n_pgsrd))}${tile('Number of VOs Requesting', fmtNum(y.n_vo_requesting_pgsrd))}</div>
          ${typeDonut?`<p class="hint tip" data-tip="${TIPS['Type of Request']}"><b>Type of Request</b></p>${typeDonut.html}`:''}
          ${items.length?`<p class="hint tip" style="margin-top:18px;" data-tip="${TIPS['Most-Requested Items']}"><b>Most-Requested Items</b></p>${tableHtml(itemCols, items)}`:''}
          ${y.sdg_theme?`<p class="hint tip" style="margin-top:18px;" data-tip="${TIPS['By Development Theme']}"><b>By Development Theme</b></p><div class="pills" style="margin-bottom:8px;">${Object.entries(y.sdg_theme).map(([k,v])=>`<span class="pill on">${k} (${fmtNum(v)})</span>`).join('')}</div>`:''}
          ${y.gpdp_area?`<p class="hint tip" style="margin-top:14px;" data-tip="${TIPS['By GPDP Focus Area']}"><b>By GPDP Focus Area</b></p><div class="pills">${Object.entries(y.gpdp_area).map(([k,v])=>`<span class="pill on">${k} (${fmtNum(v)})</span>`).join('')}</div>`:''}
        </div></section>`;
    }
    function renderVprpSdp(){
      const y = currentVprpYearData();
      if(!y || !y.n_sdp) return emptyYearNote('vprp_sdp');
      const showGp = CURRENT_VIEW === 'clf';
      const issues = (y.sdp_issues||[]).map(it=>{
        const row = [it.social_issue, fmtNum(it.n), it.affected!==null?fmtNum(it.affected):'not reported', fmtNum(it.n_vo)];
        if(showGp) row.push(vprpGpNamesCell(it.gp_names));
        return row;
      });
      const issueCols = [{label:'Issue'},{label:'# Occurrences',num:true},{label:'People Affected',num:true},{label:'VOs',num:true}];
      if(showGp) issueCols.push({label:'GPs'});
      const sectorDonut = y.sdp_sector ? donutBlock('ring-sdp-sector', y.sdp_sector, SDP_COLORS, {size:150, fmt:v=>v+'%'}) : null;
      return `<section><div class="section-head"><h2 class="serif">Social Development Plan</h2></div>
        <div class="panel">
          <div class="tiles n3" style="margin-bottom:16px;">
            ${tile('Total Social Issues Raised', fmtNum(y.n_sdp))}
            ${tile('Number of VOs', fmtNum(y.n_vo))}
            ${tile('Government Departments Involved', fmtNum(y.n_departments))}
          </div>
          ${sectorDonut?`<p class="hint tip" data-tip="${TIPS['By Sector']}"><b>By Sector</b></p>${sectorDonut.html}`:''}
          ${issues.length?`<p class="hint tip" style="margin-top:18px;" data-tip="${TIPS['Most-Raised Issues']}"><b>Most-Raised Issues</b></p>${tableHtml(issueCols, issues)}`:''}
          ${y.departments?`<p class="hint tip" style="margin-top:18px;" data-tip="${TIPS['Government Departments Involved']}"><b>Government Departments Involved</b></p><div class="pills">${Object.entries(y.departments).map(([k,v])=>`<span class="pill on">${k} (${fmtNum(v)})</span>`).join('')}</div>`:''}
        </div></section>`;
    }
    function renderVPRP(sub){
      const body = sub==='entitlements' ? renderVprpEnt() : sub==='pgsrd' ? renderVprpPgsrd() : renderVprpSdp();
      document.getElementById('panel-vprp').innerHTML = contextBox('vprp') + yearDropdown() + body;
      document.getElementById('yr-select').addEventListener('change', e=>{ vprpYear=+e.target.value; vprpGp='ALL'; renderVPRP(sub); });
      const gpSel = document.getElementById('gp-select');
      if(gpSel) gpSel.addEventListener('change', e=>{ vprpGp=e.target.value; renderVPRP(sub); });
      const y = currentVprpYearData();
      if(sub==='pgsrd' && y && y.pgsrd_type_split) donutBlock('ring-pgsrd-type', y.pgsrd_type_split, PGSRD_COLORS, {size:150, fmt:v=>v+'%'}).draw();
      if(sub==='sdp' && y && y.sdp_sector) donutBlock('ring-sdp-sector', y.sdp_sector, SDP_COLORS, {size:150, fmt:v=>v+'%'}).draw();
    }
    """

    # Loans tab - CLF-level prototype only (not wired into District/State yet).
    # Sourced from clf_loan_tracker.dta (1_Code/clf_loan_cleaning.do), currently
    # Araria district only; every other CLF's json simply has no "loans" key,
    # which renderLoans() treats the same as DATA.loans.found===false.
    JS_LOANS = r"""
    function fmtLoanDate(d){ return d ? new Date(d).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'}) : '—'; }
    function loanStatusPill(status){
      const cls = status==='Active' ? 'status-active' : status==='Closed' ? 'status-closed' : 'status-none';
      return `<span class="pill-sm ${cls}">${status}</span>`;
    }
    function monthStatusPill(status){
      const cls = status==='Paid' ? 'status-active' : status==='Not Paid' ? 'status-negative' : 'status-none';
      return `<span class="pill-sm ${cls}">${status}</span>`;
    }
    function LOAN_DETAIL_COLS(){
      return [
        {label:'VO Name', key:'vo_name'},
        {label:'Loan No.', num:true, key:'loan_no'},
        {label:'Fund Source', key:'fund_source'},
        {label:'Type', key:'loan_type'},
        {label:'Amount Disbursed', num:true, key:'loan_amount'},
        {label:'Current Outstanding', num:true, key:'current_outstanding'},
        {label:'Repaid to Date', num:true, key:'cumulative_amount_repaid'},
        {label:'Last Repayment', key:'last_repayment_date'},
        {label:'Status', key:'status'},
        {label:'Arrears', num:true, key:'arrears', tip:TIPS['Loans in Arrears']},
      ];
    }
    function loanDetailRow(r){
      return [
        r.vo_name, r.loan_no,
        r.fund_source ? `<span class="pill-sm">${r.fund_source}</span>` : '—',
        r.loan_type ? `<span class="pill-sm">${r.loan_type}</span>` : '—',
        fmtRs(r.loan_amount), fmtRs(r.current_outstanding), fmtRs(r.cumulative_amount_repaid),
        fmtLoanDate(r.last_repayment_date),
        loanStatusPill(r.status),
        r.arrears>0 ? `<span class="pill-sm status-negative">${fmtRs(r.arrears)}</span>` : (r.arrears==null?'—':fmtRs(r.arrears)),
      ];
    }
    const LOAN_FUND_COLORS = ['var(--primary)','var(--gold)','#5B8AA6','var(--low)','var(--grey)'];
    const LOAN_TYPE_COLORS = ['var(--primary)','var(--gold)'];
    const LOAN_STATUS_COLORS = ['var(--primary)','var(--gold)','var(--grey)'];
    // Same vertical-bar style as drawGradeHistogram, generalised (no fixed grade
    // colour map, X axis is loan-count buckets 0..5+ rather than letter grades).
    function drawVoLoanHistogram(svgId, hist){
      const svg = document.getElementById(svgId); if(!svg || !hist.length) return;
      const W=460,H=220,padL=28,padR=20,padT=24,padB=30, innerW=W-padL-padR, innerH=H-padT-padB;
      const maxPct = Math.max.apply(null, hist.map(h=>h.pct).concat([1]));
      const slot = innerW/hist.length, barW = Math.min(slot*0.5, 46);
      let bars='', labels='', valueLabels='';
      hist.forEach((h,i)=>{
        const hgt = (h.pct/maxPct)*innerH;
        const x = padL + i*slot + (slot-barW)/2;
        const y = padT + innerH - hgt;
        const n = h.n_loans;
        bars += `<rect class="tip" data-tip="${n} loan${n===1?'':'s'}: ${fmtNum(h.n_vo)} VOs (${h.pct}%)" x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barW.toFixed(1)}" height="${hgt.toFixed(1)}" fill="${cssVar('--primary')}" rx="3"/>`;
        valueLabels += `<text x="${(x+barW/2).toFixed(1)}" y="${(y-6).toFixed(1)}" font-size="9" font-weight="700" text-anchor="middle" fill="${cssVar('--ink')}">${h.pct}%</text>`;
        labels += `<text x="${(x+barW/2).toFixed(1)}" y="${H-10}" font-size="12" font-weight="600" text-anchor="middle" fill="${cssVar('--ink-soft')}">${n}</text>`;
      });
      const axis = `<line x1="${padL}" y1="${padT+innerH}" x2="${W-padR}" y2="${padT+innerH}" stroke="${cssVar('--line')}" stroke-width="1"/>`;
      svg.innerHTML = axis + bars + valueLabels + labels;
    }
    function renderLoansOverview(){
      const l = DATA.loans;
      const turnoverCls = l.kpi.active_lending_turnover===0 ? 'neg' : 'neutral';
      const turnoverVal = l.kpi.active_lending_turnover!=null ? l.kpi.active_lending_turnover.toFixed(2)+'×' : null;
      const totalXirrVal = l.kpi.total_xirr!=null ? (l.kpi.total_xirr>=0?'+':'')+l.kpi.total_xirr.toFixed(1)+'%' : null;
      const fundDonut = Object.keys(l.fund_source_mix).length ? donutBlock('ring-loan-fund', l.fund_source_mix, LOAN_FUND_COLORS, {size:150}) : null;
      const typeDonut = Object.keys(l.loan_type_mix).length ? donutBlock('ring-loan-type', l.loan_type_mix, LOAN_TYPE_COLORS, {size:150, fmt:v=>fmtNum(v)+' loans'}) : null;
      const statusDonut = Object.keys(l.loan_status_mix).length ? donutBlock('ring-loan-status', l.loan_status_mix, LOAN_STATUS_COLORS, {size:150, fmt:v=>fmtNum(v)+' loans'}) : null;
      const neverBorrowed = l.kpi.n_vo_never_borrowed;
      const neverBorrowedLine = neverBorrowed>0
        ? `<p class="callout-attn" style="margin-top:10px;">${fmtNum(neverBorrowed)} VO${neverBorrowed===1?'':'s'} in your CLF ${neverBorrowed===1?'has':'have'} not taken a loan.</p>`
        : (neverBorrowed===0 ? `<p class="callout-good" style="margin-top:10px;">All VOs in your CLF have taken a loan.</p>` : '');
      return `<section><div class="section-head"><h2 class="serif">Loan Portfolio</h2></div>
        <div class="panel"><div class="tiles n3">
          ${tile('Active Loans', fmtNum(l.kpi.n_active_loans))}
          ${tile('Total Disbursed', fmtRs(l.kpi.total_disbursed))}
          ${tile('Current Outstanding', fmtRs(l.kpi.total_outstanding))}
        </div></div></section>
        <section><div class="section-head"><h2 class="serif">Repayment and Returns</h2></div>
        <div class="panel"><div class="tiles n3">
          ${tile('Repayment Rate', fmtPct(l.kpi.repayment_rate,1))}
          ${tile('Active Lending Turnover', turnoverVal, null, turnoverCls)}
          ${tile('Total XIRR', totalXirrVal, null, totalXirrVal && l.kpi.total_xirr<0 ? 'neg' : 'neutral')}
        </div></div></section>
        <section><div class="section-head"><h2 class="serif">Portfolio Composition</h2></div>
          <div class="panel"><div class="health-grid">
            <div class="health-card"><h3 class="tip" data-tip="${TIPS['Fund Source']||''}">By Fund Source</h3>${fundDonut?fundDonut.html:'<p class="disclaimer">No data.</p>'}</div>
            <div class="health-card"><h3 class="tip" data-tip="${TIPS['Type']||''}">By Repayment Type</h3>${typeDonut?typeDonut.html:'<p class="disclaimer">No data.</p>'}</div>
            <div class="health-card"><h3 class="tip" data-tip="${TIPS['Status']||''}">By Loan Status</h3>${statusDonut?statusDonut.html:'<p class="disclaimer">No data.</p>'}</div>
            <div class="health-card"><h3 class="tip" data-tip="${TIPS['VO Loan Count Distribution']||''}">VOs by Loan Count</h3><svg id="ring-vo-loan-hist" viewBox="0 0 460 220" style="width:100%;max-width:460px;height:auto;"></svg>${neverBorrowedLine}</div>
          </div></div></section>
        ${l.loan_table ? `<section><div class="section-head"><h2 class="serif">All Loans</h2><span class="hint">click a column to sort</span></div>
          <div class="panel"><div id="loan-detail-table-container"></div></div></section>` : ''}`;
    }
    let loanScheduleVo = '';
    let loanScheduleLoanNo = '';
    function loanScheduleCols(){
      return [
        {label:'Month'}, {label:'Amount Due', num:true}, {label:'Amount Repaid', num:true},
        {label:'Status', tip:'Whether that month\'s scheduled installment was fully paid, partially paid, or not paid at all, based on real repayment transactions recorded in that calendar month.'},
        {label:'Arrear', num:true}, {label:'Outstanding', num:true},
      ];
    }
    function loanScheduleRows(rows){
      return rows.map(r=>[
        r.month, fmtRs(r.amount_due), fmtRs(r.amount_repaid), monthStatusPill(r.status),
        r.arrear!=null ? fmtRs(r.arrear) : '—', fmtRs(r.outstanding),
      ]);
    }
    function renderLoansSchedule(){
      const l = DATA.loans;
      const voOpts = l.vo_options.map(v=>`<option value="${v.vo_code}" ${v.vo_code===loanScheduleVo?'selected':''}>${v.vo_name}</option>`).join('');
      const loanOpts = (loanScheduleVo && l.loan_options[loanScheduleVo]) ? l.loan_options[loanScheduleVo].map(o=>`<option value="${o.loan_no}" ${String(o.loan_no)===loanScheduleLoanNo?'selected':''}>${o.label}</option>`).join('') : '';
      let tableSection;
      const selectedLoanOpt = (loanScheduleVo && loanScheduleLoanNo && l.loan_options[loanScheduleVo]) ? l.loan_options[loanScheduleVo].find(o=>String(o.loan_no)===loanScheduleLoanNo) : null;
      const demandLine = selectedLoanOpt ? `<div class="tiles n2" style="margin-bottom:16px;">
          ${tile('Current Demand', fmtRs(selectedLoanOpt.current_demand))}
          ${tile('Arrears', fmtRs(selectedLoanOpt.arrears), null, selectedLoanOpt.arrears>0?'neg':'neutral')}
        </div>` : '';
      const sched = (loanScheduleVo && loanScheduleLoanNo && l.schedules[loanScheduleVo]) ? l.schedules[loanScheduleVo][loanScheduleLoanNo] : null;
      if(sched){
        if(!sched.reliable){
          tableSection = `<p class="disclaimer">This loan's raw LokOS schedule shows signs of being restructured or rescheduled, so a from-scratch monthly reconstruction would not reliably match what actually happened - not shown for this loan.</p>`;
        } else if(!sched.rows.length){
          tableSection = `<p class="disclaimer">No months fall within our repayment observation window (${l.schedule_window.start} – ${l.schedule_window.end}) for this loan.</p>`;
        } else {
          tableSection = tableHtml(loanScheduleCols(), loanScheduleRows(sched.rows));
        }
      } else if(loanScheduleVo){
        tableSection = `<p class="disclaimer">Select a loan number to see its month-wise repayment schedule.</p>`;
      } else {
        tableSection = `<p class="disclaimer">Select a Village Organisation and loan number to see its month-wise repayment schedule.</p>`;
      }
      return `<section><div class="section-head"><h2 class="serif">Loan Schedule</h2><span class="hint">shown for ${l.schedule_window.start} – ${l.schedule_window.end}, the window we have real repayment records for</span></div>
        <div class="panel">
          <div class="selectbar">
            <label for="loan-vo-select">Village Organisation:</label>
            <select id="loan-vo-select"><option value="">Select a VO…</option>${voOpts}</select>
            <label for="loan-no-select">Loan Number:</label>
            <select id="loan-no-select" ${loanScheduleVo?'':'disabled'}><option value="">Select a loan…</option>${loanOpts}</select>
          </div>
          ${demandLine}
          ${tableSection}
        </div></section>`;
    }
    function wireLoanScheduleDropdowns(){
      document.getElementById('loan-vo-select').addEventListener('change', e=>{
        loanScheduleVo = e.target.value; loanScheduleLoanNo = ''; renderLoans('schedule');
      });
      const loanSel = document.getElementById('loan-no-select');
      if(loanSel) loanSel.addEventListener('change', e=>{
        loanScheduleLoanNo = e.target.value; renderLoans('schedule');
      });
    }
    function renderLoans(sub){
      if(!DATA.loans || !DATA.loans.found){
        document.getElementById('panel-loans').innerHTML = contextBox('loans') + `<section><div class="panel"><p class="disclaimer">${ERR_MSG_JS.loans}</p></div></section>`;
        return;
      }
      const body = sub==='schedule' ? renderLoansSchedule() : renderLoansOverview();
      document.getElementById('panel-loans').innerHTML = contextBox('loans') + body;
      if(sub==='schedule'){
        wireLoanScheduleDropdowns();
      } else {
        const l = DATA.loans;
        if(Object.keys(l.fund_source_mix).length) donutBlock('ring-loan-fund', l.fund_source_mix, LOAN_FUND_COLORS, {size:150}).draw();
        if(Object.keys(l.loan_type_mix).length) donutBlock('ring-loan-type', l.loan_type_mix, LOAN_TYPE_COLORS, {size:150, fmt:v=>fmtNum(v)+' loans'}).draw();
        if(Object.keys(l.loan_status_mix).length) donutBlock('ring-loan-status', l.loan_status_mix, LOAN_STATUS_COLORS, {size:150, fmt:v=>fmtNum(v)+' loans'}).draw();
        drawVoLoanHistogram('ring-vo-loan-hist', l.vo_loan_histogram);
        if(l.loan_table) makeSortableTable('loan-detail-table-container', LOAN_DETAIL_COLS(), l.loan_table, loanDetailRow);
      }
    }
    // ---- District/State: Portfolio Overview only (no per-loan schedule, no full
    // "All Loans" table - both drop at group scope, same reasoning as VRF's
    // VO-Level Breakdown being CLF-only). Reuses renderLoans('overview') UNCHANGED
    // via GROUP_PSEUDO_CLF, same trick as renderGroupFinancial/renderGroupVRF. ----
    function renderGroupLoans(sub){
      if(!GROUP_DATA.loans.found){
        document.getElementById('panel-loans').innerHTML = contextBox('loans') + `<section><div class="panel"><p class="disclaimer">${ERR_MSG_JS.loans}</p></div></section>`;
        return;
      }
      DATA = GROUP_PSEUDO_CLF;
      const scope = isState() ? 'statewide' : `in ${GROUP_DATA.name}`;
      const note = `<p class="hint" style="margin-bottom:14px;">Aggregated across ${fmtNum(GROUP_DATA.loans.n_clfs_with_loans)} of ${fmtNum(GROUP_DATA.loans.n_total)} CLFs ${scope} with loan data.</p>`;
      const groupNoun = isState() ? 'Bihar' : `${GROUP_DATA.name} district`;
      const body = renderLoansOverview().replace(/in your CLF/g, isState() ? 'in Bihar' : `in ${groupNoun}`);
      document.getElementById('panel-loans').innerHTML = contextBox('loans') + note + body;
      const l = DATA.loans;
      if(Object.keys(l.fund_source_mix).length) donutBlock('ring-loan-fund', l.fund_source_mix, LOAN_FUND_COLORS, {size:150}).draw();
      if(Object.keys(l.loan_type_mix).length) donutBlock('ring-loan-type', l.loan_type_mix, LOAN_TYPE_COLORS, {size:150, fmt:v=>fmtNum(v)+' loans'}).draw();
      if(Object.keys(l.loan_status_mix).length) donutBlock('ring-loan-status', l.loan_status_mix, LOAN_STATUS_COLORS, {size:150, fmt:v=>fmtNum(v)+' loans'}).draw();
      drawVoLoanHistogram('ring-vo-loan-hist', l.vo_loan_histogram);
    }
    """

    JS_SCORING = r"""
    let scoreQtrIdx = 0;
    function currentScoring(){ return DATA.scoring.by_quarter[DATA.scoring.quarters[scoreQtrIdx]]; }
    function renderScoringOverall(){
      const s = currentScoring();
      const cats = Object.entries(s.categories);
      const worst = cats.filter(([k,v])=> v.score!==null && v.score<50);
      const attnSentence = worst.length ? `<p class="callout-attn">Your CLF needs to pay the most attention to ${catListPhrase(worst.map(([k])=>k))}.</p>` : '';
      return `<section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Overall Score']}">Overall Score</h2><span class="hint">equal weight across all 7 categories by default - customize below</span></div>
        <div class="panel">
          <div style="display:flex;justify-content:flex-end;margin-bottom:14px;"><button id="btn-open-weights" class="weight-btn">&#9881;&#65039; Choose Category Weights</button></div>
          <div class="tiles n1" style="margin-bottom:16px;">${tile('Overall Score', s.overall_score+' / 100', null, 'big-value')}</div>
          <div class="standing-grid">
            ${standingCard('district', s.overall_district_score, s.overall_district_rank, s.overall_n_district, 'Overall Score', 'Standing vs. District')}
            ${standingCard('state', s.overall_score, s.overall_state_rank, s.overall_n_state, 'Overall Score', 'Standing vs. State')}
          </div></div></section>
      <section><div class="section-head"><h2 class="serif">Category Summary</h2></div>
        <div class="panel">
          ${attnSentence}
          <div style="margin-top:${worst.length?'16px':'0'};">${cats.map(([k,v])=>`
            <div style="margin-bottom:18px;">
              ${scorePercent(k, v.score!==null?v.score:0)}
              ${pctlRow('', {district_pctl: v.district_score, state_pctl: v.score, district_rank: v.district_rank, state_rank: v.state_rank, n_district: v.n_district, n_state: v.n_state})}
            </div>`).join('')}</div>
        </div></section>`;
    }
    // ---- Data Coverage detail: the underlying per-source checklist behind the
    // "Data Sources Available" metric in the Data Coverage category (By Category
    // tab) - the category score itself IS scored/weighted like any other (a
    // percentile on count of sources available), this is just the human-readable
    // breakdown of what that count is made of. ----
    function renderDataCoverage(){
      const da = DATA.data_availability;
      if(!da) return '';
      return `<p class="hint" style="margin:10px 0 8px;">${da.n_available} of ${da.n_total} data sources found for your CLF.</p>
        <div class="pills">${da.sources.map(s=>`<span class="pill ${s.available?'on':''}">${s.label}</span>`).join('')}</div>`;
    }
    function renderScoringByCategory(){
      const s = currentScoring();
      const cats = Object.entries(s.categories);
      // bottom-half here means individual metrics/items below the 50th state
      // percentile, not whole categories - a category's own composite score can
      // clear 50 while still containing one or two genuinely weak metrics inside it.
      const allMetrics = cats.flatMap(([catName,cat])=>cat.metrics.map(([label,m])=>({label,m})));
      const bottomHalf = allMetrics.filter(({m})=> m && m.state_pctl!==null && m.state_pctl!==undefined && m.state_pctl<50);
      const topSentence = bottomHalf.length ? `<p class="callout-attn" style="margin:0 0 16px;">Your CLF falls in the bottom-half of performance for ${listPhrase(bottomHalf.map(({label})=>label))}.</p>` : '';
      return topSentence + cats.map(([catName,cat])=>`
        <section><div class="section-head"><h2 class="serif">${catName}</h2><span class="hint"><b>Score: ${cat.score!==null?cat.score+'/100':ERR_MSG_JS.not_found}</b></span></div>
          <div class="panel">${cat.metrics.map(([label,m])=>pctlRow(label, m)).join('')}${catName==='Data Coverage'?renderDataCoverage():''}</div></section>`).join('');
    }
    // ============================================================================
    // Customizable category weights. The recomputed score itself needs only this
    // CLF's own category scores (already in DATA); a rank against other CLFs
    // needs everyone else's category scores too, which don't exist anywhere in
    // this CLF's own JSON - data/scoring_summary.json (a compact, statewide,
    // category-scores-only file) is fetched once, lazily, on first open, and
    // cached like every other fetch in this app.
    // ============================================================================
    const WEIGHT_CATS = ['Financial Health', 'Fund Utilization', 'Loan Portfolio', 'VRF Fund Health', 'Governance & Compliance', 'Welfare and Livelihood', 'Data Coverage'];
    let categoryWeights = null;
    let scoringSummaryCache = null;

    function currentCategoryScores(){
      const s = currentScoring();
      const out = {};
      WEIGHT_CATS.forEach(cat=>{ out[cat] = (s.categories[cat] && s.categories[cat].score!=null) ? s.categories[cat].score : null; });
      return out;
    }
    function computeWeightedScore(catScores, weights){
      let num = 0, den = 0;
      WEIGHT_CATS.forEach(cat=>{
        if(catScores[cat] != null){ num += weights[cat] * catScores[cat]; den += weights[cat]; }
      });
      return den > 0 ? num/den : null;
    }
    async function ensureScoringSummary(){
      if(!scoringSummaryCache) scoringSummaryCache = await fetchJson('data/scoring_summary.json');
      return scoringSummaryCache;
    }
    function weightModalHtml(){
      return `<div class="weight-modal-backdrop" id="weight-modal-backdrop">
        <div class="weight-modal">
          <div class="weight-modal-head"><h3>Choose Category Weights</h3><button class="weight-modal-close" id="btn-close-weights">&times;</button></div>
          <p class="weight-modal-hint">Drag each slider to change how much that category counts toward the Overall Score, for the quarter currently selected on this tab. Weights are relative to each other, not fixed percentages - move any slider and the rest adjust automatically.</p>
          <div id="weight-rows"></div>
          <div class="weight-result" id="weight-result"></div>
          <div class="weight-modal-actions">
            <button class="weight-reset-btn" id="btn-reset-weights">Reset to Equal Weights</button>
            <button class="weight-done-btn" id="btn-done-weights">Done</button>
          </div>
        </div>
      </div>`;
    }
    function renderWeightRows(){
      const total = WEIGHT_CATS.reduce((a,c)=>a+categoryWeights[c],0) || 1;
      document.getElementById('weight-rows').innerHTML = WEIGHT_CATS.map(cat=>{
        const pct = Math.round(categoryWeights[cat]/total*100);
        return `<div class="weight-row">
          <div class="weight-row-head"><span class="wname">${cat}</span><span class="wpct">${pct}%</span></div>
          <input type="range" min="0" max="100" value="${categoryWeights[cat]}" class="weight-slider" data-cat="${cat}">
        </div>`;
      }).join('');
      document.querySelectorAll('.weight-slider').forEach(el=>{
        el.addEventListener('input', e=>{ categoryWeights[e.target.dataset.cat] = +e.target.value; renderWeightRows(); renderWeightResult(); });
      });
    }
    let weightRequestId = 0;
    async function renderWeightResult(){
      const myRequestId = ++weightRequestId;
      const catScores = currentCategoryScores();
      const myScore = computeWeightedScore(catScores, categoryWeights);
      const scoreHtml = myScore!=null ? Math.round(myScore)+' / 100' : ERR_MSG_JS.not_found;
      const box = document.getElementById('weight-result');
      box.innerHTML = `
        <div class="weight-result-row"><span class="wlabel">Recomputed Overall Score</span><span class="wval">${scoreHtml}</span></div>
        <div class="weight-result-row"><span class="wlabel">State Rank</span><span class="wval gold">Loading&hellip;</span></div>
        <div class="weight-result-row"><span class="wlabel">District Rank</span><span class="wval gold">Loading&hellip;</span></div>`;
      const summary = await ensureScoringSummary();
      const qLabel = DATA.scoring.quarters[scoreQtrIdx];
      const myId = DATA.overview.mis_id, myDistrict = DATA.overview.district;
      const scored = summary.clfs.map(c=>({
        id: c.id, district: c.district,
        score: computeWeightedScore(c.by_quarter[qLabel] || {}, categoryWeights),
      })).filter(c=>c.score!=null);
      scored.sort((a,b)=>b.score-a.score);
      const stateRank = scored.findIndex(c=>c.id===myId) + 1;
      const distScored = scored.filter(c=>c.district===myDistrict);
      const distRank = distScored.findIndex(c=>c.id===myId) + 1;
      // a newer call (from another slider move) may have started and finished
      // while this fetch/sort was in flight - discard this stale result rather
      // than clobber whatever the newer call already rendered
      if(myRequestId !== weightRequestId) return;
      box.innerHTML = `
        <div class="weight-result-row"><span class="wlabel">Recomputed Overall Score</span><span class="wval">${scoreHtml}</span></div>
        <div class="weight-result-row"><span class="wlabel">State Rank</span><span class="wval gold">${stateRank>0?stateRank+ord(stateRank)+' of '+scored.length:ERR_MSG_JS.not_found}</span></div>
        <div class="weight-result-row"><span class="wlabel">District Rank</span><span class="wval gold">${distRank>0?distRank+ord(distRank)+' of '+distScored.length:ERR_MSG_JS.not_found}</span></div>`;
    }
    function openWeightModal(){
      if(!categoryWeights){
        categoryWeights = {};
        WEIGHT_CATS.forEach(cat=>categoryWeights[cat]=50);
      }
      if(!document.getElementById('weight-modal-backdrop')){
        document.body.insertAdjacentHTML('beforeend', weightModalHtml());
        document.getElementById('btn-close-weights').addEventListener('click', closeWeightModal);
        document.getElementById('btn-done-weights').addEventListener('click', closeWeightModal);
        document.getElementById('btn-reset-weights').addEventListener('click', ()=>{
          WEIGHT_CATS.forEach(cat=>categoryWeights[cat]=50); renderWeightRows(); renderWeightResult();
        });
        document.getElementById('weight-modal-backdrop').addEventListener('click', e=>{
          if(e.target.id==='weight-modal-backdrop') closeWeightModal();
        });
      }
      renderWeightRows(); renderWeightResult();
      document.getElementById('weight-modal-backdrop').classList.add('open');
    }
    function closeWeightModal(){
      const el = document.getElementById('weight-modal-backdrop');
      if(el) el.classList.remove('open');
    }
    function renderScoring(sub){
      const opts = DATA.scoring.quarters.map((label,i)=>`<option value="${i}" ${i===scoreQtrIdx?'selected':''}>${label}</option>`).join('');
      const dropdown = `<div class="selectbar"><label for="score-qtr-select">Quarter:</label><select id="score-qtr-select">${opts}</select></div>
        <p class="note-inline">Fund Deployment, Surplus / Deficit, Bookkeeping Accuracy, every Loan Portfolio, Data Coverage, VRF Fund Health, Governance &amp; Compliance, and Welfare and Livelihood metric reflect current standing and don't change by quarter — only the other Fund Utilization &amp; Financial Health line items update.</p>`;
      document.getElementById('panel-scoring').innerHTML =
        contextBox('scoring') + dropdown + (sub==='overall' ? renderScoringOverall() : renderScoringByCategory());
      document.getElementById('score-qtr-select').addEventListener('change', e=>{ scoreQtrIdx=+e.target.value; renderScoring(sub); });
      if(sub==='overall'){
        document.getElementById('btn-open-weights').addEventListener('click', openWeightModal);
      }
    }
    """

    JS_DISTRICT_STATE = r"""
    let GROUP_DATA = null;      // currently loaded District or State aggregate
    let GROUP_PSEUDO_CLF = null;

    const STATUS_COLORS = ['var(--primary)','var(--gold)','#5B8AA6','var(--low)','var(--grey)'];
    function renderGroupOverviewProfile(){
      const o = GROUP_DATA.overview;
      const statusDonut = donutBlock('ring-group-status', o.status_distribution, STATUS_COLORS, {size:190, fmt:v=>fmtNum(v)+' CLFs'});
      const subcomRows = Object.entries(o.subcom_agg).map(([k,v])=>[k, fmtNum(v)]);
      const scopeHint = isState() ? 'statewide' : `aggregate of ${fmtNum(o.n_clfs)} CLFs`;
      return `
      <section><div class="section-head"><h2 class="serif">${isState()?'Statewide':'District'} Snapshot</h2><span class="hint">${scopeHint}</span></div>
        <div class="panel"><div class="tiles n4">
          ${tile('Number of CLFs', fmtNum(o.n_clfs), null, 'info')}${tile('Number of VOs', fmtNum(o.n_vo), null, 'info')}
          ${tile('Number of SHGs', fmtNum(o.n_shg), null, 'info')}${tile('Total Members', fmtNum(o.n_members), null, 'info')}
        </div>
        <div class="tiles n1" style="margin-top:14px;">${tile('Active Members', fmtNum(o.n_active), o.pct_active+'% of total')}</div>
        </div></section>
      <section><div class="section-head"><h2 class="serif">CLF Status Distribution</h2></div>
        <div class="panel">${statusDonut.html}</div></section>
      <section><div class="section-head"><h2 class="serif">Governance Structure (Total)</h2></div>
        <div class="panel">
          <div class="tiles n2" style="margin-bottom:16px;">${tile('Total Executive Committee Members', fmtNum(o.total_ec), null, 'info')}${tile('Avg. EC Size per CLF', o.avg_ec!=null?o.avg_ec.toFixed(1):'—')}</div>
          ${tableHtml([{label:'Subcommittee', tip:TIPS['Subcommittee Membership']},{label:'Members (summed)',num:true}], subcomRows)}
        </div></section>`;
    }
    function renderGroupOverviewMembers(){
      const o = GROUP_DATA.overview;
      const covRows = Object.entries(o.coverage).map(([k,[pct,n]])=>[k, fmtNum(n), pct+'%']);
      const eduRows = Object.entries(o.education).map(([k,[pct,n]])=>[k, fmtNum(n), pct+'%']);
      const lvColors = ['var(--primary)','var(--gold)','#5B8AA6','var(--low)','var(--grey)'];
      const lvDonut = donutBlock('ring-group-livelihood', o.livelihood_split, lvColors, {size:190, fmt:v=>v+'%'});
      const cadreRows = Object.entries(o.cadre_roster).slice(0,15).map(([k,v])=>[k, fmtNum(v)]);
      const active = Object.entries(o.spa_agg).filter(([,v])=>v>0);
      const scopeText = isState() ? 'member-weighted statewide average' : 'member-weighted district average';
      return `
      <section><div class="section-head"><h2 class="serif">Social Inclusion / Welfare Coverage</h2><span class="hint">${scopeText}</span></div>
        <div class="panel">${tableHtml([{label:'Group'},{label:'#',num:true},{label:'%',num:true}], covRows)}</div></section>
      <section><div class="section-head"><h2 class="serif">Education &amp; Literacy</h2></div>
        <div class="panel">${tableHtml([{label:'Milestone'},{label:'#',num:true},{label:'%',num:true}], eduRows)}</div></section>
      <section><div class="section-head"><h2 class="serif">Livelihoods Diversification</h2></div>
        <div class="panel">
          <div class="tiles n2" style="margin-bottom:18px;">${tile('Members With a Livelihood', o.pct_has_livelihood+'%')}${tile('Members With Multiple Livelihoods', o.pct_multi_livelihood+'%')}</div>
          ${lvDonut.html}
        </div></section>
      <section><div class="section-head"><h2 class="serif">Special Project Activities</h2><span class="hint">% of CLFs engaged, of ${fmtNum(o.spa_n_clfs_found)} CLFs reporting &middot; activities no CLF engages in are omitted</span></div>
        <div class="panel">${active.length ? `<div class="pills">${active.map(([k,v])=>`<span class="pill on">${k} (${v}%)</span>`).join('')}</div>` : `<p class="disclaimer">${ERR_MSG_JS.not_found}</p>`}</div></section>
      <section><div class="section-head"><h2 class="serif">Cadre Roster (Total)</h2><span class="hint">top 15 roles, summed across all CLFs</span></div>
        <div class="panel">${tableHtml([{label:'Position'},{label:'Members',num:true}], cadreRows)}</div></section>`;
    }
    function renderGroupOverview(sub){
      document.getElementById('panel-overview').innerHTML = contextBox('overview') +
        (sub==='profile' ? renderGroupOverviewProfile() : renderGroupOverviewMembers());
      if(sub==='profile'){
        donutBlock('ring-group-status', GROUP_DATA.overview.status_distribution, STATUS_COLORS, {size:190, fmt:v=>fmtNum(v)+' CLFs'}).draw();
      }
      if(sub==='members'){
        const lvColors = ['var(--primary)','var(--gold)','#5B8AA6','var(--low)','var(--grey)'];
        donutBlock('ring-group-livelihood', GROUP_DATA.overview.livelihood_split, lvColors, {size:190, fmt:v=>v+'%'}).draw();
      }
    }

    // ---- Audit: same visual language as JS_AUDIT, sourced from the raw/max-
    // recomputed aggregate. District gets a REAL "vs. State" standing card
    // (computed against all 38 districts by build_district_state_data.py);
    // State has no card at all - it's the top of the hierarchy, nothing to
    // compare it against. ----
    function renderGroupAuditScores(){
      const a = GROUP_DATA.audit;
      if(!a.n_found) return `<section><div class="panel"><p class="disclaimer">${ERR_MSG_JS.audit_scores}</p></div></section>`;
      const worst = a.category_breakdown.filter(c=>grade(c.pct)==='var(--low)');
      const scopeNoun = isState() ? 'Bihar statewide' : `${GROUP_DATA.name} district`;
      const attnSentence = worst.length ? `<p class="callout-attn">${scopeNoun} needs to pay most attention to ${catListPhrase(worst.map(c=>c.label))}.</p>` : '';
      const standingBlock = isState() ? '' : `<div class="standing-grid single">${standingCard('state', a.avg_score, a.state_rank, a.n_state, 'Score '+a.avg_score+' / 100', 'Standing vs. State')}</div>`;
      const grouped = a.category_breakdown.map(c=>({cat:c.label, items:a.item_scores.filter(it=>it.category===c.label)})).filter(g=>g.items.length);
      return `
      <section><div class="section-head"><h2 class="serif">Grade &amp; Standing</h2><span class="hint">Q4 only &middot; ${fmtNum(a.n_found)} of ${fmtNum(a.n_total)} CLFs have an audit</span></div>
        <div class="panel">
          <div class="tiles n2" style="margin-bottom:18px;">${tile('Average Audit Score', a.avg_score+' / 100', null, 'big-value')}${tile('Most Common Grade', a.top_grade||'—', null, 'big-value')}</div>
          ${standingBlock}
        </div></section>
      <section><div class="section-head"><h2 class="serif">Grade Distribution</h2><span class="hint">${fmtNum(a.n_found)} CLFs with an audit</span></div>
        <div class="panel"><div class="chart-box" style="height:auto;"><svg id="group-grade-hist" viewBox="0 0 500 220" style="width:100%;height:auto;display:block;"></svg></div></div></section>
      <section><div class="section-head"><h2 class="serif">Category Breakdown</h2><span class="hint">average %, averaged across CLFs with an audit (not summed)</span></div>
        <div class="panel"><div class="donut-row">${a.category_breakdown.map((c,i)=>`
          <div class="donut-card"><div style="position:relative;width:76px;height:76px;margin:0 auto;">
            <svg id="group-cat-ring-${i}" viewBox="0 0 190 190" width="76" height="76" style="display:block;"></svg>
            <div style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;"><span style="font-weight:700;font-size:1.05rem;color:${grade(c.pct)}">${c.pct}%</span></div>
          </div><div class="dlabel"><b>${c.label}</b><br><span style="font-size:11px;">avg ${c.raw!=null?c.raw.toFixed(1):'—'}/${c.max}</span></div></div>`).join('')}
        </div>${attnSentence}</div></section>
      <section><div class="section-head"><h2 class="serif">Individual Item Scores</h2><span class="hint">average %, averaged across CLFs with an audit (not summed)</span></div>
        <div class="panel">${grouped.map(g=>`
          <div class="cat-group">
            <div class="cat-bracket-label">${g.cat}</div><div class="cat-bracket"></div>
            <div class="cat-group-items">${g.items.map(it=>gradedBar(it.label+' ('+(it.raw!=null?it.raw.toFixed(1):'—')+'/'+it.max+')', it.pct)).join('')}</div>
          </div>`).join('')}</div></section>`;
    }
    function renderGroupAuditIrreg(){
      const a = GROUP_DATA.audit;
      if(!a.n_found) return `<section><div class="section-head"><h2 class="serif">Financial Irregularities</h2></div>
        <div class="panel"><p class="disclaimer">${ERR_MSG_JS.financial_irregularities}</p></div></section>`;
      const irregRows = Object.entries(a.irregularity_categories).map(([k,v])=>[k, fmtNum(v)]);
      const gapKnown = a.gap_total_abs != null;
      return `<section><div class="section-head"><h2 class="serif">Financial Irregularities</h2><span class="hint">Q4 only</span></div>
        <div class="panel">
          <div class="tiles n1" style="margin-bottom:16px;">${tile('CLFs Flagged', fmtNum(a.n_flagged)+' of '+fmtNum(a.n_found), null, a.n_flagged>0?'neg':'')}</div>
          ${irregRows.length?tableHtml([{label:'Category'},{label:'CLFs Flagged',num:true}], irregRows):'<p class="disclaimer">No irregularity categories to show.</p>'}
          <div class="tiles n1" style="margin-top:16px;">${tile('Total Cash Book vs. Physical Cash Gap', gapKnown?fmtRs(a.gap_total_abs):'—', fmtNum(a.n_cash_higher)+' CLFs cash-book-higher &middot; '+fmtNum(a.n_physical_higher)+' physical-higher &middot; '+fmtNum(a.n_exact_match)+' exact match', gapKnown&&a.gap_total_abs>0?'neg':'')}</div>
        </div></section>`;
    }
    // Vertical bar chart, grade on the X axis (alphabetical), % of CLFs on the
    // Y axis - thin bars, one fixed colour per grade (A green / B blue / C gold
    // / D red); label shows only the % (kept small - it's a secondary label,
    // not the chart's main content); hovering a bar shows the exact count too.
    const GRADE_COLORS = {A:'#2F9C74', B:'#5B8AA6', C:'#CE9C3C', D:'#C55F49'};
    function drawGradeHistogram(svgId, entries, nFound){
      const svg = document.getElementById(svgId); if(!svg) return;
      const sorted = entries.slice().sort((a,b)=> a[0]<b[0]?-1:a[0]>b[0]?1:0);
      const W=500,H=220,padL=28,padR=20,padT=24,padB=30, innerW=W-padL-padR, innerH=H-padT-padB;
      const maxN = Math.max.apply(null, sorted.map(([,n])=>n).concat([1]));
      const slot = innerW/sorted.length, barW = Math.min(slot*0.38, 30);
      let bars = '', labels = '', valueLabels = '';
      sorted.forEach(([g,n],i)=>{
        const pct = n/nFound*100;
        const h = (n/maxN)*innerH;
        const x = padL + i*slot + (slot-barW)/2;
        const y = padT + innerH - h;
        const color = GRADE_COLORS[g] || cssVar('--primary');
        bars += `<rect class="tip" data-tip="Grade ${g}: ${fmtNum(n)} CLFs (${pct.toFixed(1)}%)" x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barW.toFixed(1)}" height="${h.toFixed(1)}" fill="${color}" rx="3"/>`;
        valueLabels += `<text x="${(x+barW/2).toFixed(1)}" y="${(y-6).toFixed(1)}" font-size="9" font-weight="700" text-anchor="middle" fill="${cssVar('--ink')}">${pct.toFixed(0)}%</text>`;
        labels += `<text x="${(x+barW/2).toFixed(1)}" y="${H-10}" font-size="12" font-weight="600" text-anchor="middle" fill="${cssVar('--ink-soft')}">${g}</text>`;
      });
      const axis = `<line x1="${padL}" y1="${padT+innerH}" x2="${W-padR}" y2="${padT+innerH}" stroke="${cssVar('--line')}" stroke-width="1"/>`;
      svg.innerHTML = axis + bars + valueLabels + labels;
    }
    function renderGroupAudit(){
      document.getElementById('panel-audit').innerHTML = contextBox('audit') + renderGroupAuditScores() + renderGroupAuditIrreg();
      const a = GROUP_DATA.audit;
      if(a.n_found){
        a.category_breakdown.forEach((c,i)=> drawRing('group-cat-ring-'+i, c.pct/100, grade(c.pct)));
        drawGradeHistogram('group-grade-hist', Object.entries(a.grade_distribution), a.n_found);
      }
    }

    // ---- Financial: DATA is pointed at GROUP_PSEUDO_CLF (shaped exactly like
    // a real CLF's .financial) so renderFinancial/renderFinSummary/
    // renderFinStatements from JS_FINANCIAL run completely UNCHANGED. The
    // district/state CONTEXT box already explains "summed across every CLF"
    // (see build_district_state_data.py's DISTRICT_CONTEXT/STATE_CONTEXT), so
    // no extra note is added here - a prior version prepended one via
    // `panel.innerHTML = note + panel.innerHTML`, which silently destroyed and
    // recreated the just-rendered #qtr-select (wiping its change listener) and
    // broke the quarter dropdown. ----
    function renderGroupFinancial(sub){
      DATA = GROUP_PSEUDO_CLF;
      renderFinancial(sub);
    }

    // ---- VRF: KPI Snapshot and Forecasts reuse renderVrfKpi/renderVrfForecasts
    // from JS_VRF UNCHANGED (DATA pointed at GROUP_PSEUDO_CLF). VO-Level
    // Breakdown / Bookkeeper Ranking are dropped entirely at this scope. "Needs
    // Attention" pictograms get their dot counts SCALED (1 square = N VOs) so
    // the total stays legible when summed across hundreds/thousands of VOs. ----
    function pictScale(total){
      const candidates = [1,2,5,10,20,25,50,100,200,250,500,1000,2000,5000,10000,20000,50000];
      for(let i=0;i<candidates.length;i++){ if(Math.ceil(total/candidates[i]) <= 48) return candidates[i]; }
      return candidates[candidates.length-1];
    }
    function scaledPictogram(containerId, counts){
      const total = counts.reduce((a,c)=>a+c.n,0);
      const scale = pictScale(total);
      const scaledCounts = counts.map(c=>({n: c.n>0 ? Math.max(Math.round(c.n/scale),1) : 0, cls:c.cls}));
      drawPictogram(containerId, scaledCounts);
      const el = document.getElementById(containerId);
      if(el && scale>1) el.insertAdjacentHTML('afterend', `<div class="scale-note" style="font-size:11px;color:var(--ink-soft);margin-top:6px;">each square &asymp; ${fmtNum(scale)} VOs</div>`);
    }
    function renderGroupVRF(sub){
      if(!GROUP_DATA.vrf.found){
        document.getElementById('panel-vrf').innerHTML = contextBox('vrf') + `<section><div class="panel"><p class="disclaimer">${ERR_MSG_JS.vrf}</p></div></section>`;
        return;
      }
      DATA = GROUP_PSEUDO_CLF;
      const scope = isState() ? 'statewide' : `in ${GROUP_DATA.name}`;
      const note = `<p class="hint" style="margin-bottom:14px;">Aggregated across ${fmtNum(GROUP_DATA.vrf.n_clfs_with_vrf)} of ${fmtNum(GROUP_DATA.vrf.n_total)} CLFs ${scope} with VRF data.</p>`;
      let body = sub==='kpi' ? renderVrfKpi() : renderVrfForecasts();
      const groupNoun = isState() ? 'Bihar' : `${GROUP_DATA.name} district`;
      body = body.replace('Your CLF has missed out on', `${groupNoun} has missed out on`)
        .replace('to the CLF for the Social Development Fund (SDF). This is the total the CLF should be collecting from all its VOs each month, and over the year, if every VO pays what it owes.',
          `to their own CLF for the Social Development Fund (SDF). This is the total every CLF ${isState()?'in Bihar':'in the district'} should be collecting from its VOs, combined, each month and over the year, if every VO pays what it owes.`);
      document.getElementById('panel-vrf').innerHTML = contextBox('vrf') + note + body;
      if(sub==='kpi'){
        const v = DATA.vrf;
        const savPct = v.expected_savings ? Math.min(v.total_savings/v.expected_savings,1) : 0;
        drawRing('ring-savings', savPct, cssVar('--primary'), cssVar('--low'));
        const tot = v.total_received+v.total_savings+v.total_interest;
        drawDonut('ring-composition', [{frac:v.total_received/tot,color:cssVar('--gold'),tip:'Government Grant: '+fmtRs(v.total_received)},
          {frac:v.total_savings/tot,color:cssVar('--primary'),tip:'Savings: '+fmtRs(v.total_savings)},
          {frac:v.total_interest/tot,color:cssVar('--ink'),tip:'Interest: '+fmtRs(v.total_interest)}]);
        scaledPictogram('pict-coverage', [{n:v.n_vo-v.incomplete_coverage,cls:'good'},{n:v.incomplete_coverage,cls:'flag'}]);
        scaledPictogram('pict-interest', [{n:v.received_vo-v.idle_vo,cls:'good'},{n:v.idle_vo,cls:'flag'},{n:v.n_vo-v.received_vo,cls:'na'}]);
      }
      if(sub==='forecasts'){
        drawVrfLineChart('savings');
        document.querySelectorAll('.chart-tab').forEach(btn=>{
          btn.addEventListener('click', ()=>{
            document.querySelectorAll('.chart-tab').forEach(b=>b.classList.remove('active'));
            btn.classList.add('active'); drawVrfLineChart(btn.dataset.series);
          });
        });
      }
    }

    // ---- VPRP: DATA pointed at GROUP_PSEUDO_CLF (vprp.years shaped exactly
    // like a real CLF's) so renderVPRP/renderVprpEnt/Pgsrd/Sdp from JS_VPRP run
    // completely UNCHANGED - all 3 subtabs, full parity. ----
    function renderGroupVPRP(sub){
      DATA = GROUP_PSEUDO_CLF;
      renderVPRP(sub);
    }

    // ---- Scoring: Overall/Category Summary use REAL cross-district ranks
    // (computed by build_district_state_data.py against all 38 districts) at
    // district scope; at state scope there's no rank to show (the state IS the
    // top level), so those sections fall back to a plain score bar with no
    // "State" comparison line - still wrapped in the same margin as
    // scoreRankBar so the rows aren't cramped. CLF Rankings is dual-mode: the
    // full CLF list (district) or the top 20 + bottom 20 statewide (state),
    // sharing one 9-column table shape - see clf_ranking_mode in the data. ----
    function categoryScoreBlock(label, score, rank, n){
      // Category Summary's 5 category names never get a hover tip, at any
      // level - matches the CLF tracker's own original Category Summary design
      // (only individual metrics in By Category get tooltips, not categories).
      if(isState()) return `<div style="margin-bottom:20px;">${gradedBar(label, score||0, '')}</div>`;
      return scoreRankBar(label, score, rank, n, '');
    }
    function scoreRankBar(label, score, rank, n, tip){
      // Same tip===undefined vs tip==='' distinction as gradedBar - see there.
      const t = tip !== undefined ? tip : (label && TIPS[label]);
      const lbl = label ? (t ? `<span class="tip" data-tip="${t}">${label}</span>` : label) : '';
      const rankTxt = rank!=null && n!=null ? `${fmtNum(rank)}${ord(rank)} of ${fmtNum(n)}` : ERR_MSG_JS.not_found;
      return `<div style="margin-bottom:20px;">
        <div style="display:flex;justify-content:space-between;align-items:baseline;gap:12px;">
          <div style="font-size:14px;font-weight:600;">${lbl}</div>
          <div style="font-size:26px;font-weight:700;color:var(--gold);font-variant-numeric:tabular-nums;white-space:nowrap;">${score!=null?score+'%':'—'}</div>
        </div>
        <div style="display:flex;justify-content:space-between;align-items:baseline;margin:2px 0 6px;">
          <span style="font-size:11.5px;font-weight:700;letter-spacing:.06em;color:var(--gold);text-transform:uppercase;">State</span>
          <span style="font-size:13px;color:var(--ink-soft);">${rankTxt}</span>
        </div>
        <div class="bar-track"><div class="fill" style="width:${score||0}%;background:var(--gold)"></div></div>
      </div>`;
    }
    // State-level "By Category" metric row: averaging a percentile against its
    // OWN reference population is close to a mathematical tautology at state
    // scope (a full population's average percentile of itself converges to ~50
    // regardless of the underlying data - confirmed empirically against the
    // real numbers here, most state avg_state_pctl values landed within 1-2
    // points of 50). So instead of a percentile bar, state shows the metric's
    // actual statewide average value (mapped 1:1 to the exact column
    // build_tracker_data.py itself scores that metric from) plus which
    // district is doing best/worst on it - both real, neither self-referential.
    // A few metrics (Platform Approval Status, Insurance/Aadhaar Coverage)
    // don't have a raw value available anywhere in the built data, so only the
    // best/worst-district line shows for those.
    function fmtMetricRaw(value, fmt){
      if(value==null || !fmt) return null;
      if(fmt==='pct') return value.toFixed(1)+'%';
      if(fmt==='pct_signed') return (value>=0?'+':'')+value.toFixed(1)+'%';
      if(fmt==='pct_of_1') return (value*100).toFixed(1)+'%';
      if(fmt==='multiplier') return value.toFixed(2)+'×';
      if(fmt==='rs') return fmtRs(value);
      if(fmt==='rs_per_member') return fmtRs(value)+' per member';
      if(fmt.indexOf('of_')===0) return value.toFixed(1)+' of '+fmt.split('_')[1];
      if(fmt==='types') return value.toFixed(1)+' types';
      return String(value);
    }
    function metricStateBlock(m){
      const t = TIPS[m.label];
      const lbl = t ? `<span class="tip" data-tip="${t}">${m.label}</span>` : m.label;
      const rawTxt = fmtMetricRaw(m.raw_value, m.raw_fmt);
      const descriptor = m.raw_descriptor ? `<div style="font-size:11.5px;color:var(--ink-soft);text-align:right;">${m.raw_descriptor}</div>` : '';
      const bw = (m.best_district && m.worst_district) ? `<div style="font-size:12.5px;color:var(--ink-soft);margin-top:6px;">
          Best: <a href="#district/${m.best_district.slug}" class="district-link">${m.best_district.name}</a>
          &nbsp;&middot;&nbsp; Worst: <a href="#district/${m.worst_district.slug}" class="district-link">${m.worst_district.name}</a>
        </div>` : '';
      return `<div style="margin-bottom:20px;padding-bottom:16px;border-bottom:1px solid var(--line);">
        <div style="display:flex;justify-content:space-between;align-items:baseline;gap:12px;">
          <div style="font-size:14px;font-weight:600;">${lbl}</div>
          <div style="font-size:18px;font-weight:700;color:var(--gold);white-space:nowrap;">${rawTxt!=null?rawTxt:'—'}</div>
        </div>
        ${descriptor}
        ${bw}
      </div>`;
    }
    function renderGroupScoringOverall(){
      const s = GROUP_DATA.scoring;
      const cats = Object.entries(s.category_scores);
      const scoreLabel = (isState()?'Statewide':'District')+' Overall Score';
      const scoredSub = `${fmtNum(s.n_clfs_scored)} of ${fmtNum(s.n_total)} CLFs scored`;
      const soloTile = isState() ? `<div class="tiles n1" style="margin-bottom:16px;">${tile(scoreLabel, s.overall_score+' / 100', scoredSub, 'big-value')}</div>` : '';
      const standingBlock = isState() ? '' : `<div class="standing-grid">
            ${tile(scoreLabel, s.overall_score+' / 100', scoredSub, 'big-value')}
            ${standingCard('state', s.overall_score, s.overall_state_rank, s.n_districts, 'Overall Score', 'Standing vs. State')}
          </div>`;
      return `<section><div class="section-head"><h2 class="serif">Overall Score</h2><span class="hint">average of each CLF's own Overall Score, equal weight per CLF</span></div>
        <div class="panel">${soloTile}${standingBlock}</div></section>
      <section><div class="section-head"><h2 class="serif">Category Summary</h2></div>
        <div class="panel">${cats.map(([k,v])=>{ const cr = s.category_ranks[k]; return categoryScoreBlock(k, v, cr&&cr.rank, cr&&cr.n); }).join('')}</div></section>`;
    }
    // ---- District/State Data Coverage detail: the per-source coverage-count
    // breakdown behind the Data Coverage category's single metric, shown within
    // that category's own section in By Category - same convention as the
    // CLF-level pill checklist, just a coverage count/bar per source instead. ----
    function renderGroupDataCoverage(){
      const da = GROUP_DATA.data_availability;
      if(!da || !da.sources || !da.sources.length) return '';
      return da.sources.map(s=>{
          const pct = s.n_total ? Math.round(100*s.n_available/s.n_total) : 0;
          return `<div style="margin-bottom:14px;">
            <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:4px;">
              <span>${s.label}</span><span style="color:var(--ink-soft);">${fmtNum(s.n_available)} of ${fmtNum(s.n_total)} CLFs</span>
            </div>
            <div class="score-bar"><div class="fill" style="width:${pct}%"></div></div>
          </div>`;
        }).join('');
    }
    function renderGroupScoringByCategory(){
      const s = GROUP_DATA.scoring;
      const scopeText = isState() ? 'across all CLFs in Bihar' : `across all CLFs in ${GROUP_DATA.name} district`;
      const desc = isState()
        ? `Each metric below shows the actual statewide average value (not a percentile - averaging a percentile against the whole state it's drawn from converges to ~50 regardless of the underlying data), plus which district is doing best and worst on it.`
        : `Each metric below is its average state percentile, averaged ${scopeText} that have a score for it.`;
      return Object.entries(s.category_scores).map(([catName,score])=>{
        const metrics = s.category_metrics[catName] || [];
        const body = isState()
          ? metrics.map(m=>metricStateBlock(m)).join('')
          : metrics.map(m=>scoreRankBar(m.label, m.avg_state_pctl, m.rank, m.n)).join('');
        return `<section><div class="section-head"><h2 class="serif">${catName}</h2><span class="hint"><b>${isState()?'Statewide':'District'} avg: ${score!=null?score+'/100':ERR_MSG_JS.not_found}</b></span></div>
          <div class="panel">
            <p class="desc" style="max-width:none;margin-bottom:14px;">${desc}</p>
            ${metrics.length ? body : `<p class="disclaimer">${ERR_MSG_JS.not_found}</p>`}
            ${catName==='Data Coverage'?renderGroupDataCoverage():''}
          </div></section>`;
      }).join('');
    }
    const CLF_RANK_COLS = () => {
      const mode = GROUP_DATA.scoring.clf_ranking_mode;
      return [
        {label:'Rank', key: mode==='top_bottom_20' ? 'state_rank' : 'district_rank'},
        {label:'CLF', key:'name'},
        {label: mode==='top_bottom_20' ? 'District' : 'Block', key: mode==='top_bottom_20' ? 'district' : 'block'},
        {label:'Financial Health', num:true, key:'Financial Health'},
        {label:'Fund Utilization', num:true, key:'Fund Utilization'},
        {label:'Loan Portfolio', num:true, key:'Loan Portfolio'},
        {label:'VRF Fund Health', num:true, key:'VRF Fund Health'},
        {label:'Governance', num:true, key:'Governance & Compliance'},
        {label:'Welfare', num:true, key:'Welfare and Livelihood'},
        {label:'Data Coverage', num:true, key:'Data Coverage'},
        {label:'Overall Score', num:true, key:'overall_score'},
      ];
    };
    // tier ('top'/'bottom') is a fixed property stamped once at data-build
    // time by build_district_state_data.py's own overall-rank sort - NOT
    // re-derived from wherever a row currently sits after the table gets
    // sorted by a different column, which would make the colour meaningless (a
    // bottom-20 CLF with one strong sub-score could sort near the top of the
    // visible list and wrongly render green).
    function clfRankRow(r){
      const mode = GROUP_DATA.scoring.clf_ranking_mode;
      const rankNum = mode==='top_bottom_20' ? r.state_rank : r.district_rank;
      const tierClass = r.tier==='bottom' ? ' tier-bottom' : r.tier==='top' ? ' tier-top' : '';
      // District name is plain text here (not linked) - the District Performance
      // tab is where district links live; this table is about CLFs.
      const locationCell = mode==='top_bottom_20' ? r.district : r.block;
      return [
        `<span class="rank-cell">${fmtNum(rankNum)||'—'}</span>`,
        `<a href="#clf/${r.mis_id}" class="clf-link${tierClass}">${r.name}</a>`, locationCell,
        r.categories['Financial Health']!=null?r.categories['Financial Health']:'—',
        r.categories['Fund Utilization']!=null?r.categories['Fund Utilization']:'—',
        r.categories['Loan Portfolio']!=null?r.categories['Loan Portfolio']:'—',
        r.categories['VRF Fund Health']!=null?r.categories['VRF Fund Health']:'—',
        r.categories['Governance & Compliance']!=null?r.categories['Governance & Compliance']:'—',
        r.categories['Welfare and Livelihood']!=null?r.categories['Welfare and Livelihood']:'—',
        r.categories['Data Coverage']!=null?r.categories['Data Coverage']:'—',
        r.overall_score!=null?`<div class="score-cell"><div class="score-bar"><div class="fill" style="width:${r.overall_score}%"></div></div><span class="score-num">${r.overall_score}</span></div>`:'—',
      ];
    }
    function flattenedRankings(){
      return GROUP_DATA.scoring.clf_rankings.map(r=>({...r, ...r.categories}));
    }
    function renderGroupScoringRankings(){
      const mode = GROUP_DATA.scoring.clf_ranking_mode;
      if(mode==='top_bottom_20'){
        const commonHint = `of all ${fmtNum(GROUP_DATA.scoring.n_total)} CLFs in Bihar &middot; click a column to sort &middot; click a CLF or District to open its tracker`;
        return `<section><div class="section-head"><h2 class="serif">Top 20 CLFs</h2><span class="hint">highest overall score ${commonHint}</span></div>
          <div class="panel"><div id="clf-rankings-top-container"></div></div></section>
        <section><div class="section-head"><h2 class="serif">Bottom 20 CLFs</h2><span class="hint">lowest overall score ${commonHint}</span></div>
          <div class="panel"><div id="clf-rankings-bottom-container"></div></div></section>`;
      }
      const hint = `all ${GROUP_DATA.scoring.clf_rankings.length} CLFs in ${GROUP_DATA.name} district &middot; click a column to sort &middot; click a CLF to open its tracker`;
      return `<section><div class="section-head"><h2 class="serif">CLF Performance</h2><span class="hint">${hint}</span></div>
        <div class="panel"><div id="clf-rankings-container"></div></div></section>`;
    }
    // ---- District Performance: same 9-column shape as CLF Rankings, one level
    // up - every district in Bihar, ranked by Overall Score (real cross-
    // district ranks, computed the same two-pass way as CLF ranks). ----
    const DISTRICT_RANK_COLS = () => [
      {label:'Rank', key:'state_rank'}, {label:'District', key:'name'}, {label:'CLFs', num:true, key:'n_clfs'},
      {label:'Financial Health', num:true, key:'Financial Health'},
      {label:'Fund Utilization', num:true, key:'Fund Utilization'},
      {label:'Loan Portfolio', num:true, key:'Loan Portfolio'},
      {label:'VRF Fund Health', num:true, key:'VRF Fund Health'},
      {label:'Governance', num:true, key:'Governance & Compliance'},
      {label:'Welfare', num:true, key:'Welfare and Livelihood'},
      {label:'Data Coverage', num:true, key:'Data Coverage'},
      {label:'Overall Score', num:true, key:'overall_score'},
    ];
    function districtRankRow(r){
      return [
        `<span class="rank-cell">${fmtNum(r.state_rank)||'—'}</span>`,
        r.slug ? `<a href="#district/${r.slug}" class="district-link">${r.name}</a>` : `<i>${r.name}</i>`, fmtNum(r.n_clfs),
        r.categories['Financial Health']!=null?r.categories['Financial Health']:'—',
        r.categories['Fund Utilization']!=null?r.categories['Fund Utilization']:'—',
        r.categories['Loan Portfolio']!=null?r.categories['Loan Portfolio']:'—',
        r.categories['VRF Fund Health']!=null?r.categories['VRF Fund Health']:'—',
        r.categories['Governance & Compliance']!=null?r.categories['Governance & Compliance']:'—',
        r.categories['Welfare and Livelihood']!=null?r.categories['Welfare and Livelihood']:'—',
        r.categories['Data Coverage']!=null?r.categories['Data Coverage']:'—',
        r.overall_score!=null?`<div class="score-cell"><div class="score-bar"><div class="fill" style="width:${r.overall_score}%"></div></div><span class="score-num">${r.overall_score}</span></div>`:'—',
      ];
    }
    function flattenedDistrictRankings(){
      return GROUP_DATA.scoring.district_rankings.map(r=>({...r, ...r.categories}));
    }
    // State's own true CLF-weighted aggregate (GROUP_DATA.scoring.category_scores/
    // overall_score), NOT a re-average of the districts array (that would be an
    // unweighted mean-of-means). Only ever called from State view.
    function stateAverageRow(){
      const cs = GROUP_DATA.scoring.category_scores;
      const os = GROUP_DATA.scoring.overall_score;
      // Raw data object, shaped like a real (flattened) district row - lets it
      // sort naturally alongside real rows in makeSortableTable (see comment
      // there), rather than being pinned to a fixed position.
      return { state_rank: null, name: 'State Average', slug: null, n_clfs: GROUP_DATA.overview.n_clfs,
        categories: cs, overall_score: os, ...cs };
    }
    function renderGroupScoringDistrictRankings(){
      return `<section><div class="section-head"><h2 class="serif">District Performance</h2><span class="hint">all ${GROUP_DATA.scoring.district_rankings.length} districts in Bihar &middot; click a column to sort &middot; click a District to open its tracker</span></div>
        <div class="panel"><div id="district-rankings-container"></div></div></section>`;
    }
    function renderGroupScoring(sub){
      const body = sub==='overall' ? renderGroupScoringOverall()
        : sub==='bycategory' ? renderGroupScoringByCategory()
        : sub==='rankings' ? renderGroupScoringRankings()
        : renderGroupScoringDistrictRankings();
      document.getElementById('panel-scoring').innerHTML = contextBox('scoring') + body;
      if(sub==='rankings'){
        const mode = GROUP_DATA.scoring.clf_ranking_mode;
        if(mode==='top_bottom_20'){
          const all = flattenedRankings();
          makeSortableTable('clf-rankings-top-container', CLF_RANK_COLS(), all.filter(r=>r.tier==='top'), clfRankRow);
          makeSortableTable('clf-rankings-bottom-container', CLF_RANK_COLS(), all.filter(r=>r.tier==='bottom'), clfRankRow);
        } else {
          makeSortableTable('clf-rankings-container', CLF_RANK_COLS(), flattenedRankings(), clfRankRow);
        }
      }
      if(sub==='districtrankings'){ makeSortableTable('district-rankings-container', DISTRICT_RANK_COLS(), flattenedDistrictRankings(), districtRankRow, undefined, stateAverageRow()); }
    }
    """

    JS_NAV = r"""
    const TABS = {
      overview: {label:'Overview', render:renderOverview, subtabs:{profile:'Profile', members:'Members', vo:'VO Overview'}},
      financial: {label:'Financial Records', render:renderFinancial, subtabs:{summary:'Summary', statements:'Statements', disbursement:'Fund Disbursement'}},
      loans: {label:'Loans', render:renderLoans, subtabs:{overview:'Portfolio Overview', schedule:'Loan Schedule'}},
      vrf: {label:'Vulnerability Reduction Fund', render:renderVRF, subtabs:{kpi:'KPI Snapshot', vobreak:'VO-Level Breakdown', bkrank:'Bookkeeper & CLF Rankings', forecasts:'Forecasts'}},
      vprp: {label:'Village Poverty Reduction Plan', render:renderVPRP, subtabs:{entitlements:'Entitlements', pgsrd:'Public Goods, Services, and Resource Development', sdp:'Social Development Plan'}},
      audit: {label:'Audit Reports', render:renderAudit, subtabs:null},
      scoring: {label:'Scoring & Ranking', render:renderScoring, subtabs:{overall:'Overall', bycategory:'By Category'}},
    };
    const TABS_DISTRICT = {
      overview: {label:'Overview', render:renderGroupOverview, subtabs:{profile:'Profile', members:'Members'}},
      financial: {label:'Financial Records', render:renderGroupFinancial, subtabs:{summary:'Summary', statements:'Statements', disbursement:'Fund Disbursement'}},
      loans: {label:'Loans', render:renderGroupLoans, subtabs:{overview:'Portfolio Overview'}},
      vrf: {label:'Vulnerability Reduction Fund', render:renderGroupVRF, subtabs:{kpi:'KPI Snapshot', forecasts:'Forecasts'}},
      vprp: {label:'Village Poverty Reduction Plan', render:renderGroupVPRP, subtabs:{entitlements:'Entitlements', pgsrd:'Public Goods, Services, and Resource Development', sdp:'Social Development Plan'}},
      audit: {label:'Audit Reports', render:renderGroupAudit, subtabs:null},
      scoring: {label:'Scoring & Ranking', render:renderGroupScoring, subtabs:{overall:'Overall', bycategory:'By Category', rankings:'CLF Performance'}},
    };
    const TABS_STATE = Object.assign({}, TABS_DISTRICT, {
      scoring: {label:'Scoring & Ranking', render:renderGroupScoring, subtabs:{overall:'Overall', bycategory:'By Category', rankings:'CLF Performance', districtrankings:'District Performance'}},
    });
    let currentTab='overview', currentSub='profile';
    let CURRENT_VIEW = 'landing'; // 'landing' | 'clf' | 'district' | 'state'
    function isState(){ return CURRENT_VIEW === 'state'; }
    function getTabs(){
      if(CURRENT_VIEW==='district') return TABS_DISTRICT;
      if(CURRENT_VIEW==='state') return TABS_STATE;
      return TABS;
    }

    function renderTabBar(){
      const tabs = getTabs();
      document.getElementById('tabbar').innerHTML = Object.entries(tabs).map(([id,t])=>
        `<button class="tabbtn ${id===currentTab?'active':''}" data-tab="${id}">${t.label}</button>`).join('');
      document.querySelectorAll('.tabbtn').forEach(b=>b.addEventListener('click',()=>{
        currentTab=b.dataset.tab; currentSub=tabs[currentTab].subtabs ? Object.keys(tabs[currentTab].subtabs)[0] : null; renderAll();
      }));
    }
    function renderSubtabBar(){
      const subs = getTabs()[currentTab].subtabs;
      const bar = document.getElementById('subtabbar');
      if(!subs){ bar.innerHTML = ''; bar.style.display = 'none'; return; }
      bar.style.display = '';
      bar.innerHTML = Object.entries(subs).map(([id,label])=>
        `<button class="subtabbtn ${id===currentSub?'active':''}" data-sub="${id}">${label}</button>`).join('');
      document.querySelectorAll('.subtabbtn').forEach(b=>b.addEventListener('click',()=>{
        currentSub=b.dataset.sub; renderAll();
      }));
    }
    // Masthead is fully dynamic now (District/State views have no fixed
    // crumb fields) - CLF view keeps the same crumb line as before, just built
    // as a template string instead of writing into static spans.
    function renderMasthead(){
      const el = document.getElementById('masthead-content');
      if(CURRENT_VIEW === 'clf'){
        const o = DATA.overview;
        const niceName = o.clf_name_lokos.replace(/\w\S*/g, t=>t.charAt(0).toUpperCase()+t.substr(1).toLowerCase());
        const slug = (SHARED.district_slugs && SHARED.district_slugs[o.district]) || '';
        el.innerHTML = `<button id="dyn-back-btn" class="back-link">&larr; Back</button>
          <h1 class="serif">${niceName}</h1>
          <div class="crumbs"><a href="#district/${slug}" class="district-link">${o.district}</a> District &nbsp;&middot;&nbsp; <b>${o.block}</b> Block &nbsp;&middot;&nbsp; MIS ID <b>${o.mis_id}</b> &nbsp;&middot;&nbsp; LokOS Code <b>${o.clfcode}</b></div>`;
      } else if(CURRENT_VIEW === 'district'){
        el.innerHTML = `<button id="dyn-back-btn" class="back-link">&larr; Back</button>
          <h1 class="serif">${GROUP_DATA.name} District</h1>
          <div class="crumbs">Aggregate of <b>${fmtNum(GROUP_DATA.overview.n_clfs)}</b> CLFs &nbsp;&middot;&nbsp; District-Level Tracker &nbsp;&middot;&nbsp; <a href="#state" class="district-link">View Bihar Statewide &rarr;</a></div>`;
      } else if(CURRENT_VIEW === 'state'){
        el.innerHTML = `<button id="dyn-back-btn" class="back-link">&larr; Back</button>
          <h1 class="serif">Bihar Statewide</h1>
          <div class="crumbs">Aggregate of <b>${fmtNum(GROUP_DATA.overview.n_clfs)}</b> CLFs across <b>${fmtNum(MANIFEST.counts.districts)}</b> districts &nbsp;&middot;&nbsp; State-Level Tracker</div>`;
      }
      // "Back" returns to wherever the user actually came from (in-app, via
      // history.back()) when there IS somewhere to go back to; falls back to
      // the search landing page for a cold/shared link with no prior in-app
      // navigation, where history.back() would just leave the site entirely.
      const backBtn = document.getElementById('dyn-back-btn');
      if(backBtn) backBtn.addEventListener('click', ()=>{ if(inAppNavCount > 1){ history.back(); } else { backToSearch(); } });
    }
    // CONTEXT/ERR_MSG_JS are swapped wholesale for the active view on every
    // render - the CLF tracker's own copies say "your CLF" throughout, which is
    // wrong once these same functions (Financial, VPRP, VRF KPI/Forecasts) run
    // against a District or State aggregate instead of one CLF. district_err_msg
    // is a {name}-templated copy (one dict for all 38 districts), substituted
    // with the currently loaded district's real name here.
    function renderAll(){
      if(CURRENT_VIEW==='district'){
        Object.assign(CONTEXT, SHARED.district_context);
        const out = {};
        Object.entries(SHARED.district_err_msg_tmpl).forEach(([k,v])=> out[k] = v.replace(/\{name\}/g, GROUP_DATA.name));
        Object.assign(ERR_MSG_JS, out);
        Object.assign(TIPS, ORIGINAL_TIPS, SHARED.district_tips || {});
      } else if(CURRENT_VIEW==='state'){
        Object.assign(CONTEXT, SHARED.state_context);
        Object.assign(ERR_MSG_JS, SHARED.state_err_msg);
        Object.assign(TIPS, ORIGINAL_TIPS, SHARED.state_tips || {});
      } else {
        Object.assign(CONTEXT, ORIGINAL_CONTEXT);
        Object.assign(ERR_MSG_JS, ORIGINAL_ERR_MSG);
        Object.assign(TIPS, ORIGINAL_TIPS);
      }
      renderMasthead(); renderTabBar(); renderSubtabBar();
      document.querySelectorAll('.tabpanel').forEach(p=>p.classList.toggle('active', p.id==='panel-'+currentTab));
      getTabs()[currentTab].render(currentSub);
    }
    """

    # ============================================================================
    # JS_SHELL - new: manifest/shared loading, search/finder view, and the fetch
    # orchestration that loads one CLF's JSON and hands off to the render code
    # above. This replaces the prototype's "bake DATA in at build time, call
    # renderAll() immediately" with a runtime fetch-then-render flow.
    # ============================================================================
    JS_SHELL = r"""
    let MANIFEST = null;
    let SHARED = null;
    let ORIGINAL_CONTEXT = {};
    let ORIGINAL_ERR_MSG = {};
    let ORIGINAL_TIPS = {};
    const CLF_INDEX = {}; // mis_id (number) -> {n, district, block}
    const JSON_CACHE = {};

    async function fetchJson(path){
      if(JSON_CACHE[path]) return JSON_CACHE[path];
      const res = await fetch(path, {cache: 'no-store'});
      if(!res.ok) throw new Error('Failed to fetch '+path+' ('+res.status+')');
      const data = await res.json();
      JSON_CACHE[path] = data;
      return data;
    }

    function indexManifest(m){
      m.districts.forEach(d=>{
        d.blocks.forEach(b=>{
          b.clfs.forEach(c=>{ CLF_INDEX[c.id] = {n:c.n, district:d.name, block:b.name}; });
        });
      });
    }

    function populateDistrictSelect(){
      const sel = document.getElementById('sel-district');
      sel.innerHTML = `<option value="">Select District…</option>` +
        MANIFEST.districts.map(d=>`<option value="${d.name}">${d.name}</option>`).join('');
    }
    function populateBlockSelect(districtName){
      const sel = document.getElementById('sel-block');
      const clfSel = document.getElementById('sel-clf');
      if(!districtName){ sel.innerHTML = `<option value="">Select Block…</option>`; sel.disabled = true;
        clfSel.innerHTML = `<option value="">Select CLF…</option>`; clfSel.disabled = true; return; }
      const d = MANIFEST.districts.find(d=>d.name===districtName);
      sel.disabled = false;
      sel.innerHTML = `<option value="">Select Block…</option>` + d.blocks.map(b=>`<option value="${b.name}">${b.name}</option>`).join('');
      clfSel.innerHTML = `<option value="">Select CLF…</option>`; clfSel.disabled = true;
    }
    function populateClfSelect(districtName, blockName){
      const sel = document.getElementById('sel-clf');
      if(!blockName){ sel.innerHTML = `<option value="">Select CLF…</option>`; sel.disabled = true; return; }
      const d = MANIFEST.districts.find(d=>d.name===districtName);
      const b = d.blocks.find(b=>b.name===blockName);
      sel.disabled = false;
      sel.innerHTML = `<option value="">Select CLF…</option>` + b.clfs.slice().sort((a,c)=>a.n.localeCompare(c.n)).map(c=>`<option value="${c.id}">${c.n}</option>`).join('');
    }
    // ---- District/State tracker finder (separate from the CLF drill-down
    // dropdowns above - "in addition to, not replacing, Find your CLF") ----
    function populateGroupDistrictSelect(){
      const sel = document.getElementById('sel-group-district');
      sel.innerHTML = `<option value="">Select District to view its Tracker…</option>` +
        MANIFEST.districts.map(d=>`<option value="${SHARED.district_slugs[d.name]}">${d.name}</option>`).join('');
    }

    function showLandingError(msg){
      const el = document.getElementById('nav-error');
      el.textContent = msg; el.style.display = 'block';
    }
    function clearLandingError(){
      document.getElementById('nav-error').style.display = 'none';
    }
    function showLanding(){
      document.getElementById('view-tracker').classList.remove('active');
      document.getElementById('view-landing').classList.add('active');
    }
    function showTracker(){
      document.getElementById('view-landing').classList.remove('active');
      document.getElementById('view-tracker').classList.add('active');
    }

    async function loadClf(misId){
      misId = +misId;
      if(!CLF_INDEX[misId]){ showLandingError(`No CLF found with MIS ID ${misId}.`); showLanding(); return; }
      clearLandingError();
      try{
        const data = await fetchJson(`data/clfs/${misId}.json`);
        DATA = data;
        CURRENT_VIEW = 'clf';
        showTracker();
        currentTab='overview'; currentSub='profile'; finQtrIdx = DATA.financial.quarters.length - 1; vprpYear = 2025; vprpGp = 'ALL'; voDetailCode = null; scoreQtrIdx = DATA.scoring.default_idx;
        renderAll();
      } catch(e){
        showLandingError(`Could not load data for MIS ID ${misId}. It may not have data available.`);
        showLanding();
      }
    }
    async function loadDistrict(slug){
      clearLandingError();
      try{
        const data = await fetchJson(`data/districts/${slug}.json`);
        GROUP_DATA = data; GROUP_PSEUDO_CLF = data.pseudo_clf;
        CURRENT_VIEW = 'district';
        showTracker();
        currentTab='overview'; currentSub='profile'; finQtrIdx = GROUP_DATA.financial.quarters.length - 1; vprpYear = 2025; vprpGp = 'ALL';
        renderAll();
      } catch(e){
        showLandingError(`Could not load the District Tracker.`);
        showLanding();
      }
    }
    async function loadState(){
      clearLandingError();
      try{
        const data = await fetchJson(`data/state.json`);
        GROUP_DATA = data; GROUP_PSEUDO_CLF = data.pseudo_clf;
        CURRENT_VIEW = 'state';
        showTracker();
        currentTab='overview'; currentSub='profile'; finQtrIdx = GROUP_DATA.financial.quarters.length - 1; vprpYear = 2025; vprpGp = 'ALL';
        renderAll();
      } catch(e){
        showLandingError(`Could not load the Statewide Tracker.`);
        showLanding();
      }
    }

    function backToSearch(){
      window.location.hash = '';
    }

    // Single hash-driven router - every navigation (dropdowns, MIS-ID search,
    // District/State buttons, in-page CLF/District links) sets window.location
    // hash and lets the resulting 'hashchange' event do the actual loading,
    // rather than each entry point calling a loader directly. That keeps there
    // being exactly one place (here) that decides what's currently on screen,
    // so a shared link or the browser's own back/forward button both work.
    // inAppNavCount counts how many times THIS page has routed since it loaded -
    // used by the masthead's "Back" button to decide whether history.back() has
    // somewhere real to go (a page visited earlier in this same session) or
    // would just leave the site entirely (a cold/shared link, nothing to go
    // back to in-app).
    let inAppNavCount = 0;
    function handleHash(){
      inAppNavCount++;
      const h = window.location.hash;
      const mClf = h.match(/^#clf\/(\d+)$/);
      const mDist = h.match(/^#district\/([a-z0-9-]+)$/);
      if(mClf) loadClf(mClf[1]);
      else if(mDist) loadDistrict(mDist[1]);
      else if(h === '#state') loadState();
      else showLanding();
    }

    async function initShell(){
      const [manifest, shared] = await Promise.all([fetchJson('data/manifest.json'), fetchJson('data/shared.json')]);
      MANIFEST = manifest; SHARED = shared;
      TIPS = shared.tips || {}; ERR_MSG_JS = shared.err_msg || {}; FOOTNOTES = shared.footnotes || {}; CONTEXT = shared.context || {};
      ORIGINAL_CONTEXT = Object.assign({}, CONTEXT);
      ORIGINAL_ERR_MSG = Object.assign({}, ERR_MSG_JS);
      ORIGINAL_TIPS = Object.assign({}, TIPS);
      indexManifest(MANIFEST);
      populateDistrictSelect();
      populateGroupDistrictSelect();
      document.getElementById('finder-count').textContent = `${MANIFEST.counts.clfs} CLFs across ${MANIFEST.counts.districts} districts`;

      document.getElementById('sel-district').addEventListener('change', e=>{ populateBlockSelect(e.target.value); });
      document.getElementById('sel-block').addEventListener('change', e=>{ populateClfSelect(document.getElementById('sel-district').value, e.target.value); });
      document.getElementById('sel-clf').addEventListener('change', e=>{ if(e.target.value) window.location.hash = 'clf/'+e.target.value; });
      document.getElementById('btn-go-id').addEventListener('click', ()=>{
        const v = document.getElementById('input-clfid').value.trim();
        if(v) window.location.hash = 'clf/'+v;
      });
      document.getElementById('input-clfid').addEventListener('keydown', e=>{ if(e.key==='Enter') document.getElementById('btn-go-id').click(); });
      document.getElementById('sel-group-district').addEventListener('change', e=>{ if(e.target.value) window.location.hash = 'district/'+e.target.value; });
      document.getElementById('btn-view-state').addEventListener('click', ()=>{ window.location.hash = 'state'; });

      initTooltips();
      window.addEventListener('hashchange', handleHash);
      handleHash();
    }
    initShell();
    """

    # ============================================================================
    # Assemble
    # ============================================================================
    FULL_JS = (JS + JS_OVERVIEW + JS_AUDIT + JS_FINANCIAL + JS_VRF + JS_VPRP + JS_LOANS + JS_SCORING + JS_DISTRICT_STATE + JS_NAV + JS_SHELL)

    # ============================================================================
    # CSS additions for the District/State tracker (cross-links, standing-card
    # grade histogram support already lives in CSS above via .chart-box/.bar-track).
    # ============================================================================
    EXTRA_CSS = r"""
    .district-link{ color:var(--primary); font-weight:600; text-decoration:none; }
    .district-link:hover{ text-decoration:underline; }
    .clf-link{ color:var(--primary); font-weight:600; text-decoration:none; }
    .clf-link:hover{ text-decoration:underline; }
    .clf-link.tier-bottom{ color:var(--low); }
    .standing-grid.single{ grid-template-columns:1fr; }
    """

    HTML = f"""<!DOCTYPE html>
    <html lang="en">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Comprehensive CLF Tracker — Bihar Jeevika</title>
    <style>{CSS}{EXTRA_CSS}</style>
    </head>
    <body>
    <div class="page">
      <div class="top-row"><span class="badge">Comprehensive CLF Tracker &middot; statewide &middot; real computed data</span></div>

      <div id="view-landing" class="active">
        <div class="intro">
          <div class="placeholder-icon">🔎</div>
          <h1 class="serif">Find a CLF</h1>
          <p>Search by District, Block, and CLF, or enter a MIS ID directly.</p>
        </div>
        <div class="finder-panel">
          <div class="nav-label">Browse</div>
          <div class="nav-dropdown-row">
            <select id="sel-district" class="nav-select"><option value="">Select District…</option></select>
            <select id="sel-block" class="nav-select" disabled><option value="">Select Block…</option></select>
            <select id="sel-clf" class="nav-select" disabled><option value="">Select CLF…</option></select>
          </div>
          <div class="nav-divider">or search by MIS ID</div>
          <div class="nav-id-row">
            <input id="input-clfid" class="nav-id-input" type="text" inputmode="numeric" placeholder="e.g. 1264478">
            <button id="btn-go-id" class="nav-go-btn">Go</button>
          </div>
          <div id="nav-error" class="nav-error" style="display:none;"></div>
          <div id="finder-count" class="finder-count"></div>
        </div>

        <div class="finder-panel" style="margin-top:20px;">
          <div class="nav-label">Or Track CLFs Within a District or State</div>
          <select id="sel-group-district" class="nav-select" style="margin-bottom:16px;"><option value="">Select District to view its Tracker…</option></select>
          <div class="nav-divider">or</div>
          <button id="btn-view-state" class="nav-go-btn" style="width:100%;margin-top:16px;">View Bihar Statewide Dashboard</button>
        </div>
      </div>

      <div id="view-tracker">
        <header class="masthead" id="masthead-content"></header>
        <div class="tabbar" id="tabbar"></div>
        <div class="subtabbar" id="subtabbar"></div>
        <div class="tabpanel active" id="panel-overview"></div>
        <div class="tabpanel" id="panel-audit"></div>
        <div class="tabpanel" id="panel-financial"></div>
        <div class="tabpanel" id="panel-vrf"></div>
        <div class="tabpanel" id="panel-vprp"></div>
        <div class="tabpanel" id="panel-loans"></div>
        <div class="tabpanel" id="panel-scoring"></div>
        <div class="foot-note">Statewide Comprehensive CLF Tracker. Data as computed by <code>build_tracker_data.py</code> and <code>build_district_state_data.py</code>.</div>
      </div>
    </div>
    <div class="tooltip-box" id="tooltip-box"></div>
    <script>
    {FULL_JS}
    </script>
    <style>
    #view-landing{{ display:none; }} #view-landing.active{{ display:block; }}
    #view-tracker{{ display:none; }} #view-tracker.active{{ display:block; }}
    </style>
    </body>
    </html>"""

    with open(OUT_PATH, "w") as f:
        f.write(HTML)
    print(f"Wrote {len(HTML)} chars to: {OUT_PATH}")



if __name__ == "__main__":
    print("=== Stage 1/6: base CLF json (build_tracker_data) ===")
    stage_1_build_clf_data()
    print("=== Stage 2/6: VO overview + VPRP detail (build_vprp_vo_data) ===")
    stage_2_build_vprp_vo_data()
    print("=== Stage 3/6: Loans + Fund Disbursement (build_loan_tab_data) ===")
    build_loan_tab_data_main()
    print("=== Stage 4/6: scoring categories + data availability (build_loan_scoring_data) ===")
    stage_4_build_loan_scoring()
    print("=== Stage 5/6: District/State aggregation (build_district_state_data) ===")
    stage_5_build_district_state()
    print("=== Stage 6/6: index.html (make_shell) ===")
    stage_6_make_shell()
    print("=== Full tracker build complete. ===")
