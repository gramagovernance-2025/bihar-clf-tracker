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
