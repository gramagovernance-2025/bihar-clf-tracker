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


def main():
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


if __name__ == "__main__":
    main()
