# Pending work

## Build pipeline — now one file

As of 2026-08-25, the 6 previously-separate build scripts (`build_tracker_data.py`,
`build_vprp_vo_data.py`, `build_loan_tab_data.py`, `build_loan_scoring_data.py`,
`build_district_state_data.py`, `make_shell.py`) were consolidated into a single
`build_full_tracker.py`. Run the whole pipeline with one command:

```
python3 build_full_tracker.py
```

Each stage's original logic is preserved exactly (wrapped in its own function,
not rewritten), in the only order that's actually valid — each stage reads
`data/clfs/*.json` written by the one before it:

1. `stage_1_build_clf_data()` — base per-CLF json (overview, audit, financial, VRF, VPRP summary, scoring skeleton)
2. `stage_2_build_vprp_vo_data()` — adds VO roster + richer per-VO/per-GP VPRP detail
3. `build_loan_tab_data_main()` — adds Loans + Fund Disbursement
4. `stage_4_build_loan_scoring()` — splits scoring categories, adds Data Coverage, recomputes Overall Score + ranks
5. `stage_5_build_district_state()` — aggregates every CLF up to District/State
6. `stage_6_make_shell()` — generates `index.html`

Note: `stage_1_build_clf_data()` alone can take 25-30+ minutes and peak around
15GB of memory on a full statewide run (1667 CLFs, two-pass scoring, heavy
regex-based text classification) — this is inherent to that stage's original
logic, not something the consolidation changed. Don't kill it partway through:
it overwrites `data/clfs/*.json` with a *fresh base* per CLF as it goes, so a
kill mid-write leaves some CLFs missing everything stages 2-6 would have added
back (loans, scoring categories, etc.) while the rest keep the full state - a
real, silent inconsistency, not just a stale run. If that happens, restore with
`git checkout HEAD -- data/` rather than trying to patch it by hand.

## Loan Tracker — scaled statewide, wired into District/State

Done as of 2026-08-25:
- **Loans data scaled** from the original Araria-only prototype (53 CLFs) to
  **546 CLFs across 15 districts** (LoanRequests ∩ LoanRepayments coverage).
  **Fund Disbursement scaled to 1,623 CLFs across all 38 districts** (statewide-
  complete) - ahead of Loans, since it's a simpler/faster scrape.
- Fixed a real bug found while scaling: Fund Disbursement was only being
  injected into CLFs that *also* matched on Loans, instead of its own broader
  coverage - split into an independent injection pass.
- **Loans tab (Portfolio Overview only) + Fund Disbursement subtab wired into
  District and State trackers.** The full "All Loans" table and the per-loan
  Loan Schedule subtab don't generalize to group scope and stay CLF-only.
- **Tab order reordered across all three views** (CLF/District/State):
  Overview, Financial Records, Loans, VRF, VPRP, Audit Reports, Scoring & Ranking.
- **State tracker → Scoring & Ranking → District Performance: State Average
  row added**, and made sortable (merges into the normal sort pool rather than
  staying pinned, so you can see where it lands relative to real districts).

Known real data-quality findings baked into the current build (don't re-litigate
these without new evidence):
- `current_outstanding_principal` = `sum(principal)` across a loan's *remaining*
  schedule rows, not LokOS's own `outstanding` column (35% of loans have a
  non-monotonic `outstanding` sequence - looks like restructuring).
- No usable "overdue" flag from the LoanRequests schedule (frozen `dueDate`,
  not synced to actual collection). The Arrears field (from `LendingDetails`,
  live-calculated) does not have this problem.
- Loan Schedule tab reconstructs amortization from scratch, only covers the
  window with real repayment transactions (~Oct/Dec 2024 onward, varies by
  district).
- `loanos_lokos` (LokOS's own live outstanding balance) correlates 0.96 with
  our derived `current_outstanding_principal` but isn't identical - kept as a
  cross-check only.
- 25 rows across 7 CLFs in `clf_fund_disbursement.dta` share an identical
  `latest_txn_date` of 2036-09-15 - almost certainly a source-data year typo
  (probably meant 2026). Visible in the Fund Disbursement subtab's "Latest
  Receipt" column. Not fixed - flagged, pre-existing raw data issue.

## Loan Tracker — pooled portfolio Total XIRR

- **Scope**: pooled per CLF, **active loans only** - LokOS drops a closed
  loan's origination metadata once it closes, so its outflow can never be
  reconstructed.
- **Realized XIRR (no terminal value) was tried and dropped**: for an active,
  mid-tenure loan book, most disbursed principal is legitimately still
  outstanding (not overdue, just not due yet), and Realized XIRR gives that
  zero credit - came out near-uniformly around -85% regardless of actual
  performance. Not shipped.
- **Total XIRR is the only figure shipped**: real disbursement/repayment cash
  flows + the currently-outstanding balance credited back as one final inflow,
  dated today.
- CIF corpus (State → CLF) is **not** part of the XIRR cash flow series - it's
  capital funding the lending activity, not a return on it.
- **District/State Total XIRR is genuinely re-solved**, not averaged: every
  matched CLF's own cash-flow series (`loans.xirr_cashflows` in the CLF json)
  is pooled and `xirr()` is re-run on the combined series at group scope -
  averaging per-CLF XIRR percentages wouldn't be mathematically valid the way
  pooling the underlying flows and re-solving is.

## Scoring & Ranking — 7 categories, Data Coverage now scored

Done as of 2026-08-25. Categories, in display order: **Financial Health, Fund
Utilization, Loan Portfolio, VRF Fund Health, Governance & Compliance, Welfare
and Livelihood, Data Coverage.**

- **"Fund Utilization & Loan Activity" split** into "Fund Utilization" (the
  original 4 F01/F03/F05-based metrics, unchanged) and a new **"Loan
  Portfolio"** category (Repayment Rate, Active Lending Turnover, Arrears
  Rate, Total XIRR) - percentile-scored against only the CLFs that actually
  have Loans data (546), not the full 1,667-CLF state population.
- **Funding Source Diversity was tried in Loan Portfolio and dropped**: Fund
  Disbursement is statewide while Loans isn't, so a CLF/district with zero
  loan-scrape coverage could still show a nonzero "Loan Portfolio" score from
  that one metric alone - misleading given the category name. It isn't scored
  anywhere currently; still visible as plain data on the Fund Disbursement
  subtab.
- **"Data Coverage" is a real, scored 7th category** (not just an
  informational panel, per explicit direction) - one metric, "Data Sources
  Available" (count out of 11: Profile, Audit, F01/F03/F05, VRF, VPRP
  Entitlements/PGSRD/SDP, Fund Disbursement, Loans), percentile-ranked against
  all 1,667 CLFs. The detailed per-source checklist/coverage breakdown is
  shown alongside the score in By Category.
  - Loans and Fund Disbursement/LendingDetails/LoanRequests/LoanRepayments are
    tracked as **2 combined points** (not 4 separate ones as originally
    scoped) - they're already merged into one `loans` block upstream with no
    way to tell them apart from the CLF json alone.
- **Category-level rank** (state_rank/district_rank, shown in Category
  Summary) is computed for the 2 new categories too, via the same full-
  population re-rank pattern used for Overall Score - this was missed on the
  first pass (only each category's individual *metric* rank was computed,
  which is what By Category reads; Category Summary reads the category's own
  composite rank, which was left `None` and showed "Not Found" until fixed).
- **Overall Score recomputed and fully re-ranked statewide** for every CLF,
  every quarter, to reflect the new categories - a partial re-rank restricted
  to just the CLFs whose score changed would have been wrong, since one CLF's
  score changing shifts every other CLF's rank too.
- CLF Status label fixed: "Neither" → "Neither Model nor Registered" (a bare
  "Neither" doesn't say neither *what*).

## Not yet done

- **Weight the new categories' internal metric mix.** Fund Utilization now has
  4 metrics and Loan Portfolio has 4 - no explicit decision was made on
  whether metrics *within* a category should be weighted unevenly (currently
  flat average, same as every other category).
- **Parent-directory cleanup** (`3_Output/CLF Tracker/`, one level up from
  `Scale-Up/` - NOT part of this git repo, no undo if deleted): superseded
  prototype scripts and their outputs from earlier in the project -
  `build_comprehensive_clf_tracker.py`, `comprehensive_clf_tracker.html`,
  `build_district_tracker_prototype.py`, `district_tracker_prototype.html`
  (5.9MB), `build_vprp_vo_extra_data.py`, `saran_vprp_vo_extra.json`, and a
  28MB `_scratch_saran_members.pkl`. None of it is referenced by the current
  pipeline. Flagged, not deleted - ask before touching anything outside git.
