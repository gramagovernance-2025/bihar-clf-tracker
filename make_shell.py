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
.nav-id-input{ flex:1; min-height:46px; font-family:inherit; font-size:15px; border:1px solid var(--line-strong); border-radius:8px; padding:10px 14px; color:var(--ink); }
.nav-go-btn{ appearance:none; cursor:pointer; font-family:inherit; font-size:14px; font-weight:700; color:#fff; background:var(--primary); border:none; border-radius:8px; padding:0 22px; min-height:46px; }
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
    ? `${rank}<sup>${ord(rank)}</sup><span class="outof">out of ${n} CLFs</span>`
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
  const dRank = m.district_rank!=null && m.n_district!=null ? `${m.district_rank}${ord(m.district_rank)} of ${m.n_district}` : ERR_MSG_JS.not_found;
  const sRank = m.state_rank!=null && m.n_state!=null ? `${m.state_rank}${ord(m.state_rank)} of ${m.n_state}` : ERR_MSG_JS.not_found;
  return `<div class="metric-pctl-row">
    <div style="font-size:13.5px;">${lbl}</div>
    <div><div style="font-size:11px;color:var(--ink-soft);display:flex;justify-content:space-between;margin-bottom:4px;"><span style="color:var(--primary);font-weight:700;">DISTRICT</span><span>${dRank}</span></div>
      <div class="bar-track"><div class="fill" style="width:${m.district_pctl||0}%;background:var(--primary)"></div></div></div>
    <div><div style="font-size:11px;color:var(--ink-soft);display:flex;justify-content:space-between;margin-bottom:4px;"><span style="color:var(--gold);font-weight:700;">STATE</span><span>${sRank}</span></div>
      <div class="bar-track"><div class="fill" style="width:${m.state_pctl||0}%;background:var(--gold)"></div></div></div>
  </div>`;
}
function gradedBar(label, pct, tip){
  const t = tip || TIPS[label];
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
function makeSortableTable(containerId, cols, data, rowBuilder, tip){
  const state = { col: null, dir: 1 };
  function draw(){
    const sorted = state.col === null ? data : data.slice().sort((a, b) => {
      let av = a[cols[state.col].key], bv = b[cols[state.col].key];
      if(typeof av === 'string'){ av = av.toUpperCase(); bv = bv.toUpperCase(); }
      if(av == null) return 1; if(bv == null) return -1;
      if(av < bv) return -1 * state.dir;
      if(av > bv) return 1 * state.dir;
      return 0;
    });
    const rows = sorted.map(rowBuilder);
    const theadHtml = `<tr>${cols.map((c,i)=>{
      const t = c.tip || tip || TIPS[c.label];
      const cls = [c.num?'num':'', t?'th-tip':'', c.key?'sortable':''].filter(Boolean).join(' ');
      const arrow = c.key ? ` <span class="sort-arrow${state.col===i?' active':''}">${state.col===i?(state.dir===1?'▲':'▼'):'⇅'}</span>` : '';
      return `<th${cls?` class="${cls}"`:''}${t?` data-tip="${t}"`:''}${c.key?` data-colidx="${i}"`:''}>${c.label}${arrow}</th>`;
    }).join('')}</tr>`;
    const tbodyHtml = rows.map(r=>`<tr>${r.map((v,i)=>`<td${cols[i].num?' class="num"':''}>${v}</td>`).join('')}</tr>`).join('');
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
  const statusCls = !o.status_tier_found ? 'neutral' : (o.status_tier==='Model & Registered' ? '' : (o.status_tier==='Neither' ? 'neg' : 'warn'));
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
function renderOverview(sub){
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
      ${q.is_real ? `
      <div class="statement-grid">
        <div><h3 style="font-size:13.5px;margin:0 0 8px;">Receipts</h3>${tableHtml([{label:'Line Item'},{label:'Amount',num:true}], recvRows)}</div>
        <div><h3 style="font-size:13.5px;margin:0 0 8px;">Payments</h3>${tableHtml([{label:'Line Item'},{label:'Amount',num:true}], payRows)}</div>
      </div>` : `<p class="disclaimer">${ERR_MSG_JS.receipts_payments}</p>`}
    </div></section>`;
}
function renderFinancial(sub){
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
      ${tile('FSF-Eligible VOs', v.fsf_eligible+' of '+v.n_vo)}
      ${tile('VOs With Incomplete Coverage', v.incomplete_coverage+' of '+v.n_vo, null, v.incomplete_coverage>0?'neg':'')}
      ${tile('Idle VOs', v.idle_vo+' of '+v.received_vo, null, v.idle_vo>0?'neg':'')}
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
      <div class="attn-card"><h3 class="tip" data-tip="${TIPS['Grant Not Fully Received']}">Grant Not Fully Received</h3><div class="attn-headline">${v.incomplete_coverage} of ${v.n_vo} VOs</div>
        <p class="desc" style="max-width:none;">Every VO can receive up to ₹1,50,000 from the government. This shows how many of your VOs haven't received the full amount yet — ${fmtRs(v.coverage_gap)} in total is still owed to them.</p>
        <div class="pictogram" id="pict-coverage"></div>
        <div class="pict-legend">
          <span><span class="sw" style="background:var(--primary)"></span><span>Fully received (${v.n_vo-v.incomplete_coverage})</span></span>
          <span><span class="sw" style="background:var(--gold)"></span><span>Still owed (${v.incomplete_coverage})</span></span>
        </div></div>
      <div class="attn-card"><h3 class="tip" data-tip="${TIPS['Fund Sitting Idle']}">Fund Sitting Idle</h3><div class="attn-headline">${v.idle_vo} of ${v.received_vo} VOs</div>
        <p class="desc" style="max-width:none;">Of the VOs that have received VRF funds, this shows how many earned no interest from lending last year — meaning that portion of the fund is sitting unused instead of helping members.</p>
        <div class="pictogram" id="pict-interest"></div>
        <div class="pict-legend">
          <span><span class="sw" style="background:var(--primary)"></span><span>Earning interest (${earning})</span></span>
          <span><span class="sw" style="background:var(--gold)"></span><span>Idle, no interest (${v.idle_vo})</span></span>
          <span><span class="sw" style="background:var(--grey)"></span><span>Not yet received VRF (${notReceived})</span></span>
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
    ${v.metrics.map(m=>pctlRow(m.label, m, TIPS['VRF Fund Health'])).join('')}
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
const PGSRD_COLORS = ['var(--primary)','var(--gold)','#5B8AA6'];
const SDP_COLORS = ['var(--primary)','var(--gold)','#5B8AA6','var(--low)','var(--grey)'];
function yearDropdown(){
  const opts = [2023,2024,2025].map(y=>`<option value="${y}" ${y===vprpYear?'selected':''}>${y}</option>`).join('');
  return `<div class="selectbar"><label for="yr-select">Year:</label><select id="yr-select">${opts}</select></div>`;
}
function emptyYearNote(domainKey){
  const msg = (ERR_MSG_JS[domainKey] || 'We could not locate data for your CLF in {year}.').replace('{year}', vprpYear);
  return `<p class="disclaimer">${msg}</p>`;
}
function renderVprpEnt(){
  const y = DATA.vprp.years[vprpYear];
  if(!y || !y.n_demands) return emptyYearNote('vprp_ent');
  const accessedNote = vprpYear===2025 ? ` — this year is recent, so accessed status may not be fully updated yet` : '';
  const schemeRows = (y.by_scheme||[]).map(s=>{
    const tds = `<td>${s.scheme}</td><td class="num">${fmtNum(s.demanded)}</td>`;
    let extra = '';
    if(s.raw_scheme==='state-specific' && y.state_scheme_breakdown){
      const subRows = Object.entries(y.state_scheme_breakdown).map(([k,v])=>`<tr><td style="padding-left:28px;color:var(--ink-soft);">${k}</td><td class="num">${fmtNum(v)}</td></tr>`).join('');
      extra = `<tr><td colspan="2" style="padding:2px 0 8px 10px;"><table style="width:100%;"><tbody>${subRows}</tbody></table></td></tr>`;
    }
    return `<tr>${tds}</tr>${extra}`;
  }).join('');
  return `<section><div class="section-head"><h2 class="serif">Entitlements</h2></div>
    <div class="panel">
      <div class="tiles n3" style="margin-bottom:16px;">
        ${tile('Number of People Requesting', fmtNum(y.n_demands))}
        ${tile('% of Job Cards Accessed', fmtPct(y.pct_nrega_accessed), vprpYear===2025?'recent year, may be incomplete':null)}
        ${tile('Non-NREGA Schemes Requested', fmtNum(y.n_other_schemes), 'distinct scheme types')}
      </div>
      ${y.by_scheme ? `<p class="hint tip" style="margin-bottom:8px;" data-tip="${TIPS['Demand & Access by Scheme']}"><b>Demand by Scheme</b>${accessedNote}</p>
        <div class="table-wrap"><table><thead><tr><th>Scheme</th><th class="num">Demanded</th></tr></thead><tbody>${schemeRows}</tbody></table></div>` : ''}
    </div></section>`;
}
function renderVprpPgsrd(){
  const y = DATA.vprp.years[vprpYear];
  if(!y || !y.n_pgsrd) return emptyYearNote('vprp_pgsrd');
  const items = (y.pgsrd_items||[]).map(it=>[it.item_demanded, it.pgsrd_type, it.n, Math.round(it.units)]);
  const typeDonut = y.pgsrd_type_split ? donutBlock('ring-pgsrd-type', y.pgsrd_type_split, PGSRD_COLORS, {size:150, fmt:v=>v+'%'}) : null;
  return `<section><div class="section-head"><h2 class="serif">Public Goods, Services, and Resource Development</h2></div>
    <div class="panel">
      <div class="tiles n1" style="margin-bottom:16px;">${tile('Total PGSRD Requests', fmtNum(y.n_pgsrd))}</div>
      ${typeDonut?`<p class="hint tip" data-tip="${TIPS['Type of Request']}"><b>Type of Request</b></p>${typeDonut.html}`:''}
      ${items.length?`<p class="hint tip" style="margin-top:18px;" data-tip="${TIPS['Most-Requested Items']}"><b>Most-Requested Items</b></p>${tableHtml([{label:'Item'},{label:'Type'},{label:'# Requests',num:true},{label:'Total Units',num:true}], items)}`:''}
      ${y.sdg_theme?`<p class="hint tip" style="margin-top:18px;" data-tip="${TIPS['By Development Theme']}"><b>By Development Theme</b></p><div class="pills" style="margin-bottom:8px;">${Object.entries(y.sdg_theme).map(([k,v])=>`<span class="pill on">${k} (${v})</span>`).join('')}</div>`:''}
      ${y.gpdp_area?`<p class="hint tip" style="margin-top:14px;" data-tip="${TIPS['By GPDP Focus Area']}"><b>By GPDP Focus Area</b></p><div class="pills">${Object.entries(y.gpdp_area).map(([k,v])=>`<span class="pill on">${k} (${v})</span>`).join('')}</div>`:''}
    </div></section>`;
}
function renderVprpSdp(){
  const y = DATA.vprp.years[vprpYear];
  if(!y || !y.n_sdp) return emptyYearNote('vprp_sdp');
  const issues = (y.sdp_issues||[]).map(it=>[it.social_issue, it.n, it.affected!==null?fmtNum(it.affected):'not reported']);
  const sectorDonut = y.sdp_sector ? donutBlock('ring-sdp-sector', y.sdp_sector, SDP_COLORS, {size:150, fmt:v=>v+'%'}) : null;
  return `<section><div class="section-head"><h2 class="serif">Social Development Plan</h2></div>
    <div class="panel">
      <div class="tiles n1" style="margin-bottom:16px;">${tile('Total Social Issues Raised', fmtNum(y.n_sdp))}</div>
      ${sectorDonut?`<p class="hint tip" data-tip="${TIPS['By Sector']}"><b>By Sector</b></p>${sectorDonut.html}`:''}
      ${issues.length?`<p class="hint tip" style="margin-top:18px;" data-tip="${TIPS['Most-Raised Issues']}"><b>Most-Raised Issues</b></p>${tableHtml([{label:'Issue'},{label:'# Occurrences',num:true},{label:'People Affected',num:true}], issues)}`:''}
      ${y.departments?`<p class="hint tip" style="margin-top:18px;" data-tip="${TIPS['Government Departments Involved']}"><b>Government Departments Involved</b></p><div class="pills">${Object.entries(y.departments).map(([k,v])=>`<span class="pill on">${k} (${v})</span>`).join('')}</div>`:''}
    </div></section>`;
}
function renderVPRP(sub){
  const y = DATA.vprp.years[vprpYear];
  const body = sub==='entitlements' ? renderVprpEnt() : sub==='pgsrd' ? renderVprpPgsrd() : renderVprpSdp();
  document.getElementById('panel-vprp').innerHTML = contextBox('vprp') + yearDropdown() + body;
  document.getElementById('yr-select').addEventListener('change', e=>{ vprpYear=+e.target.value; renderVPRP(sub); });
  if(sub==='pgsrd' && y && y.pgsrd_type_split) donutBlock('ring-pgsrd-type', y.pgsrd_type_split, PGSRD_COLORS, {size:150, fmt:v=>v+'%'}).draw();
  if(sub==='sdp' && y && y.sdp_sector) donutBlock('ring-sdp-sector', y.sdp_sector, SDP_COLORS, {size:150, fmt:v=>v+'%'}).draw();
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
  return `<section><div class="section-head"><h2 class="serif tip" data-tip="${TIPS['Overall Score']}">Overall Score</h2><span class="hint">equal weight across all 5 categories by default - customize below</span></div>
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
      <div class="panel">${cat.metrics.map(([label,m])=>pctlRow(label, m, catName==='VRF Fund Health'?TIPS['VRF Fund Health']:undefined)).join('')}</div></section>`).join('');
}
// ============================================================================
// Customizable category weights. The recomputed score itself needs only this
// CLF's own category scores (already in DATA); a rank against other CLFs
// needs everyone else's category scores too, which don't exist anywhere in
// this CLF's own JSON - data/scoring_summary.json (a compact, statewide,
// category-scores-only file) is fetched once, lazily, on first open, and
// cached like every other fetch in this app.
// ============================================================================
const WEIGHT_CATS = ['Fund Utilization & Loan Activity', 'Financial Health', 'VRF Fund Health', 'Governance & Compliance', 'Welfare and Livelihood'];
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
    <p class="note-inline">Fund Deployment, Surplus / Deficit, Bookkeeping Accuracy, and every VRF Fund Health, Governance &amp; Compliance, and Welfare and Livelihood metric reflect current standing and don't change by quarter — only the other Fund Utilization &amp; Financial Health line items update.</p>`;
  document.getElementById('panel-scoring').innerHTML =
    contextBox('scoring') + dropdown + (sub==='overall' ? renderScoringOverall() : renderScoringByCategory());
  document.getElementById('score-qtr-select').addEventListener('change', e=>{ scoreQtrIdx=+e.target.value; renderScoring(sub); });
  if(sub==='overall'){
    document.getElementById('btn-open-weights').addEventListener('click', openWeightModal);
  }
}
"""

JS_NAV = r"""
const TABS = {
  overview: {label:'Overview', render:renderOverview, subtabs:{profile:'Profile', members:'Members'}},
  audit: {label:'Audit Reports', render:renderAudit, subtabs:null},
  financial: {label:'Financial Records', render:renderFinancial, subtabs:{summary:'Summary', statements:'Statements'}},
  vrf: {label:'Vulnerability Reduction Fund', render:renderVRF, subtabs:{kpi:'KPI Snapshot', vobreak:'VO-Level Breakdown', bkrank:'Bookkeeper & CLF Rankings', forecasts:'Forecasts'}},
  vprp: {label:'Village Poverty Reduction Plan', render:renderVPRP, subtabs:{entitlements:'Entitlements', pgsrd:'Public Goods, Services, and Resource Development', sdp:'Social Development Plan'}},
  scoring: {label:'Scoring & Ranking', render:renderScoring, subtabs:{overall:'Overall', bycategory:'By Category'}},
};
let currentTab='overview', currentSub='profile';

function renderTabBar(){
  document.getElementById('tabbar').innerHTML = Object.entries(TABS).map(([id,t])=>
    `<button class="tabbtn ${id===currentTab?'active':''}" data-tab="${id}">${t.label}</button>`).join('');
  document.querySelectorAll('.tabbtn').forEach(b=>b.addEventListener('click',()=>{
    currentTab=b.dataset.tab; currentSub=TABS[currentTab].subtabs ? Object.keys(TABS[currentTab].subtabs)[0] : null; renderAll();
  }));
}
function renderSubtabBar(){
  const subs = TABS[currentTab].subtabs;
  const bar = document.getElementById('subtabbar');
  if(!subs){ bar.innerHTML = ''; bar.style.display = 'none'; return; }
  bar.style.display = '';
  bar.innerHTML = Object.entries(subs).map(([id,label])=>
    `<button class="subtabbtn ${id===currentSub?'active':''}" data-sub="${id}">${label}</button>`).join('');
  document.querySelectorAll('.subtabbtn').forEach(b=>b.addEventListener('click',()=>{
    currentSub=b.dataset.sub; renderAll();
  }));
}
function renderAll(){
  renderTabBar(); renderSubtabBar();
  document.querySelectorAll('.tabpanel').forEach(p=>p.classList.toggle('active', p.id==='panel-'+currentTab));
  TABS[currentTab].render(currentSub);
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

function showLandingError(msg){
  const el = document.getElementById('nav-error');
  el.textContent = msg; el.style.display = 'block';
}
function clearLandingError(){
  document.getElementById('nav-error').style.display = 'none';
}

async function loadClf(misId){
  misId = +misId;
  if(!CLF_INDEX[misId]){ showLandingError(`No CLF found with MIS ID ${misId}.`); return; }
  clearLandingError();
  try{
    const data = await fetchJson(`data/clfs/${misId}.json`);
    DATA = data;
    const info = CLF_INDEX[misId];
    document.getElementById('crumb-name').textContent = data.overview.clf_name_lokos.replace(/\w\S*/g, t=>t.charAt(0).toUpperCase()+t.substr(1).toLowerCase());
    document.getElementById('crumb-district').textContent = info.district;
    document.getElementById('crumb-block').textContent = info.block;
    document.getElementById('crumb-mis').textContent = misId;
    document.getElementById('crumb-code').textContent = data.overview.clfcode;
    document.getElementById('view-landing').classList.remove('active');
    document.getElementById('view-tracker').classList.add('active');
    currentTab='overview'; currentSub='profile'; finQtrIdx = DATA.financial.quarters.length - 1; vprpYear = 2025; scoreQtrIdx = DATA.scoring.default_idx;
    renderAll();
    window.location.hash = 'clf/'+misId;
  } catch(e){
    showLandingError(`Could not load data for MIS ID ${misId}. It may not have data available.`);
  }
}

function backToSearch(){
  document.getElementById('view-tracker').classList.remove('active');
  document.getElementById('view-landing').classList.add('active');
  window.location.hash = '';
}

async function initShell(){
  const [manifest, shared] = await Promise.all([fetchJson('data/manifest.json'), fetchJson('data/shared.json')]);
  MANIFEST = manifest; TIPS = shared.tips || {}; ERR_MSG_JS = shared.err_msg || {}; FOOTNOTES = shared.footnotes || {}; CONTEXT = shared.context || {};
  indexManifest(MANIFEST);
  populateDistrictSelect();
  document.getElementById('finder-count').textContent = `${MANIFEST.counts.clfs} CLFs across ${MANIFEST.counts.districts} districts`;

  document.getElementById('sel-district').addEventListener('change', e=>{ populateBlockSelect(e.target.value); });
  document.getElementById('sel-block').addEventListener('change', e=>{ populateClfSelect(document.getElementById('sel-district').value, e.target.value); });
  document.getElementById('sel-clf').addEventListener('change', e=>{ if(e.target.value) loadClf(e.target.value); });
  document.getElementById('btn-go-id').addEventListener('click', ()=>{
    const v = document.getElementById('input-clfid').value.trim();
    if(v) loadClf(v);
  });
  document.getElementById('input-clfid').addEventListener('keydown', e=>{ if(e.key==='Enter') document.getElementById('btn-go-id').click(); });
  document.getElementById('back-link').addEventListener('click', backToSearch);

  initTooltips();

  const hashMatch = window.location.hash.match(/^#clf\/(\d+)$/);
  if(hashMatch) loadClf(hashMatch[1]);
}
initShell();
"""

# ============================================================================
# Assemble
# ============================================================================
FULL_JS = (JS + JS_OVERVIEW + JS_AUDIT + JS_FINANCIAL + JS_VRF + JS_VPRP + JS_SCORING + JS_NAV + JS_SHELL)

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Comprehensive CLF Tracker — Bihar Jeevika</title>
<style>{CSS}</style>
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
  </div>

  <div id="view-tracker">
    <button id="back-link" class="back-link">&larr; Back to search</button>
    <header class="masthead">
      <h1 class="serif" id="crumb-name"></h1>
      <div class="crumbs"><b id="crumb-district"></b> District &nbsp;&middot;&nbsp; <b id="crumb-block"></b> Block &nbsp;&middot;&nbsp; MIS ID <b id="crumb-mis"></b> &nbsp;&middot;&nbsp; LokOS Code <b id="crumb-code"></b></div>
    </header>
    <div class="tabbar" id="tabbar"></div>
    <div class="subtabbar" id="subtabbar"></div>
    <div class="tabpanel active" id="panel-overview"></div>
    <div class="tabpanel" id="panel-audit"></div>
    <div class="tabpanel" id="panel-financial"></div>
    <div class="tabpanel" id="panel-vrf"></div>
    <div class="tabpanel" id="panel-vprp"></div>
    <div class="tabpanel" id="panel-scoring"></div>
    <div class="foot-note">Statewide Comprehensive CLF Tracker. Data as computed by <code>build_tracker_data.py</code>.</div>
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
