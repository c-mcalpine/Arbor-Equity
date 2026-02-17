"""
Public Equity Command Center — Phase 1 + Phase 2.
Tabs: Portfolio Monitor (Triage), Earnings Control Room.
Styling aligned with PEG dashboard (institutional theme).
"""
import os
import sys
from datetime import date, timedelta, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import streamlit as st
import pandas as pd

from models.db import get_connection

# Page config (match PEG)
st.set_page_config(
    page_title="Equity Command Center",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS styling — same institutional theme as PEG dashboard
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    .main {
        background-color: #FAFAFA;
        padding-top: 0.5rem !important;
    }
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
        max-width: 1400px;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .institutional-header {
        background: linear-gradient(90deg, #0F4C81 0%, #1a5a96 100%);
        color: white;
        padding: 0.75rem 1.5rem;
        margin: -1rem -1rem 1.5rem -1rem;
        border-bottom: 3px solid #0d3d6b;
    }
    .institutional-header h1 {
        font-size: 20px;
        font-weight: 600;
        margin: 0;
        letter-spacing: 0.02em;
    }
    .institutional-header .subtitle {
        font-size: 11px;
        opacity: 0.85;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-top: 2px;
    }
    h3 {
        font-size: 14px !important;
        font-weight: 600 !important;
        color: #1F2937 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-top: 1.5rem !important;
        margin-bottom: 0.75rem !important;
        border-bottom: 1px solid #D1D5DB;
        padding-bottom: 0.5rem;
    }
    h4 {
        font-size: 12px !important;
        font-weight: 600 !important;
        color: #374151 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-top: 1rem !important;
        margin-bottom: 0.5rem !important;
    }
    div[data-testid="metric-container"] {
        background-color: #FFFFFF;
        border: 1px solid #D1D5DB;
        padding: 0.5rem 0.75rem;
        border-radius: 2px;
        box-shadow: none;
    }
    div[data-testid="metric-container"] > label {
        font-size: 10px !important;
        font-weight: 600;
        color: #6B7280 !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    div[data-testid="metric-container"] > div {
        font-size: 20px !important;
        font-weight: 700 !important;
        color: #111827 !important;
        font-variant-numeric: tabular-nums;
    }
    div[data-testid="metric-container"] [data-testid="stMetricDelta"] { font-size: 12px !important; }
    div[data-testid="metric-container"] [data-testid="stMetricDelta"] svg { display: none; }
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        background: linear-gradient(to bottom, #FFFFFF 0%, #F3F4F6 100%);
        padding: 0;
        border-radius: 0;
        border: none;
        border-bottom: 2px solid #D1D5DB;
        margin-bottom: 1.5rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 2.5rem;
        padding: 0 1.25rem;
        font-size: 11px;
        font-weight: 600;
        color: #4B5563;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        border-radius: 0;
        border-right: 1px solid #E5E7EB;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background-color: #F9FAFB;
        color: #1F2937;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(to bottom, #0F4C81 0%, #0d3d6b 100%);
        color: white;
        border-bottom: 3px solid #0a2e4f;
    }
    .dataframe {
        font-size: 12px !important;
        border: none !important;
        font-variant-numeric: tabular-nums;
    }
    .dataframe thead th {
        background: linear-gradient(to bottom, #F9FAFB 0%, #F3F4F6 100%);
        color: #1F2937 !important;
        font-weight: 700;
        text-transform: uppercase;
        font-size: 10px !important;
        letter-spacing: 0.08em;
        padding: 0.5rem 0.75rem !important;
        border-bottom: 2px solid #D1D5DB !important;
        border-top: 1px solid #D1D5DB !important;
        text-align: left;
    }
    .dataframe tbody td {
        padding: 0.4rem 0.75rem !important;
        border-bottom: 1px solid #F3F4F6 !important;
        color: #374151;
    }
    .dataframe tbody tr:hover { background-color: #F9FAFB !important; }
    .stAlert {
        background-color: #F9FAFB;
        border: 1px solid #D1D5DB;
        border-left: 3px solid #4A637D;
        border-radius: 2px;
        padding: 0.75rem 1rem;
        font-size: 12px;
    }
    .stWarning { border-left-color: #92400E !important; background-color: #FFFBEB; }
    .stSuccess { border-left-color: #3A6F4C !important; background-color: #F0FDF4; }
    .stInfo { border-left-color: #0F4C81 !important; background-color: #EFF6FF; }
    .stButton button {
        background: linear-gradient(to bottom, #0F4C81 0%, #0d3d6b 100%);
        color: white;
        border: none;
        border-radius: 2px;
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        padding: 0.5rem 1rem;
    }
    .stButton button:hover {
        background: linear-gradient(to bottom, #0d3d6b 0%, #0a2e4f 100%);
    }
    .stSelectbox label, .stTextInput label {
        font-size: 11px !important;
        font-weight: 600;
        color: #4B5563 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .streamlit-expanderHeader {
        font-size: 12px;
        font-weight: 600;
        color: #374151;
        background-color: #F9FAFB;
        border-radius: 2px;
    }
    .element-container { margin-bottom: 0.5rem; }
</style>
""", unsafe_allow_html=True)

# Institutional header bar (match PEG)
current_date_str = datetime.now().strftime("%B %d, %Y")
st.markdown(f'''
<div class="institutional-header">
    <div style="display: flex; justify-content: space-between; align-items: center;">
        <div>
            <div style="font-size: 10px; font-weight: 500; opacity: 0.7; letter-spacing: 0.15em; margin-bottom: 4px;">EQUITY INFRA</div>
            <h1 style="margin: 0;">PUBLIC EQUITY COMMAND CENTER</h1>
        </div>
        <div style="text-align: right; font-size: 10px; opacity: 0.85;">
            <div>As of {current_date_str}</div>
            <div style="margin-top: 2px;">Portfolio Monitor — Triage & Earnings</div>
        </div>
    </div>
    <div class="subtitle">100+ Names — Returns, Alpha, Vol, Earnings Workflow</div>
</div>
''', unsafe_allow_html=True)

def pct(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    return f"{float(x) * 100:.2f}%"

conn = get_connection()
cur = conn.cursor()

# --- Portfolio KPIs (above tabs) ---
cur.execute("""
    SELECT as_of_date, return_24h, return_7d, return_mtd, return_qtd, return_ytd,
           alpha_vs_sp500_24h, alpha_vs_sp500_7d, alpha_vs_sp500_mtd, alpha_vs_sp500_qtd, alpha_vs_sp500_ytd,
           alpha_vs_nasdaq_24h, alpha_vs_nasdaq_7d, alpha_vs_nasdaq_mtd, alpha_vs_nasdaq_qtd, alpha_vs_nasdaq_ytd,
           alpha_vs_tbill_24h, alpha_vs_tbill_7d, alpha_vs_tbill_mtd, alpha_vs_tbill_qtd, alpha_vs_tbill_ytd
    FROM feat.feat_portfolio ORDER BY as_of_date DESC LIMIT 1
""")
row = cur.fetchone()
if row:
    as_of, r24, r7, rm, rq, ry, a_sp24, a_sp7, a_spm, a_spq, a_spy, a_na24, a_na7, a_nam, a_naq, a_nay, a_tb24, a_tb7, a_tbm, a_tbq, a_tby = row
    st.markdown(f"### Portfolio KPIs (as of {as_of})")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("24h", pct(r24), None)
    c2.metric("7d", pct(r7), None)
    c3.metric("MTD", pct(rm), None)
    c4.metric("QTD", pct(rq), None)
    c5.metric("YTD", pct(ry), None)
    st.caption("Alpha vs S&P 500: " + " | ".join([f"24h {pct(a_sp24)}", f"7d {pct(a_sp7)}", f"MTD {pct(a_spm)}", f"QTD {pct(a_spq)}", f"YTD {pct(a_spy)}"]) +
               "  |  vs Nasdaq: " + " | ".join([f"24h {pct(a_na24)}", f"YTD {pct(a_nay)}"]) +
               "  |  vs 3M T-bill: YTD " + pct(a_tby))
else:
    st.info("No portfolio KPIs yet. Run bootstrap → ingest → feat_returns.")

tab1, tab2 = st.tabs(["Portfolio Monitor (Triage)", "Earnings Control Room"])

# ---------- Tab 1: Portfolio Monitor (Triage) ----------
with tab1:
    cur.execute("SELECT MAX(as_of_date) FROM feat.feat_returns")
    latest_ret = cur.fetchone()[0]
    if not latest_ret:
        st.write("Run feat_returns job to populate monitor.")
    else:
        pos_date_sql = "(SELECT MAX(as_of_date) FROM core.core_positions)"
        q = f"""
        SELECT m.ticker, COALESCE(p.weight, 0) AS weight,
               r.return_24h, r.return_7d, r.return_mtd, r.return_qtd, r.return_ytd,
               r.vol_spike_ratio, r.drawdown_52w, r.what_changed_score,
               e.next_earnings_date,
               COALESCE(v.pct_historical, 0) AS val_pct,
               (SELECT EXISTS (SELECT 1 FROM feat.feat_rpo fr WHERE fr.security_id = m.id)) AS has_rpo,
               (SELECT EXISTS (SELECT 1 FROM core.core_estimates ce WHERE ce.security_id = m.id)) AS has_estimates
        FROM core.core_security_master m
        LEFT JOIN core.core_positions p ON p.security_id = m.id AND p.as_of_date = {pos_date_sql}
        LEFT JOIN feat.feat_returns r ON r.security_id = m.id AND r.as_of_date = %s
        LEFT JOIN (
            SELECT security_id, MIN(event_date) AS next_earnings_date
            FROM core.core_events_earnings WHERE event_date >= CURRENT_DATE GROUP BY security_id
        ) e ON e.security_id = m.id
        LEFT JOIN (
            SELECT security_id, pct_historical FROM feat.feat_valuation
            WHERE as_of_date = (SELECT MAX(as_of_date) FROM feat.feat_valuation)
        ) v ON v.security_id = m.id
        """
        cur.execute(q, (latest_ret,))
        rows = cur.fetchall()
        df_raw = pd.DataFrame(rows, columns=[
            "Ticker", "Weight", "24h", "7d", "MTD", "QTD", "YTD",
            "Vol spike", "Drawdown 52w", "What changed", "Next earnings", "Val pct", "RPO", "Est"
        ])
        df_raw["Weight"] = df_raw["Weight"].fillna(0)
        df_raw["What changed"] = df_raw["What changed"].fillna(0)
        df_raw["Vol spike"] = pd.to_numeric(df_raw["Vol spike"], errors="coerce")
        df_raw["Drawdown 52w"] = pd.to_numeric(df_raw["Drawdown 52w"], errors="coerce")

        with st.expander("Filters", expanded=True):
            earnings_14d = st.checkbox("Earnings in next 14d", value=False)
            largest_dislocations = st.checkbox("Largest dislocations (by |drawdown|)", value=False)
            largest_vol_spikes = st.checkbox("Largest vol spikes", value=False)
            big_weights_only = st.checkbox("Big weights only (≥5%)", value=False)

        out = df_raw.copy()
        if earnings_14d:
            today = date.today()
            out = out[out["Next earnings"].notna() & out["Next earnings"].apply(lambda d: (d - today).days <= 14 and (d - today).days >= 0)]
        if big_weights_only:
            out = out[out["Weight"] >= 0.05]
        if largest_dislocations:
            out = out.copy()
            out["_abs_dd"] = out["Drawdown 52w"].abs().fillna(0)
            out = out.sort_values("_abs_dd", ascending=False).head(50).drop(columns=["_abs_dd"]).reset_index(drop=True)
        elif largest_vol_spikes:
            out = out.sort_values("Vol spike", ascending=False, na_position="last").head(50).reset_index(drop=True)
        else:
            out = out.sort_values("What changed", ascending=False).reset_index(drop=True)

        # Format for display
        out = out.copy()
        out["Weight"] = out["Weight"].apply(lambda x: pct(x))
        out["RPO"] = out["RPO"].apply(lambda x: "Y" if x else "—")
        out["Est"] = out["Est"].apply(lambda x: "Y" if x else "—")
        for c in ["24h", "7d", "MTD", "QTD", "YTD"]:
            out[c] = out[c].apply(lambda x: pct(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else "—")
        out["Vol spike"] = out["Vol spike"].apply(lambda x: f"{float(x):.2f}x" if x is not None and not (isinstance(x, float) and pd.isna(x)) else "—")
        out["Drawdown 52w"] = out["Drawdown 52w"].apply(lambda x: pct(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else "—")
        out["Next earnings"] = out["Next earnings"].astype(str).replace("NaT", "—").replace("nan", "—")
        out["Val pct"] = out["Val pct"].apply(lambda x: f"{float(x):.0f}%" if x is not None else "—")
        out["What changed"] = out["What changed"].apply(lambda x: f"{float(x):.2f}" if x is not None and not (isinstance(x, float) and pd.isna(x)) else "—")
        st.dataframe(out, use_container_width=True, hide_index=True)

# ---------- Tab 2: Earnings Control Room ----------
with tab2:
    horizon = st.radio("Calendar horizon", [30, 60, 90], horizontal=True, format_func=lambda x: f"{x} days")
    end = date.today() + timedelta(days=horizon)
    cur.execute("""
        SELECT m.ticker, e.event_date, e.event_time, e.fiscal_period, e.expected_move, e.notes,
               e.reported_rev, e.guide_rev, e.post_notes, e.thesis_impact
        FROM core.core_events_earnings e
        JOIN core.core_security_master m ON m.id = e.security_id
        WHERE e.event_date >= CURRENT_DATE AND e.event_date <= %s
        ORDER BY e.event_date, m.ticker
    """, (end,))
    cal = cur.fetchall()
    if cal:
        cal_df = pd.DataFrame(cal, columns=["Ticker", "Date", "Time", "Fiscal period", "Expected move", "Notes", "Reported rev", "Guide rev", "Post notes", "Thesis impact"])
        display_cols = ["Ticker", "Date", "Time", "Fiscal period", "Expected move", "Notes"]
        st.markdown(f"#### Earnings calendar (next {horizon} days)")
        st.dataframe(cal_df[display_cols], use_container_width=True, hide_index=True)
    else:
        st.write("No earnings dates in the next " + str(horizon) + " days. Add events below.")

    st.markdown("#### Add / edit earnings event")
    cur.execute("SELECT ticker FROM core.core_security_master ORDER BY ticker")
    tickers_list = [r[0] for r in cur.fetchall()]
    add_ticker = st.selectbox("Ticker", tickers_list, key="add_ticker")
    add_date = st.date_input("Event date", key="add_date")
    add_time = st.text_input("Time (optional)", placeholder="e.g. 16:00", key="add_time")
    add_fiscal = st.text_input("Fiscal period (optional)", placeholder="e.g. Q3 FY25", key="add_fiscal")
    add_expected_move = st.number_input("Expected move % (manual)", value=None, format="%.2f", placeholder="e.g. 5.0", key="add_em")
    add_notes = st.text_area("Notes (optional)", key="add_notes")
    if st.button("Save earnings event"):
        cur.execute("SELECT id FROM core.core_security_master WHERE ticker = %s", (add_ticker,))
        sid = cur.fetchone()
        if sid:
            t = None
            if add_time and add_time.strip():
                try:
                    from datetime import datetime as dt
                    t = dt.strptime(add_time.strip(), "%H:%M").time()
                except Exception:
                    pass
            cur.execute("""
                INSERT INTO core.core_events_earnings (security_id, event_date, event_time, fiscal_period, expected_move, notes)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (sid[0], add_date, t, add_fiscal or None, add_expected_move, add_notes or None))
            conn.commit()
            st.success("Saved.")
        else:
            st.error("Ticker not found.")

    st.markdown("#### Prep checklist (auto)")
    prep_tickers = [""] + tickers_list
    selected_ticker = st.selectbox("For ticker", prep_tickers, key="prep_ticker")
    if selected_ticker:
        cur.execute("SELECT id FROM core.core_security_master WHERE ticker = %s", (selected_ticker,))
        sid = cur.fetchone()
        if sid:
            cur.execute("SELECT event_date, fiscal_period, expected_move, notes FROM core.core_events_earnings WHERE security_id = %s AND event_date >= CURRENT_DATE ORDER BY event_date LIMIT 1", (sid[0],))
            ev = cur.fetchone()
            if ev:
                ed, fp, em, n = ev
                em_str = f"{em}%" if em is not None else "—"
                checklist = f"""• Earnings date: {ed} | Period: {fp or '—'}
• Expected move: {em_str} (manual) — use for sizing / strangles
• Notes: {n or '—'}
• Pre: Review thesis, recent guide, consensus rev/EPS
• Post: Log reported rev, guide, and thesis impact"""
                st.text_area("Checklist", value=checklist, height=140, disabled=True, key="checklist")
            else:
                st.write("No upcoming earnings for this ticker.")

    st.markdown("#### Post-earnings input")
    post_ticker = st.selectbox("Ticker", tickers_list, key="post_ticker")
    cur.execute("SELECT id FROM core.core_security_master WHERE ticker = %s", (post_ticker,))
    post_sid = cur.fetchone()
    if post_sid:
        cur.execute("SELECT id, event_date, fiscal_period FROM core.core_events_earnings WHERE security_id = %s ORDER BY event_date DESC LIMIT 10", (post_sid[0],))
        events = cur.fetchall()
        if events:
            event_options = {f"{r[1]} {r[2] or ''}": r[0] for r in events}
            chosen_label = st.selectbox("Event", list(event_options.keys()), key="post_event")
            eid = event_options[chosen_label]
            cur.execute("SELECT reported_rev, guide_rev, post_notes, thesis_impact FROM core.core_events_earnings WHERE id = %s", (eid,))
            existing = cur.fetchone()
            post_rev = st.number_input("Reported rev (optional)", value=int(existing[0]) if existing and existing[0] is not None else None, format="%d", key="post_rev")
            post_guide = st.number_input("Guide rev (optional)", value=int(existing[1]) if existing and existing[1] is not None else None, format="%d", key="post_guide")
            post_notes = st.text_area("Notes", value=existing[2] or "", key="post_notes")
            thesis_opts = ["", "Bullish", "Neutral", "Bearish", "Mixed"]
            thesis_idx = thesis_opts.index(existing[3]) if existing and existing[3] in thesis_opts else 0
            post_thesis = st.selectbox("Thesis impact", thesis_opts, index=thesis_idx, key="thesis")
            if st.button("Save post-earnings"):
                cur.execute("""
                    UPDATE core.core_events_earnings SET reported_rev = %s, guide_rev = %s, post_notes = %s, thesis_impact = %s WHERE id = %s
                """, (post_rev, post_guide, post_notes or None, post_thesis or None, eid))
                conn.commit()
                st.success("Saved.")
        else:
            st.write("No earnings events for this ticker.")

st.markdown("---")
st.markdown(
    '<div style="text-align: center; color: #6B7280; padding: 1.5rem 0;">'
    '<p style="font-size: 0.85rem;">Data reflects portfolio positions, returns, and earnings events from Equity Infra MVP (SimFin + SEC). Run ingestion and feat_returns for latest.</p>'
    '</div>',
    unsafe_allow_html=True
)
conn.close()
