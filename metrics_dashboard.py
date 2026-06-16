"""
Bajaj Life Insurance – Speech Analytics
API Usage & Cost Metrics Dashboard (Streamlit)

Run with:  streamlit run metrics_dashboard.py
Place this file at the same level as your backend/ folder (next to processed/).

Password is set via METRICS_PASSWORD env var (default: bajaj@metrics2024)
"""

import os
import json
import hashlib
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="API Metrics – Bajaj Life Speech Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Paths (resolve relative to this file so it works from any cwd) ───────────
BASE_DIR = Path(__file__).parent
PROC_DIR = BASE_DIR / "processed"
DB_FILE  = PROC_DIR / "calls_db.json"
USAGE_FILE = PROC_DIR / "api_usage_log.json"   # written by the patched backend

# ── Auth ─────────────────────────────────────────────────────────────────────
METRICS_PASSWORD = os.environ.get("METRICS_PASSWORD", "bajaj@metrics2024")

def _check_password() -> bool:
    """Returns True once the correct password has been entered."""
    if st.session_state.get("authenticated"):
        return True

    st.markdown(
        """
        <div style='max-width:360px;margin:80px auto;text-align:center'>
            <h2 style='margin-bottom:4px'>🔒 API Metrics</h2>
            <p style='color:#888;margin-bottom:24px'>Bajaj Life Speech Analytics</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        pwd = st.text_input("Password", type="password", key="pwd_input",
                            placeholder="Enter dashboard password")
        if st.button("Login", use_container_width=True, type="primary"):
            if pwd == METRICS_PASSWORD:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Incorrect password.")
    return False


if not _check_password():
    st.stop()

# ── Pricing constants ─────────────────────────────────────────────────────────
# All prices in USD per 1M tokens; convert to INR at the rate below.
USD_TO_INR = float(os.environ.get("USD_TO_INR", "84.0"))

MODELS = {
    "gpt-4o": {
        "label": "GPT-4o",
        "input_usd_per_1m":  2.50,
        "output_usd_per_1m": 10.00,
        "note": "OpenAI – strong structured JSON output & tool use",
    },
    "gpt-4o-mini": {
        "label": "GPT-4o mini (current)",
        "input_usd_per_1m": 0.15,
        "output_usd_per_1m": 0.60,
        "note": "OpenAI – lightweight, low-cost model optimized for fast responses and high throughput"
    },
    "claude-sonnet-4-6": {
        "label": "Claude Sonnet 4.6",
        "input_usd_per_1m":  3.00,
        "output_usd_per_1m": 15.00,
        "note": "Anthropic – stronger reasoning & long-form analysis, 200K context",
    },
    "claude-haiku-4-5": {
        "label": "Claude Haiku 4.5",
        "input_usd_per_1m":  1.00,
        "output_usd_per_1m":  5.00,
        "note": "Anthropic – fastest & cheapest; good for high-volume classification",
    },
}

CURRENT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

# ── Data loaders ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_calls() -> list:
    try:
        data = json.loads(DB_FILE.read_text())
        return data.get("calls", [])
    except Exception:
        return []


@st.cache_data(ttl=30)
def load_usage_log() -> list:
    """Load the dedicated usage log if it exists (written by the patched backend).
    Falls back to reconstructing estimates from the calls DB."""
    try:
        if USAGE_FILE.exists():
            return json.loads(USAGE_FILE.read_text())
    except Exception:
        pass
    return []


# ── Token estimation (fallback when no usage log) ────────────────────────────
# Rough average characters-per-token ≈ 4 for English.
# System prompt is ~3 500 tokens; product context adds ~800 tokens on average.
SYSTEM_PROMPT_TOKENS  = 3_500
PRODUCT_CONTEXT_TOKENS = 800
CHARS_PER_TOKEN = 4

def estimate_tokens(calls: list) -> dict:
    """Estimate input / output token counts from the calls DB when no usage log exists."""
    total_input = 0
    total_output = 0
    for c in calls:
        raw = c.get("raw_text", "")
        input_tokens = SYSTEM_PROMPT_TOKENS + PRODUCT_CONTEXT_TOKENS + max(len(raw) // CHARS_PER_TOKEN, 200)
        # JSON output: ~800 tokens on average (10 scores + flags + checks + summary etc.)
        output_tokens = 800
        total_input  += input_tokens
        total_output += output_tokens
    return {
        "total_input":  total_input,
        "total_output": total_output,
        "count": len(calls),
    }


def aggregate_usage(usage_log: list, calls: list) -> dict:
    """Return aggregated token / cost figures from whichever source is available."""
    if usage_log:
        total_input  = sum(r.get("input_tokens",  0) for r in usage_log)
        total_output = sum(r.get("output_tokens", 0) for r in usage_log)
        count        = len(usage_log)
    else:
        est = estimate_tokens(calls)
        total_input  = est["total_input"]
        total_output = est["total_output"]
        count        = est["count"]

    return {
        "total_input":  total_input,
        "total_output": total_output,
        "count":        count,
        "avg_input":    total_input  / count if count else 0,
        "avg_output":   total_output / count if count else 0,
    }


def cost_usd(input_tokens: int, output_tokens: int, model_key: str) -> float:
    m = MODELS.get(model_key, MODELS["gpt-4o-mini"])
    return (
        input_tokens  / 1_000_000 * m["input_usd_per_1m"] +
        output_tokens / 1_000_000 * m["output_usd_per_1m"]
    )


def cost_inr(input_tokens: int, output_tokens: int, model_key: str) -> float:
    return cost_usd(input_tokens, output_tokens, model_key) * USD_TO_INR


def calls_this_month(calls: list) -> list:
    now  = datetime.now()
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return [
        c for c in calls
        if c.get("processed_at", "") >= start.isoformat()
    ]


def usage_log_this_month(usage_log: list) -> list:
    now   = datetime.now()
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    return [r for r in usage_log if r.get("timestamp", "") >= start]


def daily_series(calls: list, usage_log: list) -> pd.DataFrame:
    """Build a day-by-day DataFrame: date, calls, input_tokens, output_tokens, cost_inr."""
    daily: dict = defaultdict(lambda: {"calls": 0, "input": 0, "output": 0})

    if usage_log:
        for r in usage_log:
            day = (r.get("timestamp") or "")[:10]
            if day:
                daily[day]["calls"]  += 1
                daily[day]["input"]  += r.get("input_tokens",  0)
                daily[day]["output"] += r.get("output_tokens", 0)
    else:
        for c in calls:
            day = (c.get("processed_at") or "")[:10]
            if day:
                raw = c.get("raw_text", "")
                inp = SYSTEM_PROMPT_TOKENS + PRODUCT_CONTEXT_TOKENS + max(len(raw) // CHARS_PER_TOKEN, 200)
                daily[day]["calls"]  += 1
                daily[day]["input"]  += inp
                daily[day]["output"] += 800

    rows = []
    for day, v in sorted(daily.items()):
        rows.append({
            "Date":         day,
            "Calls":        v["calls"],
            "Input Tokens": v["input"],
            "Output Tokens":v["output"],
            "Cost (₹)":    round(cost_inr(v["input"], v["output"], CURRENT_MODEL), 2),
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["Date", "Calls", "Input Tokens", "Output Tokens", "Cost (₹)"]
    )


# ── Projection helper ─────────────────────────────────────────────────────────

def project_monthly(avg_input: float, avg_output: float, volume: int, model_key: str) -> float:
    return cost_inr(int(avg_input * volume), int(avg_output * volume), model_key)


# ── Styling helpers ───────────────────────────────────────────────────────────

def metric_card(label: str, value: str, delta: str = "", help_text: str = ""):
    st.metric(label=label, value=value, delta=delta if delta else None, help=help_text)


def inr(v: float) -> str:
    """Format a float as ₹ with Indian-style thousands separator."""
    if v >= 1_00_000:
        return f"₹{v/1_00_000:,.2f}L"
    return f"₹{v:,.2f}"


def fmt_tokens(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

# Sidebar
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/6/6b/Bajaj_Allianz_Life_Insurance_logo.svg/320px-Bajaj_Allianz_Life_Insurance_logo.svg.png",
             width=200, use_column_width=False)
    st.markdown("## ⚙️ Settings")
    usd_rate = st.number_input("USD → INR rate", value=USD_TO_INR, step=0.5, format="%.2f")
    USD_TO_INR = usd_rate

    st.markdown("---")
    proj_volume = st.number_input("Monthly volume projection (calls)", value=10_000, step=500)
    proj_avg_input  = st.number_input("Avg input tokens / call",  value=5_100, step=100)
    proj_avg_output = st.number_input("Avg output tokens / call", value=800,   step=50)

    st.markdown("---")
    if st.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.caption(f"Connected to: `{DB_FILE}`")
    st.caption(f"Active model: `{CURRENT_MODEL}`")

# ── Load data ─────────────────────────────────────────────────────────────────
calls      = load_calls()
usage_log  = load_usage_log()
month_calls = calls_this_month(calls)
month_log   = usage_log_this_month(usage_log)

all_agg   = aggregate_usage(usage_log, calls)
month_agg = aggregate_usage(month_log, month_calls)

has_usage_log = bool(usage_log)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📊 API Usage & Cost Metrics")
st.caption(
    f"Model: **{MODELS.get(CURRENT_MODEL, {}).get('label', CURRENT_MODEL)}** · "
    f"Prices: ${MODELS['gpt-4o-mini']['input_usd_per_1m']:.2f}/$"
    f"{MODELS['gpt-4o-mini']['output_usd_per_1m']:.2f} per 1M tokens · "
    f"Rate: 1 USD = ₹{USD_TO_INR:.2f} · "
    + ("🟢 Live usage log" if has_usage_log else "🟡 Estimated from call data")
)

if not has_usage_log:
    st.info(
        "**Token estimates** are calculated from transcript lengths + system-prompt overhead. "
        "For exact figures, add the usage-logging patch to `main.py` (see instructions below).",
        icon="ℹ️",
    )

st.markdown("---")

# ═══════════════════════════════════════════
# SECTION 1 – KPI CARDS
# ═══════════════════════════════════════════
st.subheader("📈 Key Metrics at a Glance")

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    metric_card(
        "Total API Calls (all time)",
        f"{all_agg['count']:,}",
        help_text="Number of GPT calls made (1 per transcript)"
    )
with c2:
    metric_card(
        "API Calls (this month)",
        f"{month_agg['count']:,}",
        help_text="Calls processed in the current calendar month"
    )
with c3:
    all_cost  = cost_inr(all_agg["total_input"], all_agg["total_output"], CURRENT_MODEL)
    metric_card(
        "Total API Cost (all time)",
        inr(all_cost),
        help_text=f"≈ ${cost_usd(all_agg['total_input'], all_agg['total_output'], CURRENT_MODEL):,.2f} USD"
    )
with c4:
    month_cost = cost_inr(month_agg["total_input"], month_agg["total_output"], CURRENT_MODEL)
    metric_card(
        "API Cost (this month)",
        inr(month_cost),
        help_text=f"≈ ${cost_usd(month_agg['total_input'], month_agg['total_output'], CURRENT_MODEL):,.2f} USD"
    )
with c5:
    metric_card(
        "Avg Score (all calls)",
        f"{sum(c['analysis'].get('weighted_score', 0) for c in calls)/len(calls):.1f}" if calls else "—",
        help_text="Average QA weighted score across all processed calls"
    )

st.markdown("---")

# ═══════════════════════════════════════════
# SECTION 2 – TOKEN BREAKDOWN
# ═══════════════════════════════════════════
st.subheader("🔢 Token Usage")

t1, t2, t3, t4 = st.columns(4)
with t1:
    metric_card("Total Input Tokens (all time)",  fmt_tokens(all_agg["total_input"]))
with t2:
    metric_card("Total Output Tokens (all time)", fmt_tokens(all_agg["total_output"]))
with t3:
    metric_card("Avg Input Tokens / Call",  f"{all_agg['avg_input']:,.0f}")
with t4:
    metric_card("Avg Output Tokens / Call", f"{all_agg['avg_output']:,.0f}")

st.markdown("---")

# ═══════════════════════════════════════════
# SECTION 3 – DAILY TREND
# ═══════════════════════════════════════════
st.subheader("📅 Daily Usage Trend")

df_daily = daily_series(calls, usage_log)

if df_daily.empty:
    st.info("No call data yet. Process some transcripts to see the trend.")
else:
    tab_calls, tab_tokens, tab_cost = st.tabs(["Call Volume", "Token Usage", "Daily Cost (₹)"])

    with tab_calls:
        st.bar_chart(df_daily.set_index("Date")["Calls"])

    with tab_tokens:
        st.line_chart(df_daily.set_index("Date")[["Input Tokens", "Output Tokens"]])

    with tab_cost:
        st.bar_chart(df_daily.set_index("Date")["Cost (₹)"])

    with st.expander("📋 Raw daily data table"):
        st.dataframe(df_daily, use_container_width=True, hide_index=True)

st.markdown("---")

# ═══════════════════════════════════════════
# SECTION 4 – MODEL COST COMPARISON
# ═══════════════════════════════════════════
st.subheader("💰 Model Cost Comparison")
st.caption(
    f"Based on your projection: **{proj_volume:,} calls/month** · "
    f"~{proj_avg_input:,} input tokens/call · ~{proj_avg_output:,} output tokens/call"
)

rows = []
for key, m in MODELS.items():
    monthly_cost_inr = project_monthly(proj_avg_input, proj_avg_output, proj_volume, key)
    monthly_cost_usd = monthly_cost_inr / USD_TO_INR
    annual_cost_inr  = monthly_cost_inr * 12
    per_call_inr     = monthly_cost_inr / proj_volume if proj_volume else 0
    rows.append({
        "Model": m["label"],
        "Input $/1M": f"${m['input_usd_per_1m']:.2f}",
        "Output $/1M": f"${m['output_usd_per_1m']:.2f}",
        "Monthly Cost (₹)": inr(monthly_cost_inr),
        "Monthly Cost ($)": f"${monthly_cost_usd:,.2f}",
        "Annual Cost (₹)": inr(annual_cost_inr),
        "Per-Call Cost (₹)": f"₹{per_call_inr:.4f}",
        "Notes": m["note"],
        "_monthly_inr": monthly_cost_inr,
        "_key": key,
    })

df_models = pd.DataFrame(rows)
best_key = min(rows, key=lambda r: r["_monthly_inr"])["_key"]

def highlight_row(row):
    if row["Model"] == MODELS[CURRENT_MODEL]["label"]:
        return ["background-color: #fff3cd"] * len(row)
    if row["Model"] == MODELS[best_key]["label"]:
        return ["background-color: #d4edda"] * len(row)
    return [""] * len(row)

display_cols = ["Model", "Input $/1M", "Output $/1M", "Monthly Cost (₹)", "Monthly Cost ($)", "Annual Cost (₹)", "Per-Call Cost (₹)", "Notes"]
st.dataframe(
    df_models[display_cols].style.apply(highlight_row, axis=1),
    use_container_width=True,
    hide_index=True,
)

st.markdown(
    f"""
    <div style='background:#d4edda;border-radius:8px;padding:12px 16px;margin-top:8px;border-left:4px solid #28a745'>
    🟢 <b>Cheapest option at {proj_volume:,} calls/month:</b> {MODELS[best_key]['label']} —
    {inr(project_monthly(proj_avg_input, proj_avg_output, proj_volume, best_key))}/month
    </div>
    """,
    unsafe_allow_html=True,
)

if CURRENT_MODEL in MODELS:
    curr_monthly = project_monthly(proj_avg_input, proj_avg_output, proj_volume, CURRENT_MODEL)
    best_monthly = project_monthly(proj_avg_input, proj_avg_output, proj_volume, best_key)
    saving = curr_monthly - best_monthly
    if saving > 0:
        st.markdown(
            f"""
            <div style='background:#cce5ff;border-radius:8px;padding:12px 16px;margin-top:8px;border-left:4px solid #004085'>
            💡 <b>Potential saving vs current ({MODELS[CURRENT_MODEL]['label']}):</b>
            {inr(saving)}/month → {inr(saving*12)}/year
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown("---")

# ═══════════════════════════════════════════
# SECTION 5 – RECOMMENDATION
# ═══════════════════════════════════════════
st.subheader("🎯 Recommendation for 10K Calls/Month")

rec_cols = st.columns(2)

with rec_cols[0]:
    st.markdown(
        """
        ### GPT-4o vs GPT-4o mini — recommended cost-optimized switch

        You're currently using **GPT-4o**  
        (`$2.50 input / $10.00 output per 1M tokens`).

        **GPT-4o mini**  
        (`$0.15 input / $0.60 output per 1M tokens`) offers a **massive cost reduction**
        while remaining highly effective for structured JSON extraction and
        high-throughput workloads.

        **Why switch to GPT-4o mini for this use case**

        - ✅ **~94% cheaper** per API call — ideal for large-scale or continuous processing
        (only change required: `model="gpt-4o"` → `model="gpt-4o-mini"`)
        - ✅ Optimized for **fast responses and high concurrency**
        - ✅ Strong performance on **structured outputs and schema-driven JSON**
        - ✅ Significantly lower operational cost for polling- and job-based pipelines

        **When GPT-4o still makes sense**

        - Complex multi-step reasoning or edge-case analysis
        - Very long or ambiguous transcripts requiring deeper contextual inference
        - Scenarios where maximal reasoning depth is more important than cost

        **Summary**

        For production pipelines focused on **cost efficiency, speed, and structured
        extraction**, **GPT-4o mini** is the better default.  
        **GPT-4o** should be reserved for selective, high-complexity tasks.
        """,
        unsafe_allow_html=True
    )

with rec_cols[1]:
    # Mini cost card for 10K calls using the sidebar sliders
    st.markdown("#### Estimated monthly cost @ 10,000 calls")
    for key, m in MODELS.items():
        c = project_monthly(proj_avg_input, proj_avg_output, 10_000, key)
        badge = " ← **current**" if key == CURRENT_MODEL else (" ← ✅ **recommended**" if key == "gpt-4.1" else "")
        st.markdown(f"**{m['label']}**: {inr(c)}/month{badge}")


st.markdown("---")

# ═══════════════════════════════════════════
# SECTION 6 – USAGE LOG SETUP INSTRUCTIONS
# ═══════════════════════════════════════════
with st.expander("🛠️  Enable exact token tracking in main.py (one-time setup)"):
    st.markdown(
        """
        The dashboard currently **estimates** token counts from transcript lengths.
        To capture **exact** token counts from the OpenAI API response, add the patch below
        to your `backend/main.py`.

        ### 1. Add a usage logger function (after the `load_db` helpers)

        ```python
        # ── API Usage Logger ─────────────────────────────────────────────────────────
        USAGE_FILE = PROC_DIR / "api_usage_log.json"

        def log_api_usage(model: str, input_tokens: int, output_tokens: int, call_name: str = ""):
            entry = {
                "timestamp":     datetime.now().isoformat(),
                "model":         model,
                "input_tokens":  input_tokens,
                "output_tokens": output_tokens,
                "call_name":     call_name,
            }
            try:
                existing = json.loads(USAGE_FILE.read_text()) if USAGE_FILE.exists() else []
                existing.append(entry)
                USAGE_FILE.write_text(json.dumps(existing, indent=2))
            except Exception as e:
                log.warning(f"Usage log write error: {e}")
        ```

        ### 2. Patch `analyze_call_with_gpt4o` to call the logger

        Find the line:
        ```python
            raw = response.choices[0].message.content
            return json.loads(raw)
        ```
        Replace with:
        ```python
            usage = response.usage
            log_api_usage(
                model=OPENAI_MODEL,
                input_tokens=usage.prompt_tokens,
                output_tokens=usage.completion_tokens,
            )
            raw = response.choices[0].message.content
            return json.loads(raw)
        ```

        Once done, restart the server — all new calls will be logged to
        `processed/api_usage_log.json` and this dashboard will switch from estimates
        to exact figures automatically.
        """
    )

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(
    f"<p style='color:#aaa;font-size:12px;text-align:center;margin-top:32px'>"
    f"Bajaj Life Insurance · Speech Analytics Platform · "
    f"Last refreshed: {datetime.now().strftime('%d %b %Y %H:%M:%S')}"
    f"</p>",
    unsafe_allow_html=True,
)
