"""
Repeat Contact Analysis — ADVOCATE LEVEL
Same as repeat_contact_analysis.py EXCEPT:
  - LOB_Insights and Overall_Insights replaced by Advocate_Insights
  - Grouped by msid (= prev_msid, same advocate handled both calls)
  - Each advocate row shows 1 concise bullet per category + % across their contacts
"""

import os, re, json, warnings
import pandas as pd
from tqdm import tqdm
from langchain_openai import AzureChatOpenAI
from langchain.schema import HumanMessage, SystemMessage

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 1. CONFIGURATION
# ─────────────────────────────────────────────
AZURE_OPENAI_API_KEY     = os.getenv("AZURE_OPENAI_API_KEY",    "YOUR_KEY_HERE")
AZURE_OPENAI_ENDPOINT    = os.getenv("AZURE_OPENAI_ENDPOINT",   "https://YOUR_RESOURCE.openai.azure.com/")
AZURE_OPENAI_DEPLOYMENT  = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4")
AZURE_OPENAI_API_VERSION = "2024-02-15-preview"

INPUT_FILE  = "your_data.csv"
OUTPUT_FILE = "repeat_contact_advocate_level.xlsx"

# ─────────────────────────────────────────────
# 2. INIT AZURE OPENAI
# ─────────────────────────────────────────────
llm = AzureChatOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    azure_deployment=AZURE_OPENAI_DEPLOYMENT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
    temperature=0,
    max_tokens=2000,
)

# ─────────────────────────────────────────────
# 3. ANALYSIS PROMPT  (unchanged from original)
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """You are a senior healthcare contact center quality analyst with deep expertise
in provider services operations. You analyze pairs of call transcripts — an ORIGINAL contact
and a REPEAT contact — made by the same provider about the same patient and topic.

STRICT OUTPUT RULES:
- Respond ONLY with a valid JSON object. No markdown, no preamble, no trailing text.
- Every field is mandatory. Never leave a field empty.
- For Yes/No flag fields respond with ONLY the word "Yes" or "No" — never include reasons in the flag field.
- All reasons must cite specific evidence from the transcripts, not generic statements.

JSON FORMAT (copy exactly, fill every field):
{
  "root_cause_summary": "Write 6-8 sentences covering: (1) what the provider called about originally, (2) what happened or failed in that call, (3) what specifically drove them to call back, (4) how the repeat call differed, (5) whether the issue was ultimately resolved, and (6) the single most likely root cause. Reference actual topics, actions, and outcomes from the transcripts.",

  "Key differences": "List the 3-5 most important differences between the two calls: what changed in the provider question or tone, what new information emerged, what the agent did differently, whether the outcome differed.",

  "Issue resolution": "Yes or No only",
  "Issue_resolution_reason": "If Yes: what exactly was resolved and how. If No: what specifically remained unresolved and why the provider need was not met.",

  "Provider Dissatisfaction": "Yes or No only",
  "Provider_Dissatisfaction_reason": "If Yes: quote or closely paraphrase the exact moment(s) where dissatisfaction was expressed and the trigger. If No: write No dissatisfaction signals detected.",

  "Process Issue": "Yes or No only",
  "Process_Issue_reason": "Answer Yes ONLY when there is clear evidence of a broken, missing, or excessively slow WORKFLOW STEP that is independent of agent behavior — e.g. claim stuck in queue, prior auth not initiated, fax not received, callback never made, multi-day policy-mandated turnaround. Do NOT flag agent communication style or knowledge gaps here. If Yes: describe the specific process failure and its impact on the provider. If No: write No process-level bottleneck identified.",

  "Technology Issue": "Yes or No only",
  "Technology_Issue_reason": "Answer Yes ONLY when there is explicit or strongly implied evidence of a SYSTEM or TOOL failure — portal error, eligibility system down, fax failure, system not updating, wrong data displayed. If Yes: name the system and describe the failure. If No: write No technology issues identified.",

  "Agent Knowledge": "Yes or No only",
  "Agent_Knowledge_reason": "Answer Yes ONLY when the agent demonstrably provided incorrect information, could not answer a reasonable provider question, escalated unnecessarily due to knowledge gaps, or gave inconsistent answers across the two calls. If Yes: describe the specific gap and its consequence. If No: write No agent knowledge gaps identified."
}"""

USER_PROMPT_TEMPLATE = """LOB: {lob}

ORIGINAL CONTACT (prev_ixn_conv_ucid: {prev_ixn_id}):
{prev_chat}

REPEAT CONTACT (ixn_conv_ucid: {ixn_id}):
{ixn_chat}

Analyze both transcripts and produce the full JSON."""

# ─────────────────────────────────────────────
# 4. ADVOCATE-LEVEL INSIGHTS PROMPT
#    Replaces LOB_INSIGHTS_PROMPT and OVERALL_INSIGHTS_PROMPT
#    One crisp bullet per category + % stats passed in directly
# ─────────────────────────────────────────────
ADVOCATE_INSIGHTS_PROMPT = """You are a healthcare contact center quality analyst reviewing
repeat contact data for a SINGLE ADVOCATE (msid: {msid}).

This advocate handled {total} repeat contact pair(s).

Pre-computed flag distribution across their contacts:
{flag_stats}

Findings from their contacts (root causes and reasons):
{findings}

Write EXACTLY 6 lines — one per category below. Each line must be:
  - A single concise bullet point (1 sentence max)
  - Grounded in the actual findings above
  - Include the % from the flag distribution in parentheses at the end

FORMAT (output only these 6 lines, no headers, no extra text):
• Top Repeat Reason: <1-sentence summary of the dominant root cause across this advocate's contacts>
• Issue Resolution: <what % of issues were resolved and the main reason they were/weren't> ({Issue_resolution_pct} resolved)
• Provider Dissatisfaction: <key trigger if any, or note absence> ({Provider_dissatisfaction_pct} flagged)
• Process Issues: <dominant process failure theme if any, else 'None identified'> ({Process_issue_pct} flagged)
• Technology Issues: <dominant tech failure theme if any, else 'None identified'> ({Tech_issue_pct} flagged)
• Agent Knowledge Gaps: <dominant knowledge gap theme if any, else 'None identified'> ({Agent_knowledge_pct} flagged)
"""

# ─────────────────────────────────────────────
# 5. TRANSCRIPT CLEANER
# ─────────────────────────────────────────────
def chat_processing(x: str) -> str:
    x = str(x).lower()
    x = re.sub(r'[<>]', '', x)
    x = re.sub(r'\bunk\b', '', x)
    x = re.sub(r'[\(\[].*?[\)\]]', '', x)
    x = re.sub(r'[.]{4}', ' ', x)
    x = re.sub(r'[.]{3}', ',', x)
    x = re.sub(r' ,', ',', x)
    x = re.sub(r'[.]{2}', '. ', x)
    x = re.sub(r'0:.\s+', '', x)
    x = re.sub(r'0: .\s+', '', x)
    x = re.sub(r'1:.\s+', '', x)
    x = re.sub(r'1: .\s+', '', x)
    x = re.sub(r'\s+', ' ', x)
    return x.strip()

# ─────────────────────────────────────────────
# 6. DATA AGGREGATION
# ─────────────────────────────────────────────
def smart_merge(series: pd.Series) -> str:
    vals = [str(v).strip() for v in series.dropna() if str(v).strip()]
    if not vals:
        return ""
    unique_vals = list(dict.fromkeys(vals))
    return unique_vals[0] if len(unique_vals) == 1 else " | ".join(unique_vals)

def join_unique_ids(series: pd.Series) -> str:
    return ", ".join(series.dropna().astype(str).str.strip().unique().tolist())

def Data_Aggregation(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ixn_chat"]  = df["ixn_chat"].apply(chat_processing)
    df["prev_chat"] = df["prev_chat"].apply(chat_processing)
    df["ixn_conv_ucid"]      = df["ixn_conv_ucid"].astype(str)
    df["prev_ixn_conv_ucid"] = df["prev_ixn_conv_ucid"].astype(str)
    df.drop_duplicates(inplace=True)
    print(f"Total call interactions: {df.shape[0]}  |  Columns: {df.shape[1]}")
    return df

# ─────────────────────────────────────────────
# 7. SINGLE-ROW LLM ANALYSIS
# ─────────────────────────────────────────────
FALLBACK = {
    "root_cause_summary": "", "Key differences": "",
    "Issue resolution": "No",         "Issue_resolution_reason": "",
    "Provider Dissatisfaction": "No", "Provider_Dissatisfaction_reason": "",
    "Process Issue": "No",            "Process_Issue_reason": "",
    "Technology Issue": "No",         "Technology_Issue_reason": "",
    "Agent Knowledge": "No",          "Agent_Knowledge_reason": "",
    "status": "error",
}

FLAG_FIELDS = [
    ("Issue resolution",         "Issue_resolution_reason"),
    ("Provider Dissatisfaction", "Provider_Dissatisfaction_reason"),
    ("Process Issue",            "Process_Issue_reason"),
    ("Technology Issue",         "Technology_Issue_reason"),
    ("Agent Knowledge",          "Agent_Knowledge_reason"),
]

def analyze_repeat_contact(row: pd.Series) -> dict:
    prompt = USER_PROMPT_TEMPLATE.format(
        lob         = row.get("lob", "Unknown"),
        ixn_id      = row.get("ixn_conv_ucid", ""),
        prev_ixn_id = row.get("prev_ixn_conv_ucid", ""),
        ixn_chat    = row.get("ixn_chat", ""),
        prev_chat   = row.get("prev_chat", ""),
    )
    try:
        response = llm.invoke([SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=prompt)])
        raw = response.content.strip().replace("```json", "").replace("```", "").strip()
        result = json.loads(raw)

        for flag, reason_key in FLAG_FIELDS:
            raw_val = str(result.get(flag, "No")).strip()
            if ":" in raw_val:
                parts = raw_val.split(":", 1)
                result[flag] = "Yes" if parts[0].strip().lower().startswith("yes") else "No"
                if not result.get(reason_key):
                    result[reason_key] = parts[1].strip()
            else:
                result[flag] = "Yes" if raw_val.lower().startswith("yes") else "No"
            if reason_key not in result:
                result[reason_key] = ""

        result["status"] = "success"

    except json.JSONDecodeError:
        result = {**FALLBACK, "root_cause_summary": raw, "status": "json_parse_error"}
    except Exception as e:
        result = {**FALLBACK, "root_cause_summary": str(e), "status": "api_error"}

    return result

# ─────────────────────────────────────────────
# 8. ADVOCATE-LEVEL FLAG STATS
# ─────────────────────────────────────────────
INSIGHT_COLS = [
    "root_cause_summary", "Key differences",
    "Issue_resolution_reason", "Provider_Dissatisfaction_reason",
    "Process_Issue_reason", "Technology_Issue_reason", "Agent_Knowledge_reason",
]

def _flag_stats(df: pd.DataFrame) -> dict:
    """Returns dict with Yes count, total, and pct string for each flag."""
    stats = {}
    for flag, _ in FLAG_FIELDS:
        if flag in df.columns:
            yes  = (df[flag].astype(str).str.strip().str.lower() == "yes").sum()
            total = len(df)
            pct  = round(yes / total * 100, 1) if total else 0
            stats[flag] = {"yes": int(yes), "total": total, "pct": f"{pct}%", "label": f"{yes}/{total} ({pct}%)"}
    return stats

# ─────────────────────────────────────────────
# 9. ADVOCATE INSIGHT GENERATOR
# ─────────────────────────────────────────────
def generate_advocate_insight(msid: str, adv_df: pd.DataFrame) -> str:
    """
    For one advocate (msid), look at ALL their repeat contact rows,
    compute flag stats, pass findings to LLM, get 6-bullet summary back.
    """
    findings = adv_df[[c for c in INSIGHT_COLS if c in adv_df.columns]].to_dict(orient="records")
    stats    = _flag_stats(adv_df)

    def pct(flag): return stats.get(flag, {}).get("pct", "0%")

    prompt = ADVOCATE_INSIGHTS_PROMPT.format(
        msid                      = msid,
        total                     = len(adv_df),
        flag_stats                = json.dumps({k: v["label"] for k, v in stats.items()}, indent=2),
        findings                  = json.dumps(findings, indent=2),
        Issue_resolution_pct      = pct("Issue resolution"),
        Provider_dissatisfaction_pct = pct("Provider Dissatisfaction"),
        Process_issue_pct         = pct("Process Issue"),
        Tech_issue_pct            = pct("Technology Issue"),
        Agent_knowledge_pct       = pct("Agent Knowledge"),
    )
    return llm.invoke([HumanMessage(content=prompt)]).content.strip()

# ─────────────────────────────────────────────
# 10. EXCEL WRITER
# ─────────────────────────────────────────────
def write_excel(final_df: pd.DataFrame, advocate_df: pd.DataFrame, output_file: str):
    """
    Sheet 1 : Report        — row-level detail (same as original)
    Sheet 2 : Advocate_Insights — one row per msid, 6 bullet columns
    """
    # ── Sanitize: NaN, Inf, list/dict and any other non-scalar types ────
    import math
    def _coerce(v):
        if isinstance(v, list):  return ", ".join(str(i) for i in v)
        if isinstance(v, dict):  return json.dumps(v)
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):  return 0
        if v is None:            return ""
        if not isinstance(v, (str, int, float, bool)):  return str(v)
        return v

    final_df = final_df.copy()
    for col in final_df.columns:
        if final_df[col].dtype == float:
            final_df[col] = (final_df[col]
                             .replace([float("inf"), float("-inf")], 0)
                             .fillna(0))
        else:
            final_df[col] = final_df[col].apply(_coerce)

    with pd.ExcelWriter(output_file, engine="xlsxwriter",
                        engine_kwargs={"options": {"nan_inf_to_errors": True}}) as writer:
        wb = writer.book

        # ── Shared formats ────────────────────────────────────────────────
        hdr_fmt = wb.add_format({
            "bold": True, "bg_color": "#1F3864", "font_color": "white",
            "border": 1, "text_wrap": True, "valign": "vcenter", "align": "center",
        })
        wrap_fmt = wb.add_format({"text_wrap": True, "valign": "top", "border": 1})
        yes_fmt  = wb.add_format({
            "bg_color": "#C6EFCE", "font_color": "#276221",
            "bold": True, "border": 1, "align": "center", "valign": "vcenter",
        })
        no_fmt = wb.add_format({
            "bg_color": "#FFCCCC", "font_color": "#9C0006",
            "bold": True, "border": 1, "align": "center", "valign": "vcenter",
        })
        adv_id_fmt = wb.add_format({
            "bold": True, "bg_color": "#2E4057", "font_color": "white",
            "font_size": 11, "border": 1, "align": "center", "valign": "vcenter",
            "text_wrap": True,
        })
        cat_hdr_fmt = wb.add_format({
            "bold": True, "bg_color": "#4472C4", "font_color": "white",
            "border": 1, "text_wrap": True, "valign": "vcenter", "align": "center",
        })
        bullet_fmt = wb.add_format({
            "text_wrap": True, "valign": "top", "border": 1, "font_size": 10,
            "bg_color": "#F8F9FA",
        })
        count_fmt = wb.add_format({
            "text_wrap": True, "valign": "vcenter", "border": 1, "font_size": 10,
            "align": "center", "bg_color": "#EBF3FB",
        })

        # ── SHEET 1: Report ───────────────────────────────────────────────
        ws = wb.add_worksheet("Report")

        col_widths = {
            "Req_ID": 10, "ixn_conv_ucid": 22, "prev_ixn_conv_ucid": 22,
            "ixn_date": 14, "msid": 14, "lob": 8, "tin": 12, "npi": 12,
            "state": 8, "prev_contact_date": 16, "prev_msid": 14,
            "sentimentscore": 16, "prev_sentimentscore": 20,
            "Req_Description": 35, "Req_Instructions": 35, "Request_Date": 14,
            "root_cause_summary": 60, "Key differences": 50,
            "Issue resolution": 16,         "Issue_resolution_reason": 45,
            "Provider Dissatisfaction": 22, "Provider_Dissatisfaction_reason": 45,
            "Process Issue": 14,            "Process_Issue_reason": 45,
            "Technology Issue": 16,         "Technology_Issue_reason": 45,
            "Agent Knowledge": 16,          "Agent_Knowledge_reason": 45,
        }

        flag_col_names = {flag for flag, _ in FLAG_FIELDS}
        col_map = {col: idx for idx, col in enumerate(final_df.columns)}

        ws.set_row(0, 32)
        for col_idx, col_name in enumerate(final_df.columns):
            ws.set_column(col_idx, col_idx, col_widths.get(col_name, 18))
            ws.write(0, col_idx, col_name, hdr_fmt)

        for row_idx, row_data in final_df.iterrows():
            excel_row = row_idx + 1
            ws.set_row(excel_row, 60)
            for col_name, col_idx in col_map.items():
                val = _coerce(row_data[col_name])   # guard every cell
                if col_name in flag_col_names:
                    is_yes = str(val).strip().lower().startswith("yes")
                    ws.write(excel_row, col_idx, val, yes_fmt if is_yes else no_fmt)
                else:
                    ws.write(excel_row, col_idx, val, wrap_fmt)

        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, len(final_df), len(final_df.columns) - 1)

        # ── SHEET 2: Advocate Insights ────────────────────────────────────
        # Layout:
        #   Col A  : msid (advocate ID)           width 16
        #   Col B  : # Contacts                   width 10
        #   Col C  : LOB(s)                        width 12
        #   Col D  : Top Repeat Reason            width 45
        #   Col E  : Issue Resolution             width 40
        #   Col F  : Provider Dissatisfaction     width 40
        #   Col G  : Process Issues               width 40
        #   Col H  : Technology Issues            width 40
        #   Col I  : Agent Knowledge Gaps         width 40
        # ──────────────────────────────────────────────────────────────────
        ws2 = wb.add_worksheet("Advocate_Insights")

        adv_col_headers = [
            ("msid",                    16),
            ("# Contacts",              10),
            ("LOB(s)",                  12),
            ("Top Repeat Reason",       45),
            ("Issue Resolution",        40),
            ("Provider Dissatisfaction",40),
            ("Process Issues",          40),
            ("Technology Issues",       40),
            ("Agent Knowledge Gaps",    40),
        ]

        ws2.set_row(0, 32)
        for ci, (hdr, w) in enumerate(adv_col_headers):
            ws2.set_column(ci, ci, w)
            ws2.write(0, ci, hdr, cat_hdr_fmt)

        ws2.freeze_panes(1, 0)

        BULLET_PREFIXES = [
            "• Top Repeat Reason:",
            "• Issue Resolution:",
            "• Provider Dissatisfaction:",
            "• Process Issues:",
            "• Technology Issues:",
            "• Agent Knowledge Gaps:",
        ]

        for ri, adv_row in advocate_df.iterrows():
            excel_row = ri + 1
            ws2.set_row(excel_row, 75)

            msid_val    = str(adv_row.get("msid", ""))
            n_contacts  = adv_row.get("n_contacts", "")
            lobs        = str(adv_row.get("lobs", ""))
            insight_raw = str(adv_row.get("insight", ""))

            # Parse the 6 bullet lines from LLM output
            bullets = [""] * 6
            lines = [l.strip() for l in insight_raw.split("\n") if l.strip()]
            for line in lines:
                for idx, prefix in enumerate(BULLET_PREFIXES):
                    # Match by prefix (case-insensitive, strip leading bullet char)
                    clean = line.lstrip("•·-").strip()
                    if clean.lower().startswith(prefix.lstrip("• ").lower()):
                        # Remove prefix and keep just the content
                        content = clean[len(prefix.lstrip("• ")):].strip().lstrip(":").strip()
                        bullets[idx] = line  # keep full bullet line for display
                        break
                else:
                    # Fallback: fill in order
                    for idx in range(6):
                        if not bullets[idx]:
                            bullets[idx] = line
                            break

            ws2.write(excel_row, 0, msid_val,   adv_id_fmt)
            ws2.write(excel_row, 1, n_contacts,  count_fmt)
            ws2.write(excel_row, 2, lobs,        count_fmt)
            for col_offset, bullet in enumerate(bullets):
                ws2.write(excel_row, 3 + col_offset, bullet, bullet_fmt)

    print(f"\n✅ Excel saved → {output_file}")

# ─────────────────────────────────────────────
# 11. MAIN PIPELINE
# ─────────────────────────────────────────────
def run_analysis(input_file: str = INPUT_FILE, output_file: str = OUTPUT_FILE):
    print(f"📂 Loading: {input_file}")
    df = pd.read_csv(input_file)
    df.columns = [c.strip() for c in df.columns]

    df_work = df.copy()
    df_work.columns = [c.strip().lower().replace(" ", "_") for c in df_work.columns]

    df_agg = Data_Aggregation(df_work)
    print(f"✅ {len(df_agg)} unique records ready for analysis")

    # ── LLM row-level analysis ─────────────────────────────────────────────
    results = []
    for _, row in tqdm(df_agg.iterrows(), total=len(df_agg), desc="Analysing contacts"):
        results.append(analyze_repeat_contact(row))

    results_df = pd.DataFrame(results)

    for col in [k for _, k in FLAG_FIELDS] + ["root_cause_summary", "Key differences"]:
        if col not in results_df.columns:
            results_df[col] = ""

    final_df = pd.concat([df_agg.reset_index(drop=True), results_df.reset_index(drop=True)], axis=1)

    ordered_cols = [
        "Req_ID", "ixn_conv_ucid", "prev_ixn_conv_ucid", "ixn_date", "msid",
        "lob", "tin", "npi", "state", "prev_contact_date", "prev_msid",
        "sentimentscore", "prev_sentimentscore", "Req_Description",
        "Req_Instructions", "Request_Date",
        "root_cause_summary", "Key differences",
        "Issue resolution",         "Issue_resolution_reason",
        "Provider Dissatisfaction", "Provider_Dissatisfaction_reason",
        "Process Issue",            "Process_Issue_reason",
        "Technology Issue",         "Technology_Issue_reason",
        "Agent Knowledge",          "Agent_Knowledge_reason",
    ]
    final_df = final_df[[c for c in ordered_cols if c in final_df.columns]]

    final_df["ixn_conv_ucid"]      = final_df["ixn_conv_ucid"].astype(str).str.zfill(20)
    final_df["prev_ixn_conv_ucid"] = final_df["prev_ixn_conv_ucid"].astype(str).str.zfill(20)

    # ── Advocate-level insights ────────────────────────────────────────────
    # Group by msid (same advocate = msid == prev_msid in your data)
    # For advocates present in multiple rows, aggregate all their contacts.
    print("\n📝 Generating Advocate-level insights...")

    advocate_rows = []

    # msid is the advocate on the REPEAT call; prev_msid on the ORIGINAL call.
    # They are the same person — group by msid as the primary key.
    grouped = final_df.groupby("msid", sort=False)

    for msid, adv_df in tqdm(grouped, desc="Advocate insights"):
        lobs       = ", ".join(adv_df["lob"].dropna().unique().tolist()) if "lob" in adv_df.columns else ""
        n_contacts = len(adv_df)
        insight    = generate_advocate_insight(msid, adv_df)

        advocate_rows.append({
            "msid":       msid,
            "n_contacts": n_contacts,
            "lobs":       lobs,
            "insight":    insight,
        })
        print(f"  ✔ msid={msid}  ({n_contacts} contact(s))")

    advocate_df = pd.DataFrame(advocate_rows)

    write_excel(final_df, advocate_df, output_file)
    return final_df, advocate_df

# ─────────────────────────────────────────────
if __name__ == "__main__":
    final_df, advocate_df = run_analysis()

