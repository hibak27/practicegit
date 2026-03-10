"""
Repeat Contact Analysis using Azure OpenAI + LangChain
Analyzes call transcripts to identify People/Process/Tech opportunities per LOB
"""

import os
import pandas as pd
import json
import warnings
from tqdm import tqdm
from configparser import ConfigParser
import xlsxwriter

from langchain_openai import AzureChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import HumanMessage, SystemMessage

warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────
# 1. CONFIGURATION — fill in your values
# ─────────────────────────────────────────────
AZURE_OPENAI_API_KEY     = os.getenv("AZURE_OPENAI_API_KEY", "YOUR_KEY_HERE")
AZURE_OPENAI_ENDPOINT    = os.getenv("AZURE_OPENAI_ENDPOINT", "https://YOUR_RESOURCE.openai.azure.com/")
AZURE_OPENAI_DEPLOYMENT  = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4")   # your deployment name
AZURE_OPENAI_API_VERSION = "2024-02-15-preview"

INPUT_FILE  = "your_data.xlsx"   # ← change to your file path
OUTPUT_FILE = "repeat_contact_opportunities.xlsx"


# ─────────────────────────────────────────────
# 2. INIT AZURE OPENAI
# ─────────────────────────────────────────────
llm = AzureChatOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    azure_deployment=AZURE_OPENAI_DEPLOYMENT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
    temperature=0,
    max_tokens=1000,
)


# ─────────────────────────────────────────────
# 3. ANALYSIS PROMPT
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """You are a healthcare contact center quality analyst.
You analyze pairs of call transcripts (original contact + repeat contact) 
from the same provider about the same patient and topic.

Your job: identify WHY the repeat contact happened and classify the root cause 
into one of three categories:
- PEOPLE: agent error, lack of knowledge, wrong info given, poor communication
- PROCESS: broken workflow, missing step, policy gap, routing issue, no follow-through
- TECH: system error, portal issue, fax failure, eligibility system down, tech glitch

Respond ONLY in this exact JSON format:
{
  "category": "PEOPLE" | "PROCESS" | "TECH",
  "root_cause_summary": "1-2 sentence summary of why repeat contact happened",
  "original_issue": "what was the issue in the first call",
  "what_went_wrong": "what failed that caused the repeat",
  "recommendation": "specific actionable fix"
}"""

USER_PROMPT_TEMPLATE = """LOB: {lob}

ORIGINAL CONTACT (ixn_conv_ucid: {ixn_id}):
{ixn_chat}

REPEAT CONTACT (prev_ixn_conv_ucid: {prev_ixn_id}):
{prev_chat}

Analyze why this provider had to call back about the same patient/topic."""


# ─────────────────────────────────────────────
# 4. ANALYZE A SINGLE ROW
# ─────────────────────────────────────────────
def analyze_repeat_contact(row: pd.Series) -> dict:
    """Send one transcript pair to Azure OpenAI and return structured result."""
    prompt = USER_PROMPT_TEMPLATE.format(
        lob=row.get("lob", "Unknown"),
        ixn_id=row.get("ixn_conv_ucid", ""),
        prev_ixn_id=row.get("prev_ixn_conv_ucid", ""),
        ixn_chat=row.get("ixn_chat", ""),
        prev_chat=row.get("prev_chat", ""),
    )
    try:
        response = llm.invoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=prompt),
        ])
        raw = response.content.strip()
        # Strip markdown fences if present
        raw = raw.replace("```json", "").replace("```", "").strip()
        result = json.loads(raw)
        result["status"] = "success"
    except json.JSONDecodeError:
        result = {
            "category": "UNKNOWN",
            "root_cause_summary": raw,
            "original_issue": "",
            "what_went_wrong": "",
            "recommendation": "",
            "status": "json_parse_error",
        }
    except Exception as e:
        result = {
            "category": "ERROR",
            "root_cause_summary": str(e),
            "original_issue": "",
            "what_went_wrong": "",
            "recommendation": "",
            "status": "api_error",
        }
    return result


# ─────────────────────────────────────────────
# 5. MAIN PIPELINE
# ─────────────────────────────────────────────
def run_analysis(input_file: str = INPUT_FILE, output_file: str = OUTPUT_FILE):
    print(f"📂 Loading data from: {input_file}")
    df = pd.read_excel(input_file)

    # Normalise column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    print(f"✅ Loaded {len(df)} rows | Columns: {list(df.columns)}")
    print(f"📊 LOBs found: {df['lob'].unique().tolist()}")

    # ── Run LLM analysis ──────────────────────────────────────────────────
    results = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Analysing contacts"):
        analysis = analyze_repeat_contact(row)
        results.append(analysis)

    # Merge results back
    results_df = pd.DataFrame(results)
    final_df = pd.concat([df.reset_index(drop=True), results_df], axis=1)

    # ── LOB Summary ───────────────────────────────────────────────────────
    summary = (
        final_df.groupby(["lob", "category"])
        .size()
        .reset_index(name="count")
        .pivot_table(index="lob", columns="category", values="count", fill_value=0)
    )
    summary["TOTAL"] = summary.sum(axis=1)
    summary = summary.reset_index()

    overall = final_df["category"].value_counts().reset_index()
    overall.columns = ["category", "count"]
    overall["pct"] = (overall["count"] / overall["count"].sum() * 100).round(1)

    # ── Write to Excel ────────────────────────────────────────────────────
    print(f"\n💾 Writing output to: {output_file}")
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        workbook = writer.book

        # Formats
        hdr_fmt  = workbook.add_format({"bold": True, "bg_color": "#1F3864", "font_color": "white", "border": 1})
        ppl_fmt  = workbook.add_format({"bg_color": "#FFD966"})  # yellow = PEOPLE
        proc_fmt = workbook.add_format({"bg_color": "#F4B942"})  # orange = PROCESS
        tech_fmt = workbook.add_format({"bg_color": "#9DC3E6"})  # blue   = TECH

        # Sheet 1: Detail
        final_df.to_excel(writer, sheet_name="Detail", index=False)
        ws = writer.sheets["Detail"]
        ws.set_column("A:A", 8)
        ws.set_column("B:C", 22)
        ws.set_column("D:E", 60)
        ws.set_column("F:F", 12)  # category
        ws.set_column("G:I", 40)

        # Sheet 2: LOB Summary
        summary.to_excel(writer, sheet_name="LOB Summary", index=False)
        ws2 = writer.sheets["LOB Summary"]
        ws2.set_column("A:A", 10)
        ws2.set_column("B:Z", 14)

        # Sheet 3: Overall
        overall.to_excel(writer, sheet_name="Overall", index=False)

    print("\n✅ Done! Output saved to:", output_file)
    print("\n📊 OVERALL BREAKDOWN:")
    print(overall.to_string(index=False))
    print("\n📊 LOB SUMMARY:")
    print(summary.to_string(index=False))

    return final_df, summary, overall


# ─────────────────────────────────────────────
# 6. OPTIONAL: BATCH SUMMARY PER LOB
# ─────────────────────────────────────────────
def generate_lob_narrative(lob: str, lob_df: pd.DataFrame) -> str:
    """Ask LLM to write an executive summary for a single LOB."""
    findings = lob_df[["category", "root_cause_summary", "recommendation"]].to_dict(orient="records")
    prompt = f"""You are summarizing repeat contact analysis findings for LOB: {lob}.

Here are the individual findings:
{json.dumps(findings, indent=2)}

Write a concise executive summary (3-5 bullet points) covering:
- Top reasons for repeat contacts
- Key People/Process/Tech breakdowns  
- Top 2-3 actionable recommendations
"""
    response = llm.invoke([HumanMessage(content=prompt)])
    return response.content


if __name__ == "__main__":
    final_df, summary, overall = run_analysis()

    # Uncomment to generate per-LOB narrative summaries:
    # for lob in final_df["lob"].unique():
    #     lob_df = final_df[final_df["lob"] == lob]
    #     narrative = generate_lob_narrative(lob, lob_df)
    #     print(f"\n=== {lob} ===\n{narrative}")
