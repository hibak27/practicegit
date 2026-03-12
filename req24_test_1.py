# ─────────────────────────────────────────────
# 3. ANALYSIS PROMPT
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """You are a healthcare contact center quality analyst.
You analyze pairs of call transcripts (original contact + repeat contact) 
from the same provider about the same patient and topic.

Your job: identify WHY the repeat contact happened and identify the root cause 
with respect to three categories:
- PEOPLE: agent error, lack of knowledge, wrong info given, poor communication
- PROCESS: broken workflow, missing step, policy gap, routing issue, no follow-through
- TECH: system error, portal issue, fax failure, eligibility system down, tech glitch

Respond ONLY in this exact JSON format:
{
  "root_cause_summary": "Detailed 6-7 line of summary of why repeat contact happened",
  "Key differences": "Key differences between both the calls",
  "Issue resolution": "did issue resolution happened in the repeat contact, respond in format Yes/No:Reason"
  "Provider Dissatisfaction": In the call transcript, if the provider expressed any dissatisfaction, respond in format- Yes/No:dissatisfaction reason. If no dissatisfaction expressed, just answer- No."
  "Process Issue":"Was there any process which is time cosuming and contributed to the need for repeat contact(this doesn't involve agent communication/clarity issue) ? respond in format if Yes:Reason else No" 
  "Technology Issue": "Were there any technology-related issues noted that may have impacted the original contact respond in format if Yes:Reason else No"
  "Agent Knowledge":"Were there any agent knowledge gap related issue  that may have impacted the original contact respond in format if Yes:Reason else No"

}"""

USER_PROMPT_TEMPLATE = """LOB: {lob}

ORIGINAL CONTACT (prev_ixn_conv_ucid: {prev_ixn_id}):
{prev_chat}
REPEAT CONTACT (ixn_conv_ucid: {ixn_id}):
{ixn_chat}


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
        #result["status"] = "success"
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
    df = pd.read_csv(input_file)
    
    def Data_Aggregation(df):
        df_0 = df[['ixn_conv_ucid', 'prev_ixn_conv_ucid','ixn_chat', 'prev_chat']]
        df_0.drop_duplicates(inplace = True)
        def chat_processing(x):
            x = x.lower()
            x = re.sub(r'[<>]', '', x)
            x = re.sub(r'\bunk\b', '', x)
            x = re.sub(r'[\(\[].*?[\)\]]', '', x)
            x = re.sub(r'[.]{4}', ' ', x)
            x = re.sub(r'[.]{3}', ',', x)
            x = re.sub(r' ,', ',', x) 
            x = re.sub(r'[.]{2}', '. ', x)
            x = re.sub(r'[.]{1}', '. ', x)
            x = re.sub(r'0:.\s+', '', x)
            x = re.sub(r'0: .\s+', '', x)
            x = re.sub(r'1:.\s+', '', x)
            x = re.sub(r'1: .\s+', '', x)
            x = re.sub(r'\s+', ' ', x)
            return x

        df_0['ixn_chat'] = df_0['ixn_chat'].apply(chat_processing)
        df_0['ixn_chat'] = df_0['ixn_chat'].astype('str')
        #df_0['wc_curr'] = df_0['ixn_chat'].map(lambda x: len(x.split()))
        df_0['ixn_conv_ucid'] = df_0['ixn_conv_ucid'].astype('str')
        df_0['prev_chat'] = df_0['prev_chat'].apply(chat_processing)
        df_0['prev_chat'] = df_0['prev_chat'].astype('str')
        #df_0['wc_prev'] = df_0['prev_chat'].map(lambda x: len(x.split()))
        df_0['prev_ixn_conv_ucid'] = df_0['prev_ixn_conv_ucid'].astype('str')
        df1=df_0[['ixn_conv_ucid', 'prev_ixn_conv_ucid','ixn_chat', 'prev_chat']]
        #df0 = df_0[['ixn_conv_ucid', 'prev_ixn_conv_ucid','ixn_chat', 'prev_chat','wc_curr','wc_prev']]
        #df1 = df0[((df0['wc_curr']&df0['wc_prev']) > 20)]
        print("\nTotal call interactions : {}\nTotal columns : {}\n".format(df1.shape[0], df1.shape[1]))
        return df1
    
    df_temp=Data_Aggregation(df)
    # Normalise column names
    df_temp.columns = [c.strip().lower().replace(" ", "_") for c in df_temp.columns]
    print(f"✅Loaded {len(df_temp)} rows | Columns: {list(df_temp.columns)}")
    #print(f"📊 LOBs found: {df_temp['lob'].unique().tolist()}")

    # ── Run LLM analysis ──────────────────────────────────────────────────
    results = []
    for _, row in tqdm(df.iterrows(), total=len(df_temp), desc="Analysing contacts"):
        analysis = analyze_repeat_contact(row)
        results.append(analysis)

    # Merge results back
    results_df = pd.DataFrame(results)
    #print(results_df)
     
    
    results_df[['Issue resolution', 'Issue_resolution_reason']] = results_df['Issue resolution'].str.split(':', expand=True, n=1)
    results_df[['Provider Dissatisfaction', 'Provider_Dissatisfaction_reason']] = results_df['Provider Dissatisfaction'].str.split(':', expand=True, n=1)
    results_df[['Process Issue', 'Process_Issue_reason']] = results_df['Process Issue'].str.split(':', expand=True, n=1)
    results_df[['Technology Issue', 'Technology_Issue_reason']] = results_df['Technology Issue'].str.split(':', expand=True, n=1)
    results_df[['Agent Knowledge', 'Agent_Knowledge_reason']] = results_df['Agent Knowledge'].str.split(':', expand=True, n=1)
    
    final_df = pd.concat([df.reset_index(drop=True), results_df], axis=1)
    final_df['ixn_conv_ucid'] = final_df['ixn_conv_ucid'].astype('str').str.zfill(20) 
    final_df['prev_ixn_conv_ucid']=final_df['prev_ixn_conv_ucid'].astype('str').str.zfill(20) 
    final_df=final_df[['Req_ID', 'ixn_conv_ucid', 'prev_ixn_conv_ucid','ixn_date', 'msid','lob', 'tin', 'npi','state', 'prev_contact_date',
       'prev_msid','sentimentscore', 'prev_sentimentscore','Req_Description',
       'Req_Instructions', 'Request_Date',"root_cause_summary", "Key differences",
        'Issue resolution',"Issue_resolution_reason",
        'Provider Dissatisfaction',"Provider_Dissatisfaction_reason",
        'Process Issue',"Process_Issue_reason",
        'Technology Issue',"Technology_Issue_reason",
        'Agent Knowledge' ,"Agent_Knowledge_reason" ]]
    
    

    def generate_lob_narrative(lob: str, lob_df: pd.DataFrame) -> str:
        """Ask LLM to write an executive summary for a single LOB."""
        findings = lob_df[["root_cause_summary", "Key differences", "Issue_resolution_reason","Provider_Dissatisfaction_reason","Process_Issue_reason","Technology_Issue_reason","Agent_Knowledge_reason"]].to_dict(orient="records")
        prompt = f"""You are summarizing repeat contact analysis findings for LOB: {lob}.

        Here are the individual findings:
        {json.dumps(findings, indent=2)}

        Write a concise summary in distribution form (3-5 bullet points) covering:
        1) Top reasons for repeat contacts :root_cause_summary
        2) Issue unresolved reasons: Issue_resolution_reason
        3) Provider Dissatisfaction reasons: Provider_Dissatisfaction_reason
        4) Process Issue reasons, if any : Process_Issue_reason
        5) Technology Issue reasons, if any: Technology_Issue_reason
        6) Agent Knowledge reason, if_any : Agent_Knowledge_reason
        
        Use crisp language. Avoid redundancy
        """
        response = llm.invoke([HumanMessage(content=prompt)])
        return response.content
    
    
    rows=[]
    for lob in final_df["lob"].unique():
        lob_df = final_df[final_df["lob"] == lob]
        narrative = generate_lob_narrative(lob, lob_df)
        rows.append({"LOB": lob, "Narrative": narrative})
        
    def generate_overall_insights(final_df: pd.DataFrame) -> str:
        
        """Ask LLM to write an executive summary for a single LOB."""
        findings = final_df[["root_cause_summary", "Key differences", "Issue_resolution_reason","Provider_Dissatisfaction_reason","Process_Issue_reason","Technology_Issue_reason","Agent_Knowledge_reason"]].to_dict(orient="records")
        prompt = f"""You are summarizing repeat contact analysis findings across ALL LOBS.

        Here are the individual findings:
        {json.dumps(findings, indent=2)}

        Write a concise summary in distribution form (3-5 bullet points) covering:
        1) Top reasons for repeat contacts :root_cause_summary
        2) Issue unresolved reasons: Issue_resolution_reason
        3) Provider Dissatisfaction reasons: Provider_Dissatisfaction_reason
        4) Process Issue reasons, if any : Process_Issue_reason
        5) Technology Issue reasons, if any: Technology_Issue_reason
        6)Agent Knowledge reason, if_any : Agent_Knowledge_reason

        Use crisp language. Avoid redundancy
        """
        response = llm.invoke([HumanMessage(content=prompt)])
        return response.content
    
    
    # Overall
    overall_text = generate_overall_insights(final_df)

    
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
        final_df.to_excel(writer, sheet_name="Report", index=False)
        ws = writer.sheets["Report"]
        ws.set_column("A:A", 8)
        ws.set_column("B:C", 22)
        ws.set_column("D:E", 60)
        ws.set_column("F:F", 12)  # category
        ws.set_column("G:I", 40)
        
        #sheet 2: lob narrative
        narrative_df = pd.DataFrame(rows)
        print(narrative_df)
        narrative_df.to_excel(writer, sheet_name="lob_Insights", index=False)
        
        #sheet_3 overall narrative
        print("overall_insights:",overall_text)
        overall_df=pd.DataFrame([{"Scope": "Overall", "Narrative": overall_text}])
        overall_df.to_excel(writer, sheet_name="overall_Insights", index=False)
        
#         # Sheet 2: LOB Summary
#         summary.to_excel(writer, sheet_name="LOB Summary", index=False)
#         ws2 = writer.sheets["LOB Summary"]
#         ws2.set_column("A:A", 10)
#         ws2.set_column("B:Z", 14)

#         # Sheet 3: Overall
#         overall.to_excel(writer, sheet_name="Overall", index=False)

    print("\n✅ Done! Output saved to:", output_file)
#     print("\n📊 OVERALL BREAKDOWN:")
#     print(overall.to_string(index=False))
#     print("\n📊 LOB SUMMARY:")
#     print(summary.to_string(index=False))

    #return final_df, summary, overall
    return final_df

# ─────────────────────────────────────────────
# 6. OPTIONAL: BATCH SUMMARY PER LOB
# ─────────────────────────────────────────────



if __name__ == "__main__":
    #final_df, summary, overall = run_analysis()
    final_df = run_analysis()
    # Uncomment to generate per-LOB narrative summaries:
#     for lob in final_df["lob"].unique():
#         lob_df = final_df[final_df["lob"] == lob]
#         narrative = generate_lob_narrative(lob, lob_df)
#         print(f"\n=== {lob} ===\n{narrative}")
