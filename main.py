from openai import OpenAI
import os
import json
import pandas as pd
from csvflow import handle_csv_flow
from excelflow import handle_excel_flow
from Tmdl_genrator import build_tmdl_with_relationships
from sqlflow import handle_sql_flow
from build_pbix import build_pbix_from_tmdl

Agent_Name = "Analysis Master"
client = OpenAI(api_key="OpenAIApiKey")



def analyze_fex(file_path):
    if not os.path.exists(file_path):
        print("❌ File Not Found")
        exit()
    with open(file_path, "r", encoding="utf-8") as fh:
        return fh.read()



def getmetadata(fexcontent):

    metadata_prompt = f"""
You are {Agent_Name}, a WebFOCUS FEX expert.

Your task:
1️⃣ Parse the FEX
2️⃣ Extract metadata
3️⃣ If something is NOT explicitly available, INTELLIGENTLY INFER it
4️⃣ ALWAYS return JSON in EXACT FORMAT below
5️⃣ NEVER return empty datasources or joins

Return ONLY JSON. NO extra text.

{{
 "report_name": "",
 "description": "",
 "author": "",
 "inputs": [],
 "datasources": [
     {{
        "table_name": "",
        "type": "explicit | inferred"
     }}
 ],
 "joins": [
     {{
        "left_table": "",
        "left_column": "",
        "right_table": "",
        "right_column": "",
        "join_type": "inner|left|right|full|unknown",
        "identified_from": "explicit|inferred"
     }}
 ],
 "filters": [],
 "output_columns": [],
 "output_type": "",
 "dependencies": [],
 "performance_risks": [],
 "recommendations": []
}}

Inference Rules:
- If FILE, TABLE or JOIN exists → mark datasources as explicit
- If only column names like CUST.CUST_ID → infer table CUSTOMER
- If JOIN keywords missing but matching keys exist → infer join
- If nothing exists → return best intelligent guess

FEX CONTENT:
{fexcontent}
"""

    response = client.responses.create(
        model="gpt-5-nano",
        input=metadata_prompt
    )

    raw = response.output_text.strip()
    print("\n===== INITIAL AI RAW OUTPUT =====\n")
    print(raw)

    try:
        metadata = json.loads(raw)
        df = pd.json_normalize(metadata)
        print("\n✅ Metadata JSON parsed successfully")
        return metadata, df
    except Exception as e:
        print("\n❌ Metadata JSON parsing failed. Creating fallback JSON.")
        print(e)

        metadata = {
            "report_name": "Unknown Report",
            "description": "AI failed to extract details",
            "author": "",
            "inputs": [],
            "datasources": [{"table_name": "Unknown", "type": "inferred"}],
            "joins": [],
            "filters": [],
            "output_columns": [],
            "output_type": "",
            "dependencies": [],
            "performance_risks": [],
            "recommendations": []
        }

        return metadata, pd.json_normalize(metadata)



if __name__ == "__main__":

    print("\n👋 Hi, this is the FEXA Agent!")
    file_path = input("Enter FEX file path: ").strip()
    print("\n📡 Analyzing FEX... Please wait...\n")

    fex_content = analyze_fex(file_path)

    metadata, metadata_df = getmetadata(fex_content)

    print("\n💾 Creating Advanced Metadata Analysis Excel...")

    report_name = metadata.get("report_name", "FEX_Report")

    datasources = metadata.get("datasources", [])
    joins = metadata.get("joins", [])
    filters = metadata.get("filters", [])
    outputs = metadata.get("output_columns", [])

    analysis_prompt = f"""
You are a Power BI and Data Modeling Expert.

Return ONLY JSON in EXACTLY this structure.
No extra text.

{{
 "measures": [
   {{ "name": "", "description": "", "expression_idea": "" }}
 ],
 "calculated_columns": [
   {{ "name": "", "description": "", "expression_idea": "" }}
 ],
 "visuals": [
   {{ "visual": "", "reason": "" }}
 ]
}}

Metadata:
{json.dumps(metadata)}
"""

    ai = client.responses.create(
        model="gpt-5-nano",
        input=analysis_prompt
    )

    try:
        ai_json = json.loads(ai.output_text)

        measures_df = pd.DataFrame(ai_json.get("measures", []))
        calc_df = pd.DataFrame(ai_json.get("calculated_columns", []))
        visuals_df = pd.DataFrame(ai_json.get("visuals", []))

    except:
        measures_df = pd.DataFrame()
        calc_df = pd.DataFrame()
        visuals_df = pd.DataFrame()


    excel_file = f"{report_name}_Metadata_Analysis.xlsx"

    with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:

        if metadata_df is not None and not metadata_df.empty:
            metadata_df.to_excel(writer, sheet_name="Metadata Summary", index=False)

        if datasources:
            pd.DataFrame(datasources).to_excel(
                writer, sheet_name="Datasources", index=False
            )

        if joins:
            pd.DataFrame(joins).to_excel(
                writer, sheet_name="Joins", index=False
            )

        if filters:
            pd.DataFrame(filters, columns=["Filters"]).to_excel(
                writer, sheet_name="Filters", index=False
            )

        if outputs:
            pd.DataFrame(outputs, columns=["Output Columns"]).to_excel(
                writer, sheet_name="Output Columns", index=False
            )

        if not measures_df.empty:
            measures_df.to_excel(writer, sheet_name="Measures", index=False)

        if not calc_df.empty:
            calc_df.to_excel(writer, sheet_name="Calculated Columns", index=False)

        if not visuals_df.empty:
            visuals_df.to_excel(writer, sheet_name="Visuals", index=False)

        if (
            metadata_df is None
            and measures_df.empty
            and calc_df.empty
            and visuals_df.empty
        ):
            pd.DataFrame(["No Metadata Found"]).to_excel(
                writer, sheet_name="Summary", index=False
            )

    print(f"\n🎯 DONE! Excel Generated Successfully → {excel_file}")
    print("\n📌 Please select your data source")
    print("Options: csv | excel | sql | quit")

    source = ""
    while source not in ["csv", "excel", "sql", "quit"]:
        source = input("Enter Source Type (csv/excel/sql/quit): ").strip().lower()

    data_df = None
    matched = False
    tables = {}

    if source == "csv":
        print("\n📂 CSV Source Selected")
        print("\n📂 CSV Assistant is called.......")

        data_df, matched, tables = handle_csv_flow(
            fex_content,
            metadata,
            metadata_df
        )

        if not tables:
            print("❌ No tables returned from CSV Assistant. Cannot proceed.")
            exit()

        print("\n🤖 TMDL Assistant is ready...")

        proceed = input(
            "\n❓ Do you want to generate TMDL + BIM model now? (yes/no): "
        ).strip().lower()

        if proceed in ["yes", "y"]:
            print("\n🤖 TMDL Assistant is called...")
            build_tmdl_with_relationships(tables, metadata)
        else:
            print("\n👍 Skipping TMDL creation. Process completed.")

        print("\n❓ Do you want to generate Power BI PBIX file now? (yes/no): ")
        pbix_confirm = input().strip().lower()
        
        if pbix_confirm in ["yes","y"]:
            try:
                pbix_file = build_pbix_from_tmdl()
                print(f"\n🎯 Power BI file ready: {pbix_file}")

            except Exception as e:
                print("❌ PBIX build failed:", e)

        else:
            print("👍 Skipping PBIX creation.")

        print("\n💬 You can now ask questions about this FEX, data, or model.")
        print("Type 'quit' to end the session.\n")

        while True:
            user_q = input("You: ").strip()

            if user_q.lower() in ["quit", "exit"]:
                print("\n👋 Session ended. Goodbye!")
                break

            schema_summary = {
                table: list(df.columns)
                for table, df in tables.items()
            }

            qa_prompt = f"""
    You are an expert BI & Power BI assistant.

    Context:
    - FEX logic:
    {fex_content}

    - Extracted Metadata:
    {json.dumps(metadata, indent=2)}

    - Source Tables & Columns:
    {json.dumps(schema_summary, indent=2)}

    User Question:
    {user_q}

    Answer clearly and concisely.
    """

            try:
                response = client.responses.create(
                    model="gpt-5-nano",
                    input=qa_prompt
                )
                print("\n🤖 Agent:", response.output_text.strip(), "\n")
            except Exception as e:
                print("⚠️ AI error:", e)


    elif source == "excel":
        print("\n📘 Excel Source Selected")
        print("\n📘 Excel Assistant called.....")

        data_df, matched, tables = handle_excel_flow(
            fex_content,
            metadata,
            metadata_df
        )

        if not tables:
            print("❌ No tables returned from Excel Assistant. Cannot proceed.")
            exit()


        proceed = input(
            "\n❓ Do you want to generate TMDL + BIM model now? (yes/no): "
        ).strip().lower()

        if proceed in ["yes", "y"]:
            print("\n🤖 TMDL Assistant is called ......")
            build_tmdl_with_relationships(tables, metadata)
        else:
            print("\n👍 Skipping TMDL creation. Process completed.")

        print("\n❓ Do you want to generate Power BI PBIX file now? (yes/no): ")
        pbix_confirm = input().strip().lower()
        
        if pbix_confirm in ["yes","y"]:
            try:
                pbix_file = build_pbix_from_tmdl()
                print(f"\n🎯 Power BI file ready: {pbix_file}")

            except Exception as e:
                print("❌ PBIX build failed:", e)

        else:
            print("👍 Skipping PBIX creation.")

        print("\n💬 You can now ask questions about this FEX, data, or model.")
        print("Type 'quit' to end the session.\n")

        while True:
            user_q = input("You: ").strip()

            if user_q.lower() in ["quit", "exit"]:
                print("\n👋 Session ended. Goodbye!")
                break

            schema_summary = {
                table: list(df.columns)
                for table, df in tables.items()
            }

            qa_prompt = f"""
            You are an expert BI & Power BI assistant.

            Context:
            - FEX logic:
            {fex_content}

            - Extracted Metadata:
            {json.dumps(metadata, indent=2)}

            - Source Tables & Columns:
            {json.dumps(schema_summary, indent=2)}

            User Question:
            {user_q}

            Answer clearly and concisely.
        """

        try:
            response = client.responses.create(
                model="gpt-5-nano",
                input=qa_prompt
            )
            print("\n🤖 Agent:", response.output_text.strip(), "\n")
        except Exception as e:
            print("⚠️ AI error:", e)


    elif source == "sql":
        print("\n🗄️ SQL Server Source Selected")
        print("📘 SQL Server Assistant has been called........")

        data_df, matched, tables = handle_sql_flow(
        fex_content,
        metadata,
        metadata_df
        )
        if tables:
            proceed = input(
                "\n❓ Do you want to generate TMDL + BIM model now? (yes/no): "
            ).strip().lower()

        if proceed in ["yes", "y"]:
            print("🤖 TMDL Assistant is called ......")
            build_tmdl_with_relationships(tables, metadata)

        else:
            print("\n👍 Skipping TMDL creation. Process completed.")

        print("\n❓ Do you want to generate Power BI PBIX file now? (yes/no): ")
        pbix_confirm = input().strip().lower()
        
        if pbix_confirm in ["yes","y"]:
            try:
                pbix_file = build_pbix_from_tmdl()
                print(f"\n🎯 Power BI file ready: {pbix_file}")

            except Exception as e:
                print("❌ PBIX build failed:", e)

        else:
            print("👍 Skipping PBIX creation.")

        print("\n💬 You can now ask questions about this FEX, data, or model.")
        print("Type 'quit' to end the session.\n")
        while True:
            user_q = input("You: ").strip()
            if user_q.lower() in ["quit", "exit"]:
                print("\n👋 Session ended. Goodbye!")
                break
            schema_summary = {
            table: list(df.columns)
            for table, df in tables.items()
            }
            qa_prompt = f"""
                    You are an expert BI & Power BI assistant.

                    Context:
                    - FEX logic:
                    {fex_content}

                    - Extracted Metadata:
                    {json.dumps(metadata, indent=2)}

                    - Source Tables & Columns:
                    {json.dumps(schema_summary, indent=2)}

                    User Question:
                    {user_q}

                    Answer clearly and concisely.
                    """
            
            response = client.responses.create(
            model="gpt-5-nano",
            input=qa_prompt
            )
            
            print("\n🤖 Agent:", response.output_text.strip(), "\n")


    elif source == "quit":
        print("\n👋 Session Ended. Goodbye!")
        exit()


