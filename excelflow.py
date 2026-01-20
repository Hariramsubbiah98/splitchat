import pandas as pd
import os
import openpyxl
import numpy as np

def infer_dtypes(series):
    s = series.dropna()
    if s.empty:
        return "string"
    if pd.api.types.is_numeric_dtype(s):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "date"
    try:
        pd.to_datetime(s,format="mixed")
        return "date"
    except:
        return "string"
    
def semantic_excel_analysis(df,metadata_columns):
    rows = []
    source_cols = list(df.columns)
    dtype_map = {c:infer_dtypes(df[c]) for c in source_cols}

    for meta_col in metadata_columns:
        best_match = None
        match_type = "None"
        dtype_match = "Unknown"

        for col in source_cols:
            if col.lower() == meta_col.lower():
                best_match = col
                match_type = "Exact"
                dtype_match = dtype_map[col]
                break

        if best_match is None:
            for col in source_cols :
                if meta_col.replace("_","").lower()[:4] in col.replace("_","").lower():
                    best_match = col
                    match_type = "Semantic"
                    dtype_match = dtype_map[col]
                    break

        rows.append({
            "Metadata Column": meta_col,
            "Matched Source Column": best_match or "Not Found",
            "Match Type": match_type,
            "Detected Data Type": dtype_match
        })

    return pd.DataFrame(rows)


def handle_excel_flow(fex_content, metadata, metadata_df):
    print("\n📘 Excel Mode Selected")
    print("Provide Excel file path (.xlsx)")

    while True:
        excel_path = input("Enter Excel file path: ").strip()
        if not os.path.exists(excel_path):
            print("❌ File not found. Try again.")
            continue
        break

    print("\n📥 Loading Excel workbook...")
    xls = pd.ExcelFile(excel_path)
    tables_dict = {}

    for sheet in xls.sheet_names:
        print(f"\n📄 Reading sheet: {sheet}")
        df = pd.read_excel(xls, sheet_name=sheet)

        if df.empty:
            print(f"⚠️ Sheet '{sheet}' is empty. Skipping.")
            continue

        tables_dict[sheet] = df
        print(f"👍 Loaded {len(df)} rows, {len(df.columns)} columns")

    if not tables_dict:
        print("❌ No usable sheets found.")
        return None, False, {}

    raw_meta_cols = metadata.get("output_columns", []) or []
    metadata_columns = []

    for item in raw_meta_cols:
        if isinstance(item, dict):
            metadata_columns.append(item.get("column"))
        else:
            metadata_columns.append(item)

    all_source_columns = set()
    for df in tables_dict.values():
        all_source_columns.update(df.columns)

    print("\n📝 Checking column compatibility...")
    print("Metadata Columns:", metadata_columns)
    print("Source Columns:", list(all_source_columns))

    meta_set = {c.lower() for c in metadata_columns if c}
    source_set = {c.lower() for c in all_source_columns}

    matched = meta_set.issubset(source_set)

    if matched:
        print("\n🤖 Agent: 🎉 Columns MATCH across Excel sheets")
        validation_df = pd.DataFrame({
            "Status": ["SUCCESS"],
            "Message": ["Metadata columns found in Excel sheets"]
        })
    else:
        missing = meta_set - source_set
        print("\n🤖 Agent: ❌ Missing metadata columns:", missing)
        validation_df = pd.DataFrame({
            "Status": ["FAILED"],
            "Missing Columns": [", ".join(missing)]
        })

    any_df = next(iter(tables_dict.values()))
    semantic_df = semantic_excel_analysis(any_df, metadata_columns)

    print("\n💾 Creating Excel validation report...")

    with pd.ExcelWriter("FEX_Excel_Validation_Report.xlsx", engine="openpyxl") as writer:

        if metadata_df is not None:
            metadata_df.to_excel(writer, sheet_name="FEX_Metadata", index=False)

        validation_df.to_excel(writer, sheet_name="Column_Validation", index=False)

        if not semantic_df.empty:
            semantic_df.to_excel(writer, sheet_name="Semantic_Analysis", index=False)

    print("✅ Excel Validation Report Generated: FEX_Excel_Validation_Report.xlsx")

    return any_df, matched, tables_dict




