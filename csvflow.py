import pandas as pd
import os
import json
import openpyxl
import chardet
import csv
import numpy as np


def smart_read_csv(path):
    with open(path, 'rb') as f:
        enc = chardet.detect(f.read())['encoding']

    with open(path, 'r', encoding=enc) as f:
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample)

    print(f"Detected Encoding: {enc} | Delimiter: {dialect.delimiter}")

    return pd.read_csv(path, encoding=enc, delimiter=dialect.delimiter, low_memory=False)


def infer_dtype(series):
    try:
        pd.to_numeric(series.dropna())
        return "numeric"
    except:
        pass

    try:
        pd.to_datetime(series.dropna(), errors="raise")
        return "date"
    except:
        pass

    return "string"


def semantic_csv_analysis(csv_df, metadata_columns):
    print("\n🧠 Performing Semantic Relationship & Metadata Compatibility Analysis...")

    if not metadata_columns:
        print("⚠️ No metadata output columns found. Skipping semantic analysis.")
        return pd.DataFrame()

    rows = []
    source_cols = list(csv_df.columns)

    dtype_map = {}
    for c in source_cols:
        dtype_map[c] = infer_dtype(csv_df[c])

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
            for col in source_cols:
                if meta_col.replace("_", "").replace(" ", "").lower()[:4] in \
                        col.replace("_", "").replace(" ", "").lower():
                    best_match = col
                    match_type = "Semantic"
                    dtype_match = dtype_map[col]
                    break

        rows.append({
            "Metadata Column": meta_col,
            "Matched Source Column": best_match if best_match else "Not Found",
            "Match Type": match_type,
            "Detected Data Type": dtype_match,
        })

    result_df = pd.DataFrame(rows)

    print("\n📊 Semantic Relationship Result:")
    print(result_df)

    return result_df


def handle_csv_flow(fex_content, metadata, metadata_df):

    print("\n📂 CSV Mode Selected")
    print("You can provide SINGLE or MULTIPLE CSV files.")
    print("Enter file paths separated by comma (,)\n")

    tables_dict = {}

    while True:
        paths = input("Enter CSV file path(s): ").strip()
        csv_files = [p.strip() for p in paths.split(",")]

        missing = [p for p in csv_files if not os.path.exists(p)]
        if missing:
            print(f"❌ These files do not exist:\n{missing}")
            continue

        try:
            for f in csv_files:
                print(f"\n📥 Loading: {f}")
                df = smart_read_csv(f)
                print(f"👍 Loaded {len(df)} rows, {len(df.columns)} columns")

                table_name = os.path.splitext(os.path.basename(f))[0]
                tables_dict[table_name] = df

            break

        except Exception as e:
            print(f"❌ Failed to process CSV files: {e}")
            continue


    csv_data = pd.concat(list(tables_dict.values()), ignore_index=True, sort=False)

    all_columns = set(csv_data.columns)
    source_columns = list(all_columns)

    raw_meta_cols = metadata.get("output_columns", []) or []

    metadata_columns = []
    for item in raw_meta_cols:
        if isinstance(item, dict) and "column" in item:
            metadata_columns.append(item["column"])
        elif isinstance(item, str):
            metadata_columns.append(item)


    print("\n📝 Checking column compatibility...")
    print("Metadata Columns:", metadata_columns)
    print("Source Columns:", source_columns)

    meta_set = set([c.lower() for c in metadata_columns])
    source_set = set([c.lower() for c in source_columns])

    matched = meta_set.issubset(source_set)

    if matched:
        print("\n🤖 Agent: 🎉 Columns MATCH")
        validation_df = pd.DataFrame({
            "Status": ["SUCCESS"],
            "Message": ["Metadata columns match"],
            "Matched Columns": [", ".join(metadata_columns)]
        })
    else:
        missing = meta_set - source_set
        print("\n🤖 Agent: ❌ Columns DO NOT MATCH!")
        print("Missing:", missing)

        validation_df = pd.DataFrame({
            "Status": ["FAILED"],
            "Reason": ["Metadata output columns missing in source"],
            "Missing Columns": [', '.join(missing)],
            "Available Source Columns": [', '.join(source_columns)]
        })


    semantic_df = semantic_csv_analysis(csv_data, metadata_columns)

    print("\n💾 Creating consolidated Excel report...")

    with pd.ExcelWriter("FEX_Validation_Report.xlsx", engine="openpyxl") as writer:

        if metadata_df is not None:
            metadata_df.to_excel(writer, sheet_name="FEX_Metadata", index=False)

        validation_df.to_excel(writer, sheet_name="Column_Validation", index=False)

        if not semantic_df.empty:
            semantic_df.to_excel(writer, sheet_name="Semantic_Analysis", index=False)

    print("✅ Consolidated Excel Generated: FEX_Validation_Report.xlsx")

    return csv_data, matched, tables_dict

