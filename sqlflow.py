import pandas as pd
import json
import os
from sql_auth import build_sql_connections
from csvflow import semantic_csv_analysis


def load_sql_creds():
    print("\n🔐 Please provide SQL credential JSON file path")

    while True:
        json_path = input("Enter SQL Credential JSON path: ").strip()

        if not os.path.exists(json_path):
            print("❌ JSON file not found. Try again.")
            continue

        with open(json_path, "r") as f:
            creds = json.load(f)

        print("✅ SQL credentials loaded successfully")
        return creds


def load_table(conn, table_name, db_type):
    """Loads table based on database type"""

    if db_type == "sqlserver":
        query = f"SELECT * FROM [{table_name}]"

    elif db_type == "mysql":
        query = f"SELECT * FROM `{table_name}`"

    else:
        raise Exception("❌ Unsupported DB type. Use: sqlserver or mysql")

    return pd.read_sql(query, conn)


def handle_sql_flow(fex_content, metadata, metadata_df):

    print("\n🗄️ SQL Mode Selected")
    creds = load_sql_creds()
    db_type = creds.get("db_type", "sqlserver").lower()

    print(f"🔌 Connecting to {db_type.upper()} ...")

    conn = build_sql_connections(creds)
    print("✅ Database connection successful!")
    while True:
        table_input = input("\nEnter table name(s) separated by comma: ").strip()
        tables = [t.strip() for t in table_input.split(",") if t.strip()]

        if not tables:
            print("❌ Enter at least one table name")
            continue

        break
    tables_dict = {}

    for table in tables:
        print(f"\n📥 Loading table: {table}")
        df = load_table(conn, table, db_type)
        print(f"👍 Loaded {len(df)} rows, {len(df.columns)} columns")
        tables_dict[table] = df
    raw_meta_cols = metadata.get("output_columns", []) or []
    metadata_columns = []

    for item in raw_meta_cols:
        if isinstance(item, dict):
            col = item.get("column")
        else:
            col = item

        if col and isinstance(col, str):
            metadata_columns.append(col.strip())

# ---------------- Collect Source Columns ----------------
    all_source_columns = set()
    for df in tables_dict.values():
        all_source_columns.update(df.columns)

    meta_set = {c.lower() for c in metadata_columns if c}
    source_set = {c.lower() for c in all_source_columns}

    matched = meta_set.issubset(source_set)
    if matched:
        print("\n🤖 Agent: 🎉 Columns MATCH across SQL tables")
        validation_df = pd.DataFrame({
            "Status": ["SUCCESS"],
            "Message": ["Metadata columns found in SQL tables"]
        })

    else:
        missing = meta_set - source_set
        print("\n🤖 Agent: ❌ Missing metadata columns:", missing)

        validation_df = pd.DataFrame({
            "Status": ["FAILED"],
            "Missing Columns": [", ".join(missing)]
        })
    any_df = next(iter(tables_dict.values()))
    semantic_df = semantic_csv_analysis(any_df, metadata_columns)
    print("\n💾 Creating SQL validation report...")

    with pd.ExcelWriter("FEX_SQL_Validation_Report.xlsx", engine="openpyxl") as writer:

        if metadata_df is not None:
            metadata_df.to_excel(writer, sheet_name="FEX_Metadata", index=False)

        validation_df.to_excel(writer, sheet_name="Column_Validation", index=False)

        if not semantic_df.empty:
            semantic_df.to_excel(writer, sheet_name="Semantic_Analysis", index=False)

    print("✅ SQL Validation Report Generated: FEX_SQL_Validation_Report.xlsx")

    return any_df, matched, tables_dict
