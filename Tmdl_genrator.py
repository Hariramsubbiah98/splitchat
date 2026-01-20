import os
import json
import pandas as pd
from datetime import datetime
from openai import OpenAI
import re

client = OpenAI(api_key="OpenAiApikey")


def predict_relationships(table_schemas):

    prompt = f"""
You are a Power BI data modeling expert.
Based on the below table schemas, predict the best possible Power BI relationships.

Rules:
- Prefer primary key -> foreign key patterns
- Match by column name similarity
- Prefer ID / Key columns
- Avoid circular relationships
- Return ONLY VALID JSON in this format:

[
  {{
    "FromTable": "",
    "FromColumn": "",
    "ToTable": "",
    "ToColumn": "",
    "Cardinality": "OneToMany",
    "CrossFilterDirection": "Both",
    "Active": true
  }}
]

TABLE SCHEMA:
{json.dumps(table_schemas)}
"""

    response = client.responses.create(
        model="gpt-5-nano",
        input=prompt
    )

    text = response.output_text.strip()

    try:
        json_match = re.search(r"\[.*\]", text, re.S)
        if json_match:
            return json.loads(json_match.group())
        return []
    except:
        print("⚠️ Could not parse relationship JSON")
        return []


def map_dtype_to_tmdl(dtype):
    dtype = str(dtype).lower()

    if "int" in dtype or "float" in dtype or "decimal" in dtype:
        return "double"
    if "date" in dtype or "time" in dtype:
        return "dateTime"
    return "string"


def build_tmdl_with_relationships(dataframes_dict, metadata):

    if not dataframes_dict:
        print("❌ No tables received. Cannot build TMDL")
        return

    model_name = metadata.get("report_name", "FEX_Semantic_Model")

    schema_dict = {table: list(df.columns) for table, df in dataframes_dict.items()}
    relationships = predict_relationships(schema_dict)

    if not relationships:
        print("\n⚠️ No relationships predicted.")
    else:
        print("\n🔮 Predicted Relationships:")
        for r in relationships:
            print(f"  {r['FromTable']}.{r['FromColumn']}  --->  {r['ToTable']}.{r['ToColumn']}")

        confirm = input("\n❓ Do you want to apply these relationships? (yes/no): ").strip().lower()
        if confirm not in ["yes", "y"]:
            relationships = []

    print("\n🏗️ Generating TMDL + BIM Models...")
    os.makedirs("TMDL_Model/Tables", exist_ok=True)

    model_tmdl = f"""
Model:
  Name: {model_name}
  Culture: en-US
  CompatibilityLevel: 1560

  Annotations:
  - Name: GeneratedBy
    Value: "FEX Analysis Agent"
  - Name: GeneratedOn
    Value: "{datetime.now()}"
"""

    if relationships:
        model_tmdl += "\n  Relationships:\n"
        for r in relationships:
            model_tmdl += f"""
  - Name: {r['FromTable']}_{r['FromColumn']}_to_{r['ToTable']}_{r['ToColumn']}
    FromTable: {r['FromTable']}
    FromColumn: {r['FromColumn']}
    ToTable: {r['ToTable']}
    ToColumn: {r['ToColumn']}
    Cardinality: {r['Cardinality']}
    CrossFilterDirection: {r['CrossFilterDirection']}
    IsActive: {str(r['Active']).lower()}
"""

    with open("TMDL_Model/model.tmd", "w", encoding="utf-8") as f:
        f.write(model_tmdl.strip())


    for table, df in dataframes_dict.items():
        os.makedirs(f"TMDL_Model/Tables/{table}", exist_ok=True)

        table_def = "Table:\n"
        table_def += f"  Name: {table}\n"
        table_def += "  Columns:\n"

        for col in df.columns:
            dtype = map_dtype_to_tmdl(df[col].dtype)
            table_def += f"  - Name: {col}\n"
            table_def += f"    DataType: {dtype}\n"

        with open(f"TMDL_Model/Tables/{table}/table.tmd", "w", encoding="utf-8") as f:
            f.write(table_def)


    bim = {
        "name": model_name,
        "compatibilityLevel": 1560,
        "model": {
            "tables": []
        }
    }

    for table, df in dataframes_dict.items():
        t = {
            "name": table,
            "columns": []
        }

        for col in df.columns:
            t["columns"].append({
                "name": col,
                "dataType": map_dtype_to_tmdl(df[col].dtype)
            })

        bim["model"]["tables"].append(t)


    if relationships:
        bim_relationships = []

        for r in relationships:
            bim_relationships.append({
                "name": f"{r['FromTable']}_{r['FromColumn']}_to_{r['ToTable']}_{r['ToColumn']}",
                "fromTable": r["FromTable"],
                "fromColumn": r["FromColumn"],
                "toTable": r["ToTable"],
                "toColumn": r["ToColumn"],
                "crossFilteringBehavior": "bothDirections",
                "isActive": r["Active"]
            })

        bim["model"]["relationships"] = bim_relationships


    with open("FEX_Semantic_Model.bim", "w") as f:
        json.dump(bim, f, indent=4)

    print("\n🎯 TMDL + BIM Generated Successfully!")
    print("📁 Output Folder: TMDL_Model")
    print("📄 BIM File: FEX_Semantic_Model.bim\n")
