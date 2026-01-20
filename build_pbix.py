import os
import subprocess

TABULAR_EDITOR_PATH = r"C:\Program Files (x86)\Tabular Editor\TabularEditor.exe"
TMDL_FOLDER = "TMDL_Model"
OUTPUT_PBIX = "FEX_Report.pbix"


def build_pbix_from_tmdl():

    if not os.path.exists(TABULAR_EDITOR_PATH):
        raise Exception("❌ TabularEditor.exe not found. Please install Tabular Editor 2.5+")

    if not os.path.exists(TMDL_FOLDER):
        raise Exception("❌ TMDL_Model folder not found. Run TMDL generator first.")

    print("\n🏗️ Building PBIX from TMDL model...")

    cmd = [
        TABULAR_EDITOR_PATH,
        TMDL_FOLDER,
        "-B",              
        OUTPUT_PBIX
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print("\n❌ Tabular Editor Error Output:\n")
        print(result.stderr)
        raise Exception("PBIX generation failed")

    print("\n🎉 PBIX GENERATED SUCCESSFULLY!")
    print(f"📄 Output File: {OUTPUT_PBIX}")

    return OUTPUT_PBIX


if __name__ == "__main__":
    build_pbix_from_tmdl()
