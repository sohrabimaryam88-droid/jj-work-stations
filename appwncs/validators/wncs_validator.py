import zipfile
import pandas as pd

REQUIRED_COLUMNS = {
    "NodeId",
    "UtranCellId",
    "localCellId",
    "primaryScramblingCode"
}

def validate_zip_contains_msmt(zip_path):
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            msmt_files = [
                f for f in z.namelist()
                if f.lower().endswith(".msmt")
            ]
    except zipfile.BadZipFile:
        raise ValueError("Invalid ZIP file")

    if not msmt_files:
        raise ValueError("ZIP file must contain at least one .msmt file")

    return msmt_files


def validate_excel_columns(excel_path):
    try:
        df = pd.read_excel(excel_path)
    except Exception:
        raise ValueError("Excel file is corrupted or unreadable")

    missing = REQUIRED_COLUMNS - set(df.columns)

    if missing:
        raise ValueError(
            f"Excel file is missing required columns: {list(missing)}"
        )

    return df
