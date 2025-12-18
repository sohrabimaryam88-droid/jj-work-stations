import pandas as pd
import time, os, zipfile, tempfile
from pathlib import Path

start_time = time.time()

############

def RowFile(zip_path):
    try:
        zip_path = Path(zip_path)

        if not zip_path.exists() or zip_path.suffix != ".zip":
            raise ValueError("Input must be a valid zip file")

        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(tmpdir)

            tmpdir_path = Path(tmpdir)

            all_files = list(tmpdir_path.rglob("*.msmt"))
            if not all_files:
                raise ValueError("No .msmt files found inside zip")

            df_list = []

            for f in all_files:
                df = pd.read_csv(f, sep="\t")
                df_list.append(df)

            df_total = pd.concat(df_list, ignore_index=True)

            # ---- پردازش دیتافریم ----
            df_total = (
                df_total[["CellName", "SC", "numberOfEvents", "numberOfDrops"]]
                .rename(columns={"SC": "PSC"})
            )

            df_total[["RNC", "UtranCellId"]] = (
                df_total["CellName"].str.split("/", n=1, expand=True)
            )

            df_total = df_total[
                ["RNC", "UtranCellId", "PSC", "numberOfEvents", "numberOfDrops"]
            ]

            df_total["CELLNAME"] = "TH" + df_total["UtranCellId"].str.extract(r"(\d{4}\w)")
            df_total["PSC"] = df_total["PSC"].astype(str)

            return df_total

    except Exception as e:
        print(f"❌ Error in processing rowfile: {str(e)}")
        raise


if __name__ == "__main__":
    try:
        zip_file_path = "D:\\3.CS-DICREPANCY\\7-WNCS\\appwncs\\rowfile\\rowfile.zip" 
        rowfile = RowFile(zip_file_path)

        rowfile.to_csv("totalcell.csv", index=False)
        print(rowfile.head())
        print("✅ Operation completed successfully")

    except Exception as e:
        print(f" Script failed: {str(e)}")
