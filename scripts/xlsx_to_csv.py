import pandas as pd
from pathlib import Path

src = Path("/mnt/c/Users/HP/Documents/University/ida/Pharma_Supply_Chain_Source_Data.xlsx")
out = Path.home() / "sdi-pipeline" / "data" / "source"

xl = pd.ExcelFile(src, engine="calamine")
print("Sheets found:", xl.sheet_names)

for sheet in xl.sheet_names:
    df = xl.parse(sheet)
    name = sheet.strip().replace(" ", "_").lower()
    path = out / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"{sheet}: {len(df):,} rows, {len(df.columns)} cols -> {path.name}")
