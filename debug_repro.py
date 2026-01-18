import pandas as pd
import os
from utils import split_datetime_columns, clean_column_name, format_all_dates, infer_sql_type
import json
import uuid

# 1. Create Dummy Excel
filename = "test_debug.xlsx"
data = {
    "Date d'achat": ["12/01/2026 10:00:00", "13/01/2026", None],
    "Nom Client": ["Dupont", "Durand", "Martin"],
    "Montant": [100.50, 200, 0],
    "Commentaire": ["Ok", "A vérifier", 0]
}
df = pd.DataFrame(data)
df.to_excel(filename, index=False)
print(f"✅ Created {filename}")

# 2. Simulate process_excel_step2 transformation
try:
    print("\n--- Simulation Transformation ---")
    # Read
    df_read = pd.read_excel(filename, engine='openpyxl')
    print("Read columns:", df_read.columns.tolist())
    
    # Split
    df_split = split_datetime_columns(df_read)
    print("Split columns:", df_split.columns.tolist())
    
    # Clean names
    df_split.columns = [clean_column_name(c) for c in df_split.columns]
    print("Clean columns:", df_split.columns.tolist())
    
    # Format dates
    df_clean = format_all_dates(df_split)
    print("Formatted Data Head:")
    print(df_clean.head())
    
    # Prepare for JSON/DB (Replace NaNs)
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    
    records = df_clean.to_dict(orient='records')
    print("\nRecords for DB Insert (Sample):")
    print(json.dumps(records[0], default=str)) # indent=2
    
    print("\n✅ Simulation Logic SUCCESS")
    
except Exception as e:
    print(f"\n❌ Simulation Logic FAILED: {e}")
    import traceback
    traceback.print_exc()

# 3. Cleanup
try:
    os.remove(filename)
    print(f"Removed {filename}")
except:
    pass
