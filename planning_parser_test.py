import pandas as pd
import datetime

# Mocking "H2258 - FOLKESTONE OPERA - Planning"
# Ligne 2 (Index 1) is header.
# Cols A, B, C are info. D+ are dates.

data_mock = [
    ["Export Planning", "", "", "", "", ""], # Row 0
    ["Room", "Type", "Status", datetime.datetime(2026,1,1), datetime.datetime(2026,1,2), datetime.datetime(2026,1,3)], # Row 1 (Header)
    ["101", "DBL", "Open", 100, 110, 120],
    ["102", "SGL", "Closed", "Fermé", "Fermé", "Fermé"],
    ["103", "SUI", "Open", 200, 220, 250]
]

# Create dummy excel
df_mock = pd.DataFrame(data_mock)
mock_file = "mock_planning.xlsx"
df_mock.to_excel(mock_file, index=False, header=False)
print(f"📄 Generated {mock_file}")

def detect_and_parse(filepath):
    print("\n--- Auto-Detection Logic ---")
    
    # Strategy 1: Planning Format (Header=1)
    try:
        df_plan = pd.read_excel(filepath, header=1, engine='openpyxl')
        # Check if Col D (index 3) is a date?
        if len(df_plan.columns) > 3:
            col_d = df_plan.columns[3]
            print(f"Checking Col D: {col_d} (Type: {type(col_d)})")
            
            is_date_col = isinstance(col_d, datetime.datetime) or "202" in str(col_d)
            
            if is_date_col:
                print("✅ Detected Format: Planning (Dates in columns)")
                return parse_planning(df_plan)
    except:
        pass

    # Strategy 2: Lighthouse (Header=4)
    try:
        df_light = pd.read_excel(filepath, header=4, engine='openpyxl')
        if "Jour Date" in df_light.columns or "Date" in df_light.columns:
             print("✅ Detected Format: Lighthouse")
             # (Reuse existing logic)
             return df_light
    except:
        pass
        
    return None

def parse_planning(df):
    # Transformation: Unpivot (Melt)
    # Vars to keep: Cols A, B, C (indices 0, 1, 2)
    base_cols = df.columns[0:3].tolist()
    date_cols = df.columns[3:].tolist()
    
    print(f"Base columns: {base_cols}")
    print(f"Date columns start at: {date_cols[0]}")
    
    df_melted = df.melt(id_vars=base_cols, value_vars=date_cols, var_name='Date', value_name='Valeur')
    
    # Clean Values
    # User logic: text -> 'x' probably applies here too? 
    # Or maybe 'Fermé' should be kept? 
    # For now let's just show the structure.
    
    return df_melted

parsed_df = detect_and_parse(mock_file)
if parsed_df is not None:
    print("\n--- Parsed Data (First 5 rows) ---")
    print(parsed_df.head())
else:
    print("❌ Detection Failed")
