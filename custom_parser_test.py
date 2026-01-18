import pandas as pd
import numpy as np
import io

# Mocking the data based on the user's screenshot ("Tarifs" tab)
# Header seems to be on line 5 (index 4)
# Columns: "Jour Date", "Demande du marché", "Folkestone Opéra", "Hôtel Madeleine Haussmann"...

data_mock = [
    ["Metadata", "", "", "", ""], # 0
    ["", "", "", "", ""],         # 1
    ["", "", "", "", ""],         # 2
    ["", "", "", "", ""],         # 3
    ["Jour Date", "Demande du marché", "Folkestone Opéra", "Hôtel Madeleine Haussmann", "Hôtel De L'Arcade"], # 4 (Header)
    ["Jeu 15/01/2026", "35%", "187", "330", "Pas de flex"],
    ["Ven 16/01/2026", "37%", "213", "300", "Pas de flex"],
    ["Sam 17/01/2026", "47%", "229", "255", "Pas de flex"],
    ["Dim 18/01/2026", "21%", "156", "210", "211"],
    ["Lun 26/01/2026", "37%", "240", "1 pax seulement", "363"],
    ["Mar 27/01/2026", "64%", "359", "480", "Épuisé"]
]

# Create dummy excel
df_mock = pd.DataFrame(data_mock)
mock_file = "mock_booking.xlsx"
df_mock.to_excel(mock_file, index=False, header=False)
print(f"📄 Validation: Created {mock_file} simulating the user file structure.")

def parse_booking_style(filepath):
    print("\n--- Parsing Logic ---")
    
    # 1. Read Excel with header=4 (Line 5)
    # We use engine='openpyxl'
    df = pd.read_excel(filepath, header=4, engine='openpyxl')
    
    print("Columns found:", df.columns.tolist())
    
    # 2. Identify Metadata cols vs Hotel cols
    # "Jour Date" -> Date
    # "Demande du marché" -> Meta
    # Others -> Hotels
    
    # Rename Date col if exists
    if "Jour Date" in df.columns:
        df.rename(columns={"Jour Date": "Date"}, inplace=True)
        
    cols = df.columns.tolist()
    
    # 3. Processing
    processed_data = df.copy()
    
    # Iterate over all columns to clean "Non-numeric" -> "x"
    # EXCEPT Date and maybe Demande (which is percentage text)
    
    for col in processed_data.columns:
        if col == "Date":
            continue
            
        # Check logic for "Demande du marché" -> Keep as is? Or clean? 
        # User said: "ces valeurs sont numérique ou text lorsqu'il s'agit d'un commentaire, les commentaire seront remplacés par 'x'"
        # "Demande du marché" values are "35%", so they are text/numeric strings. User likely refers to PRICES columns.
        
        # Heuristic: If column name is a Hotel, apply the "x" logic.
        # How to distinguish? "Demande du marché" is explicit. "Date" is explicit.
        # Everything else is likely a hotel.
        
        if col not in ["Date", "Demande du marché"]:
            print(f"Cleaning Hotel Column: {col}")
            # Logic: Try convert to float. If fail, set to 'x'.
            # But "330" is valid. "Pas de flex" is not.
            
            def clean_value(val):
                if pd.isna(val): return None
                s_val = str(val).strip()
                # Try to see if it is a number
                try:
                    float(s_val.replace(',', '.')) # Handle French decimals if any
                    return s_val # Keep number as string or number? User said "valeurs sont numérique".
                except ValueError:
                    return "x"
            
            processed_data[col] = processed_data[col].apply(clean_value)

    return processed_data

try:
    df_clean = parse_booking_style(mock_file)
    print("\n--- Result Head ---")
    print(df_clean.head(10))
    print("\n✅ Parsing & Cleaning Successful")
except Exception as e:
    print(f"❌ Error: {e}")
