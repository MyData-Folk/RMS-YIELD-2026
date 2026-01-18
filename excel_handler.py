import pandas as pd
import openpyxl
import datetime

def list_sheets(file_path):
    """
    Returns a list of sheet names from an Excel file.
    """
    try:
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        return xls.sheet_names
    except Exception as e:
        raise ValueError(f"Erreur lors de la lecture du fichier Excel: {e}")

def read_excel_sheet(file_path, sheet_name):
    """
    Reads a specific sheet from an Excel file into a DataFrame (Standard format).
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
        return df
    except Exception as e:
        raise ValueError(f"Erreur lecture standard: {e}")

def read_smart_excel(file_path, sheet_name):
    """
    Tries to detect the format (Planning vs Lighthouse) and returns a standardized DataFrame.
    """
    
    # --- STRATEGY 1: Format "Planning" (Header Row 1 or 2) ---
    # We try both header=0 and header=1
    for h_idx in [0, 1]:
        try:
            df_plan = pd.read_excel(file_path, sheet_name=sheet_name, header=h_idx, engine='openpyxl')
            # Look at columns starting at index 3 (Col D)
            if len(df_plan.columns) > 3:
                # We check the first few potential date columns
                potential_dates = df_plan.columns[3:6]
                is_planning = any(
                    isinstance(c, (pd.Timestamp, datetime.datetime)) 
                    or (isinstance(c, str) and ("202" in c or "/" in c))
                    for c in potential_dates
                )
                
                if is_planning:
                    print(f"✅ Format Detected: PLANNING (Header row {h_idx+1})")
                    df_melted = parse_planning_format(df_plan)
                    # For Planning, we DON'T replace text with 'x'. 
                    # We only ensure basic string cleaning (no-break spaces etc)
                    return clean_generic_numeric_cols(df_melted, exclude=["Date"], apply_x_rule=False)
        except Exception as e:
            print(f"Info: Planning check (header={h_idx}) failed: {e}")

    # --- STRATEGY 2: Format "Lighthouse / Booking" (Header Line 5 / Index 4) ---
    try:
        df_light = pd.read_excel(file_path, sheet_name=sheet_name, header=4, engine='openpyxl')
        # Detection Heuristic: "Jour Date" or "Date" column
        if "Jour Date" in df_light.columns or "Date" in df_light.columns:
            print("✅ Format Detected: LIGHTHOUSE")
            if "Jour Date" in df_light.columns:
                df_light.rename(columns={"Jour Date": "Date"}, inplace=True)
            # ONLY Lighthouse gets the "text to x" rule
            return clean_generic_numeric_cols(df_light, exclude=["Date", "Demande du marché"], apply_x_rule=True)
    except Exception as e:
        print(f"Info: Lighthouse format check failed: {e}")

    raise ValueError("Format non reconnu (Ni Planning ligne 1/2, ni Lighthouse ligne 5).")

def parse_planning_format(df):
    """
    Transforme le format Planning (Dates en colonnes) en format Base de Données (Dates en lignes)
    """
    # Columns before D are metadata
    fixed_cols = df.columns[0:3].tolist() 
    date_cols = df.columns[3:].tolist()
    
    # Unpivot (Melt)
    df_melted = df.melt(id_vars=fixed_cols, value_vars=date_cols, var_name='Date', value_name='Valeur')
    
    # Clean Date header
    df_melted['Date'] = pd.to_datetime(df_melted['Date'], errors='coerce')
    df_melted = df_melted.dropna(subset=['Date'])
    
    return df_melted

def clean_generic_numeric_cols(df, exclude, apply_x_rule=True):
    """
    Replaces non-numeric values in columns NOT in 'exclude' with 'x' (IF apply_x_rule is True).
    """
    df_clean = df.copy()
    for col in df_clean.columns:
        if col in exclude:
            continue
            
        def clean_val(val):
            if pd.isna(val) or val == "" or val is None: return None
            s = str(val).strip().replace('\xa0', ' ') # Clean non-breaking spaces
            
            # Simple numeric check
            try:
                # Handle French format 188,00 -> 188.00
                v_str = s.replace(',', '.').replace(' ', '')
                float(v_str) # test
                return v_str
            except ValueError:
                # It's a comment or status. 
                # If apply_x_rule is True (Lighthouse), replace with 'x'.
                # Else (Planning), keep original string.
                return "x" if apply_x_rule else s
                
        df_clean[col] = df_clean[col].apply(clean_val)
    return df_clean
