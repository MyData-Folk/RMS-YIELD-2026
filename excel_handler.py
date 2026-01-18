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
    
    # --- STRATEGY 1: Format "Planning" (Header Line 2 / Index 1) ---
    # Structure: Cols A,B,C data. Col D+ Dates.
    try:
        df_plan = pd.read_excel(file_path, sheet_name=sheet_name, header=1, engine='openpyxl')
        
        # Detection Heuristic: Check if 4th column (index 3) looks like a date/timestamp
        if len(df_plan.columns) > 3:
            col_d = df_plan.columns[3]
            # Verify if column name is timestamp or contains 202x
            is_date = (
                isinstance(col_d, (pd.Timestamp, datetime.datetime)) 
                or (isinstance(col_d, str) and "202" in col_d)
            )
            
            if is_date:
                print("✅ Format Detected: PLANNING (Melt required)")
                return parse_planning_format(df_plan)
                
    except Exception as e:
        print(f"Info: Planning format check failed: {e}")

    # --- STRATEGY 2: Format "Lighthouse / Booking" (Header Line 5 / Index 4) ---
    try:
        df_light = pd.read_excel(file_path, sheet_name=sheet_name, header=4, engine='openpyxl')
        # Detection Heuristic: "Jour Date" or "Date" column
        if "Jour Date" in df_light.columns or "Date" in df_light.columns:
            print("✅ Format Detected: LIGHTHOUSE")
            return clean_lighthouse_data(df_light)
    except Exception as e:
        print(f"Info: Lighthouse format check failed: {e}")

    raise ValueError("Format non reconnu (Ni Planning ligne 2, ni Lighthouse ligne 5).")

def parse_planning_format(df):
    """
    Transforme le format Planning (Dates en colonnes) en format Base de Données (Dates en lignes)
    """
    # On suppose que les 3 premières colonnes sont des infos fixes (Chambre, Type, Statut...)
    fixed_cols = df.columns[0:3].tolist() 
    date_cols = df.columns[3:].tolist()
    
    # Unpivot (Melt)
    df_melted = df.melt(id_vars=fixed_cols, value_vars=date_cols, var_name='Date', value_name='Valeur')
    
    # Nettoyage Date (Si c'est un header string, convertir)
    df_melted['Date'] = pd.to_datetime(df_melted['Date'], errors='coerce')
    
    return df_melted

def clean_lighthouse_data(df):
    """
    Applies specific cleaning for Lighthouse files:
    - Renames 'Jour Date' -> 'Date'
    - Replaces non-numeric values in Hotel columns with 'x'
    """
    df_clean = df.copy()
    
    # 1. Rename Date
    if "Jour Date" in df_clean.columns:
        df_clean.rename(columns={"Jour Date": "Date"}, inplace=True)
        
    # 2. Clean Hotel Columns
    for col in df_clean.columns:
        if col in ["Date", "Demande du marché"]:
            continue
            
        def clean_val(val):
            if pd.isna(val): return None
            s = str(val).strip()
            # Test si numérique
            try:
                float(s.replace(',', '.'))
                return s
            except ValueError:
                # Si texte (ex: "Pas de flex", "Épuisé") -> 'x'
                return "x"
                
        df_clean[col] = df_clean[col].apply(clean_val)
        
    return df_clean
