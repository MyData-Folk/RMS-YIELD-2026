import pandas as pd
import openpyxl
from utils import clean_column_name

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
    Reads a specific sheet from an Excel file into a DataFrame.
    """
    try:
        # Lire sans header d'abord pour detecter où commence les données ??
        # Non, on assume que la 1ere ligne est le header pour l'instant.
        # Si besoin on pourrait ajouter une option "skiprows" dans l'UI.
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
        
        # On retourne le DF brut (le nettoyage se fera par l'appelant pour compatibilité avec le logic de split)
        return df
    except Exception as e:
        raise ValueError(f"Erreur lors de la lecture de l'onglet '{sheet_name}': {e}")

def read_lighthouse_excel(file_path, sheet_name):
    """
    Reads a Lighthouse style Excel file (Header at line 5).
    """
    try:
        # Header=4 signifie la 5ème ligne (0-indexed)
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=4, engine='openpyxl')
        return df
    except Exception as e:
        raise ValueError(f"Erreur lecture format Lighthouse: {e}")

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
    # Tout ce qui n'est pas 'Date' ou 'Demande du marché' est considéré comme un Hotel
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
