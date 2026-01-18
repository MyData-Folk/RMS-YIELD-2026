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
