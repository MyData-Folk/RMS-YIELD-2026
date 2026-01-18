import pandas as pd
from unidecode import unidecode
import re

# Colonnes datetime à splitter
DATETIME_COLUMNS = {
    "Date d'achat": ("date_d_achat", "heure_d_achat"),
    "Dernière modification": ("date_modification", "heure_modification"),
    "Date d'annulation": ("date_d_annulation", "heure_d_annulation")
}

def clean_column_name(name):
    name = unidecode(str(name).strip()) # Ensure string
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    name = re.sub(r'_+', '_', name)
    if name and name[0].isdigit():
        name = 'col_' + name
    return name.lower()

def infer_sql_type(series):
    col_name = series.name.lower() if series.name else ""
    dtype = str(series.dtype)

    if 'heure' in col_name:
        return 'TEXT' # TIME est trop strict pour des CSV sales
    elif any(p in col_name for p in ['date', 'debut', 'fin']):
        return 'DATE'
    elif 'int' in dtype:
        return 'BIGINT' # Au cas où
    elif 'float' in dtype:
        return 'TEXT' # Eviter NUMERIC sur des floats qui peuvent être NaN ou scientific notation
    elif 'email' in col_name:
        return 'VARCHAR(255)'
    else:
        return 'TEXT' # Sécurité maximale

def parse_datetime_safe(val):
    if not isinstance(val, str):
        return None  # ← Important : pas pd.NaT
    val = val.strip()
    if not val:
        return None
    val = ' '.join(val.split())
    try:
        dt = pd.to_datetime(val, format='%d/%m/%Y %H:%M:%S', errors='coerce', dayfirst=True)
        if pd.notna(dt):
            return dt
        dt = pd.to_datetime(val, format='%d/%m/%Y %H:%M', errors='coerce', dayfirst=True)
        if pd.notna(dt):
            return dt
        dt = pd.to_datetime(val, errors='coerce', dayfirst=True)
        return dt
    except Exception:
        return None

def split_datetime_columns(df):
    new_df = df.copy()
    datetime_cols = {}
    for col_fr, (date_col, time_col) in DATETIME_COLUMNS.items():
        if col_fr in df.columns:
            temp_series = df[col_fr].apply(parse_datetime_safe)
            temp_series = pd.to_datetime(temp_series, errors='coerce')
            # Modification: Format ISO YYYY-MM-DD pour compatibilité SQL, None pour dates vides
            date_vals = temp_series.dt.strftime('%Y-%m-%d').where(temp_series.notna(), None)
            time_vals = temp_series.dt.strftime('%H:%M:%S').where(temp_series.notna(), None)
            datetime_cols[date_col] = date_vals
            datetime_cols[time_col] = time_vals
            new_df.drop(columns=[col_fr], inplace=True)
    for col, data in datetime_cols.items():
        new_df[col] = data
    return new_df

def format_all_dates(df, force_dates=None):
    if force_dates is None: force_dates = []
    df_formatted = df.copy()
    for col in df.columns:
        col_clean = col.lower()
        # On touche si 'date', 'debut', 'fin' est dans le nom OU si forcé par UI
        is_date_col = any(p in col_clean for p in ['date', 'debut', 'fin']) or (col in force_dates)
        
        if is_date_col and 'heure' not in col_clean:
            # Si déjà datetime (cas Excel), on formate direct
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                 df_formatted[col] = df[col].dt.strftime('%Y-%m-%d').where(df[col].notna(), None)
            else:
                 # Logic for string parsing
                 temp_series = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                 df_formatted[col] = temp_series.dt.strftime('%Y-%m-%d').where(temp_series.notna(), None)
    return df_formatted
