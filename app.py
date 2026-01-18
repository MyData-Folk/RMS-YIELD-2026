from flask import Flask, request, render_template, send_file, jsonify
import os
import pandas as pd
import uuid
from unidecode import unidecode
from supabase import create_client, Client
from dotenv import load_dotenv
import re
import math

load_dotenv()

# Configuration Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Debug: Vérifier si les variables sont chargées
print(f"DEBUG: SUPABASE_URL = {SUPABASE_URL[:30] if SUPABASE_URL else 'None'}...")
print(f"DEBUG: SUPABASE_KEY = {'Loaded' if SUPABASE_KEY else 'None'}")

supabase: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("DEBUG: Supabase client created successfully!")
    except Exception as e:
        print(f"Erreur init Supabase: {e}")

UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'outputs'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER

# Colonnes datetime à splitter
DATETIME_COLUMNS = {
    "Date d'achat": ("date_d_achat", "heure_d_achat"),
    "Dernière modification": ("date_modification", "heure_modification"),
    "Date d'annulation": ("date_d_annulation", "heure_d_annulation")
}

def clean_column_name(name):
    name = unidecode(name.strip())
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
    elif 'date' in col_name:
        # On ne se base QUE sur le mot 'date' pour être sûr. 
        # 'date_d_annulation' contient 'date', donc ça marchera.
        # 'motif_annulation' ne contient pas 'date', donc ce sera TEXT.
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

def format_all_dates(df):
    df_formatted = df.copy()
    for col in df.columns:
        col_clean = col.lower()
        # On ne touche QUE si 'date' est dans le nom (pas 'annulation' tout court)
        if 'date' in col_clean and 'heure' not in col_clean:
            temp_series = pd.to_datetime(
                df[col],
                format='%d/%m/%Y', # parsing source (suppose format FR en entrée)
                errors='coerce',
                dayfirst=True      # Important pour l'input
            )
            if temp_series.isna().any():
                # Fallback générique
                temp_series = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            
            # Modification: Output en YYYY-MM-DD, mais None pour les dates invalides (pas "")
            df_formatted[col] = temp_series.dt.strftime('%Y-%m-%d').where(temp_series.notna(), None)
    return df_formatted

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "Aucun fichier fourni"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Nom de fichier vide"}), 400
    if not file.filename.endswith('.csv'):
        return jsonify({"error": "Seuls les fichiers CSV sont acceptés"}), 400

    filename = str(uuid.uuid4()) + '.csv'
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)

    try:
        df = pd.read_csv(filepath, sep=';', on_bad_lines='skip', encoding='utf-8')
    except Exception:
        try:
            df = pd.read_csv(filepath, sep=';', on_bad_lines='skip', encoding='latin1')
        except Exception:
            return jsonify({"error": "Impossible de lire le fichier CSV."}), 400

    # Simulation du pré-traitement pour avoir les 'bonnes' colonnes (avec split date/heure)
    # On travaille sur une copie légère pour ne pas écraser le fichier source si on veut garder l'original
    # Mais ici on veut juste extraire les noms de colonnes futurs.
    
    # 1. Split Datetime
    df_preview = split_datetime_columns(df)
    
    # 2. Clean Names (optionnel ici, mais utile pour proposer un mapping auto propre)
    # L'utilisateur a demandé que l'app prenne en compte la transformation (accents, etc)
    # Donc on renvoie les colonnes "brutes post-split" ou "nettoyées" ?
    # Le frontend affichera "Colonnes CSV".
    # Si on renvoie nettoyé : "reservation" (au lieu de Réservation).
    # C'est mieux pour le mapping auto si la DB a aussi "reservation".
    
    # On renvoie les noms nettoyés comme référence "source" pour le mapping
    cleaned_columns = [clean_column_name(c) for c in df_preview.columns]
    
    return jsonify({
        "filename": filename,
        "columns": cleaned_columns, # Ces colonnes correspondent à ce qui sera disponible pour l'import
        "raw_columns_count": len(df.columns)
    })

@app.route('/tables', methods=['GET'])
def list_tables():
    if not supabase:
        return jsonify({"error": "Supabase non connecté"}), 500
    try:
        res = supabase.rpc("get_public_tables", {}).execute()
        return jsonify(res.data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/tables/<table_name>/columns', methods=['GET'])
def list_table_columns(table_name):
    if not supabase:
        return jsonify({"error": "Supabase non connecté"}), 500
    try:
        res = supabase.rpc("get_table_columns", {"t_name": table_name}).execute()
        return jsonify(res.data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/filter', methods=['POST'])
def filter_columns():
    data = request.json
    filename = data.get('filename')
    selected_columns = data.get('columns', [])
    mode = data.get('mode', 'create') # create | append
    target_table_name = data.get('table_name', '').strip()
    column_mapping = data.get('column_mapping', {}) # {csv_col: db_col}

    if not filename or not selected_columns:
        return jsonify({"error": "Fichier ou colonnes manquants"}), 400

    if not target_table_name and mode == 'append':
         return jsonify({"error": "Nom de la table requis pour le mode 'Mettre à jour'"}), 400

    if not target_table_name:
        target_table_name = 'reservations_' + uuid.uuid4().hex[:8]
    
    # Nettoyage du nom de la table pour sécurité SQL si création
    if mode == 'create':
        target_table_name = clean_column_name(target_table_name)

    input_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    output_filename = f"filtered_{uuid.uuid4().hex}.csv"
    output_path = os.path.join(app.config['OUTPUT_FOLDER'], output_filename)
    sql_path = os.path.join(app.config['OUTPUT_FOLDER'], f"{output_filename.replace('.csv', '.sql')}")

    # Tentative de lecture robuste (détection séparateur)
    try:
        # Essai 1: Séparateur ';' (classique FR)
        df = pd.read_csv(input_path, sep=';', on_bad_lines='skip', encoding='utf-8', engine='python')
        if len(df.columns) < 2: # Si tout est dans une colonne, c'était probablement pas ';'
            raise ValueError("Sep not ;")
    except Exception:
        try:
             # Essai 2: Séparateur ',' (standard)
            df = pd.read_csv(input_path, sep=',', on_bad_lines='skip', encoding='utf-8', engine='python')
        except Exception:
             # Fallback: Encoding latin1 + séparateur ';'
            df = pd.read_csv(input_path, sep=';', on_bad_lines='skip', encoding='latin1', engine='python')

    # Transformation pré-sélection (pour matcher ce qu'on a envoyé au front lors de l'upload)
    
    # 1. Split datetime (créer date_d_achat, heure_d_achat...)
    df_filtered = split_datetime_columns(df)
    
    # 2. Nettoyer TOUS les noms pour matcher ceux du frontend
    original_cols = df_filtered.columns.tolist()
    clean_cols = [clean_column_name(c) for c in original_cols]
    df_filtered.columns = clean_cols
    
    # 3. Maintenant on peut filtrer selon la sélection du front (qui utilise les noms clean)
    # Attention: Si une colonne n'existe pas (ex: ignorée par erreur), on l'ignore ou on error ?
    # Filtrage robuste
    valid_cols = [c for c in selected_columns if c in df_filtered.columns]
    df_filtered = df_filtered[valid_cols]

    # Plus besoin de re-splitter ou re-cleaner après, c'est fait.
    
    # Étape 3 : Réorganiser après 'reference' (si pertinent avec les nouveaux noms)
    # Étape 3 : Logique spécifique par mode
    if mode == 'create':
        # Réorganiser colonnes (Business Logic pour placer les dates après la référence)
        cols = df_filtered.columns.tolist()
        new_order = []
        inserted = False
        
        # Liste des colonnes dates techniques qu'on veut regrouper
        technical_date_cols = [
            'date_d_achat', 'heure_d_achat',
            'date_modification', 'heure_modification',
            'date_d_annulation', 'heure_d_annulation'
        ]

        for col in cols:
            if col == 'reference' and not inserted:
                new_order.append(col)
                for dt_col in technical_date_cols:
                    if dt_col in cols:
                        new_order.append(dt_col)
                inserted = True
            elif col not in technical_date_cols:
                new_order.append(col)
        
        if not inserted:
            # Fallback si 'reference' pas trouvé
            new_order = [c for c in cols if c not in technical_date_cols] + [c for c in technical_date_cols if c in cols]
            
        df_filtered = df_filtered[new_order]

    elif mode == 'append':
        # Appliquer mapping
        # l'utilisateur envoie { "col_csv_clean": "col_db" }
        if column_mapping:
            df_filtered.rename(columns=column_mapping, inplace=True)
            # On ne garde que les colonnes mappées (celles qui sont maintenant des noms DB valides)
            target_cols = set(column_mapping.values())
            final_cols = [c for c in df_filtered.columns if c in target_cols]
            df_filtered = df_filtered[final_cols]


    # Étape 4 : Nettoyer données textuelles (toujours)
    # Exclure colonnes date/heure pour éviter d'introduire "0" dans des champs DATE/TIME
    for col in df_filtered.select_dtypes(include='object').columns:
        if 'date' in col or 'heure' in col:
            continue
        df_filtered[col] = df_filtered[col].astype(str).apply(lambda x: unidecode(x) if pd.notna(x) and x != 'nan' else '')

    # Nettoyage explicite des colonnes date/heure
    for col in df_filtered.columns:
        if 'date' in col or 'heure' in col:
            df_filtered[col] = df_filtered[col].replace({'0': None, 0: None, '': None})

    # Étape 5 : Formater toutes les dates en jj/mm/aaaa (toujours)
    df_filtered = format_all_dates(df_filtered)

    # Sauvegarder CSV
    df_filtered.to_csv(output_path, index=False, sep=';', encoding='utf-8', na_rep='')

    import_status = "⚠️ Supabase non configuré."
    storage_url = ""
    save_storage = data.get('save_storage', False)

    if supabase:
        # Upload Storage (Si demandé)
        if save_storage:
            try:
                # Création/Vérif bucket 'exports' (échouera si existe déjà, pas grave)
                # supabase.storage.create_bucket("exports", public=True) 
                
                with open(output_path, 'rb') as f:
                    storage_path = f"exports/{output_filename}"
                    supabase.storage.from_("exports").upload(storage_path, f)
                    storage_url = supabase.storage.from_("exports").get_public_url(storage_path)
            except Exception as e:
                print(f"Info/Erreur Storage: {e}") 

    create_table_sql = "-- Mode Mise à jour (APPEND) : Pas de CREATE TABLE"
    
    if mode == 'create':
        # Générer SQL CREATE TABLE
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {target_table_name} (\n"
        columns_defs = []
        for col in df_filtered.columns:
            sql_type = infer_sql_type(df_filtered[col])
            columns_defs.append(f"    {col} {sql_type}")
        create_table_sql += ",\n".join(columns_defs) + "\n);"

        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write(create_table_sql)

    if supabase:
        try:
            if mode == 'create':
                # 1. Création de la table via RPC
                supabase.rpc("exec_sql", {"query": create_table_sql}).execute()
                import time
                time.sleep(2)
            
            # 2. Préparation des données
            import json
            # Remplacer None/NaN par np.nan pour que to_json les convertisse en null JSON
            # Remplacer None/NaN par np.nan pour que to_json les convertisse en null JSON
            # Nettoyage ULTIME : Remplacer tout "0" ou 0 par None dans tout le dataframe
            df_clean = df_filtered.replace({'0': None, 0: None, '': None, pd.NA: None, float('nan'): None})
            # S'assurer que les NaNs sont None
            df_clean = df_clean.where(pd.notnull(df_clean), None)
            
            records = json.loads(df_clean.to_json(orient='records', date_format='iso'))
            
            # 3. Insertion par lots (batch)
            batch_size = 100
            total_inserted = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                # supabase.table(target_table_name).insert(batch).execute()
                
                supabase.table(target_table_name).insert(batch).execute()
                total_inserted += len(batch)
                
            action = "créée et remplie" if mode == 'create' else "mise à jour"
            import_status = f"✅ Table '{target_table_name}' {action} ({total_inserted} lignes ajoutées)."
            
        except Exception as e:
            import_status = f"⚠️ Erreur Supabase API : {str(e)}"
    
    return jsonify({
        "download_url": f"/download/{output_filename}",
        "sql_url": f"/download/{os.path.basename(sql_path)}" if mode == 'create' else "",
        "table_name": target_table_name,
        "import_status": import_status,
        "create_table_sql": create_table_sql,
        "storage_url": storage_url
    })

@app.route('/download/<filename>')
def download_file(filename):
    path = os.path.join(app.config['OUTPUT_FOLDER'], filename)
    if not os.path.exists(path):
        return "Fichier non trouvé", 404
    return send_file(path, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
