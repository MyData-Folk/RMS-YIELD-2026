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

app = Flask(__name__, template_folder='.')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER

from utils import clean_column_name, infer_sql_type, split_datetime_columns, format_all_dates
import excel_handler

# ... (Configuration Supabase reste ici)

@app.route('/upload_excel', methods=['POST'])
def upload_excel_step1():
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Aucun fichier sélectionné'}), 400
    
    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({'error': 'Format fichier invalide (attendu: .xlsx, .xls)'}), 400

    filename = str(uuid.uuid4()) + "_" + unidecode(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)
    
    try:
        sheets = excel_handler.list_sheets(filepath)
        return jsonify({
            'filename': filename,
            'sheets': sheets
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/preview_excel', methods=['POST'])
def preview_excel_sheet():
    data = request.json
    filename = data.get('filename')
    sheet_name = data.get('sheet_name')
    is_lighthouse = data.get('is_lighthouse', False)
    
    if not filename or not sheet_name:
         return jsonify({'error': 'Paramètres manquants'}), 400

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    try:
        # Custom Lighthouse Logic
        if is_lighthouse:
            df = excel_handler.read_lighthouse_excel(filepath, sheet_name)
            df = excel_handler.clean_lighthouse_data(df)
        else:
            # Standard logic
            df = excel_handler.read_excel_sheet(filepath, sheet_name)
            
        # Split datetime logic (to match what we do for CSVs)
        # Note: If Lighthouse format, 'Date' is likely already a date or string 'Jeu 15/01/2026'
        # split_datetime_columns might be redundant or safe to run. 
        # Lighthouse dates are like "Jeu 15/01/2026". 
        # split_datetime_columns looks for ' ' or 'T'.
        # Let's keep it generic.
        df = split_datetime_columns(df)
        
        cleaned_columns = [clean_column_name(c) for c in df.columns]
        return jsonify({
            'filename': filename,
            'columns': cleaned_columns
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@app.route('/process_excel', methods=['POST'])
def process_excel_step2():
    data = request.json
    filename = data.get('filename')
    sheet_name = data.get('sheet_name')
    target_table_name = data.get('table_name')
    state_mode = data.get('mode', 'create') # create or update
    selected_columns = data.get('columns', [])
    column_mapping = data.get('column_mapping', {})
    is_lighthouse = data.get('is_lighthouse', False)

    if not filename or not sheet_name or not target_table_name:
        return jsonify({'error': 'Paramètres manquants'}), 400
    
    # Nettoyage du nom de la table pour sécurité SQL si création
    if state_mode == 'create':
        target_table_name = clean_column_name(target_table_name)
    
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if not os.path.exists(filepath):
        return jsonify({'error': 'Fichier introuvable'}), 404

    try:
        # Lecture
        if is_lighthouse:
            df = excel_handler.read_lighthouse_excel(filepath, sheet_name)
            df = excel_handler.clean_lighthouse_data(df)
        else:
            df = excel_handler.read_excel_sheet(filepath, sheet_name)
        
        # 1. Split colonnes Datetime
        df = split_datetime_columns(df)
        
        # 2. Nettoyage des noms de colonnes
        df.columns = [clean_column_name(c) for c in df.columns]
        
        # 3. Filtrage & Mapping (Similaire à CSV)
        if selected_columns:
            # On ne garde que les colonnes demandées
            valid_cols = [c for c in selected_columns if c in df.columns]
            df = df[valid_cols]
            
        if state_mode == 'append' and column_mapping:
            df.rename(columns=column_mapping, inplace=True)
            # Garder uniquement les colonnes mappées (Destination DB names)
            target_cols = set(column_mapping.values())
            df = df[[c for c in df.columns if c in target_cols]]

        # 4. Formatage dates
        df_clean = format_all_dates(df)
        
        # 5. Nettoyage '0' / Empty -> None
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
    # Push 
        try:
             response_msg = push_to_supabase(df_clean, target_table_name, state_mode)
             return jsonify({'status': 'success', 'message': response_msg})
        except Exception as e_push:
             import traceback
             traceback.print_exc()
             return jsonify({'error': f"Erreur Supabase: {str(e_push)}"}), 500
        
    except Exception as e:
        print(f"Erreur process excel: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

import requests # Add import

def push_to_supabase(df, table_name, mode):
    if not supabase:
         return "Supabase non configuré (Mode local seulement)"
    
    print(f"DEBUG: push_to_supabase table={table_name} mode={mode} rows={len(df)}")

    # Generate Create Table SQL
    if mode == 'create':
        cols_def = []
        for col in df.columns:
            sql_type = infer_sql_type(df[col])
            cols_def.append(f"{col} {sql_type}")
        
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols_def)});"
        print(f"DEBUG: Executing SQL: {create_table_sql[:100]}...")
        
        try:
            supabase.rpc("exec_sql", {"query": create_table_sql}).execute()
        except Exception as e:
             print(f"❌ Error creating table: {e}")
             raise e

        import time
        time.sleep(1) # Wait for propagation

    # Insert Data
    # Convert to standard python types for JSON (handles datetimes -> ISO strings)
    # We use json.loads(df.to_json) to ensure everything is JSON-compliant (NaN -> null, Dates -> ISO)
    import json
    df_final = df.where(pd.notnull(df), None) # Ensure NaNs are None (null in JSON)
    records = json.loads(df_final.to_json(orient='records', date_format='iso'))
    
    # Batch insert
    batch_size = 500 # Can increase back to 500 or 1000 since we use POST body now
    total_inserted = 0
    
    print(f"DEBUG: Starting batch insert for {len(records)} records (Direct HTTP)")
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal" # Don't return inserted rows (saves bandwidth)
    }
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table_name}"

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            # We use direct requests to avoid SDK weirdness (URI Too Long 414)
            response = requests.post(url, json=batch, headers=headers)
            
            if response.status_code not in (200, 201):
                raise Exception(f"HTTP {response.status_code}: {response.text}")
                
            total_inserted += len(batch)
        except Exception as e:
            print(f"❌ Error inserting batch {i}: {e}")
            raise e
        
    action = "créée et remplie" if mode == 'create' else "mise à jour"
    return f"✅ Table Excel '{table_name}' {action} ({total_inserted} lignes)."


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
