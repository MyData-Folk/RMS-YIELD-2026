import os
from supabase import create_client, Client
from dotenv import load_dotenv
import time

load_dotenv()

url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

print("Provisionnement des fonctions d'introspection via exec_sql...")

sql_get_tables = """
CREATE OR REPLACE FUNCTION get_public_tables()
RETURNS TABLE(table_name text)
LANGUAGE sql
SECURITY DEFINER
AS $$
  SELECT tablename::text 
  FROM pg_tables 
  WHERE schemaname = 'public';
$$;
"""

sql_get_columns = """
CREATE OR REPLACE FUNCTION get_table_columns(t_name text)
RETURNS TABLE(column_name text, data_type text)
LANGUAGE sql
SECURITY DEFINER
AS $$
  SELECT column_name::text, data_type::text 
  FROM information_schema.columns 
  WHERE table_schema = 'public' AND table_name = t_name;
$$;
"""

try:
    print("Création de get_public_tables...")
    supabase.rpc("exec_sql", {"query": sql_get_tables}).execute()
    print("✅ OK")
    
    print("Création de get_table_columns...")
    supabase.rpc("exec_sql", {"query": sql_get_columns}).execute()
    print("✅ OK")
    
    # Pause pour propagation schema cache
    time.sleep(2)
    
    # Test
    print("Test get_public_tables...")
    res = supabase.rpc("get_public_tables", {}).execute()
    print("Tables:", res.data)
    
except Exception as e:
    print(f"❌ Erreur : {e}")
