import psycopg2
from config import DB_CONFIG
import sys

print(f"Tentative de connexion à {DB_CONFIG.get('host')}...")
print(f"User: {DB_CONFIG.get('user')}")
pwd = DB_CONFIG.get('password')
print(f"Password starts with: {pwd[:3]}... and has length {len(pwd)}")

try:
    conn = psycopg2.connect(**DB_CONFIG)
    print("✅ Connexion réussie !")
    conn.close()
except Exception as e:
    print(f"❌ Échec de la connexion : {e}")
    sys.exit(1)
