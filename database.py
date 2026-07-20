import sqlite3
import json
from datetime import datetime

DB_NAME = "matrix_cache.db"

def init_db():
    """Cria a tabela de cache no SQLite caso não exista."""
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_cache (
                data_ref TEXT,
                endpoint TEXT,
                conta TEXT,
                parametros TEXT,
                page INTEGER,
                resposta_json TEXT,
                PRIMARY KEY (data_ref, endpoint, conta, parametros, page)
            )
        ''')
        conn.commit()

# Inicializa o banco ao importar
init_db()

def get_cache(data_ref, endpoint, conta, parametros, page=1):
    """Busca o payload JSON no cache SQLite."""
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT resposta_json FROM api_cache 
            WHERE data_ref = ? AND endpoint = ? AND conta = ? AND parametros = ? AND page = ?
        ''', (data_ref, endpoint, str(conta), parametros, page))
        row = cursor.fetchone()
        if row:
            return json.loads(row[0])
    return None

def set_cache(data_ref, endpoint, conta, parametros, page, resposta_json):
    """Salva o payload JSON no cache SQLite."""
    with sqlite3.connect(DB_NAME, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO api_cache (data_ref, endpoint, conta, parametros, page, resposta_json)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (data_ref, endpoint, str(conta), parametros, page, json.dumps(resposta_json)))
        conn.commit()

def is_cacheable(dia_str):
    """Verifica se a data fornecida é anterior a hoje. Só cacheamos dias passados."""
    hoje = datetime.now().date()
    try:
        dt = datetime.strptime(dia_str, "%Y-%m-%d").date()
        return dt < hoje
    except:
        return False