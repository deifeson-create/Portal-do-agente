import streamlit as st
import pandas as pd
import requests
import os
import concurrent.futures
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# ==============================================================================
# 1. CONFIGURAÇÃO VISUAL E VARIÁVEIS GLOBAIS
# ==============================================================================
st.set_page_config(
    layout="wide",
    page_title="Portal do Agente NRC",
    page_icon="🎧",
    initial_sidebar_state="expanded"
)

# Estilos CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .stApp { background-color: #0f1116; }
    section[data-testid="stSidebar"] { background-color: #161b22; border-right: 1px solid #30363d; }
    
    /* Top Bar */
    .top-bar {
        background-color: #1f2937; padding: 1rem 1.5rem; border-radius: 12px;
        border: 1px solid #374151; display: flex; justify-content: space-between;
        align-items: center; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    
    /* Headers dos Serviços */
    .service-header {
        color: #e5e7eb; font-size: 1.2rem; font-weight: 700; margin-top: 25px; margin-bottom: 15px;
        border-left: 5px solid #6366f1; padding-left: 15px; background-color: #1f2937;
        padding-top: 5px; padding-bottom: 5px; border-radius: 0 8px 8px 0;
    }
    
    /* Pódio (Rankings) */
    .podium-card {
        background: linear-gradient(145deg, #1f2937, #111827); border: 1px solid #374151; 
        border-radius: 10px; padding: 15px; text-align: center; margin-bottom: 10px;
    }
    .podium-pos { font-size: 2rem; margin-bottom: 5px; }
    .podium-name { font-weight: 700; color: #f3f4f6; font-size: 1.1rem; }
    .podium-val { color: #9ca3af; font-size: 0.9rem; margin-top: 5px; }

    /* Card MVP */
    .mvp-card {
        background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%);
        border-radius: 15px; padding: 20px; text-align: center; color: white;
        box-shadow: 0 10px 15px -3px rgba(99, 102, 241, 0.4); margin-bottom: 25px;
        border: 1px solid #818cf8;
    }
    
    /* Card Real Time */
    .realtime-card {
        background-color: #1f2937; padding: 15px; border-radius: 10px; 
        margin-bottom: 10px; border: 1px solid #374151; 
        display: flex; align-items: center; justify-content: space-between;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; border-bottom: 1px solid #374151; margin-bottom: 20px; }
    .stTabs [data-baseweb="tab"] { height: 60px; white-space: pre-wrap; background-color: transparent; border: none; color: #9ca3af; font-size: 1.1rem; font-weight: 600; padding: 0 20px; }
    .stTabs [data-baseweb="tab"]:hover { color: #e5e7eb; background-color: #1f2937; border-radius: 8px 8px 0 0; }
    .stTabs [data-baseweb="tab"][aria-selected="true"] { color: #6366f1 !important; border-bottom: 3px solid #6366f1; }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# CREDENCIAIS VIA SECRETS (SEGURANÇA ATIVADA)
# ------------------------------------------------------------------------------
try:
    BASE_URL = st.secrets["api"]["BASE_URL"]
    ADMIN_USER = st.secrets["api"]["ADMIN_USER"]
    ADMIN_PASS = st.secrets["api"]["ADMIN_PASS"]
    ID_CONTA = st.secrets["api"]["ID_CONTA"]
    
    SUPERVISOR_LOGIN = st.secrets["auth"]["SUPERVISOR_LOGIN"]
    SUPERVISOR_PASS = st.secrets["auth"]["SUPERVISOR_PASS"]
    
    PESQUISAS_IDS = st.secrets["ids"]["PESQUISAS_IDS"]
    IDS_PERGUNTAS_VALIDAS = st.secrets["ids"]["IDS_PERGUNTAS_VALIDAS"]
except Exception as e:
    st.error(f"Erro crítico: Não foi possível carregar os Segredos (Secrets). Verifique a configuração no Streamlit Cloud. Detalhe: {e}")
    st.stop()

# Filtros Técnicos Fixos
CANAIS_ALVO = ['appchat', 'chat', 'botmessenger', 'instagram', 'whatsapp']

# SERVIÇOS MONITORADOS
SERVICOS_ALVO = ['COMERCIAL', 'FINANCEIRO', 'NOVOS CLIENTES', 'LIBERAÇÃO']

# LISTA NRC (OFICIAL)
LISTA_NRC = [
    'RILDYVAN', 'MILENA', 'ALVES', 'MONICKE', 'AYLA', 'MARIANY', 'EDUARDA', 
    'MENEZES', 'JUCIENNY', 'MARIA', 'ANDREZA', 'LUZILENE', 'IGO', 'AIDA', 
    'Caribé', 'Michelly', 'ADRIA', 'ERICA', 'HENRIQUE', 'SHYRLEI', 
    'ANNA', 'JULIA', 'FERNANDES'
]
NOMES_COMUNS_PRIMEIRO = ['MARIA', 'ANNA', 'JULIA', 'ERICA']

# REGRAS DE PAUSA
LIMITES_PAUSA = { "CURTA": 15.0, "LONGA": 120.0 }
TOLERANCIA_MENSAL_EXCESSO = 20.0 
TOLERANCIA_VISUAL_ALMOCO = 2.0

# ==============================================================================
# 2. FUNÇÕES DE AUTENTICAÇÃO E UTILITÁRIOS
# ==============================================================================

@st.cache_data(ttl=600)
def get_admin_token():
    try:
        r = requests.post(f"{BASE_URL}/authuser", json={"login": ADMIN_USER, "chave": ADMIN_PASS}, timeout=5)
        if r.status_code == 200 and r.json().get("success"): return r.json()["result"]["token"]
    except: pass
    return None

def validar_agente_api(token, email_input):
    url = f"{BASE_URL}/agentes"
    headers = {"Authorization": f"Bearer {token}"}
    input_limpo = email_input.strip().lower()
    try:
        r = requests.get(url, headers=headers, params={"login": input_limpo}, timeout=5)
        if r.status_code == 200:
            result = r.json().get("result", [])
            if result:
                agente = result[0]
                return { "id": str(agente.get("cod_agente")), "nome": agente.get("nome_exibicao") or agente.get("agente"), "email": agente.get("email", "").lower() }
    except: pass
    return None

def time_str_to_seconds(t_str):
    if not t_str or not isinstance(t_str, str): return 0
    try:
        parts = list(map(int, t_str.split(':')))
        if len(parts) == 3: return parts[0]*3600 + parts[1]*60 + parts[2]
    except: pass
    return 0

def seconds_to_hms(seconds):
    if not seconds or seconds < 0: return "00:00:00"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def formatar_tempo_humano(minutos_float):
    """Converte minutos (float) em string legível (ex: 2h 05m)."""
    if not minutos_float: return "0m"
    minutos_int = int(minutos_float)
    horas, mins = divmod(minutos_int, 60)
    if horas > 0: return f"{horas}h {mins:02d}m"
    else: return f"{mins}m"

@st.cache_data(ttl=3600)
def buscar_ids_canais(token):
    url = f"{BASE_URL}/canais"
    headers = {"Authorization": f"Bearer {token}"}
    ids = []
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            for c in r.json():
                if any(alvo in str(c.get("canal", "")).lower() for alvo in CANAIS_ALVO): ids.append(str(c.get("id_canal")))
    except: pass
    return ids

# ==============================================================================
# 3. FUNÇÕES ESPECÍFICAS DO AGENTE
# ==============================================================================

@st.cache_data(ttl=60)
def buscar_historico_login(token, id_agente, data_ini, data_fim):
    url = f"{BASE_URL}/relAgenteLogin"
    headers = {"Authorization": f"Bearer {token}"}
    logins_por_dia = {}
    page = 1
    while page <= 3: 
        params = {"data_inicial": data_ini.strftime("%Y-%m-%d"), "data_final": data_fim.strftime("%Y-%m-%d"), "agente": id_agente, "page": page, "limit": 100}
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            if r.status_code != 200: break
            rows = r.json().get("rows", [])
            if not rows: break
            for row in rows:
                data_str = row.get("data_login")
                if data_str:
                    dt_obj = datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S")
                    dia_str = dt_obj.strftime("%Y-%m-%d")
                    if dia_str not in logins_por_dia: logins_por_dia[dia_str] = dt_obj
                    else:
                        if dt_obj < logins_por_dia[dia_str]: logins_por_dia[dia_str] = dt_obj
            if len(rows) < 100: break
            page += 1
        except: break
    if not logins_por_dia: return None, "Sem Login", pd.DataFrame()
    lista_dados = []
    for dia in sorted(logins_por_dia.keys(), reverse=True):
        dt = logins_por_dia[dia]
        lista_dados.append({"Data": dt.strftime("%d/%m/%Y"), "Primeira Entrada": dt.strftime("%H:%M:%S"), "Dia da Semana": dt.strftime("%A")})
    dt_recente = logins_por_dia[sorted(logins_por_dia.keys(), reverse=True)[0]]
    return dt_recente, dt_recente.strftime("%H:%M"), pd.DataFrame(lista_dados)

@st.cache_data(ttl=300)
def buscar_estatisticas_agente(token, id_agente, data_ini, data_fim):
    url = f"{BASE_URL}/relAtEstatistico"
    headers = {"Authorization": f"Bearer {token}"}
    ids_canais = buscar_ids_canais(token)
    params = {"data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00", "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59", "agrupador": "agente", "agente[]": [id_agente], "canal[]": ids_canais, "id_conta": ID_CONTA}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=15)
        if r.status_code == 200:
            dados = r.json()
            if dados and isinstance(dados, list): return dados[0] 
    except: pass
    return None

@st.cache_data(ttl=600)
def buscar_csat_nrc(token, id_agente, data_ini, data_fim):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/RelPesqAnalitico"
    todas_respostas = []
    filtro_agentes = [id_agente] if id_agente else []
    for p_id in PESQUISAS_IDS:
        page = 1
        while True:
            params = {"data_inicial": data_ini.strftime("%Y-%m-%d"), "data_final": data_fim.strftime("%Y-%m-%d"), "pesquisa": p_id, "id_conta": ID_CONTA, "limit": 1000, "page": page}
            if filtro_agentes: params["agente[]"] = filtro_agentes
            try:
                r = requests.get(url, headers=headers, params=params, timeout=30)
                if r.status_code != 200: break
                data = r.json()
                if not data or not isinstance(data, list): break
                total_respostas_api = 0
                encontrou_valida = False
                for bloco in data:
                    id_perg = str(bloco.get("id_pergunta", ""))
                    if id_perg not in IDS_PERGUNTAS_VALIDAS: continue
                    encontrou_valida = True
                    sintetico = bloco.get("sintetico", [])
                    if sintetico:
                        for item in sintetico: total_respostas_api += int(item.get("num_quantidade", 0))
                    respostas = bloco.get("respostas", [])
                    for resp in respostas:
                        todas_respostas.append({"Nota": resp.get("nom_valor"), "Comentario": resp.get("nom_resposta"), "Data": resp.get("dat_resposta"), "Cliente": resp.get("nom_contato"), "Protocolo": resp.get("num_protocolo")})
                if encontrou_valida:
                    if (page * 1000) >= total_respostas_api: break
                if len(data) < 2 and not encontrou_valida: break
                page += 1
            except: break
    df = pd.DataFrame(todas_respostas)
    score_final = 0.0
    total_pesquisas = 0
    if not df.empty:
        df['Nota_Num'] = pd.to_numeric(df['Nota'], errors='coerce').fillna(0).astype(int)
        total_pesquisas = len(df)
        if total_pesquisas > 0:
            positivas = len(df[df['Nota_Num'] >= 8])
            score_final = (positivas / total_pesquisas) * 100
    return score_final, total_pesquisas, df

@st.cache_data(ttl=300)
def buscar_pausas_detalhado(token, id_agente, data_ini, data_fim):
    url = f"{BASE_URL}/relAgentePausa"
    headers = {"Authorization": f"Bearer {token}"}
    todas_pausas = []
    page = 1
    while True:
        params = {"dat_inicial": data_ini.strftime("%Y-%m-%d"), "dat_final": data_fim.strftime("%Y-%m-%d"), "cod_agente": id_agente, "limit": 100, "pagina": page}
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            if r.status_code != 200: break
            data = r.json()
            rows = data.get("rows", [])
            if not rows: break
            todas_pausas.extend(rows)
            if len(rows) < 100: break
            page += 1
        except: break
    return pd.DataFrame(todas_pausas)

# ==============================================================================
# FUNÇÕES DE BANCO DE DADOS (GOOGLE SHEETS) - SUBSTITUI O EXCEL
# ==============================================================================

def conectar_gsheets():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        # Pega as credenciais do Secrets
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        # Abre a planilha pelo nome exato
        sheet = client.open("solicitacoes_nrc").sheet1
        return sheet
    except Exception as e:
        return None

def salvar_solicitacao_gsheets(nome, id_agente, motivo, mensagem):
    sheet = conectar_gsheets()
    if sheet:
        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        try:
            # Adiciona linha: Data, ID, Nome, Motivo, Mensagem
            sheet.append_row([data_hora, id_agente, nome, motivo, mensagem])
            return True, "Solicitação salva na nuvem com sucesso!"
        except Exception as e:
            return False, f"Erro ao escrever na planilha: {e}"
    else:
        return False, "Erro de conexão com Google Sheets. Verifique o compartilhamento."

def ler_solicitacoes_gsheets():
    sheet = conectar_gsheets()
    if sheet:
        try:
            data = sheet.get_all_records()
            return pd.DataFrame(data)
        except: return pd.DataFrame()
    return pd.DataFrame()

# ==============================================================================
# 4. FUNÇÕES DO SUPERVISOR (VERSÃO GOLD 9.0)
# ==============================================================================

def buscar_agentes_online_filtrado_nrc(token):
    """Busca agentes online e filtra apenas os do NRC."""
    headers = {"Authorization": f"Bearer {token}"}
    agentes_online_nrc = []
    
    nrc_upper = [x.strip().upper() for x in LISTA_NRC]
    
    try:
        r = requests.get(f"{BASE_URL}/agentesOnline", headers=headers)
        if r.status_code == 200:
            todos_online = r.json()
            for ag in todos_online:
                nome_full = str(ag.get("nom_agente", "")).strip().upper()
                partes = nome_full.split()
                if not partes: continue
                match = False
                for alvo in nrc_upper:
                    if alvo in NOMES_COMUNS_PRIMEIRO:
                        if alvo == partes[0]: match = True; break
                    else:
                        if alvo in partes: match = True; break
                if match:
                    agentes_online_nrc.append(ag)
    except: pass
    return agentes_online_nrc

def forcar_logout(token, id_agente):
    """Executa o logout forçado via API."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/deslogarAgente"
    payload = {"id_agente": int(id_agente)}
    try:
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code == 200:
            return True, "Sucesso"
        else:
            return False, f"Erro API: {r.text}"
    except Exception as e:
        return False, str(e)

@st.cache_data(ttl=300)
def buscar_dados_completos_supervisor(token, data_ini, data_fim):
    headers = {"Authorization": f"Bearer {token}"}
    ids_agentes = []
    nrc_upper = [x.strip().upper() for x in LISTA_NRC]
    mapa_agentes = {} 
    page = 1
    while True:
        try:
            r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": page, "bol_cancelado": 0})
            if r.status_code != 200: break
            data = r.json()
            rows = data.get("result", [])
            if not rows: break
            for ag in rows:
                nome_raw = str(ag.get("nome_exibicao") or ag.get("agente")).strip()
                nome_upper = nome_raw.upper() 
                partes = nome_upper.split()
                if not partes: continue
                match = False
                for alvo in nrc_upper:
                    if alvo in NOMES_COMUNS_PRIMEIRO:
                        if alvo == partes[0]: match = True; break
                    else:
                        if alvo in partes: match = True; break
                if match: 
                    cod = str(ag.get("cod_agente"))
                    ids_agentes.append(cod)
                    mapa_agentes[cod] = nome_upper
            if page * 100 >= data.get("total", 0): break
            page += 1
        except: break
    ids_canais = buscar_ids_canais(token)
    resultados = {s: {"num_qtd": 0, "tma": "--:--", "tme": "--:--", "csat_pos": 0, "csat_total": 0} for s in SERVICOS_ALVO}
    if not ids_agentes: return resultados, 0.0, 0, mapa_agentes

    for servico in SERVICOS_ALVO:
        params = {"data_inicial": f"{data_ini} 00:00:00", "data_final": f"{data_fim} 23:59:59", "agrupador": "servico", "agente[]": ids_agentes, "canal[]": ids_canais, "id_conta": ID_CONTA, "servico": servico}
        try:
            r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params)
            if r.status_code == 200:
                lista = r.json()
                if lista and isinstance(lista, list):
                    item = lista[0]
                    qtd = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                    resultados[servico]["num_qtd"] = qtd
                    resultados[servico]["tma"] = item.get("tma", "--:--")
                    resultados[servico]["tme"] = item.get("tme", "--:--")
        except: pass

    csat_geral_pos = 0; csat_geral_total = 0
    for p_id in PESQUISAS_IDS:
        p_page = 1
        while True:
            p_params = {"data_inicial": data_ini, "data_final": data_fim, "pesquisa": p_id, "id_conta": ID_CONTA, "limit": 1000, "page": p_page, "agente[]": ids_agentes}
            try:
                r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=p_params)
                if r.status_code != 200: break
                data = r.json()
                if not data or not isinstance(data, list): break
                total_api = 0
                for bloco in data:
                    if str(bloco.get("id_pergunta", "")) in IDS_PERGUNTAS_VALIDAS:
                        if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                        for resp in bloco.get("respostas", []):
                            try:
                                servico_resp = str(resp.get("nom_servico", "")).upper().strip()
                                val_raw = resp.get("nom_valor"); nota = 0
                                if val_raw and val_raw != "": nota = int(float(val_raw))
                                if nota >= 0: 
                                    csat_geral_total += 1
                                    if nota >= 8: csat_geral_pos += 1
                                    if servico_resp in SERVICOS_ALVO:
                                        resultados[servico_resp]["csat_total"] += 1
                                        if nota >= 8: resultados[servico_resp]["csat_pos"] += 1
                            except: pass
                if (p_page * 1000) >= total_api: break
                if len(data) < 2: break
                p_page += 1
            except: break
    score_geral = (csat_geral_pos / csat_geral_total * 100) if csat_geral_total > 0 else 0.0
    return resultados, score_geral, csat_geral_total, mapa_agentes

def _processar_agente_pausas(token, cod_agente, nome_agente, data_ini, data_fim):
    headers = {"Authorization": f"Bearer {token}"}
    local_curtas, local_almoco, local_logins, local_ranking = [], [], [], []
    pausas_agente = []
    page = 1
    while True:
        if page > 5: break
        params = {"dat_inicial": data_ini, "dat_final": data_fim, "cod_agente": cod_agente, "limit": 100, "pagina": page}
        try:
            r = requests.get(f"{BASE_URL}/relAgentePausa", headers=headers, params=params, timeout=10)
            if r.status_code != 200: break
            rows = r.json().get("rows", [])
            if not rows: break
            pausas_agente.extend(rows)
            if len(rows) < 100: break
            page += 1
        except: break
    acumulado_excesso_curta = 0.0
    for p in pausas_agente:
        motivo = str(p.get("pausa", "")).upper()
        try: seg = float(p.get("seg_pausado", 0)); minutos = seg/60
        except: minutos = 0
        if any(x in motivo for x in ["MANHA", "MANHÃ", "TARDE", "NOITE"]):
            if minutos > LIMITES_PAUSA["CURTA"]: acumulado_excesso_curta += (minutos - LIMITES_PAUSA["CURTA"])
        if any(x in motivo for x in ["ALMOÇO", "ALMOCO", "PLANTÃO", "PLANTAO"]):
            if minutos > (LIMITES_PAUSA["LONGA"] + TOLERANCIA_VISUAL_ALMOCO):
                excesso = minutos - LIMITES_PAUSA["LONGA"]
                local_almoco.append({"Agente": nome_agente, "Data": p.get("data_pausa", "")[:10], "Duração": formatar_tempo_humano(minutos), "Status": f"Estourou {formatar_tempo_humano(excesso)}"})
    status_curta = "Normal"
    if acumulado_excesso_curta > TOLERANCIA_MENSAL_EXCESSO: status_curta = "ADVERTÊNCIA"
    if acumulado_excesso_curta > 0:
        local_curtas.append({"Agente": nome_agente, "Excesso Acumulado": formatar_tempo_humano(acumulado_excesso_curta), "Valor Num": acumulado_excesso_curta, "Status": status_curta})
    qtd_pausas = len([p for p in pausas_agente if "TERMINO" not in str(p.get("pausa")).upper() and "EXPEDIENTE" not in str(p.get("pausa")).upper()])
    if qtd_pausas > 0: local_ranking.append({"Agente": nome_agente, "Qtd Pausas": qtd_pausas})

    page_log = 1; logins_raw = []
    while page_log <= 2:
        params_log = {"data_inicial": data_ini, "data_final": data_fim, "agente": cod_agente, "page": page_log, "limit": 100}
        try:
            r = requests.get(f"{BASE_URL}/relAgenteLogin", headers=headers, params=params_log, timeout=10)
            if r.status_code != 200: break
            rows = r.json().get("rows", [])
            if not rows: break
            logins_raw.extend(rows)
            page_log += 1
        except: break
    min_logins = {}
    for l in logins_raw:
        d_str = l.get("data_login")
        if not d_str: continue
        try:
            dt = datetime.strptime(d_str, "%Y-%m-%d %H:%M:%S"); d_key = dt.strftime("%Y-%m-%d")
            if d_key not in min_logins: min_logins[d_key] = dt
            else:
                if dt < min_logins[d_key]: min_logins[d_key] = dt
        except: pass
    for d, dt in min_logins.items():
        mins = dt.minute
        if 1 < mins <= 40: local_logins.append({"Agente": nome_agente, "Data": d, "Hora Entrada": dt.strftime("%H:%M:%S"), "Atraso": f"{mins}m"})
    return local_curtas, local_almoco, local_logins, local_ranking

@st.cache_data(ttl=300)
def processar_dados_pausas_supervisor(token, data_ini, data_fim, mapa_agentes):
    curtas, almoco, logins, ranking = [], [], [], []
    my_bar = st.progress(0, text="Auditando pausas e horários...")
    total = len(mapa_agentes); done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_processar_agente_pausas, token, cod, nome, data_ini, data_fim): nome for cod, nome in mapa_agentes.items()}
        for f in concurrent.futures.as_completed(futures):
            done += 1
            my_bar.progress(int(done/total * 100))
            try:
                c, a, l, r = f.result()
                curtas.extend(c); almoco.extend(a); logins.extend(l); ranking.extend(r)
            except: pass
    my_bar.empty()
    return curtas, almoco, logins, ranking

@st.cache_data(ttl=300)
def processar_ranking_geral(token, data_ini, data_fim, mapa_agentes):
    """Gera dados completos: Volume, TMA, TME, TMIA, TMIC e CSAT"""
    headers = {"Authorization": f"Bearer {token}"}
    lista_rank = []
    ids_validos = list(mapa_agentes.keys())
    ids_canais = buscar_ids_canais(token)
    
    dados_stats = {cod: {"Vol": 0, "TMA": "--:--", "TME": "--:--", "TMIA": "--:--", "TMIC": "--:--"} for cod in ids_validos}
    params_stats = {"data_inicial": f"{data_ini} 00:00:00", "data_final": f"{data_fim} 23:59:59", "agrupador": "agente", "agente[]": ids_validos, "canal[]": ids_canais, "id_conta": ID_CONTA}
    try:
        r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_stats)
        if r.status_code == 200:
            for item in r.json():
                nome_api = str(item.get("agrupador", "")).upper()
                cod_match = next((c for c, n in mapa_agentes.items() if n == nome_api or n in nome_api), None)
                if cod_match:
                    dados_stats[cod_match]["Vol"] += int(item.get("num_qtd", 0))
                    dados_stats[cod_match]["TMA"] = item.get("tma", "--:--")
                    dados_stats[cod_match]["TME"] = item.get("tme", "--:--")
                    dados_stats[cod_match]["TMIA"] = item.get("tmia", "--:--")
                    dados_stats[cod_match]["TMIC"] = item.get("tmic", "--:--")
    except: pass

    def _fetch_csat(cod_ag):
        pos, tot = 0, 0
        for pid in PESQUISAS_IDS:
            pg = 1
            while True:
                pars = {"data_inicial": data_ini, "data_final": data_fim, "pesquisa": pid, "id_conta": ID_CONTA, "limit": 1000, "page": pg, "agente[]": [cod_ag]}
                try:
                    rr = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=pars)
                    if rr.status_code != 200: break
                    dd = rr.json()
                    if not dd or not isinstance(dd, list): break
                    found_pg = False
                    for b in dd:
                        if str(b.get("id_pergunta","")) in IDS_PERGUNTAS_VALIDAS:
                            found_pg = True
                            for rsp in b.get("respostas", []):
                                try:
                                    val = float(rsp.get("nom_valor", -1))
                                    if val >= 0:
                                        tot += 1
                                        if val >= 8: pos += 1
                                except: pass
                    if not found_pg and len(dd) < 5: break
                    if len(dd) < 100: break
                    pg += 1
                except: break
        return pos, tot

    dados_csat = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futs = {executor.submit(_fetch_csat, cod): cod for cod in ids_validos}
        for f in concurrent.futures.as_completed(futs):
            cod = futs[f]
            try: dados_csat[cod] = f.result()
            except: dados_csat[cod] = (0, 0)
            
    for cod, nome in mapa_agentes.items():
        st_dat = dados_stats[cod]
        pos, tot = dados_csat.get(cod, (0, 0))
        score = (pos/tot*100) if tot > 0 else 0.0
        
        if st_dat["Vol"] > 0 or tot > 0:
            lista_rank.append({
                "Agente": nome,
                "Volume": st_dat["Vol"],
                "TMA": st_dat["TMA"],
                "TME": st_dat["TME"],
                "TMIA": st_dat["TMIA"],
                "TMIC": st_dat["TMIC"],
                "CSAT Score": score,
                "CSAT Qtd": tot
            })
    return lista_rank

def eleger_melhor_do_mes(df_rank):
    if df_rank.empty: return None
    df_calc = df_rank.copy()
    df_calc['TMA_Seg'] = df_calc['TMA'].apply(time_str_to_seconds)
    df_calc['TMIA_Seg'] = df_calc['TMIA'].apply(time_str_to_seconds)
    df_calc = df_calc[(df_calc['Volume'] > 0) & (df_calc['CSAT Qtd'] > 0)].copy()
    if df_calc.empty: return None
    df_calc['Rank_TMA'] = df_calc['TMA_Seg'].rank(ascending=True)
    df_calc['Rank_TMIA'] = df_calc['TMIA_Seg'].rank(ascending=True)
    df_calc['Rank_CSAT'] = df_calc['CSAT Score'].rank(ascending=False)
    df_calc['Score_Final'] = df_calc['Rank_TMA'] + df_calc['Rank_TMIA'] + df_calc['Rank_CSAT']
    min_score = df_calc['Score_Final'].min()
    mvps = df_calc[df_calc['Score_Final'] == min_score]
    nomes = mvps['Agente'].tolist()
    return ", ".join(nomes)

# ==============================================================================
# 5. COMPONENTES VISUAIS
# ==============================================================================

def render_podium(titulo, dados, metrica, formato, inverso=False):
    st.markdown(f"##### {titulo}")
    if not dados: return st.info("Sem dados.")
    c1, c2, c3 = st.columns(3)
    if metrica in ["TMA", "TMIA"]: 
        rev = True if inverso else False 
        top = sorted(dados, key=lambda x: time_str_to_seconds(x[metrica]), reverse=rev)[:3]
    else:
        rev = False if inverso else True
        top = sorted(dados, key=lambda x: x[metrica], reverse=rev)[:3]
    
    emojis = ["🥇", "🥈", "🥉"] if not inverso else ["🔻", "🔻", "🔻"]
    cols = [c1, c2, c3]
    for i, item in enumerate(top):
        with cols[i]:
            val_str = f"{item[metrica]:.1f}%" if formato == "%" else str(item[metrica])
            st.markdown(f"""
            <div class="podium-card">
                <div class="podium-pos">{emojis[i]}</div>
                <div class="podium-name">{item['Agente']}</div>
                <div class="podium-val">{val_str}</div>
            </div>
            """, unsafe_allow_html=True)

def render_kpi_card(titulo, valor, subtitulo, cor_borda="#6366f1"):
    st.markdown(f"""
    <div style="background: linear-gradient(145deg, #1f2937, #111827); border: 1px solid #374151; border-left: 4px solid {cor_borda}; border-radius: 10px; padding: 15px 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.2); margin-bottom: 10px;">
        <div style="font-size: 0.85rem; color: #9ca3af; text-transform: uppercase; font-weight: 600; margin-bottom: 5px;">{titulo}</div>
        <div style="font-size: 1.8rem; color: #f3f4f6; font-weight: 700;">{valor}</div>
        <div style="font-size: 0.75rem; color: #6b7280; margin-top: 2px;">{subtitulo}</div>
    </div>
    """, unsafe_allow_html=True)

def render_link_card(titulo, url, icon="🚀", cor_borda="#ec4899"):
    st.markdown(f"""
    <a href="{url}" target="_blank" style="text-decoration: none;">
        <div style="background: linear-gradient(145deg, #1f2937, #111827); border: 1px solid #374151; border-left: 4px solid {cor_borda}; border-radius: 10px; padding: 15px 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.2); margin-bottom: 10px; transition: transform 0.2s;">
            <div style="font-size: 0.85rem; color: #9ca3af; text-transform: uppercase; font-weight: 600; margin-bottom: 5px;">{titulo}</div>
            <div style="font-size: 1.5rem; color: #f3f4f6; font-weight: 700;">{icon} Acessar</div>
            <div style="font-size: 0.75rem; color: #6b7280; margin-top: 2px;">Clique para abrir</div>
        </div>
    </a>
    """, unsafe_allow_html=True)

def render_top_bar(nome, id_agente):
    iniciais = nome[:2].upper() if nome else "AG"
    display_id = id_agente if id_agente else "SUP"
    st.markdown(f"""
    <div class="top-bar">
        <div><span style="color:#6366f1; font-weight:bold;">PORTAL</span> <span style="color:white;">DO AGENTE</span></div>
        <div style="display:flex; align-items:center; gap:15px;">
            <div style="text-align:right; line-height:1.2;">
                <div style="color:white; font-weight:600; font-size:0.9rem;">{nome}</div>
                <div style="color:#9ca3af; font-size:0.75rem;">ID: {display_id}</div>
            </div>
            <div style="background:#374151; width:35px; height:35px; border-radius:50%; display:flex; align-items:center; justify-content:center; color:white; font-weight:bold; border:2px solid #6366f1;">{iniciais}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def gerar_link_protocolo(protocolo):
    if not protocolo: return None
    s_proto = str(protocolo).strip()
    if len(s_proto) < 7: suffix = s_proto
    else: suffix = s_proto[-7:]
    return f"https://ateltelecom.matrixdobrasil.ai/atendimento/view/cod_atendimento/{suffix}/readonly/true#atendimento-div"

def barra_lateral_com_changelog():
    with st.sidebar:
        st.header("📅 Filtros")
        opcao = st.radio("Período:", ["Hoje", "Ontem", "Últimos 7 Dias", "Este Mês", "Personalizado"])
        hoje = datetime.now().date()
        if opcao == "Hoje": d_ini = d_fim = hoje
        elif opcao == "Ontem": d_ini = d_fim = hoje - timedelta(days=1)
        elif opcao == "Últimos 7 Dias": d_fim = hoje; d_ini = hoje - timedelta(days=6)
        elif opcao == "Este Mês": d_fim = hoje; d_ini = hoje.replace(day=1)
        else: d_ini = st.date_input("Início", hoje-timedelta(1)); d_fim = st.date_input("Fim", hoje)
        st.info(f"De: {d_ini.strftime('%d/%m')} até {d_fim.strftime('%d/%m')}")
        st.markdown("---")
        with st.expander("📜 Versão Platinum 10.0"):
            st.markdown("""
            **v10.0 - Cloud Ready**
            - **Segurança:** Integração com Secrets do Streamlit (Sem senhas no código).
            - **Google Sheets:** Integração completa para salvar solicitações na nuvem.
            - **v9.0:** Tempo Real.
            - **v8.0:** Visão Individual.
            """)
        if st.button("🚪 Sair", use_container_width=True):
            st.session_state.auth_status = False; st.session_state.user_data = None; st.rerun()
        return d_ini, d_fim

# ==============================================================================
# 6. EXECUÇÃO
# ==============================================================================

if "auth_status" not in st.session_state:
    st.session_state.auth_status = False
    st.session_state.user_data = None
    st.session_state.user_role = None

if not st.session_state.auth_status:
    st.markdown("<br><br>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1.5, 1])
    with c2:
        st.title("🔐 Acesso ao Portal")
        with st.form("login_form"):
            usuario = st.text_input("E-mail ou Login")
            senha = st.text_input("Senha", type="password")
            if st.form_submit_button("Entrar", use_container_width=True):
                if usuario == SUPERVISOR_LOGIN and senha == SUPERVISOR_PASS:
                    st.session_state.auth_status = True; st.session_state.user_role = "supervisor"; st.session_state.user_data = {"nome": "Supervisor NRC", "id": "SUPERVISOR"}; st.rerun()
                else:
                    with st.spinner("Autenticando..."):
                        token = get_admin_token()
                        if token:
                            agente = validar_agente_api(token, usuario)
                            if agente:
                                st.session_state.auth_status = True; st.session_state.user_role = "agente"; st.session_state.user_data = agente; st.rerun()
                            else: st.error("Usuário não encontrado.")
                        else: st.error("Erro de conexão.")
else:
    d_inicial, d_final = barra_lateral_com_changelog()
    render_top_bar(st.session_state.user_data['nome'], st.session_state.user_data['id'])
    token = get_admin_token()
    
    # LOGICA DE VISÃO SUPERVISOR (GERAL OU INDIVIDUAL)
    modo_visao_supervisor = "Geral"
    id_alvo = None
    nome_alvo = None

    if st.session_state.user_role == "supervisor":
        # Carrega lista apenas para o selectbox
        with st.spinner("Carregando equipe..."):
            _, _, _, mapa_agentes_sidebar = buscar_dados_completos_supervisor(token, d_inicial, d_final)
        
        st.sidebar.markdown("---")
        st.sidebar.header("👤 Visão Individual")
        lista_nomes = ["Visão Geral"] + sorted(list(mapa_agentes_sidebar.values()))
        opcao_agente = st.sidebar.selectbox("Selecionar Agente", lista_nomes)
        
        if opcao_agente != "Visão Geral":
            modo_visao_supervisor = "Individual"
            for cod, nome in mapa_agentes_sidebar.items():
                if nome == opcao_agente:
                    id_alvo = cod; nome_alvo = nome; break
    
    # --------------------------------------------------------------------------
    # PAINEL AGENTE / VISÃO INDIVIDUAL
    # --------------------------------------------------------------------------
    if st.session_state.user_role == "agente" or (st.session_state.user_role == "supervisor" and modo_visao_supervisor == "Individual"):
        if st.session_state.user_role == "agente":
            target_id = st.session_state.user_data['id']
            target_name = st.session_state.user_data['nome']
        else:
            target_id = id_alvo
            target_name = nome_alvo
            st.warning(f"👁️‍🗨️ MODO ESPIÃO: Visualizando painel de **{target_name}**")

        st.markdown(f"### 👋 Bem-vindo, {target_name}")
        abas = st.tabs(["📊 Visão Geral", "⏸️ Pausas", "⭐ Qualidade", "🆘 Suporte"])
        with abas[0]:
            if token:
                dt_obj, texto_login, df_logins = buscar_historico_login(token, target_id, d_inicial, d_final)
                stats_data = buscar_estatisticas_agente(token, target_id, d_inicial, d_final)
                val_qtd = stats_data.get('num_qtd', '0') if stats_data else '0'
                val_tma = stats_data.get('tma', '--:--') if stats_data else '--:--'
                val_tmia = stats_data.get('tmia', '--:--') if stats_data else '--:--'
                val_tmic = stats_data.get('tmic', '--:--') if stats_data else '--:--'
                val_tme = stats_data.get('tme', '--:--') if stats_data else '--:--'
                csat_score, csat_qtd, df_csat = buscar_csat_nrc(token, target_id, d_inicial, d_final)
                c1, c2, c3 = st.columns(3)
                with c1:
                    cor = "#10b981" if dt_obj and dt_obj.date() == datetime.now().date() else "#3b82f6"
                    sub = f"Data: {dt_obj.strftime('%d/%m')}" if dt_obj else "Sem registro"
                    render_kpi_card("Primeiro Login", texto_login, sub, cor)
                with c2: render_kpi_card("Volume Total", str(val_qtd), "Atendimentos Finalizados", "#8b5cf6")
                with c3:
                    cor_csat = "#10b981" if csat_score >= 85 else "#f59e0b"
                    render_kpi_card("CSAT (Qualidade)", f"{csat_score:.2f}%", f"Base: {csat_qtd} avaliações", cor_csat)
                c4, c5, c6 = st.columns(3)
                with c4: render_kpi_card("T.M.A", val_tma, "Tempo Médio Atendimento", "#f59e0b")
                with c5: render_kpi_card("T.M.I.A", val_tmia, "Inatividade Agente", "#10b981")
                with c6: render_kpi_card("T.M.I.C", val_tmic, "Inatividade Cliente", "#3b82f6")
                st.markdown("---")
                c7, c8, c9 = st.columns(3)
                with c7: render_kpi_card("T.M.E", val_tme, "Tempo Médio Espera", "#ef4444")
                st.markdown("---")
                if not df_logins.empty:
                    with st.expander("📅 Ver Histórico de Entradas Detalhado", expanded=False): st.dataframe(df_logins, use_container_width=True, hide_index=True)
            else: st.error("Sem conexão API.")
        with abas[1]:
            if token:
                df_pausas = buscar_pausas_detalhado(token, target_id, d_inicial, d_final)
                if not df_pausas.empty:
                    df_pausas['seg_pausado'] = pd.to_numeric(df_pausas['seg_pausado'], errors='coerce').fillna(0)
                    df_pausas['Minutos'] = df_pausas['seg_pausado'] / 60
                    def calcular_excesso_linha(row):
                        nome = str(row['pausa']).upper().strip()
                        duracao = row['Minutos']
                        limite = 0; tipo = "Normal"; excesso = 0.0
                        if any(x in nome for x in ["MANHA", "MANHÃ", "TARDE", "NOITE"]):
                            limite = LIMITES_PAUSA["CURTA"]; tipo = "Penalizável"
                            if duracao > limite: excesso = duracao - limite
                        elif any(x in nome for x in ["ALMOÇO", "ALMOCO", "PLANTÃO", "PLANTAO"]):
                            limite = LIMITES_PAUSA["LONGA"]; tipo = "Atenção"
                            if duracao > limite: excesso = duracao - limite
                        return pd.Series([tipo, limite, excesso])
                    df_pausas[['Tipo', 'Limite', 'Excesso_Calc']] = df_pausas.apply(calcular_excesso_linha, axis=1)
                    total_excesso_penalizavel = df_pausas[df_pausas['Tipo'] == "Penalizável"]['Excesso_Calc'].sum()
                    pausas_longas_criticas = df_pausas[(df_pausas['Tipo'] == "Atenção") & (df_pausas['Minutos'] > (LIMITES_PAUSA["LONGA"] + TOLERANCIA_VISUAL_ALMOCO))]
                    kp1, kp2 = st.columns(2)
                    with kp1:
                        status_msg = "Dentro do Limite"; status_cor = "#10b981"
                        if total_excesso_penalizavel > TOLERANCIA_MENSAL_EXCESSO: status_msg = "LIMITE ESTOURADO"; status_cor = "#ef4444"
                        render_kpi_card("Status de Pausas (Mês)", status_msg, f"Tolerância: {TOLERANCIA_MENSAL_EXCESSO} min | Acumulado: {total_excesso_penalizavel:.2f} min", status_cor)
                    with kp2:
                        cor_card_total = "#ef4444" if total_excesso_penalizavel > TOLERANCIA_MENSAL_EXCESSO else ("#f59e0b" if total_excesso_penalizavel > 0 else "#3b82f6")
                        render_kpi_card("Excesso Penalizável Total", f"{total_excesso_penalizavel:.2f} min", "Soma dos minutos acima de 15min (Manhã/Tarde/Noite)", cor_card_total)
                    if not pausas_longas_criticas.empty: st.warning(f"⚠️ Atenção: {len(pausas_longas_criticas)} pausas de Almoço/Plantão excederam consideravelmente o limite.")
                    st.markdown("---")
                    df_view = df_pausas[['data_pausa', 'pausa', 'tempo_pausado', 'Tipo', 'Excesso_Calc']].copy()
                    df_view['Excesso_Calc'] = df_view['Excesso_Calc'].apply(lambda x: f"{x:.2f} min" if x > 0 else "-")
                    df_view.columns = ['Data/Hora', 'Motivo', 'Duração', 'Classificação', 'Tempo Excedido']
                    with st.expander("📋 Relatório Detalhado de Pausas", expanded=True): st.dataframe(df_view, use_container_width=True, hide_index=True)
                else: st.info("Nenhuma pausa registrada no período.")
            else: st.error("Erro de conexão.")
        with abas[2]:
            if df_csat is not None and not df_csat.empty:
                cor_csat = "#10b981" if csat_score >= 85 else "#f59e0b"
                render_kpi_card("Seu CSAT no Período", f"{csat_score:.2f}%", f"{csat_qtd} avaliações", cor_csat)
                st.markdown("---")
                st.markdown("#### 📋 Histórico de Avaliações")
                df_csat['Acesso'] = df_csat['Protocolo'].apply(gerar_link_protocolo)
                st.dataframe(df_csat[['Data', 'Cliente', 'Nota', 'Comentario', 'Acesso']], column_config={"Acesso": st.column_config.LinkColumn("Link", display_text="Abrir Atendimento"), "Nota": st.column_config.NumberColumn("Nota", format="%d ⭐")}, use_container_width=True, hide_index=True)
                detratores = df_csat[df_csat['Nota_Num'] < 7].copy()
                st.markdown("<br>", unsafe_allow_html=True)
                with st.expander(f"🔻 Ranking de Detratores ({len(detratores)})", expanded=False):
                    if not detratores.empty: st.dataframe(detratores[['Data', 'Cliente', 'Nota', 'Comentario', 'Acesso']], column_config={"Acesso": st.column_config.LinkColumn("Link", display_text="Verificar Motivo"), "Nota": st.column_config.NumberColumn("Nota", format="%d 🔴")}, use_container_width=True, hide_index=True)
                    else: st.success("Nenhum detrator no período!")
            else: st.info("Sem pesquisas de satisfação.")
        with abas[3]:
            st.markdown("### 🆘 Canal Direto com o Monitoramento")
            st.info("Utilize este formulário para solicitar ajustes, enviar denúncias ou pedir ajuda.")
            with st.form("form_suporte"):
                motivo = st.selectbox("Motivo do Contato", ["Ajuste de Ponto", "Contestação de Pausa", "Denúncia Anônima", "Dúvida/Ajuda", "Outros"])
                msg = st.text_area("Descreva sua solicitação com detalhes:", height=150)
                submitted = st.form_submit_button("Enviar Solicitação", type="primary", use_container_width=True)
                if submitted:
                    with st.spinner("Salvando solicitação..."):
                        if motivo == "Denúncia Anônima": nome_save = "ANÔNIMO"; id_save = "ANÔNIMO"
                        else: nome_save = target_name; id_save = target_id
                        sucesso, retorno = salvar_solicitacao_gsheets(nome_save, id_save, motivo, msg)
                        if sucesso: st.success("✅ " + retorno)
                        else: st.error(retorno)

    # --------------------------------------------------------------------------
    # PAINEL SUPERVISOR GERAL
    # --------------------------------------------------------------------------
    elif st.session_state.user_role == "supervisor" and modo_visao_supervisor == "Geral":
        st.markdown("## 🏢 Painel de Gestão - Setor NRC")
        
        # Abas Sup (COM A NOVA ABA TEMPO REAL)
        abas_sup = st.tabs(["👁️ Visão Geral", "🏆 Rankings", "⏸️ Pausas", "⚡ Tempo Real", "🆘 Solicitações"])
        
        # ABA 1: VISÃO GERAL
        with abas_sup[0]:
            if token:
                with st.spinner("Sincronizando estatísticas..."):
                    dados_servicos, csat_geral, base_geral, mapa_agentes = buscar_dados_completos_supervisor(token, d_inicial, d_final)
                    st.markdown("#### ⭐ Visão Global da Equipe")
                    k1, k2 = st.columns([1, 2])
                    with k1:
                        cor_geral = "#10b981" if csat_geral >= 85 else ("#f59e0b" if csat_geral >= 75 else "#ef4444")
                        render_kpi_card("CSAT Global (NRC)", f"{csat_geral:.1f}%", f"Base Total: {base_geral}", cor_geral)
                    with k2: render_link_card("Ferramenta Externa", "https://fideliza-nator-live.streamlit.app/", "FIDELIZA-NATOR")
                    st.markdown("---")
                    for servico in SERVICOS_ALVO:
                        dado = dados_servicos.get(servico, {})
                        st.markdown(f"<div class='service-header'>{servico}</div>", unsafe_allow_html=True)
                        total_s = dado["csat_total"]; pos_s = dado["csat_pos"]
                        score_s = (pos_s / total_s * 100) if total_s > 0 else 0.0
                        c1, c2, c3, c4 = st.columns(4)
                        with c1: render_kpi_card("Volume", str(dado["num_qtd"]), "Atendimentos", "#8b5cf6")
                        with c2: render_kpi_card("T.M.A", str(dado["tma"]), "Tempo Médio", "#3b82f6")
                        with c3: render_kpi_card("T.M.E", str(dado["tme"]), "Fila/Espera", "#ef4444")
                        cor_s = "#10b981" if score_s >= 85 else ("#f59e0b" if score_s >= 75 else "#ef4444")
                        with c4: render_kpi_card("Satisfação", f"{score_s:.1f}%", f"Base: {total_s}", cor_s)
        
        # ABA 2: RANKINGS
        with abas_sup[1]:
            if token and 'mapa_agentes' in locals():
                with st.spinner("Calculando o MVP do Mês..."):
                    lista_rank = processar_ranking_geral(token, d_inicial, d_final, mapa_agentes)
                    if lista_rank:
                        df_rank = pd.DataFrame(lista_rank)
                        mvp_nome = eleger_melhor_do_mes(df_rank)
                        if mvp_nome:
                            st.markdown(f"""<div class="mvp-card"><div style="font-size: 1rem; opacity: 0.8; text-transform: uppercase;">⭐ Destaque do Período ⭐</div><div style="font-size: 2.5rem; font-weight: 800; margin: 10px 0;">{mvp_nome}</div><div style="font-size: 0.9rem;">Melhor equilíbrio entre TMA, TMIA e Satisfação</div></div>""", unsafe_allow_html=True)
                        st.markdown("### 🚀 Top Produtividade (Volume)")
                        render_podium("Campeões de Volume", lista_rank, "Volume", "")
                        st.markdown("---")
                        st.markdown("### ⭐ Top Qualidade (CSAT)")
                        render_podium("Campeões de Nota", lista_rank, "CSAT Score", "%")
                        st.markdown("---")
                        st.markdown("#### 📊 Tabela Geral de Desempenho (Todos os Tempos)")
                        df_display = df_rank[['Agente', 'Volume', 'TMA', 'TME', 'TMIA', 'TMIC', 'CSAT Score', 'CSAT Qtd']].sort_values(by="Volume", ascending=False)
                        st.dataframe(df_display, column_config={"CSAT Score": st.column_config.NumberColumn("CSAT Score", format="%.1f%%")}, use_container_width=True, hide_index=True)
                        st.markdown("---")
                        st.error("🔻 Pontos de Atenção (Detratores)")
                        c_d1, c_d2, c_d3 = st.columns(3)
                        with c_d1:
                            st.markdown("**Menores Notas (CSAT)**")
                            render_podium("Baixa Satisfação", lista_rank, "CSAT Score", "%", inverso=True)
                        with c_d2:
                            st.markdown("**Maiores Tempos (TMA)**")
                            render_podium("Mais Lentos", lista_rank, "TMA", "", inverso=True)
                        with c_d3:
                            st.markdown("**Maior Ociosidade (TMIA)**")
                            render_podium("Mais Ociosos", lista_rank, "TMIA", "", inverso=True)
                    else: st.warning("Sem dados suficientes para gerar ranking.")
            else: st.info("Aguarde o carregamento da Visão Geral.")

        # ABA 3: PAUSAS
        with abas_sup[2]:
            if token and 'mapa_agentes' in locals():
                l_curtas, l_almoco, l_logins, l_ranking = processar_dados_pausas_supervisor(token, d_inicial, d_final, mapa_agentes)
                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("1. 🚨 Risco de Estouro (Manhã/Tarde)")
                    if l_curtas:
                        df_c = pd.DataFrame(l_curtas).sort_values(by="Valor Num", ascending=False)
                        st.dataframe(df_c[['Agente', 'Excesso Acumulado', 'Status']], use_container_width=True, hide_index=True)
                    else: st.success("Ninguém estourou!")
                with c2:
                    st.subheader("2. 🍽️ Atrasos de Almoço")
                    if l_almoco: st.dataframe(pd.DataFrame(l_almoco), use_container_width=True, hide_index=True)
                    else: st.success("Sem atrasos.")
                st.markdown("---")
                c3, c4 = st.columns(2)
                with c3:
                    st.subheader("3. ⏰ Pontualidade (Logins)")
                    if l_logins: st.dataframe(pd.DataFrame(l_logins).sort_values(by="Data", ascending=False), use_container_width=True, hide_index=True)
                    else: st.success("Todos pontuais!")
                with c4:
                    st.subheader("4. 🏆 Ranking Pausas (Qtd)")
                    if l_ranking: st.dataframe(pd.DataFrame(l_ranking).sort_values(by="Qtd Pausas", ascending=False), use_container_width=True, hide_index=True)
            else: st.info("Aguarde o carregamento da Visão Geral.")
            
        # ABA 4: TEMPO REAL (NOVA FUNCIONALIDADE)
        with abas_sup[3]:
            if token:
                if st.button("🔄 Atualizar Lista Online"): st.rerun()
                
                lista_online = buscar_agentes_online_filtrado_nrc(token)
                
                if lista_online:
                    st.markdown(f"### 🟢 {len(lista_online)} Agentes Online (NRC)")
                    
                    for ag in lista_online:
                        aid = ag.get("cod")
                        nome = ag.get("nom_agente", "Desconhecido")
                        status = ag.get("status", "Online")
                        tempo = ag.get("tempo_status", "--:--")
                        
                        cor_status = "#10b981" # Verde
                        if "Pausa" in status: cor_status = "#f59e0b"
                        
                        st.markdown(f"""
                        <div class="realtime-card">
                            <div style="flex:1;">
                                <div style="font-weight:bold; color:white; font-size:1.1rem;">{nome}</div>
                                <div style="color:#9ca3af; font-size:0.8rem;">ID: {aid}</div>
                            </div>
                            <div style="flex:1; text-align:center;">
                                <span style="background-color:{cor_status}; color:black; padding:2px 10px; border-radius:12px; font-weight:bold; font-size:0.8rem;">{status}</span>
                                <div style="margin-top:5px; color:#e5e7eb; font-family:monospace;">⏱ {tempo}</div>
                            </div>
                            <div style="flex:1; text-align:right;">
                                </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Botão de Ação (Streamlit)
                        col_btn = st.columns([4, 1])[1]
                        with col_btn:
                            if st.button("🔴 Deslogar", key=f"btn_logout_{aid}"):
                                with st.spinner(f"Deslogando {nome}..."):
                                    suc, msg = forcar_logout(token, aid)
                                    if suc: 
                                        st.success(f"{nome} deslogado!")
                                        time.sleep(1)
                                        st.rerun()
                                    else: st.error(msg)
                else:
                    st.warning("Nenhum agente da equipe NRC está online no momento.")
            else: st.error("Erro de conexão.")

        # ABA 5: SOLICITAÇÕES
        with abas_sup[4]:
            st.info("Visualização das solicitações registradas no Google Sheets.")
            df_gs = ler_solicitacoes_gsheets()
            if not df_gs.empty:
                st.dataframe(df_gs, use_container_width=True)
            else: st.warning("Nenhuma solicitação encontrada na planilha.")
