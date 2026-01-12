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

# ESTILOS CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .stApp { background-color: #0f1116; }
    section[data-testid="stSidebar"] { background-color: #161b22; border-right: 1px solid #30363d; }
    .top-bar { background-color: #1f2937; padding: 1rem 1.5rem; border-radius: 12px; border: 1px solid #374151; display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
    .service-header { color: #e5e7eb; font-size: 1.2rem; font-weight: 700; margin-top: 25px; margin-bottom: 15px; border-left: 5px solid #6366f1; padding-left: 15px; background-color: #1f2937; padding-top: 5px; padding-bottom: 5px; border-radius: 0 8px 8px 0; }
    .kpi-card { background: linear-gradient(145deg, #1f2937, #111827); border: 1px solid #374151; border-radius: 12px; padding: 15px; box-shadow: 0 4px 6px rgba(0,0,0,0.2); min-height: 110px; margin-bottom: 10px; }
    .kpi-title { font-size: 0.75rem; color: #9ca3af; text-transform: uppercase; font-weight: 600; margin-bottom: 5px; }
    .kpi-value { font-size: 1.4rem; color: #f3f4f6; font-weight: 700; }
    .kpi-sub { font-size: 0.7rem; color: #6b7280; margin-top: 4px; }
    .podium-card { background: linear-gradient(145deg, #1f2937, #111827); border: 1px solid #374151; border-radius: 10px; padding: 15px; text-align: center; margin-bottom: 10px; }
    .podium-pos { font-size: 2rem; margin-bottom: 5px; }
    .podium-name { font-weight: 700; color: #f3f4f6; font-size: 1.1rem; }
    .podium-val { color: #9ca3af; font-size: 0.9rem; margin-top: 5px; }
    .mvp-card { background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%); border-radius: 15px; padding: 20px; text-align: center; color: white; box-shadow: 0 10px 15px -3px rgba(99, 102, 241, 0.4); margin-bottom: 25px; border: 1px solid #818cf8; }
    .realtime-card { background-color: #1f2937; padding: 15px; border-radius: 10px; margin-bottom: 10px; border: 1px solid #374151; display: flex; align-items: center; justify-content: space-between; }
    .stTabs [data-baseweb="tab-list"] { gap: 10px; border-bottom: 1px solid #374151; margin-bottom: 20px; }
    .stTabs [data-baseweb="tab"] { height: 60px; white-space: pre-wrap; background-color: transparent; border: none; color: #9ca3af; font-size: 1.1rem; font-weight: 600; padding: 0 20px; }
    .stTabs [data-baseweb="tab"]:hover { color: #e5e7eb; background-color: #1f2937; border-radius: 8px 8px 0 0; }
    .stTabs [data-baseweb="tab"][aria-selected="true"] { color: #6366f1 !important; border-bottom: 3px solid #6366f1; }
</style>
""", unsafe_allow_html=True)

# 🔒 BLOQUEIO DE SEGURANÇA
if "app_unlocked" not in st.session_state:
    st.session_state.app_unlocked = False

def check_master_password():
    if st.session_state.app_unlocked: return
    st.markdown("<br><br><br>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1, 1])
    with c2:
        st.markdown("<h3 style='text-align: center;'>🔒 Acesso Restrito</h3>", unsafe_allow_html=True)
        pwd = st.text_input("Senha do Sistema", type="password", key="master_pwd")
        if st.button("Liberar Acesso", use_container_width=True):
            try:
                if pwd == st.secrets["security"]["MASTER_PASSWORD"]:
                    st.session_state.app_unlocked = True
                    st.rerun()
                else: st.error("Senha incorreta.")
            except: st.error("Erro: Configure [security] MASTER_PASSWORD no Secrets.")
    st.stop()

check_master_password()

# CREDENCIAIS
try:
    BASE_URL = st.secrets["api"]["BASE_URL"]
    ADMIN_USER = st.secrets["api"]["ADMIN_USER"]
    ADMIN_PASS = st.secrets["api"]["ADMIN_PASS"]
    ID_CONTA = st.secrets["api"]["ID_CONTA"]
    SUPERVISOR_LOGIN = st.secrets["auth"]["SUPERVISOR_LOGIN"]
    SUPERVISOR_PASS = st.secrets["auth"]["SUPERVISOR_PASS"]
    SUPERVISOR_CANCELAMENTO_LOGIN = st.secrets["auth"].get("SUPERVISOR_CANCELAMENTO_LOGIN", "admin_cancel")
    SUPERVISOR_CANCELAMENTO_PASS = st.secrets["auth"].get("SUPERVISOR_CANCELAMENTO_PASS", "senha_cancel")
    SUPERVISOR_SUPORTE_LOGIN = st.secrets["auth"].get("SUPERVISOR_SUPORTE_LOGIN", "admin_sup")
    SUPERVISOR_SUPORTE_PASS = st.secrets["auth"].get("SUPERVISOR_SUPORTE_PASS", "senha_sup")
    SUPERVISOR_NEGOCIACAO_LOGIN = st.secrets["auth"].get("SUPERVISOR_NEGOCIACAO_LOGIN", "admin_neg")
    SUPERVISOR_NEGOCIACAO_PASS = st.secrets["auth"].get("SUPERVISOR_NEGOCIACAO_PASS", "senha_neg")
    PESQUISAS_IDS = st.secrets["ids"]["PESQUISAS_IDS"]
    IDS_PERGUNTAS_VALIDAS = st.secrets["ids"]["IDS_PERGUNTAS_VALIDAS"]
except Exception as e:
    st.error(f"Erro Secrets: {e}")
    st.stop()

# CONSTANTES
CANAIS_ALVO = ['appchat', 'chat', 'botmessenger', 'instagram', 'whatsapp']
SERVICOS_ALVO = ['COMERCIAL', 'FINANCEIRO', 'NOVOS CLIENTES', 'LIBERAÇÃO']
LISTA_NRC = ['RILDYVAN', 'MILENA', 'ALVES', 'MONICKE', 'AYLA', 'MARIANY', 'EDUARDA', 'MENEZES', 'JUCIENNY', 'MARIA', 'ANDREZA', 'LUZILENE', 'IGO', 'AIDA', 'Caribé', 'Michelly', 'ADRIA', 'ERICA', 'HENRIQUE', 'SHYRLEI', 'ANNA', 'JULIA', 'FERNANDES']
NOMES_COMUNS_PRIMEIRO = ['MARIA', 'ANNA', 'JULIA', 'ERICA']

SETORES_AGENTES = {
    "NRC": LISTA_NRC, 
    "CANCELAMENTO": ['BARBOSA', 'ELOISA', 'LARISSA', 'EDUARDO', 'CAMILA', 'SAMARA'],
    "NEGOCIACAO": ['Carla', 'Lenk', 'Ana Luiza', 'JULIETTI', 'RODRIGO', 'Monalisa', 'Ramom', 'Ednael', 'Leticia', 'Rita', 'Mariana', 'Flavia s', 'Uri', 'Clara', 'Wanderson', 'Aparecida', 'Cristina', 'Caio', 'LUKAS'],
    "SUPORTE": ['VALERIO', 'TARCISIO', 'GRANJA', 'ALICE', 'FERNANDO', 'SANTOS', 'RENAN', 'FERREIRA', 'HUEMILLY', 'LOPES', 'LAUDEMILSON', 'RAYANE', 'LAYS', 'JORGE', 'LIGIA', 'ALESSANDRO', 'GEIBSON', 'ROBERTO', 'OLIVEIRA', 'MAURÍCIO', 'AVOLO', 'CLEBER', 'ROMERIO', 'JUNIOR', 'ISABELA', 'RENAN', 'WAGNER', 'CLAUDIA', 'ANTONIO', 'JOSE', 'LEONARDO', 'KLEBSON', 'OZENAIDE']
}
SETORES_SERVICOS = {
    "NRC": SERVICOS_ALVO,
    "CANCELAMENTO": ['CANCELAMENTO'], 
    "NEGOCIACAO": ['NEGOCIAÇÃO ATIVA', 'NEGOCIAÇÃO  PASSIVA', 'CLINTES CORPORATIVOS-LINK DEDICADO', 'CLIENTES CORPORATIVOS-LINK DEDICADO'], 
    "SUPORTE": ['SUPORTE', 'LIBERAÇÃO']
}
LISTA_PLANTAO = ['TARCISIO', 'GEIBSON', 'LEONARDO', 'FERNANDO', 'RENAN']
ID_CONTA_CLIENTE_INTERNO = "5"
CONTAS_NEGOCIACAO = ["1", "14"]
LIMITES_PAUSA = { "CURTA": 15.0, "LONGA": 120.0 }
TOLERANCIA_MENSAL_EXCESSO = 20.0 
TOLERANCIA_VISUAL_ALMOCO = 2.0

# ==============================================================================
# 2. FUNÇÕES AUXILIARES E CONEXÃO
# ==============================================================================

def conectar_gsheets():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open("solicitacoes_nrc").sheet1
    except: return None

def salvar_solicitacao_gsheets(nome, id_agente, motivo, msg):
    sheet = conectar_gsheets()
    if sheet:
        try:
            sheet.append_row([datetime.now().strftime("%d/%m/%Y %H:%M:%S"), id_agente, nome, motivo, msg])
            return True, "Salvo com sucesso!"
        except Exception as e: return False, str(e)
    return False, "Erro conexão"

def ler_solicitacoes_gsheets():
    sheet = conectar_gsheets()
    if sheet:
        try: return pd.DataFrame(sheet.get_all_records())
        except: return pd.DataFrame()
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_admin_token():
    try:
        r = requests.post(f"{BASE_URL}/authuser", json={"login": ADMIN_USER, "chave": ADMIN_PASS}, timeout=5)
        if r.status_code == 200 and r.json().get("success"): return r.json()["result"]["token"]
    except: pass
    return None

def validar_agente_api(token, user_input):
    try:
        r = requests.get(f"{BASE_URL}/agentes", headers={"Authorization": f"Bearer {token}"}, params={"login": user_input.strip().lower()}, timeout=5)
        if r.status_code == 200:
            res = r.json().get("result", [])
            if res: return {"id": str(res[0].get("cod_agente")), "nome": res[0].get("nome_exibicao") or res[0].get("agente"), "email": res[0].get("email","").lower()}
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

def formatar_tempo_humano(minutos):
    if not minutos: return "0m"
    h, m = divmod(int(minutos), 60)
    return f"{h}h {m:02d}m" if h > 0 else f"{m}m"

@st.cache_data(ttl=3600)
def buscar_ids_canais(token):
    ids = []
    try:
        r = requests.get(f"{BASE_URL}/canais", headers={"Authorization": f"Bearer {token}"})
        if r.status_code == 200:
            for c in r.json():
                if any(alvo in str(c.get("canal","")).lower() for alvo in CANAIS_ALVO): ids.append(str(c.get("id_canal")))
    except: pass
    return ids

@st.cache_data(ttl=600)
def mapear_todos_agentes(token):
    headers = {"Authorization": f"Bearer {token}"}
    mapa = {}
    page = 1
    # Adicionando visual de carregamento para não parecer que travou
    status = st.empty()
    while True:
        status.caption(f"Carregando base de agentes... Página {page}")
        try:
            r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": page, "bol_cancelado": 0}, timeout=10)
            if r.status_code != 200: break
            data = r.json()
            rows = data.get("result", [])
            if not rows: break
            for row in rows:
                nome = str(row.get("nome_exibicao") or row.get("agente")).strip().upper()
                cod = str(row.get("cod_agente"))
                mapa[nome] = cod
                primeiro = nome.split()[0]
                if primeiro not in mapa: mapa[primeiro] = cod
            if len(rows) < 100: break
            page += 1
        except: break
    status.empty()
    return mapa

# ==============================================================================
# 3. FUNÇÕES ESPECÍFICAS AGENTE
# ==============================================================================

@st.cache_data(ttl=60)
def buscar_historico_login(token, id_agente, d_ini, d_fim):
    headers = {"Authorization": f"Bearer {token}"}
    logins = []
    page = 1
    while page <= 3:
        try:
            r = requests.get(f"{BASE_URL}/relAgenteLogin", headers=headers, params={"data_inicial": d_ini.strftime("%Y-%m-%d"), "data_final": d_fim.strftime("%Y-%m-%d"), "agente": id_agente, "page": page, "limit": 100}, timeout=10)
            if r.status_code != 200: break
            rows = r.json().get("rows", [])
            if not rows: break
            logins.extend(rows)
            if len(rows) < 100: break
            page += 1
        except: break
    
    df = pd.DataFrame(logins)
    txt_prim = "Sem Login"
    dt_prim = None
    if not df.empty:
        # Pega o primeiro registro do dia mais recente
        datas = []
        for x in logins:
            if x.get("data_login"):
                datas.append(datetime.strptime(x["data_login"], "%Y-%m-%d %H:%M:%S"))
        if datas:
            datas.sort(reverse=True)
            dt_prim = datas[0]
            txt_prim = dt_prim.strftime("%H:%M")
    return dt_prim, txt_prim, df

@st.cache_data(ttl=300)
def buscar_estatisticas_agente(token, id_agente, d_ini, d_fim):
    ids_can = buscar_ids_canais(token)
    try:
        r = requests.get(f"{BASE_URL}/relAtEstatistico", headers={"Authorization": f"Bearer {token}"}, 
                         params={"data_inicial": f"{d_ini.strftime('%Y-%m-%d')} 00:00:00", "data_final": f"{d_fim.strftime('%Y-%m-%d')} 23:59:59", "agrupador": "agente", "agente[]": [id_agente], "canal[]": ids_can, "id_conta": ID_CONTA})
        if r.status_code == 200:
            l = r.json()
            if l: return l[0]
    except: pass
    return {}

@st.cache_data(ttl=600)
def buscar_csat_nrc(token, id_agente, d_ini, d_fim):
    headers = {"Authorization": f"Bearer {token}"}
    respostas = []
    for pid in PESQUISAS_IDS:
        pg = 1
        while True:
            try:
                r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params={"data_inicial": d_ini.strftime("%Y-%m-%d"), "data_final": d_fim.strftime("%Y-%m-%d"), "pesquisa": pid, "id_conta": ID_CONTA, "limit": 1000, "page": pg, "agente[]": [id_agente]})
                if r.status_code != 200: break
                data = r.json()
                if not data or not isinstance(data, list): break
                total_api = 0
                for b in data:
                    if str(b.get("id_pergunta","")) in IDS_PERGUNTAS_VALIDAS:
                        total_api += sum(int(x.get("num_quantidade", 0)) for x in b["sintetico"])
                        for rsp in b.get("respostas", []):
                            respostas.append({"Nota": rsp.get("nom_valor"), "Comentario": rsp.get("nom_resposta"), "Data": rsp.get("dat_resposta"), "Cliente": rsp.get("nom_contato"), "Protocolo": rsp.get("num_protocolo")})
                if pg * 1000 >= total_api: break
                if len(data) < 2: break
                pg += 1
            except: break
    df = pd.DataFrame(respostas)
    score = 0.0
    if not df.empty:
        # CORREÇÃO KEY ERROR: Garantindo a coluna Nota_Num
        df['Nota_Num'] = pd.to_numeric(df['Nota'], errors='coerce').fillna(0).astype(int)
        tot = len(df)
        if tot > 0: score = (len(df[df['Nota_Num'] >= 8]) / tot) * 100
    else:
        df['Nota_Num'] = 0 # Garante a coluna se vazio
        
    return score, len(df), df

@st.cache_data(ttl=300)
def buscar_pausas_detalhado(token, id_agente, d_ini, d_fim):
    pausas = []
    pg = 1
    while True:
        try:
            r = requests.get(f"{BASE_URL}/relAgentePausa", headers={"Authorization": f"Bearer {token}"}, params={"dat_inicial": d_ini.strftime("%Y-%m-%d"), "dat_final": d_fim.strftime("%Y-%m-%d"), "cod_agente": id_agente, "limit": 100, "pagina": pg})
            if r.status_code != 200: break
            rows = r.json().get("rows", [])
            if not rows: break
            pausas.extend(rows)
            pg += 1
        except: break
    return pd.DataFrame(pausas)

def gerar_link_protocolo(protocolo):
    if not protocolo: return None
    s = str(protocolo).strip()
    return f"https://ateltelecom.matrixdobrasil.ai/atendimento/view/cod_atendimento/{s[-7:] if len(s)>7 else s}/readonly/true#atendimento-div"

# ==============================================================================
# 4. FUNÇÕES GERAIS SUPERVISOR
# ==============================================================================

def buscar_agentes_online_filtrado_setor(token, setor_nome):
    headers = {"Authorization": f"Bearer {token}"}
    on_filtrados = []
    nomes_alvo = [x.strip().upper() for x in SETORES_AGENTES.get(setor_nome, [])]
    try:
        r = requests.get(f"{BASE_URL}/agentesOnline", headers=headers)
        if r.status_code == 200:
            for ag in r.json():
                nm = str(ag.get("nom_agente","")).strip().upper()
                parts = nm.split()
                if not parts: continue
                match = False
                for alvo in nomes_alvo:
                    if alvo in parts: match=True; break
                    if alvo in NOMES_COMUNS_PRIMEIRO and alvo == parts[0]: match=True; break
                if match: on_filtrados.append(ag)
    except: pass
    return on_filtrados

def buscar_agentes_online_filtrado_nrc(token):
    return buscar_agentes_online_filtrado_setor(token, "NRC")

def forcar_logout(token, id_agente):
    try:
        r = requests.post(f"{BASE_URL}/deslogarAgente", headers={"Authorization": f"Bearer {token}"}, json={"id_agente": int(id_agente)})
        if r.status_code == 200: return True, "Sucesso"
        return False, r.text
    except Exception as e: return False, str(e)

# ==============================================================================
# 5. DADOS ESTATÍSTICOS MULTISETOR
# ==============================================================================

@st.cache_data(ttl=300)
def buscar_dados_completos_supervisor(token, d_ini, d_fim):
    return buscar_dados_supervisor_multisetor(token, d_ini, d_fim, "NRC")

@st.cache_data(ttl=300)
def buscar_dados_supervisor_multisetor(token, d_ini, d_fim, setor_nome):
    headers = {"Authorization": f"Bearer {token}"}
    ids_agentes = []
    mapa_agentes = {}
    
    # Mapeamento
    mapa_full = mapear_todos_agentes(token)
    nomes_alvo = [x.strip().upper() for x in SETORES_AGENTES.get(setor_nome, [])]
    
    for nome in nomes_alvo:
        if nome in mapa_full:
            cid = mapa_full[nome]
            if cid not in ids_agentes: ids_agentes.append(cid); mapa_agentes[cid] = nome
        else:
            for real_n, c in mapa_full.items():
                if nome in real_n.split():
                    if nome in NOMES_COMUNS_PRIMEIRO:
                        if nome == real_n.split()[0]: ids_agentes.append(c); mapa_agentes[c] = real_n; break
                    else:
                         ids_agentes.append(c); mapa_agentes[c] = real_n; break
    
    ids_canais = buscar_ids_canais(token)
    servicos = SETORES_SERVICOS.get(setor_nome, [])
    resultados = {s: {"num_qtd": 0, "tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--", "csat_pos": 0, "csat_total": 0} for s in servicos}
    
    if not ids_agentes: return resultados, 0.0, 0, mapa_agentes, {"tma":"--","tme":"--","tmia":"--","tmic":"--"}

    # Globais
    dados_g = {"tma": "--", "tme": "--", "tmia": "--", "tmic": "--"}
    try:
        r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params={"data_inicial": f"{d_ini.strftime('%Y-%m-%d')} 00:00:00", "data_final": f"{d_fim.strftime('%Y-%m-%d')} 23:59:59", "agrupador": "conta", "agente[]": ids_agentes, "canal[]": ids_canais, "id_conta": ID_CONTA})
        if r.status_code == 200:
            l = r.json()
            if l: dados_g = l[0]
    except: pass

    # Por Serviço
    for s in servicos:
        try:
            r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params={"data_inicial": f"{d_ini.strftime('%Y-%m-%d')} 00:00:00", "data_final": f"{d_fim.strftime('%Y-%m-%d')} 23:59:59", "agrupador": "servico", "agente[]": ids_agentes, "canal[]": ids_canais, "id_conta": ID_CONTA, "servico": s})
            if r.status_code == 200:
                l = r.json()
                if l:
                    i = l[0]
                    resultados[s]["num_qtd"] = int(i.get("num_qtd",0)) - int(i.get("num_qtd_abandonado",0))
                    resultados[s]["tma"] = i.get("tma"); resultados[s]["tme"] = i.get("tme"); resultados[s]["tmia"] = i.get("tmia"); resultados[s]["tmic"] = i.get("tmic")
        except: pass

    # CSAT
    pos = 0; tot = 0
    for pid in PESQUISAS_IDS:
        pg = 1
        while True:
            try:
                r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params={"data_inicial": d_ini.strftime("%Y-%m-%d"), "data_final": d_fim.strftime("%Y-%m-%d"), "pesquisa": pid, "id_conta": ID_CONTA, "limit": 1000, "page": pg, "agente[]": ids_agentes})
                if r.status_code != 200: break
                d = r.json()
                if not d or not isinstance(d, list): break
                t_api = 0
                for b in d:
                    if str(b.get("id_pergunta","")) in IDS_PERGUNTAS_VALIDAS:
                        t_api += sum(int(x.get("num_quantidade", 0)) for x in b["sintetico"])
                        for rsp in b.get("respostas", []):
                            s_nm = str(rsp.get("nom_servico", "")).upper().strip()
                            v = float(rsp.get("nom_valor", -1))
                            if v >= 0:
                                tot += 1
                                if v >= 8: pos += 1
                                if s_nm in servicos:
                                    resultados[s_nm]["csat_total"] += 1
                                    if v >= 8: resultados[s_nm]["csat_pos"] += 1
                if pg * 1000 >= t_api: break
                if len(d) < 2: break
                pg += 1
            except: break
            
    score = (pos/tot*100) if tot > 0 else 0.0
    return resultados, score, tot, mapa_agentes, dados_g

# ==============================================================================
# 5.4 NEGOCIAÇÃO (MULTI CONTA)
# ==============================================================================

def calcular_media_ponderada_tempos(lista):
    q_t = 0; s_tma=0; s_tme=0; s_tmia=0; s_tmic=0
    for i in lista:
        q = i.get('qtd', 0)
        if q <= 0: continue
        q_t += q
        s_tma += time_str_to_seconds(i.get('tma', '00:00:00')) * q
        s_tme += time_str_to_seconds(i.get('tme', '00:00:00')) * q
        s_tmia += time_str_to_seconds(i.get('tmia', '00:00:00')) * q
        s_tmic += time_str_to_seconds(i.get('tmic', '00:00:00')) * q
    if q_t == 0: return "--:--", "--:--", "--:--", "--:--"
    return seconds_to_hms(round(s_tma/q_t)), seconds_to_hms(round(s_tme/q_t)), seconds_to_hms(round(s_tmia/q_t)), seconds_to_hms(round(s_tmic/q_t))

@st.cache_data(ttl=300)
def buscar_dados_negociacao_multiconta(token, d_ini, d_fim):
    headers = {"Authorization": f"Bearer {token}"}
    ids_can = buscar_ids_canais(token)
    servicos = SETORES_SERVICOS["NEGOCIACAO"]
    res = {s: {"num_qtd": 0, "tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--", "csat_pos": 0, "csat_total": 0} for s in servicos}
    acum_global = []
    
    # Tempos
    for s in servicos:
        pond = []
        vol = 0
        for c_id in CONTAS_NEGOCIACAO:
            try:
                r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params={"data_inicial": f"{d_ini.strftime('%Y-%m-%d')} 00:00:00", "data_final": f"{d_fim.strftime('%Y-%m-%d')} 23:59:59", "agrupador": "servico", "canal[]": ids_can, "id_conta": c_id, "servico": s})
                if r.status_code == 200:
                    l = r.json()
                    if l:
                        i = l[0]
                        q = int(i.get("num_qtd", 0)) - int(i.get("num_qtd_abandonado", 0))
                        if q > 0:
                            d = {'qtd': q, 'tma': i.get("tma"), 'tme': i.get("tme"), 'tmia': i.get("tmia"), 'tmic': i.get("tmic")}
                            pond.append(d); acum_global.append(d); vol += q
            except: pass
        tma, tme, tmia, tmic = calcular_media_ponderada_tempos(pond)
        res[s]["num_qtd"] = vol; res[s]["tma"] = tma; res[s]["tme"] = tme; res[s]["tmia"] = tmia; res[s]["tmic"] = tmic
    
    gtma, gtme, gtmia, gtmic = calcular_media_ponderada_tempos(acum_global)
    globais = {"tma": gtma, "tme": gtme, "tmia": gtmia, "tmic": gtmic}

    # CSAT
    agentes_alvo = [x.strip().upper() for x in SETORES_AGENTES["NEGOCIACAO"]]
    mapa_s = {" ".join(s.split()).upper(): s for s in servicos}
    pos_g = 0; tot_g = 0
    
    for c_id in CONTAS_NEGOCIACAO:
        for pid in PESQUISAS_IDS:
            pg = 1
            while True:
                try:
                    r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params={"data_inicial": d_ini.strftime("%Y-%m-%d"), "data_final": d_fim.strftime("%Y-%m-%d"), "pesquisa": pid, "id_conta": c_id, "limit": 1000, "page": pg})
                    if r.status_code != 200: break
                    d = r.json()
                    if not d or not isinstance(d, list): break
                    api_tot = 0
                    for b in d:
                        if str(b.get("id_pergunta","")) in IDS_PERGUNTAS_VALIDAS:
                            api_tot += sum(int(x.get("num_quantidade", 0)) for x in b["sintetico"])
                            for rsp in b.get("respostas", []):
                                nm = str(rsp.get("nom_agente", "")).upper()
                                # Filtro equipe
                                is_team = False
                                for al in agentes_alvo:
                                    if al in nm: is_team = True; break
                                if not is_team: continue
                                
                                s_raw = str(rsp.get("nom_servico", "")).upper()
                                s_clean = " ".join(s_raw.split())
                                k_s = None
                                if s_clean in mapa_s: k_s = mapa_s[s_clean]
                                elif "LINK" in s_clean or "CLINTES" in s_clean:
                                    for s in servicos:
                                        if "LINK" in s or "CLINTES" in s: k_s = s; break
                                
                                if k_s:
                                    v = float(rsp.get("nom_valor", -1))
                                    if v >= 0:
                                        tot_g += 1
                                        if v >= 8: pos_g += 1
                                        res[k_s]["csat_total"] += 1
                                        if v >= 8: res[k_s]["csat_pos"] += 1
                    if pg * 1000 >= api_tot: break
                    if len(d) < 2: break
                    pg += 1
                except: break
    
    score_g = (pos/tot*100) if tot_g > 0 else 0.0
    return res, score_g, tot_g, globais

@st.cache_data(ttl=300)
def buscar_auditoria_volumetria(token, d_ini, d_fim, lista_agentes):
    ids_can = buscar_ids_canais(token)
    stats = {n.upper(): {"Ativo": 0, "Passivo": 0} for n in lista_agentes}
    nomes = list(stats.keys())
    servicos_u = [s.upper() for s in SETORES_SERVICOS["NEGOCIACAO"]]
    
    for c in CONTAS_NEGOCIACAO:
        pg = 1
        while True:
            try:
                r = requests.get(f"{BASE_URL}/relAtAnalitico", headers={"Authorization": f"Bearer {token}"}, params={"id_conta": c, "data_inicial": f"{d_ini.strftime('%Y-%m-%d')} 00:00:00", "data_final": f"{d_fim.strftime('%Y-%m-%d')} 23:59:59", "limit": 100, "page": pg})
                if r.status_code != 200: break
                rows = r.json().get("rows", [])
                if not rows: break
                for rw in rows:
                    if str(rw.get("id_tipo_integracao")) not in ids_can: continue
                    s_api = str(rw.get("servico", "")).upper()
                    
                    is_t = False
                    for s in servicos_u: 
                        if s in s_api: is_t = True; break
                    if "LINK" in s_api or "CLINTES" in s_api: is_t = True
                    
                    if is_t:
                        ag_api = str(rw.get("agente", "")).strip().upper()
                        for nm in nomes:
                            if nm in ag_api:
                                if "PASSIVA" in s_api: stats[nm]["Passivo"] += 1
                                else: stats[nm]["Ativo"] += 1
                                break
                if len(rows) < 100: break
                pg += 1
            except: break
            
    res = []
    for n, d in stats.items():
        t = d["Ativo"] + d["Passivo"]
        if t > 0: res.append({"Agente": n, "Total": t, "Ativo": d["Ativo"], "Passivo": d["Passivo"]})
    return res

# ==============================================================================
# 5.5 PAUSAS (COM CORREÇÃO DE LÓGICA DE GAP + FIX CRASH EMPTY FLOAT)
# ==============================================================================

def _processar_agente_pausas(token, cod_agente, nome_agente, d_ini, d_fim, setor_nome):
    headers = {"Authorization": f"Bearer {token}"}
    curtas, almoco, logins, ranking = [], [], [], []
    
    # 1. Pausas Sistema
    pausas = []
    pg = 1
    while pg <= 5:
        try:
            r = requests.get(f"{BASE_URL}/relAgentePausa", headers=headers, params={"dat_inicial": d_ini.strftime("%Y-%m-%d"), "dat_final": d_fim.strftime("%Y-%m-%d"), "cod_agente": cod_agente, "limit": 100, "pagina": pg}, timeout=10)
            if r.status_code != 200: break
            rows = r.json().get("rows", [])
            if not rows: break
            pausas.extend(rows)
            pg += 1
        except: break
        
    acum_curta = 0.0
    for p in pausas:
        m = str(p.get("pausa", "")).upper()
        # FIX FLOAT VAZIO
        try: seg = float(p.get("seg_pausado", 0))
        except: seg = 0.0
            
        mins = seg / 60
        if any(x in m for x in ["MANHA", "TARDE", "NOITE"]):
            if mins > LIMITES_PAUSA["CURTA"]: acum_curta += (mins - LIMITES_PAUSA["CURTA"])
        if any(x in m for x in ["ALMOÇO", "ALMOCO", "PLANTÃO"]):
            if mins > (LIMITES_PAUSA["LONGA"] + TOLERANCIA_VISUAL_ALMOCO):
                almoco.append({"Agente": nome_agente, "Data": p.get("data_pausa", "")[:10], "Duração": formatar_tempo_humano(mins), "Status": f"Estourou {formatar_tempo_humano(mins - LIMITES_PAUSA['LONGA'])}"})
    
    if acum_curta > 0:
        curtas.append({"Agente": nome_agente, "Excesso Acumulado": formatar_tempo_humano(acum_curta), "Valor Num": acum_curta, "Status": "ADVERTÊNCIA" if acum_curta > TOLERANCIA_MENSAL_EXCESSO else "Normal"})
        
    q_pausas = len([p for p in pausas if "TERMINO" not in str(p.get("pausa")).upper() and "EXPEDIENTE" not in str(p.get("pausa")).upper()])
    if q_pausas > 0: ranking.append({"Agente": nome_agente, "Qtd Pausas": q_pausas})

    # 2. Logins (Pontualidade + Gaps)
    logs = []
    pg = 1
    while pg <= 5:
        try:
            r = requests.get(f"{BASE_URL}/relAgenteLogin", headers=headers, params={"data_inicial": d_ini.strftime("%Y-%m-%d"), "data_final": d_fim.strftime("%Y-%m-%d"), "agente": cod_agente, "page": pg, "limit": 100})
            rows = r.json().get("rows", [])
            if not rows: break
            logs.extend(rows)
            pg += 1
        except: break
        
    logs_ord = []
    for l in logs:
        di = l.get("data_login")
        do = l.get("data_logout")
        if di:
            try:
                dti = datetime.strptime(di, "%Y-%m-%d %H:%M:%S")
                dto = datetime.strptime(do, "%Y-%m-%d %H:%M:%S") if do else None
                logs_ord.append({"in": dti, "out": dto})
            except: pass
    logs_ord.sort(key=lambda x: x["in"])
    
    # Gap (Só para setores que deslogam)
    if setor_nome in ["NEGOCIACAO", "CANCELAMENTO"]:
        dias_com_almoco = set()
        for i in range(len(logs_ord)-1):
            sa = logs_ord[i]; sp = logs_ord[i+1]
            if sa["out"]:
                gap = (sp["in"] - sa["out"]).total_seconds() / 60
                h = sa["out"].hour
                d_str = sa["out"].strftime("%Y-%m-%d")
                if (11 <= h <= 15 or 18 <= h <= 19) and gap > 20:
                    if d_str not in dias_com_almoco:
                        dias_com_almoco.add(d_str)
                        st_g = "Ok"
                        if gap > 120: st_g = f"⚠️ Longo (+{int(gap-120)}m)"
                        tp = "Almoço (Inf.)" if h <= 15 else "Jantar (Inf.)"
                        almoco.append({"Agente": nome_agente, "Data": sa["out"].strftime("%d/%m"), "Duração": formatar_tempo_humano(gap), "Status": f"{tp} - {st_g}"})

    # Pontualidade
    prim_logs = {}
    for l in logs_ord:
        k = l["in"].strftime("%Y-%m-%d")
        if k not in prim_logs: prim_logs[k] = l["in"]
    for d, dt in prim_logs.items():
        if 1 < dt.minute <= 40: logins.append({"Agente": nome_agente, "Data": d, "Hora Entrada": dt.strftime("%H:%M:%S"), "Atraso": f"{dt.minute}m"})
        
    return curtas, almoco, logins, ranking

@st.cache_data(ttl=300)
def processar_dados_pausas_supervisor(token, d_ini, d_fim, mapa, setor_nome):
    c, a, l, r = [], [], [], []
    # Loop Sequencial para evitar MissingContext
    barra = st.progress(0, text="Auditando Pausas...")
    tot = len(mapa)
    for i, (cod, nome) in enumerate(mapa.items()):
        cc, aa, ll, rr = _processar_agente_pausas(token, cod, nome, d_ini, d_fim, setor_nome)
        c.extend(cc); a.extend(aa); l.extend(ll); r.extend(rr)
        barra.progress((i+1)/tot)
    barra.empty()
    return c, a, l, r

@st.cache_data(ttl=60)
def buscar_pre_pausas_detalhado(token, id_agente, data_ini, data_fim):
    # CORREÇÃO DA VARIÁVEL: todas_pre_pausas em vez de pausas
    todas_pre_pausas = []
    pg = 1
    while True:
        try:
            r = requests.get(f"{BASE_URL}/relPausasAgendadas", headers={"Authorization": f"Bearer {token}"}, 
                             params={"data_inicial": data_ini.strftime("%Y-%m-%d"), "data_final": data_fim.strftime("%Y-%m-%d"), "agente": id_agente, "page": pg, "limit": 100}, timeout=10)
            if r.status_code != 200: break
            rows = r.json().get("rows", [])
            if not rows: break
            todas_pre_pausas.extend(rows)
            pg += 1
        except: break
    return todas_pre_pausas

def processar_dados_pre_pausas_geral(token, data_ini, data_fim, mapa_agentes):
    resultados = {}
    # Loop Sequencial
    for cod, nome in mapa_agentes.items():
        raw_data = buscar_pre_pausas_detalhado(token, cod, data_ini, data_fim)
        if raw_data:
            lst = []
            for p in raw_data:
                try:
                    ini = datetime.strptime(p.get("data_pre", ""), "%Y-%m-%d %H:%M:%S").strftime("%d/%m %H:%M")
                    fim = datetime.strptime(p.get("data_fim", ""), "%Y-%m-%d %H:%M:%S").strftime("%d/%m %H:%M") if p.get("data_fim") else "-"
                    lst.append({"Início": ini, "Término": fim, "Duração": p.get("tempo_pre_pausado", "00:00:00"), "Motivo": p.get("pausa", "Agendada")})
                except: pass
            if lst: resultados[nome] = lst
    return resultados

@st.cache_data(ttl=300)
def processar_ranking_geral(token, d_ini, d_fim, mapa, contas=[ID_CONTA]):
    ids = list(mapa.keys())
    can = buscar_ids_canais(token)
    stats = {c: {"Vol": 0, "TMA": "--:--", "TME": "--:--", "TMIA": "--:--", "TMIC": "--:--"} for c in ids}
    
    # Volume e Tempos
    try:
        r = requests.get(f"{BASE_URL}/relAtEstatistico", headers={"Authorization": f"Bearer {token}"}, params={"data_inicial": f"{d_ini} 00:00:00", "data_final": f"{d_fim} 23:59:59", "agrupador": "agente", "agente[]": ids, "canal[]": can, "id_conta": ID_CONTA})
        if r.status_code == 200:
            for item in r.json():
                nome_api = str(item.get("agrupador", "")).upper()
                cod_match = next((c for c, n in mapa.items() if n == nome_api or n in nome_api), None)
                if cod_match:
                    qtd = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                    stats[cod_match]["Vol"] += qtd
                    stats[cod_match]["TMA"] = item.get("tma", "--:--")
                    stats[cod_match]["TME"] = item.get("tme", "--:--")
                    stats[cod_match]["TMIA"] = item.get("tmia", "--:--")
                    stats[cod_match]["TMIC"] = item.get("tmic", "--:--")
    except: pass

    # CSAT Sequencial
    rnk = []
    for cod in ids:
        p, t = 0, 0
        for ct in contas:
            for pi in PESQUISAS_IDS:
                pg = 1
                while True:
                    try:
                        req = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers={"Authorization": f"Bearer {token}"}, params={"data_inicial": d_ini, "data_final": d_fim, "pesquisa": pi, "id_conta": ct, "limit": 1000, "page": pg, "agente[]": [cod]})
                        if req.status_code!=200: break
                        d = req.json()
                        if not d or not isinstance(d, list): break
                        tot_k = 0
                        for b in d:
                            if str(b.get("id_pergunta","")) in IDS_PERGUNTAS_VALIDAS:
                                tot_k += sum(int(x.get("num_quantidade", 0)) for x in b["sintetico"])
                                for rs in b.get("respostas", []):
                                    v = float(rs.get("nom_valor", -1))
                                    if v >= 0: t += 1; 
                                    if v >= 8: p += 1
                        if pg*1000 >= tot_k: break
                        if len(d) < 2: break
                        pg += 1
                    except: break
        
        st_data = stats[cod]
        if st_data["Vol"] > 0 or t > 0:
            rnk.append({
                "Agente": mapa[cod],
                "Volume": st_data["Vol"],
                "TMA": st_data["TMA"],
                "TME": st_data["TME"],
                "TMIA": st_data["TMIA"],
                "TMIC": st_data["TMIC"],
                "CSAT Score": (p/t*100) if t>0 else 0.0,
                "CSAT Qtd": t
            })
            
    return rnk

def eleger_melhor_do_mes(df):
    if df.empty: return None
    df = df[df['Volume'] > 0].copy()
    if df.empty: return None
    df['S_TMA'] = df['TMA'].apply(time_str_to_seconds).rank()
    df['S_TMIA'] = df['TMIA'].apply(time_str_to_seconds).rank()
    df['S_CSAT'] = df['CSAT Score'].rank(ascending=False)
    df['Final'] = df['S_TMA'] + df['S_TMIA'] + df['S_CSAT']
    return df.sort_values('Final').iloc[0]['Agente']

def obter_ids_do_setor(mapa_completo, setor_nome):
    nomes_alvo = [n.strip().upper() for n in SETORES_AGENTES.get(setor_nome, [])]
    agentes_finais = {}
    for alvo in nomes_alvo:
        if alvo in mapa_completo:
            agentes_finais[mapa_completo[alvo]] = alvo
        else:
            for nome_real, cod in mapa_completo.items():
                if alvo in nome_real.split():
                    if alvo in NOMES_COMUNS_PRIMEIRO:
                         if alvo == nome_real.split()[0]:
                            agentes_finais[cod] = nome_real; break
                    else:
                        agentes_finais[cod] = nome_real; break
    return agentes_finais

def calcular_produtividade_meta_liquida(token, id_agente, nome_agente, data_ini, data_fim):
    headers = {"Authorization": f"Bearer {token}"}
    total_logado = 0
    dias_trabalhados = set()
    pg = 1
    while pg <= 10:
        try:
            r = requests.get(f"{BASE_URL}/relAgenteLogin", headers=headers, params={"data_inicial": data_ini.strftime("%Y-%m-%d"), "data_final": data_fim.strftime("%Y-%m-%d"), "agente": id_agente, "page": pg, "limit": 100})
            rows = r.json().get("rows", [])
            if not rows: break
            for row in rows:
                d_in = row.get("data_login")
                d_out = row.get("data_logout")
                if d_in:
                    dt_in = datetime.strptime(d_in, "%Y-%m-%d %H:%M:%S")
                    dias_trabalhados.add(dt_in.strftime("%Y-%m-%d"))
                    if not d_out: dt_out = datetime.now() if datetime.now().date() <= data_fim else dt_in
                    else: dt_out = datetime.strptime(d_out, "%Y-%m-%d %H:%M:%S")
                    total_logado += (dt_out - dt_in).total_seconds()
            if len(rows) < 100: break
            pg += 1
        except: break

    total_pausa = 0
    pp = 1
    while pp <= 10:
        try:
            r = requests.get(f"{BASE_URL}/relAgentePausa", headers=headers, params={"dat_inicial": data_ini.strftime("%Y-%m-%d"), "dat_final": data_fim.strftime("%Y-%m-%d"), "cod_agente": id_agente, "pagina": pp, "limit": 100})
            rows = r.json().get("rows", [])
            if not rows: break
            for row in rows:
                try: total_pausa += float(row.get("seg_pausado", 0))
                except: pass
            pp += 1
        except: break

    qtd_dias = len(dias_trabalhados)
    if qtd_dias == 0: return {"Agente": nome_agente, "Dias": 0, "Logado": "-", "Pausado": "-", "Líquido Real": "-", "Meta Líquida": "-", "Saldo": "-", "_sort": -1}
        
    meta_diaria_liquida = 7.5 * 3600 
    meta_total_liquida = qtd_dias * meta_diaria_liquida
    liquido_real = total_logado - total_pausa
    saldo = liquido_real - meta_total_liquida
    
    saldo_str = seconds_to_hms(abs(saldo))
    saldo_fmt = f"✅ +{saldo_str}" if saldo >= 0 else f"🔻 -{saldo_str}"

    return {
        "Agente": nome_agente,
        "Dias": qtd_dias,
        "Logado": seconds_to_hms(total_logado),
        "Pausado": seconds_to_hms(total_pausa),
        "Líquido Real": seconds_to_hms(liquido_real),
        "Meta Líquida": seconds_to_hms(meta_total_liquida),
        "Saldo": saldo_fmt,
        "_sort": liquido_real
    }

def processar_produtividade_geral(token, data_ini, data_fim, setor_sel):
    mapa_completo = mapear_todos_agentes(token)
    agentes_do_setor = obter_ids_do_setor(mapa_completo, setor_sel)
    resultados = []
    
    # FIX: Loop Sequencial
    bar = st.progress(0, text="Calculando Produtividade...")
    tot = len(agentes_do_setor)
    for i, (aid, anome) in enumerate(agentes_do_setor.items()):
        res = calcular_produtividade_meta_liquida(token, aid, anome, data_ini, data_fim)
        if res["_sort"] != -1: resultados.append(res)
        bar.progress((i+1)/tot)
    bar.empty()
    return resultados

# ==============================================================================
# 5.7 FUNÇÕES DE PLANTÃO E CLIENTE INTERNO (RESTAURADAS COM LÓGICA)
# ==============================================================================

def buscar_dados_plantao(token, data_ini, data_fim):
    """Lógica para o plantão: Filtra agentes da LISTA_PLANTAO e busca estatísticas."""
    headers = {"Authorization": f"Bearer {token}"}
    mapa = mapear_todos_agentes(token)
    ids_plantao = []
    mapa_plantao = {}
    
    for nome in LISTA_PLANTAO:
        nome_upper = nome.strip().upper()
        # Busca exata ou primeiro nome
        for nome_real, cod in mapa.items():
            if nome_upper in nome_real.split():
                if cod not in ids_plantao:
                    ids_plantao.append(cod)
                    mapa_plantao[cod] = nome_real
                break
    
    if not ids_plantao:
        return pd.DataFrame(), {}, 0.0

    # Busca CSAT
    res_csat = []
    pos = 0; tot = 0
    for pid in PESQUISAS_IDS:
        pg = 1
        while True:
            try:
                r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params={"data_inicial": data_ini, "data_final": data_fim, "pesquisa": pid, "id_conta": ID_CONTA, "limit": 1000, "page": pg, "agente[]": ids_plantao})
                if r.status_code != 200: break
                d = r.json()
                if not d or not isinstance(d, list): break
                t_k = 0
                for b in d:
                    if str(b.get("id_pergunta","")) in IDS_PERGUNTAS_VALIDAS:
                        t_k += sum(int(x.get("num_quantidade",0)) for x in b["sintetico"])
                        for rsp in b.get("respostas", []):
                            v = float(rsp.get("nom_valor", -1))
                            if v >= 0:
                                tot += 1; 
                                if v >= 8: pos += 1
                                res_csat.append({"Agente": rsp.get("nom_agente"), "Nota": v})
                if pg * 1000 >= t_k: break
                if len(d) < 2: break
                pg += 1
            except: break
            
    score = (pos/tot*100) if tot > 0 else 0.0
    
    # Busca Volume por Serviço (Estatístico)
    stats_serv = {}
    ids_can = buscar_ids_canais(token)
    try:
        r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params={"data_inicial": f"{data_ini} 00:00:00", "data_final": f"{data_fim} 23:59:59", "agrupador": "servico", "agente[]": ids_plantao, "canal[]": ids_can, "id_conta": ID_CONTA})
        if r.status_code == 200:
            for item in r.json():
                s = item.get("agrupador")
                stats_serv[s] = {
                    "num_qtd": int(item.get("num_qtd",0)) - int(item.get("num_qtd_abandonado",0)),
                    "tma": item.get("tma"), "tme": item.get("tme"), "tmia": item.get("tmia"), "tmic": item.get("tmic")
                }
    except: pass
    
    # Monta DF básico por agente
    stats_ag = []
    try:
        r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params={"data_inicial": f"{data_ini} 00:00:00", "data_final": f"{data_fim} 23:59:59", "agrupador": "agente", "agente[]": ids_plantao, "canal[]": ids_can, "id_conta": ID_CONTA})
        if r.status_code == 200:
            for item in r.json():
                ag_nm = item.get("agrupador")
                vol = int(item.get("num_qtd",0)) - int(item.get("num_qtd_abandonado",0))
                # Calcula CSAT individual simples
                p_ag = 0; t_ag = 0
                for c in res_csat:
                    if str(c["Agente"]).upper() == str(ag_nm).upper():
                        t_ag += 1
                        if c["Nota"] >= 8: p_ag += 1
                sc_ag = (p_ag/t_ag*100) if t_ag > 0 else 0.0
                
                stats_ag.append({"Agente": ag_nm, "Volume": vol, "TMA": item.get("tma"), "CSAT": sc_ag})
    except: pass
    
    return pd.DataFrame(stats_ag), stats_serv, score

def buscar_dados_cliente_interno(token, data_ini, data_fim, lista_nomes):
    """Lógica para Cliente Interno (Conta 5) filtrada por agentes de Suporte."""
    headers = {"Authorization": f"Bearer {token}"}
    mapa = mapear_todos_agentes(token)
    ids_suporte = []
    
    for nome in lista_nomes:
        nome_upper = nome.strip().upper()
        for nome_real, cod in mapa.items():
            if nome_upper in nome_real.split():
                if cod not in ids_suporte: ids_suporte.append(cod)
                break
                
    if not ids_suporte:
        return {"TMA": "--", "TME": "--", "TMIA": "--"}, 0.0, 0, pd.DataFrame()

    ids_can = buscar_ids_canais(token) # Canais globais funcionam para conta 5 também
    
    # 1. Estatísticas Globais da Conta 5 (Filtradas pelos agentes)
    stats = {"TMA": "--", "TME": "--", "TMIA": "--"}
    try:
        r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params={"data_inicial": f"{data_ini} 00:00:00", "data_final": f"{data_fim} 23:59:59", "agrupador": "conta", "agente[]": ids_suporte, "canal[]": ids_can, "id_conta": ID_CONTA_CLIENTE_INTERNO})
        if r.status_code == 200:
            l = r.json()
            if l:
                i = l[0]
                stats = {"TMA": i.get("tma"), "TME": i.get("tme"), "TMIA": i.get("tmia")}
    except: pass
    
    # 2. CSAT e Lista Analítica
    lista_analitica = []
    pos = 0; tot = 0
    
    for pid in PESQUISAS_IDS:
        pg = 1
        while True:
            try:
                r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params={"data_inicial": data_ini, "data_final": data_fim, "pesquisa": pid, "id_conta": ID_CONTA_CLIENTE_INTERNO, "limit": 1000, "page": pg, "agente[]": ids_suporte})
                if r.status_code != 200: break
                d = r.json()
                if not d or not isinstance(d, list): break
                t_k = 0
                for b in d:
                    if str(b.get("id_pergunta","")) in IDS_PERGUNTAS_VALIDAS:
                        t_k += sum(int(x.get("num_quantidade",0)) for x in b["sintetico"])
                        for rsp in b.get("respostas", []):
                            v = float(rsp.get("nom_valor", -1))
                            if v >= 0:
                                tot += 1
                                if v >= 8: pos += 1
                                lista_analitica.append({
                                    "Data": rsp.get("dat_resposta"),
                                    "Agente": rsp.get("nom_agente"),
                                    "Cliente": rsp.get("nom_contato"),
                                    "Nota": v,
                                    "Comentario": rsp.get("nom_resposta"),
                                    "Protocolo": rsp.get("num_protocolo")
                                })
                if pg * 1000 >= t_k: break
                if len(d) < 2: break
                pg += 1
            except: break
            
    score = (pos/tot*100) if tot > 0 else 0.0
    
    # CRITICAL FIX: Ensure 'Nota_Num' column exists for filtering later
    df_ret = pd.DataFrame(lista_analitica)
    if not df_ret.empty:
        df_ret['Nota_Num'] = pd.to_numeric(df_ret['Nota'], errors='coerce').fillna(0).astype(int)
    else:
        df_ret['Nota_Num'] = 0

    return stats, score, tot, df_ret

# ==============================================================================
# 5.8 COMPONENTES VISUAIS (VISUAL)
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
            val_str = f"{item[metrica]:.2f}%" if formato == "%" else str(item[metrica])
            st.markdown(f"""
            <div class="podium-card">
                <div class="podium-pos">{emojis[i]}</div>
                <div class="podium-name">{item['Agente']}</div>
                <div class="podium-val">{val_str}</div>
            </div>
            """, unsafe_allow_html=True)

def render_kpi_card(titulo, valor, subtitulo, cor_borda="#6366f1"):
    st.markdown(f"""
    <div class="kpi-card" style="border-left: 4px solid {cor_borda};">
        <div class="kpi-title">{titulo}</div>
        <div class="kpi-value">{valor}</div>
        <div class="kpi-sub">{subtitulo}</div>
    </div>
    """, unsafe_allow_html=True)

def render_link_card(titulo, url, icon="🚀", cor_borda="#ec4899"):
    st.markdown(f"""
    <a href="{url}" target="_blank" style="text-decoration: none;">
        <div class="kpi-card" style="border-left: 4px solid {cor_borda}; text-align:center;">
            <div class="kpi-title">{titulo}</div>
            <div style="font-size: 1.5rem; color: #f3f4f6; font-weight: 700;">{icon}</div>
            <div class="kpi-sub">Clique para abrir</div>
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
        with st.expander("📜 Versão Platinum 16.10"):
            st.markdown("""**v16.10 - Visual Refix**\n- Plantão Cards restaurados.\n- Detratores Ranking OK.\n- Produtividade Líquida.""")
        if st.button("🚪 Sair", use_container_width=True):
            st.session_state.auth_status = False; st.session_state.user_data = None; st.rerun()
        return d_ini, d_fim

# ==============================================================================
# 7. EXECUÇÃO PRINCIPAL
# ==============================================================================

if "auth_status" not in st.session_state:
    st.session_state.auth_status = False
    st.session_state.user_data = None
    st.session_state.user_role = None
    st.session_state.user_setor = "NRC"

if not st.session_state.auth_status:
    st.markdown("<br><br>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1.5, 1])
    with c2:
        st.title("🔐 Acesso ao Portal")
        with st.form("login_form"):
            usuario = st.text_input("E-mail ou Login")
            senha = st.text_input("Senha", type="password")
            submitted = st.form_submit_button("Entrar", use_container_width=True)
            if submitted:
                if usuario == SUPERVISOR_LOGIN and senha == SUPERVISOR_PASS:
                    st.session_state.auth_status = True; st.session_state.user_role = "supervisor"; st.session_state.user_setor = "NRC"; st.session_state.user_data = {"nome": "Sup NRC", "id": "SUP"}
                    st.rerun()
                elif usuario == SUPERVISOR_NEGOCIACAO_LOGIN and senha == SUPERVISOR_NEGOCIACAO_PASS:
                    st.session_state.auth_status = True; st.session_state.user_role = "supervisor"; st.session_state.user_setor = "NEGOCIACAO"; st.session_state.user_data = {"nome": "Sup Negociação", "id": "SUP_NEG"}
                    st.rerun()
                elif usuario == SUPERVISOR_SUPORTE_LOGIN and senha == SUPERVISOR_SUPORTE_PASS:
                    st.session_state.auth_status = True; st.session_state.user_role = "supervisor"; st.session_state.user_setor = "SUPORTE"; st.session_state.user_data = {"nome": "Sup Suporte", "id": "SUP_SUP"}
                    st.rerun()
                elif usuario == SUPERVISOR_CANCELAMENTO_LOGIN and senha == SUPERVISOR_CANCELAMENTO_PASS:
                    st.session_state.auth_status = True; st.session_state.user_role = "supervisor"; st.session_state.user_setor = "CANCELAMENTO"; st.session_state.user_data = {"nome": "Sup Cancelamento", "id": "SUP_CANC"}
                    st.rerun()
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
    setor_atual = st.session_state.get("user_setor", "NRC")
    
    modo_visao_supervisor = "Geral"
    id_alvo = None; nome_alvo = None

    if st.session_state.user_role == "supervisor":
        with st.spinner("Carregando equipe..."):
            if setor_atual == "NRC":
                _, _, _, mapa_agentes_sidebar, _ = buscar_dados_completos_supervisor(token, d_inicial, d_final)
            elif setor_atual == "NEGOCIACAO":
                mapa_completo = mapear_todos_agentes(token)
                nomes_alvo = [x.strip().upper() for x in SETORES_AGENTES["NEGOCIACAO"]]
                mapa_agentes_sidebar = {}
                for nome in nomes_alvo:
                    if nome in mapa_completo: mapa_agentes_sidebar[mapa_completo[nome]] = nome
                    else: 
                        for n, c in mapa_completo.items():
                            if nome in n.split():
                                if nome in NOMES_COMUNS_PRIMEIRO and nome == n.split()[0]: mapa_agentes_sidebar[c] = n; break
                                else: mapa_agentes_sidebar[c] = n; break
            else:
                _, _, _, mapa_agentes_sidebar, _ = buscar_dados_supervisor_multisetor(token, d_inicial, d_final, setor_atual)
        
        st.sidebar.markdown("---")
        lista_nomes = ["Visão Geral"] + sorted(list(mapa_agentes_sidebar.values()))
        opcao_agente = st.sidebar.selectbox("Selecionar Agente", lista_nomes)
        if opcao_agente != "Visão Geral":
            modo_visao_supervisor = "Individual"
            for cod, nome in mapa_agentes_sidebar.items():
                if nome == opcao_agente: id_alvo = cod; nome_alvo = nome; break
    
    # --- PAINEL INDIVIDUAL (AGENTE OU ESPIÃO) ---
    if st.session_state.user_role == "agente" or (st.session_state.user_role == "supervisor" and modo_visao_supervisor == "Individual"):
        if st.session_state.user_role == "agente":
            target_id = st.session_state.user_data['id']; target_name = st.session_state.user_data['nome']
        else:
            target_id = id_alvo; target_name = nome_alvo
            st.warning(f"👁️‍🗨️ MODO ESPIÃO: {target_name}")

        abas = st.tabs(["📊 Visão Geral", "⏸️ Pausas", "⭐ Qualidade", "🆘 Suporte"])
        with abas[0]:
            if token:
                dt_obj, texto_login, df_logins = buscar_historico_login(token, target_id, d_inicial, d_final)
                stats = buscar_estatisticas_agente(token, target_id, d_inicial, d_final)
                csat, csat_qtd, df_csat = buscar_csat_nrc(token, target_id, d_inicial, d_final)
                
                c1, c2, c3 = st.columns(3)
                with c1: 
                    cor = "#10b981" if dt_obj and dt_obj.date() == datetime.now().date() else "#3b82f6"
                    sub = f"Data: {dt_obj.strftime('%d/%m')}" if dt_obj else "Sem registro"
                    render_kpi_card("Primeiro Login", texto_login, sub, cor)
                with c2: render_kpi_card("Volume Total", str(stats.get('num_qtd', '0') if stats else '0'), "Atendimentos Finalizados", "#8b5cf6")
                with c3:
                    cor_csat = "#10b981" if csat >= 85 else "#f59e0b"
                    render_kpi_card("CSAT (Qualidade)", f"{csat:.2f}%", f"Base: {csat_qtd} avaliações", cor_csat)
                
                c4, c5, c6 = st.columns(3)
                with c4: render_kpi_card("T.M.A", str(stats.get('tma', '--:--') if stats else '--:--'), "Tempo Médio Atendimento", "#f59e0b")
                with c5: render_kpi_card("T.M.I.A", str(stats.get('tmia', '--:--') if stats else '--:--'), "Inatividade Agente", "#10b981")
                with c6: render_kpi_card("T.M.I.C", str(stats.get('tmic', '--:--') if stats else '--:--'), "Inatividade Cliente", "#3b82f6")
                
                st.markdown("---")
                c7, c8, c9 = st.columns(3)
                with c7: render_kpi_card("T.M.E", str(stats.get('tme', '--:--') if stats else '--:--'), "Tempo Médio Espera", "#ef4444")
                st.markdown("---")
                
                if not df_logins.empty:
                    with st.expander("📅 Ver Histórico de Entradas Detalhado", expanded=False): st.dataframe(df_logins, use_container_width=True, hide_index=True)

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
                cor_csat = "#10b981" if csat >= 85 else "#f59e0b"
                render_kpi_card("Seu CSAT no Período", f"{csat:.2f}%", f"{csat_qtd} avaliações", cor_csat)
                st.markdown("---")
                st.markdown("#### 📋 Histórico de Avaliações")
                df_csat['Acesso'] = df_csat['Protocolo'].apply(gerar_link_protocolo)
                st.dataframe(df_csat[['Data', 'Cliente', 'Nota', 'Comentario', 'Acesso']], column_config={"Acesso": st.column_config.LinkColumn("Link", display_text="Abrir Atendimento"), "Nota": st.column_config.NumberColumn("Nota", format="%d ⭐")}, use_container_width=True, hide_index=True)
                
                # FIXED: Nota_Num column is guaranteed now
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

    # --- PAINEL GERAL SUPERVISOR ---
    elif st.session_state.user_role == "supervisor" and modo_visao_supervisor == "Geral":
        st.markdown(f"## 🏢 Painel de Gestão - Setor {setor_atual}")
        lista_abas = ["👁️ Visão Geral", "🏆 Rankings", "⏸️ Pausas", "⚡ Tempo Real", "🆘 Solicitações", "🚀 Produtividade (Em Andamento)"]
        if setor_atual == "SUPORTE": lista_abas.extend(["🌙 Plantão", "🏢 Cliente Interno"])
        elif setor_atual == "NEGOCIACAO": lista_abas.extend(["📊 Auditoria"])
        
        abas_sup = st.tabs(lista_abas)
        
        # 1. Visão Geral
        with abas_sup[0]:
            if token:
                with st.spinner("Sincronizando estatísticas..."):
                    if setor_atual == "NRC":
                        dados_servicos, csat_geral, base_geral, mapa_agentes, dados_globais = buscar_dados_completos_supervisor(token, d_inicial, d_final)
                        lista_servicos_exibir = SERVICOS_ALVO
                    elif setor_atual == "NEGOCIACAO":
                        dados_servicos, csat_geral, base_geral, dados_globais = buscar_dados_negociacao_multiconta(token, d_inicial, d_final)
                        _, _, _, mapa_agentes, _ = buscar_dados_supervisor_multisetor(token, d_inicial, d_final, setor_atual)
                        lista_servicos_exibir = SETORES_SERVICOS.get(setor_atual, [])
                    else:
                        dados_servicos, csat_geral, base_geral, mapa_agentes, dados_globais = buscar_dados_supervisor_multisetor(token, d_inicial, d_final, setor_atual)
                        lista_servicos_exibir = SETORES_SERVICOS.get(setor_atual, [])

                    st.markdown(f"#### ⭐ Visão Global da Equipe ({setor_atual})")
                    col_kpi1, col_kpi2 = st.columns([1, 2])
                    with col_kpi1:
                        cor_geral = "#10b981" if csat_geral >= 85 else ("#f59e0b" if csat_geral >= 75 else "#ef4444")
                        render_kpi_card("CSAT Global (Setor)", f"{csat_geral:.2f}%", f"Base Total: {base_geral}", cor_geral)
                    with col_kpi2:
                        if setor_atual == "NRC": render_link_card("Ferramenta Externa", "https://fideliza-nator-live.streamlit.app/", "FIDELIZA-NATOR")
                        elif setor_atual == "CANCELAMENTO": render_link_card("Acesso Rápido", "https://docs.google.com/spreadsheets/d/1y-7_w8RuzE2SSWatbdZj0SjsIa-aJyZCV0_1OxwD7bs/edit?gid=0#gid=0", "CLIENTE CRITICO", cor_borda="#ef4444")

                    st.markdown("<br>", unsafe_allow_html=True)
                    c_g1, c_g2, c_g3, c_g4 = st.columns(4)
                    with c_g1: render_kpi_card("T.M.A (Global)", dados_globais["tma"], "Tempo Médio Atend.", "#3b82f6")
                    with c_g2: render_kpi_card("T.M.E (Global)", dados_globais["tme"], "Tempo Médio Esp.", "#ef4444")
                    with c_g3: render_kpi_card("T.M.I.A (Global)", dados_globais["tmia"], "Inativ. Agente", "#f59e0b")
                    with c_g4: render_kpi_card("T.M.I.C (Global)", dados_globais["tmic"], "Inativ. Cliente", "#6366f1")

                    st.markdown("---")
                    for servico in lista_servicos_exibir:
                        dado = dados_servicos.get(servico, {})
                        st.markdown(f"<div class='service-header'>{servico}</div>", unsafe_allow_html=True)
                        total_s = dado.get("csat_total", 0); pos_s = dado.get("csat_pos", 0)
                        score_s = (pos_s / total_s * 100) if total_s > 0 else 0.0
                        col1, col2, col3, col4, col5, col6 = st.columns(6)
                        with col1: render_kpi_card("Volume", str(dado.get("num_qtd", 0)), "Atendimentos", "#8b5cf6")
                        cor_s = "#10b981" if score_s >= 85 else ("#f59e0b" if score_s >= 75 else "#ef4444")
                        with col2: render_kpi_card("Satisfação", f"{score_s:.2f}%", f"Base: {total_s}", cor_s)
                        with col3: render_kpi_card("T.M.A", str(dado.get("tma", "--:--")), "Tempo Médio", "#3b82f6")
                        with col4: render_kpi_card("T.M.E", str(dado.get("tme", "--:--")), "Fila/Espera", "#ef4444")
                        with col5: render_kpi_card("T.M.I.A", str(dado.get("tmia", "--:--")), "Inatividade Agt", "#f59e0b")
                        with col6: render_kpi_card("T.M.I.C", str(dado.get("tmic", "--:--")), "Inatividade Cli", "#6366f1")
        
        # ABA 2: RANKINGS
        with abas_sup[1]:
            if token and 'mapa_agentes_sidebar' in locals():
                with st.spinner("Calculando o MVP do Mês..."):
                    contas_rank = CONTAS_NEGOCIACAO if setor_atual == "NEGOCIACAO" else [ID_CONTA]
                    lista_rank = processar_ranking_geral(token, d_inicial, d_final, mapa_agentes_sidebar, contas_rank)
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
                        st.dataframe(df_display, column_config={"CSAT Score": st.column_config.NumberColumn("CSAT Score", format="%.2f%%")}, use_container_width=True, hide_index=True)
                        
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
            if token and 'mapa_agentes_sidebar' in locals():
                lista_curtas, lista_almoco, lista_logins, lista_ranking = processar_dados_pausas_supervisor(token, d_inicial, d_final, mapa_agentes_sidebar, setor_atual)
                c_p1, c_p2 = st.columns(2)
                with c_p1:
                    st.subheader("1. 🚨 Risco de Estouro (Manhã/Tarde)")
                    if lista_curtas:
                        df_c = pd.DataFrame(lista_curtas).sort_values(by="Valor Num", ascending=False)
                        st.dataframe(df_c[['Agente', 'Excesso Acumulado', 'Status']], use_container_width=True, hide_index=True)
                    else: st.success("Ninguém estourou!")
                with c_p2:
                    st.subheader("2. 🍽️ Almoço/Jantar (Detecção)")
                    if lista_almoco:
                        st.dataframe(pd.DataFrame(lista_almoco), use_container_width=True, hide_index=True)
                    else: st.info("Nenhum intervalo detectado ou não aplicável ao setor.")
                st.markdown("---")
                c_p3, c_p4 = st.columns(2)
                with c_p3:
                    st.subheader("3. ⏰ Pontualidade (Logins)")
                    if lista_logins:
                        st.dataframe(pd.DataFrame(lista_logins).sort_values(by="Data", ascending=False), use_container_width=True, hide_index=True)
                    else: st.success("Todos pontuais!")
                with c_p4:
                    st.subheader("4. 🏆 Ranking Pausas (Qtd)")
                    if lista_ranking:
                        st.dataframe(pd.DataFrame(lista_ranking).sort_values(by="Qtd Pausas", ascending=False), use_container_width=True, hide_index=True)
                st.markdown("---")
                st.subheader("5. ⏳ Monitoramento de Pré-Pausas (Agendadas)")
                with st.spinner("Buscando pré-pausas..."):
                    mapa_pre = processar_dados_pre_pausas_geral(token, d_inicial, d_final, mapa_agentes_sidebar)
                    if mapa_pre:
                        agentes_ord = sorted(mapa_pre.keys(), key=lambda x: len(mapa_pre[x]), reverse=True)
                        for ag in agentes_ord:
                            qtd = len(mapa_pre[ag])
                            lista_p = mapa_pre[ag]
                            with st.expander(f"➕ {ag} ({qtd} pré-pausas)"):
                                df_pre = pd.DataFrame(lista_p)
                                st.dataframe(df_pre, use_container_width=True, hide_index=True)
                    else: st.info("Nenhuma pré-pausa registrada no período.")
            else: st.info("Aguarde o carregamento da Visão Geral.")
            
        # ABA 4: TEMPO REAL
        with abas_sup[3]:
            if token:
                if st.button("🔄 Atualizar Lista Online"): st.rerun()
                if setor_atual == "NRC": lista_online = buscar_agentes_online_filtrado_nrc(token)
                else: lista_online = buscar_agentes_online_filtrado_setor(token, setor_atual)
                if lista_online:
                    st.markdown(f"### 🟢 {len(lista_online)} Agentes Online ({setor_atual})")
                    for agente_online in lista_online:
                        aid = agente_online.get("cod")
                        nome_online = agente_online.get("nom_agente", "Desconhecido")
                        status_online = agente_online.get("status", "Online")
                        tempo_online = agente_online.get("tempo_status", "--:--")
                        cor_status_online = "#10b981" if "Pausa" not in status_online else "#f59e0b"
                        st.markdown(f"""<div class="realtime-card"><div style="flex:1;"><div style="font-weight:bold; color:white; font-size:1.1rem;">{nome_online}</div><div style="color:#9ca3af; font-size:0.8rem;">ID: {aid}</div></div><div style="flex:1; text-align:center;"><span style="background-color:{cor_status_online}; color:black; padding:2px 10px; border-radius:12px; font-weight:bold; font-size:0.8rem;">{status_online}</span><div style="margin-top:5px; color:#e5e7eb; font-family:monospace;">⏱ {tempo_online}</div></div><div style="flex:1; text-align:right;"></div></div>""", unsafe_allow_html=True)
                        col_btn = st.columns([4, 1])[1]
                        with col_btn:
                            if st.button("🔴 Deslogar", key=f"btn_logout_{aid}"):
                                with st.spinner(f"Deslogando {nome_online}..."):
                                    sucesso_logout, msg_logout = forcar_logout(token, aid)
                                    if sucesso_logout: st.success(f"{nome_online} deslogado!"); time.sleep(1); st.rerun()
                                    else: st.error(msg_logout)
                else: st.warning(f"Nenhum agente da equipe {setor_atual} está online no momento.")
            else: st.error("Erro de conexão.")

        # ABA 5: SOLICITAÇÕES
        with abas_sup[4]:
            st.info("Visualização das solicitações registradas no Google Sheets.")
            df_gsheets = ler_solicitacoes_gsheets()
            if not df_gsheets.empty: st.dataframe(df_gsheets, use_container_width=True)
            else: st.warning("Nenhuma solicitação encontrada na planilha.")

        # 6. NOVA ABA: PRODUTIVIDADE
        with abas_sup[5]:
            if token:
                with st.spinner("Calculando produtividade líquida..."):
                    lista_prod = processar_produtividade_geral(token, d_inicial, d_final, setor_atual)
                    st.subheader(f"🚀 Produtividade Líquida (Meta 07h30/dia)")
                    st.markdown("Cálculo: **(Dias Trabalhados × 7.5h) - (Tempo Logado - Tempo Pausado)**")
                    if lista_prod:
                        df_prod = pd.DataFrame(lista_prod).sort_values("_sort", ascending=False)
                        st.dataframe(df_prod.drop(columns=["_sort"]), column_config={"Líquido Real": st.column_config.TextColumn("Realizado", help="Logado - Pausas"), "Saldo": st.column_config.TextColumn("Saldo", help="Diferença para a Meta")}, use_container_width=True, hide_index=True)
                    else: st.warning("Sem dados de produtividade para o período.")

        # ABA 6: PLANTÃO (ESPECÍFICO SUPORTE)
        if setor_atual == "SUPORTE":
            with abas_sup[6]:
                if token:
                    with st.spinner("Carregando dados do Plantão..."):
                        df_plantao, stats_servico_plantao, _ = buscar_dados_plantao(token, d_inicial, d_final)
                        st.markdown("### 🌙 Equipe de Plantão (Madrugada)")
                        if not df_plantao.empty:
                            st.dataframe(df_plantao, column_config={"CSAT": st.column_config.NumberColumn("CSAT (%)", format="%.2f%%")}, use_container_width=True, hide_index=True)
                            total_vol_plantao = df_plantao['Volume'].sum()
                            st.markdown(f"**Volume Total Plantão:** {total_vol_plantao} atendimentos")
                            st.markdown("---")
                            st.markdown("#### 📊 Métricas por Setor (Plantão)")
                            if len(stats_servico_plantao) > 0:
                                cols_plantao = st.columns(len(stats_servico_plantao))
                                for i, (servico, dados) in enumerate(stats_servico_plantao.items()):
                                    with cols_plantao[i % len(cols_plantao)]:
                                        # VISUAL FIX: Usando render_kpi_card
                                        render_kpi_card(servico, str(dados["num_qtd"]), f"TMA: {dados['tma']} | TME: {dados['tme']}", "#8b5cf6")
                        else: st.warning("Sem dados para a equipe de plantão neste período.")
            
            with abas_sup[7]:
                if token:
                    nomes_suporte = SETORES_AGENTES["SUPORTE"]
                    with st.spinner("Conectando à Conta 5 (Cliente Interno)..."):
                        stats_ci, score_ci, total_ci, df_ci = buscar_dados_cliente_interno(token, d_inicial, d_final, nomes_suporte)
                        st.markdown("### 🏢 Cliente Interno (Conta 5)")
                        c1, c2, c3, c4 = st.columns(4)
                        cor_ci = "#10b981" if score_ci >= 85 else "#f59e0b"
                        with c1: render_kpi_card("CSAT Interno", f"{score_ci:.2f}%", f"Base: {total_ci}", cor_ci)
                        with c2: render_kpi_card("T.M.A", stats_ci["TMA"], "Tempo Médio", "#3b82f6")
                        with c3: render_kpi_card("T.M.E", stats_ci["TME"], "Espera", "#ef4444")
                        with c4: render_kpi_card("T.M.I.A", stats_ci["TMIA"], "Inatividade", "#f59e0b")
                        st.markdown("---")
                        if not df_ci.empty:
                            st.markdown("#### 📋 Histórico de Chamados Internos (Filtrado: Equipe Suporte)")
                            df_ci['Acesso'] = df_ci['Protocolo'].apply(gerar_link_protocolo)
                            st.dataframe(df_ci[['Data', 'Agente', 'Cliente', 'Nota', 'Comentario', 'Acesso']], column_config={"Acesso": st.column_config.LinkColumn("Link", display_text="Abrir"), "Nota": st.column_config.NumberColumn("Nota", format="%d ⭐")}, use_container_width=True, hide_index=True)
                            
                            # Fixed Key Error on Nota Num
                            detratores_ci = df_ci[df_ci['Nota_Num'] < 7].copy() if 'Nota_Num' in df_ci.columns else pd.DataFrame()
                            st.markdown("<br>", unsafe_allow_html=True)
                            with st.expander(f"🔻 Detratores Internos ({len(detratores_ci)})", expanded=True):
                                if not detratores_ci.empty: st.dataframe(detratores_ci[['Data', 'Agente', 'Cliente', 'Nota', 'Comentario', 'Acesso']], column_config={"Acesso": st.column_config.LinkColumn("Link", display_text="Verificar"), "Nota": st.column_config.NumberColumn("Nota", format="%d 🔴")}, use_container_width=True, hide_index=True)
                                else: st.success("Nenhum detrator interno no período!")
                        else: st.info("Nenhum chamado de cliente interno atendido pela equipe de Suporte no período.")

        # ABA 9: AUDITORIA (ESPECÍFICO NEGOCIAÇÃO)
        if setor_atual == "NEGOCIACAO":
            with abas_sup[6]:
                if token:
                    with st.spinner("Auditando volumetria por origem (Contas 1 e 14)..."):
                        lista_agentes_neg = SETORES_AGENTES["NEGOCIACAO"]
                        dados_auditoria = buscar_auditoria_volumetria(token, d_inicial, d_final, lista_agentes_neg)
                        st.markdown("### 📊 Auditoria de Volumetria (Ativo vs Passivo)")
                        st.caption("ℹ️ Nota: A contagem abaixo exclui atendimentos do Canal Voz e soma os dados das Contas 1 e 14.")
                        if dados_auditoria:
                            dados_auditoria.sort(key=lambda x: x["Total"], reverse=True)
                            for item in dados_auditoria:
                                nome = item["Agente"]
                                total = item["Total"]
                                ativo = item["Ativo"]; passivo = item["Passivo"]
                                perc_ativo = (ativo/total*100) if total > 0 else 0
                                perc_passivo = (passivo/total*100) if total > 0 else 0
                                with st.expander(f"👤 {nome} - Volume Total: {total}"):
                                    c_a1, c_a2 = st.columns(2)
                                    with c_a1: st.metric("Negociação ATIVA", ativo, f"{perc_ativo:.1f}%")
                                    with c_a2: st.metric("Negociação PASSIVA", passivo, f"{perc_passivo:.1f}%")
                        else: st.info("Sem dados de volumetria para auditoria.")
