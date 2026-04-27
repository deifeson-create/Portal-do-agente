import streamlit as st
import pandas as pd
import requests
import os
import concurrent.futures
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from collections import defaultdict

# ==============================================================================
# 1. CONFIGURAÇÃO VISUAL E VARIÁVEIS GLOBAIS
# ==============================================================================
st.set_page_config(
    layout="wide",
    page_title="Portal do Agente NRC",
    page_icon="🎧",
    initial_sidebar_state="expanded"
)

# 🔒 BLOQUEIO DE SEGURANÇA (SENHA MESTRA)
if "app_unlocked" not in st.session_state:
    st.session_state.app_unlocked = False

def check_master_password():
    """Verifica a senha mestra antes de carregar o app."""
    if st.session_state.app_unlocked:
        return

    st.markdown("<br><br><br>", unsafe_allow_html=True)
    coluna_esq, coluna_centro, coluna_dir = st.columns([1, 1, 1])
    with coluna_centro:
        st.markdown("<h3 style='text-align: center;'>🔒 Acesso Restrito</h3>", unsafe_allow_html=True)
        senha_digitada = st.text_input("Senha do Sistema", type="password", key="master_pwd")
        
        if st.button("Liberar Acesso", use_container_width=True):
            try:
                # Verifica no Secrets
                if senha_digitada == st.secrets["security"]["MASTER_PASSWORD"]:
                    st.session_state.app_unlocked = True
                    st.rerun()
                else:
                    st.error("Senha incorreta.")
            except:
                st.error("Erro: Configure [security] MASTER_PASSWORD no Secrets.")
    
    # PARA A EXECUÇÃO AQUI SE NÃO ESTIVER DESBLOQUEADO
    st.stop()

# Executa o bloqueio
check_master_password()

# ESTILOS CSS (ORIGINAIS RESTAURADOS - SEM ABREVIAÇÕES)
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
    
    /* Cards KPI (Visual Original Restaurado) */
    .kpi-card {
        background: linear-gradient(145deg, #1f2937, #111827); 
        border: 1px solid #374151; 
        border-radius: 12px; 
        padding: 15px; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.2);
        min-height: 110px;
        margin-bottom: 10px;
    }
    .kpi-title { font-size: 0.75rem; color: #9ca3af; text-transform: uppercase; font-weight: 600; margin-bottom: 5px; }
    .kpi-value { font-size: 1.4rem; color: #f3f4f6; font-weight: 700; }
    .kpi-sub { font-size: 0.7rem; color: #6b7280; margin-top: 4px; }

    /* Pódio e MVP */
    .podium-card { background: linear-gradient(145deg, #1f2937, #111827); border: 1px solid #374151; border-radius: 10px; padding: 15px; text-align: center; margin-bottom: 10px; }
    .podium-pos { font-size: 2rem; margin-bottom: 5px; }
    .podium-name { font-weight: 700; color: #f3f4f6; font-size: 1.1rem; }
    .podium-val { color: #9ca3af; font-size: 0.9rem; margin-top: 5px; }
    .mvp-card { background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%); border-radius: 15px; padding: 20px; text-align: center; color: white; box-shadow: 0 10px 15px -3px rgba(99, 102, 241, 0.4); margin-bottom: 25px; border: 1px solid #818cf8; }
    
    /* Tempo Real */
    .realtime-card { background-color: #1f2937; padding: 15px; border-radius: 10px; margin-bottom: 10px; border: 1px solid #374151; display: flex; align-items: center; justify-content: space-between; }
    
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
    
    SUPERVISOR_CANCELAMENTO_LOGIN = st.secrets["auth"].get("SUPERVISOR_CANCELAMENTO_LOGIN", "admin_cancel")
    SUPERVISOR_CANCELAMENTO_PASS = st.secrets["auth"].get("SUPERVISOR_CANCELAMENTO_PASS", "senha_cancel")
    
    SUPERVISOR_SUPORTE_LOGIN = st.secrets["auth"].get("SUPERVISOR_SUPORTE_LOGIN", "admin_sup")
    SUPERVISOR_SUPORTE_PASS = st.secrets["auth"].get("SUPERVISOR_SUPORTE_PASS", "senha_sup")

    SUPERVISOR_NEGOCIACAO_LOGIN = st.secrets["auth"].get("SUPERVISOR_NEGOCIACAO_LOGIN", "admin_neg")
    SUPERVISOR_NEGOCIACAO_PASS = st.secrets["auth"].get("SUPERVISOR_NEGOCIACAO_PASS", "senha_neg")

    MASTER_LOGIN = st.secrets["auth"].get("MASTER_LOGIN", "master_admin")
    MASTER_PASS = st.secrets["auth"].get("MASTER_PASS", "master_senha")
    
    PESQUISAS_IDS = st.secrets["ids"]["PESQUISAS_IDS"]
    IDS_PERGUNTAS_VALIDAS = st.secrets["ids"]["IDS_PERGUNTAS_VALIDAS"]
except Exception as erro:
    st.error(f"⚠️ Erro crítico: Não foi possível carregar os Segredos (Secrets). Verifique a configuração no Streamlit Cloud. Detalhe: {erro}")
    st.stop()

# Filtros Técnicos Fixos
CANAIS_ALVO = ['appchat', 'chat', 'botmessenger', 'instagram', 'whatsapp']

# SERVIÇOS MONITORADOS (ORIGINAL NRC)
SERVICOS_ALVO = ['COMERCIAL', 'FINANCEIRO', 'NOVOS CLIENTES', 'LIBERAÇÃO']

# LISTA NRC (OFICIAL - ORIGINAL)
LISTA_NRC = [
    'RILDYVAN', 'MILENA', 'ALVES', 'MONICKE', 'AYLA', 'MARIANY', 'EDUARDA', 
    'MENEZES', 'JUCIENNY', 'MARIA', 'ANDREZA', 'LUZILENE', 'IGO', 'AIDA', 
    'Caribé', 'Michelly', 'ADRIA', 'ERICA', 'HENRIQUE', 'SHYRLEI', 
    'ANNA', 'JULIA', 'FERNANDES'
]
NOMES_COMUNS_PRIMEIRO = ['MARIA', 'ANNA', 'JULIA', 'ERICA']

# ==============================================================================
# 1.1 CONFIGURAÇÃO DE NOVOS SETORES (ATUALIZADO COM JOVENS APRENDIZES)
# ==============================================================================
JOVENS_APRENDIZES_SUPORTE = ['GAELL', 'LUYLLA', 'SHANNA', 'RUAN JA']
JOVENS_APRENDIZES_NRC = ['CICERA', 'RIBEIRO', 'SILVA', 'NUNES']

SETORES_AGENTES = {
    "NRC": LISTA_NRC + JOVENS_APRENDIZES_NRC, 
    "CANCELAMENTO": ['BARBOSA', 'ELOISA', 'LARISSA', 'EDUARDO', 'CAMILA', 'SAMARA'],
    "NEGOCIACAO": ['Carla', 'Lenk', 'Ana Luiza', 'JULIETTI', 'RODRIGO', 'Monalisa', 'Ramom', 'Ednael', 'Leticia', 'Rita', 'Mariana', 'Flavia s', 'Uri', 'Clara', 'Wanderson', 'Aparecida', 'Cristina', 'Caio', 'LUKAS'],
    "SUPORTE": ['VALERIO', 'TARCISIO', 'GRANJA', 'ALICE', 'FERNANDO', 'SANTOS', 'RENAN', 'FERREIRA', 'HUEMILLY', 'LOPES', 'LAUDEMILSON', 'RAYANE', 'LAYS', 'JORGE', 'LIGIA', 'ALESSANDRO', 'GEIBSON', 'ROBERTO', 'OLIVEIRA', 'MAURÍCIO', 'AVOLO', 'CLEBER', 'ROMERIO', 'JUNIOR', 'ISABELA', 'WAGNER', 'CLAUDIA', 'ANTONIO', 'JOSE', 'LEONARDO', 'KLEBSON', 'OZENAIDE', 'ALEXANDER', 'VITORIA', 'ANA L.', 'MELISON', 'TAYNARA', 'RAFAELA' ] + JOVENS_APRENDIZES_SUPORTE
}

SETORES_SERVICOS = {
    "NRC": SERVICOS_ALVO,
    "CANCELAMENTO": ['CANCELAMENTO'], 
    "NEGOCIACAO": ['NEGOCIAÇÃO ATIVA', 'NEGOCIAÇÃO  PASSIVA'], 
    "SUPORTE": ['SUPORTE', 'LIBERAÇÃO'] 
}

# Configurações Especiais Suporte
LISTA_PLANTAO = ['TARCISIO', 'GEIBSON', 'LEONARDO', 'FERNANDO', 'RENAN']
ID_CONTA_CLIENTE_INTERNO = "5"

# REGRAS DE PAUSA
LIMITES_PAUSA = { "CURTA": 15.0, "LONGA": 120.0 }
TOLERANCIA_MENSAL_EXCESSO = 20.0 
TOLERANCIA_VISUAL_ALMOCO = 2.0

# ==============================================================================
# 1.2 CONTAS DISPONÍVEIS (FILTRO OPCIONAL MULTICONTA)
# ==============================================================================
CONTAS_DISPONIVEIS = {
    "1": "17628-ATEL (Principal)",
    "17": "ATEL- Romerio",
    "16": "ATEL- Lucas Valões",
    "15": "ATEL Telecom - Disparos",
    "14": "ATELAtivo-V2",
    "13": "ClienteInterno_V2",
    "12": "TráfegoPago_V2",
    "7": "Tráfego pago",
    "5": "CLIENTE INTERNO",
    "3": "LABORATÓRIO"
}

# ==============================================================================
# 2. CONEXÃO GOOGLE SHEETS (BANCO DE DADOS)
# ==============================================================================
def conectar_gsheets():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.open("solicitacoes_nrc").sheet1
        return sheet
    except Exception as erro:
        return None

def conectar_gsheets_aba(nome_aba):
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open("solicitacoes_nrc")
        worksheet = spreadsheet.worksheet(nome_aba)
        return worksheet
    except Exception as erro:
        return None

def salvar_solicitacao_gsheets(nome_agente, id_agente, motivo, mensagem):
    sheet = conectar_gsheets()
    if sheet:
        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        try:
            sheet.append_row([data_hora, id_agente, nome_agente, motivo, mensagem])
            return True, "Solicitação salva na nuvem com sucesso!"
        except Exception as erro:
            return False, f"Erro ao escrever na planilha: {erro}"
    else:
        return False, "Erro de conexão com Google Sheets. Verifique o compartilhamento."

def salvar_diario_bordo(supervisor_nome, setor_atual, nome_agente, tipo_ponto, descricao):
    sheet = conectar_gsheets_aba("Feedback_Gestao")
    if sheet:
        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        try:
            sheet.append_row([data_hora, supervisor_nome, setor_atual, nome_agente, tipo_ponto, descricao, "Pendente"])
            return True, "Registro salvo no Diário de Bordo!"
        except Exception as erro:
            return False, f"Erro ao salvar no Diário de Bordo: {erro}"
    else:
        return False, "Erro: Aba 'Feedback_Gestao' não encontrada na planilha."

def ler_solicitacoes_gsheets():
    sheet = conectar_gsheets()
    if sheet:
        try:
            data = sheet.get_all_records()
            return pd.DataFrame(data)
        except:
            return pd.DataFrame()
    return pd.DataFrame()

def ler_diario_bordo(setor_filtro=None):
    sheet = conectar_gsheets_aba("Feedback_Gestao")
    if sheet:
        try:
            data = sheet.get_all_records()
            df = pd.DataFrame(data)
            if not df.empty and setor_filtro and setor_filtro != "MASTER":
                if 'Setor' in df.columns:
                    df = df[df['Setor'] == setor_filtro]
            return df
        except:
            return pd.DataFrame()
    return pd.DataFrame()

# ==============================================================================
# 3. FUNÇÕES DE BACKEND (MATRIX API)
# ==============================================================================

@st.cache_data(ttl=600)
def get_admin_token():
    try:
        r = requests.post(f"{BASE_URL}/authuser", json={"login": ADMIN_USER, "chave": ADMIN_PASS}, timeout=5)
        time.sleep(0.3)
        if r.status_code == 200 and r.json().get("success"): return r.json()["result"]["token"]
    except: pass
    return None

def validar_agente_api(token, email_input):
    url = f"{BASE_URL}/agentes"
    headers = {"Authorization": f"Bearer {token}"}
    input_limpo = email_input.strip().lower()
    try:
        r = requests.get(url, headers=headers, params={"login": input_limpo}, timeout=5)
        time.sleep(0.3)
        if r.status_code == 200:
            result = r.json().get("result", [])
            if result:
                agente = result[0]
                return { "id": str(agente.get("cod_agente")), "nome": agente.get("nome_exibicao") or agente.get("agente"), "email": agente.get("email", "").lower() }
    except: pass
    return None

def time_str_to_seconds(tempo_str):
    if not tempo_str or not isinstance(tempo_str, str): return 0
    try:
        parts = list(map(int, tempo_str.split(':')))
        if len(parts) == 3: return parts[0]*3600 + parts[1]*60 + parts[2]
    except: pass
    return 0

def seconds_to_hms(seconds):
    if not seconds or seconds < 0: return "00:00:00"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def formatar_tempo_humano(minutos_float):
    if not minutos_float: return "0m"
    minutos_int = int(minutos_float)
    horas, mins = divmod(minutos_int, 60)
    if horas > 0: return f"{horas}h {mins:02d}m"
    else: return f"{mins}m"

def calcular_media_tempos(lista_tempos, pesos):
    total_seg = 0
    total_peso = 0
    for t, p in zip(lista_tempos, pesos):
        if p > 0:
            total_seg += time_str_to_seconds(t) * p
            total_peso += p
    return seconds_to_hms(total_seg / total_peso) if total_peso > 0 else "--:--"

@st.cache_data(ttl=3600)
def buscar_ids_canais(token):
    url = f"{BASE_URL}/canais"
    headers = {"Authorization": f"Bearer {token}"}
    ids = []
    try:
        r = requests.get(url, headers=headers)
        time.sleep(0.3)
        if r.status_code == 200:
            for c in r.json():
                if any(alvo in str(c.get("canal", "")).lower() for alvo in CANAIS_ALVO): ids.append(str(c.get("id_canal")))
    except: pass
    return ids

# ==============================================================================
# 4. FUNÇÕES ESPECÍFICAS DO AGENTE
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
            time.sleep(0.5)
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
        time.sleep(0.3)
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
                time.sleep(0.5)
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
            time.sleep(0.5)
            if r.status_code != 200: break
            data = r.json()
            rows = data.get("rows", [])
            if not rows: break
            todas_pausas.extend(rows)
            if len(rows) < 100: break
            page += 1
        except: break
    return pd.DataFrame(todas_pausas)

def gerar_link_protocolo(protocolo):
    if not protocolo: return None
    s_proto = str(protocolo).strip()
    if len(s_proto) < 7: suffix = s_proto
    else: suffix = s_proto[-7:]
    return f"https://ateltelecom.matrixdobrasil.ai/atendimento/view/cod_atendimento/{suffix}/readonly/true#atendimento-div"

# ==============================================================================
# 5. FUNÇÕES DO SUPERVISOR (VERSÃO GOLD 9.0)
# ==============================================================================

def buscar_agentes_online_filtrado_nrc(token):
    headers = {"Authorization": f"Bearer {token}"}
    agentes_online_nrc = []
    nrc_upper = [x.strip().upper() for x in LISTA_NRC + JOVENS_APRENDIZES_NRC]
    
    try:
        r = requests.get(f"{BASE_URL}/agentesOnline", headers=headers)
        time.sleep(0.3)
        if r.status_code == 200:
            todos_online = r.json()
            for agente in todos_online:
                nome_full = str(agente.get("nom_agente", "")).strip().upper()
                partes_nome = nome_full.split()
                if not partes_nome: continue
                
                match_encontrado = False
                for alvo in nrc_upper:
                    if alvo in partes_nome: match_encontrado = True; break
                    if alvo in NOMES_COMUNS_PRIMEIRO and alvo == partes_nome[0]: match_encontrado = True; break
                    if " " in alvo and alvo in nome_full: match_encontrado = True; break
                
                if match_encontrado:
                    agentes_online_nrc.append(agente)
    except: pass
    return agentes_online_nrc

def forcar_logout(token, id_agente):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/deslogarAgente"
    payload = {"id_agente": int(id_agente)}
    try:
        r = requests.post(url, headers=headers, json=payload)
        time.sleep(0.3)
        if r.status_code == 200:
            return True, "Sucesso"
        else:
            return False, f"Erro API: {r.text}"
    except Exception as e:
        return False, str(e)

@st.cache_data(ttl=300)
def buscar_dados_completos_supervisor(token, data_ini, data_fim, contas_selecionadas):
    headers = {"Authorization": f"Bearer {token}"}
    ids_agentes = []
    mapa_agentes = {}
    nrc_upper = [x.strip().upper() for x in LISTA_NRC + JOVENS_APRENDIZES_NRC]
    
    pagina = 1
    while True:
        try:
            r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": pagina, "bol_cancelado": 0})
            time.sleep(0.5)
            if r.status_code != 200: break
            data = r.json()
            rows = data.get("result", [])
            if not rows: break
            for agente in rows:
                nome_raw = str(agente.get("nome_exibicao") or agente.get("agente")).strip()
                nome_upper = nome_raw.upper() 
                partes_nome = nome_upper.split()
                if not partes_nome: continue
                match_encontrado = False
                for alvo in nrc_upper:
                    if alvo in partes_nome: match_encontrado = True; break
                    if alvo in NOMES_COMUNS_PRIMEIRO and alvo == partes_nome[0]: match_encontrado = True; break
                    if " " in alvo and alvo in nome_upper: match_encontrado = True; break

                if match_encontrado: 
                    cod = str(agente.get("cod_agente"))
                    ids_agentes.append(cod)
                    mapa_agentes[cod] = nome_upper
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break

    ids_canais = buscar_ids_canais(token)
    
    resultados = {
        s: {
            "num_qtd": 0, "tma": "--:--", "tme": "--:--", 
            "tmia": "--:--", "tmic": "--:--", "csat_pos": 0, "csat_total": 0
        } 
        for s in SERVICOS_ALVO
    }

    # EXCLUSÃO ISOLADA DE JOVEM APRENDIZ DOS DADOS GERAIS
    ids_agentes_stats = []
    nomes_aprendizes = [x.strip().upper() for x in JOVENS_APRENDIZES_NRC]
    for cod, nome in mapa_agentes.items():
        is_aprendiz = False
        for alvo in nomes_aprendizes:
            if alvo in nome.split() or (" " in alvo and alvo in nome): 
                is_aprendiz = True
                break
        if not is_aprendiz:
            ids_agentes_stats.append(cod)

    if not ids_agentes_stats: return resultados, 0.0, 0, mapa_agentes, {"tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--"}, 0
    
    dados_globais = {"tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--"}
    tempos_globais_agregados = {"tma": [], "tme": [], "tmia": [], "tmic": []}
    pesos_globais = []
    volume_total_setor = 0
    
    for conta in contas_selecionadas:
        try:
            params_globais = {
                "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
                "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
                "agrupador": "agente", 
                "agente[]": ids_agentes_stats, # Apenas Agentes Regulares
                "canal[]": ids_canais,
                "id_conta": conta
            }
            r_global = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_globais)
            time.sleep(0.5)
            if r_global.status_code == 200:
                lista_global = r_global.json()
                if lista_global and isinstance(lista_global, list):
                    for item in lista_global:
                        nome_api = str(item.get("agrupador", "")).upper()
                        cod_match = None
                        for cod in ids_agentes_stats:
                            nome_ag = mapa_agentes[cod]
                            if nome_ag == nome_api or nome_ag in nome_api:
                                cod_match = cod
                                break
                                
                        if cod_match:
                            vol = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                            if vol > 0:
                                volume_total_setor += vol
                                pesos_globais.append(vol)
                                tempos_globais_agregados["tma"].append(item.get("tma", "00:00:00"))
                                tempos_globais_agregados["tme"].append(item.get("tme", "00:00:00"))
                                tempos_globais_agregados["tmia"].append(item.get("tmia", "00:00:00"))
                                tempos_globais_agregados["tmic"].append(item.get("tmic", "00:00:00"))
        except: pass

    if pesos_globais:
        dados_globais["tma"] = calcular_media_tempos(tempos_globais_agregados["tma"], pesos_globais)
        dados_globais["tme"] = calcular_media_tempos(tempos_globais_agregados["tme"], pesos_globais)
        dados_globais["tmia"] = calcular_media_tempos(tempos_globais_agregados["tmia"], pesos_globais)
        dados_globais["tmic"] = calcular_media_tempos(tempos_globais_agregados["tmic"], pesos_globais)

    tempos_srv_agregados = {s: {"vols": [], "tma": [], "tme": [], "tmia": [], "tmic": []} for s in SERVICOS_ALVO}
    
    for conta in contas_selecionadas:
        params_srv = {
            "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
            "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
            "agrupador": "servico",
            "agente[]": ids_agentes_stats, # Apenas Agentes Regulares
            "canal[]": ids_canais,
            "id_conta": conta
        }
        try:
            r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_srv)
            time.sleep(0.5)
            if r.status_code == 200:
                lista = r.json()
                if lista and isinstance(lista, list):
                    for item in lista:
                        nome_servico = str(item.get("agrupador", "")).upper()
                        match_srv = None
                        
                        for s_alvo in SERVICOS_ALVO:
                            if " ".join(s_alvo.upper().split()) == " ".join(nome_servico.split()):
                                match_srv = s_alvo
                                break
                        
                        if match_srv:
                            qtd_bruta = int(item.get("num_qtd", 0))
                            qtd_aband = int(item.get("num_qtd_abandonado", 0))
                            qtd_liquida = qtd_bruta - qtd_aband
                            if qtd_liquida > 0:
                                resultados[match_srv]["num_qtd"] += qtd_liquida
                                tempos_srv_agregados[match_srv]["vols"].append(qtd_liquida)
                                tempos_srv_agregados[match_srv]["tma"].append(item.get("tma", "00:00:00"))
                                tempos_srv_agregados[match_srv]["tme"].append(item.get("tme", "00:00:00"))
                                tempos_srv_agregados[match_srv]["tmia"].append(item.get("tmia", "00:00:00"))
                                tempos_srv_agregados[match_srv]["tmic"].append(item.get("tmic", "00:00:00"))
        except: pass
        
    for s_alvo in SERVICOS_ALVO:
        vols = tempos_srv_agregados[s_alvo]["vols"]
        if sum(vols) > 0:
            resultados[s_alvo]["tma"] = calcular_media_tempos(tempos_srv_agregados[s_alvo]["tma"], vols)
            resultados[s_alvo]["tme"] = calcular_media_tempos(tempos_srv_agregados[s_alvo]["tme"], vols)
            resultados[s_alvo]["tmia"] = calcular_media_tempos(tempos_srv_agregados[s_alvo]["tmia"], vols)
            resultados[s_alvo]["tmic"] = calcular_media_tempos(tempos_srv_agregados[s_alvo]["tmic"], vols)

    csat_geral_pos = 0; csat_geral_total = 0
    for p_id in PESQUISAS_IDS:
        for conta in contas_selecionadas:
            p_page = 1
            while True:
                p_params = {
                    "data_inicial": data_ini.strftime("%Y-%m-%d"), 
                    "data_final": data_fim.strftime("%Y-%m-%d"), 
                    "pesquisa": p_id, "id_conta": conta, "limit": 1000, 
                    "page": p_page, "agente[]": ids_agentes_stats # Apenas Agentes Regulares
                }
                try:
                    r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=p_params)
                    time.sleep(0.5)
                    if r.status_code != 200: break
                    data = r.json()
                    if not data or not isinstance(data, list): break
                    total_api = 0
                    for bloco in data:
                        if str(bloco.get("id_pergunta", "")) in IDS_PERGUNTAS_VALIDAS:
                            if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                            for resp in bloco.get("respostas", []):
                                try:
                                    nom_ag = str(resp.get("nom_agente", "")).upper()
                                    cod_match = None
                                    for cod in ids_agentes_stats:
                                        nome_ag = mapa_agentes[cod]
                                        if nome_ag == nom_ag or nome_ag in nom_ag:
                                            cod_match = cod
                                            break
                                    
                                    if cod_match:
                                        servico_resp = str(resp.get("nom_servico", "")).upper().strip()
                                        val_raw = resp.get("nom_valor")
                                        if val_raw and val_raw != "": nota = int(float(val_raw))
                                        else: nota = -1
                                        
                                        if nota >= 0: 
                                            csat_geral_total += 1
                                            if nota >= 8: csat_geral_pos += 1
                                            
                                            for s_alvo in SERVICOS_ALVO:
                                                if " ".join(s_alvo.upper().split()) == " ".join(servico_resp.split()):
                                                    resultados[s_alvo]["csat_total"] += 1
                                                    if nota >= 8: resultados[s_alvo]["csat_pos"] += 1
                                                    break
                                except: pass
                    if (p_page * 1000) >= total_api: break
                    if len(data) < 2: break
                    p_page += 1
                except: break

    score_geral = (csat_geral_pos / csat_geral_total * 100) if csat_geral_total > 0 else 0.0
    return resultados, score_geral, csat_geral_total, mapa_agentes, dados_globais, volume_total_setor

def _processar_agente_pausas(token, cod_agente, nome_agente, data_ini, data_fim):
    headers = {"Authorization": f"Bearer {token}"}
    local_curtas, local_almoco, local_logins, local_ranking = [], [], [], []
    
    pausas_agente = []
    pagina = 1
    while True:
        if pagina > 5: break
        params = {"dat_inicial": data_ini.strftime("%Y-%m-%d"), "dat_final": data_fim.strftime("%Y-%m-%d"), "cod_agente": cod_agente, "limit": 100, "pagina": pagina}
        try:
            r = requests.get(f"{BASE_URL}/relAgentePausa", headers=headers, params=params, timeout=10)
            time.sleep(0.5)
            if r.status_code != 200: break
            rows = r.json().get("rows", [])
            if not rows: break
            pausas_agente.extend(rows)
            pagina += 1
        except: break
    
    acumulado_excesso_curta = 0.0
    
    for p in pausas_agente:
        motivo = str(p.get("pausa", "")).upper()
        try: seg = float(p.get("seg_pausado", 0))
        except: seg = 0
        minutos = seg / 60
        
        if any(x in motivo for x in ["MANHA", "MANHÃ", "TARDE", "NOITE"]):
            if minutos > LIMITES_PAUSA["CURTA"]:
                acumulado_excesso_curta += (minutos - LIMITES_PAUSA["CURTA"])
                
        if any(x in motivo for x in ["ALMOÇO", "ALMOCO", "PLANTÃO", "PLANTAO"]):
            if minutos > (LIMITES_PAUSA["LONGA"] + TOLERANCIA_VISUAL_ALMOCO):
                excesso = minutos - LIMITES_PAUSA["LONGA"]
                local_almoco.append({
                    "Agente": nome_agente,
                    "Data": p.get("data_pausa", "")[:10],
                    "Duração": formatar_tempo_humano(minutos),
                    "Status": f"Estourou {formatar_tempo_humano(excesso)}"
                })
    
    status_curta = "Normal"
    if acumulado_excesso_curta > TOLERANCIA_MENSAL_EXCESSO: status_curta = "ADVERTÊNCIA"
    if acumulado_excesso_curta > 0:
        local_curtas.append({
            "Agente": nome_agente,
            "Excesso Acumulado": formatar_tempo_humano(acumulado_excesso_curta),
            "Valor Num": acumulado_excesso_curta,
            "Status": status_curta
        })
        
    qtd_pausas = len([p for p in pausas_agente if "TERMINO" not in str(p.get("pausa")).upper() and "EXPEDIENTE" not in str(p.get("pausa")).upper()])
    if qtd_pausas > 0:
        local_ranking.append({"Agente": nome_agente, "Qtd Pausas": qtd_pausas})

    page_log = 1
    logins_raw = []
    while page_log <= 2:
        params_log = {"data_inicial": data_ini.strftime("%Y-%m-%d"), "data_final": data_fim.strftime("%Y-%m-%d"), "agente": cod_agente, "page": page_log, "limit": 100}
        try:
            r = requests.get(f"{BASE_URL}/relAgenteLogin", headers=headers, params=params_log, timeout=10)
            time.sleep(0.5)
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
            dt = datetime.strptime(d_str, "%Y-%m-%d %H:%M:%S")
            d_key = dt.strftime("%Y-%m-%d")
            if d_key not in min_logins: min_logins[d_key] = dt
            else:
                if dt < min_logins[d_key]: min_logins[d_key] = dt
        except: pass
        
    for d, dt in min_logins.items():
        mins = dt.minute
        if 1 < mins <= 55:
            local_logins.append({
                "Agente": nome_agente,
                "Data": d,
                "Hora Entrada": dt.strftime("%H:%M:%S"),
                "Atraso": f"{mins}m"
            })
    
    logins_ordenados = []
    for l in logins_raw:
        if l.get("data_login") and l.get("data_logout"):
             try:
                 d_in = datetime.strptime(l.get("data_login"), "%Y-%m-%d %H:%M:%S")
                 d_out = datetime.strptime(l.get("data_logout"), "%Y-%m-%d %H:%M:%S")
                 logins_ordenados.append((d_in, d_out))
             except: pass
    
    logins_ordenados.sort(key=lambda x: x[0])
    
    mapa_dias = defaultdict(list)
    for i, o in logins_ordenados:
        mapa_dias[i.date()].append((i, o))
        
    for dia, periodos in mapa_dias.items():
        for i in range(len(periodos) - 1):
            logout_atual = periodos[i][1]
            login_proximo = periodos[i+1][0]
            
            delta_min = (login_proximo - logout_atual).total_seconds() / 60.0
            
            if delta_min > 30:
                 limite_longa = LIMITES_PAUSA["LONGA"]
                 if delta_min > (limite_longa + TOLERANCIA_VISUAL_ALMOCO):
                     excesso = delta_min - limite_longa
                     local_almoco.append({
                        "Agente": nome_agente,
                        "Data": dia.strftime("%d/%m/%Y"),
                        "Duração": formatar_tempo_humano(delta_min),
                        "Status": f"Gap Deslogue: Estourou {formatar_tempo_humano(excesso)}"
                    })
            
    return local_curtas, local_almoco, local_logins, local_ranking

@st.cache_data(ttl=300)
def processar_dados_pausas_supervisor(token, data_ini, data_fim, mapa_agentes):
    curtas, almoco, logins, ranking = [], [], [], []
    barra_progresso = st.progress(0, text="Auditando pausas e horários...")
    total = len(mapa_agentes)
    concluidos = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(_processar_agente_pausas, token, cod, nome, data_ini, data_fim): nome for cod, nome in mapa_agentes.items()}
        for f in concurrent.futures.as_completed(futures):
            concluidos += 1
            barra_progresso.progress(int(concluidos/total * 100))
            try:
                c, a, l, r = f.result()
                curtas.extend(c)
                almoco.extend(a)
                logins.extend(l)
                ranking.extend(r)
            except: pass
    barra_progresso.empty()
    return curtas, almoco, logins, ranking

@st.cache_data(ttl=300)
def processar_ranking_geral(token, data_ini, data_fim, mapa_agentes, contas_selecionadas):
    headers = {"Authorization": f"Bearer {token}"}
    lista_rank = []
    ids_validos = list(mapa_agentes.keys())
    ids_canais = buscar_ids_canais(token)
    
    dados_stats = {cod: {"Vol": 0, "pesos": [], "TMA": [], "TME": [], "TMIA": [], "TMIC": []} for cod in ids_validos}
    
    for conta in contas_selecionadas:
        params_stats = {
            "data_inicial": f"{data_ini} 00:00:00",
            "data_final": f"{data_fim} 23:59:59",
            "agrupador": "agente",
            "agente[]": ids_validos,
            "canal[]": ids_canais,
            "id_conta": conta
        }
        try:
            r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_stats)
            time.sleep(0.5)
            if r.status_code == 200:
                for item in r.json():
                    nome_api = str(item.get("agrupador", "")).upper()
                    cod_match = next((c for c, n in mapa_agentes.items() if n == nome_api or n in nome_api), None)
                    if cod_match:
                        qtd = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                        if qtd > 0:
                            dados_stats[cod_match]["Vol"] += qtd
                            dados_stats[cod_match]["pesos"].append(qtd)
                            dados_stats[cod_match]["TMA"].append(item.get("tma", "--:--"))
                            dados_stats[cod_match]["TME"].append(item.get("tme", "--:--"))
                            dados_stats[cod_match]["TMIA"].append(item.get("tmia", "--:--"))
                            dados_stats[cod_match]["TMIC"].append(item.get("tmic", "--:--"))
        except: pass

    def _fetch_csat_agente(cod_ag):
        pos, tot = 0, 0
        for pid in PESQUISAS_IDS:
            for conta in contas_selecionadas:
                pg = 1
                while True:
                    pars = {"data_inicial": data_ini, "data_final": data_fim, "pesquisa": pid, "id_conta": conta, "limit": 1000, "page": pg, "agente[]": [cod_ag]}
                    try:
                        rr = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=pars)
                        time.sleep(0.5)
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futs = {executor.submit(_fetch_csat_agente, cod): cod for cod in ids_validos}
        for f in concurrent.futures.as_completed(futs):
            cod = futs[f]
            try:
                p, t = f.result()
                dados_csat[cod] = (p, t)
            except: dados_csat[cod] = (0, 0)
            
    for cod, nome in mapa_agentes.items():
        st_data = dados_stats[cod]
        pos, tot = dados_csat.get(cod, (0, 0))
        score = (pos/tot*100) if tot > 0 else 0.0
        
        vol_final = st_data["Vol"]
        if vol_final > 0 or tot > 0:
            lista_rank.append({
                "Agente": nome,
                "Volume": vol_final,
                "TMA": calcular_media_tempos(st_data["TMA"], st_data["pesos"]) if vol_final > 0 else "--:--",
                "TME": calcular_media_tempos(st_data["TME"], st_data["pesos"]) if vol_final > 0 else "--:--",
                "TMIA": calcular_media_tempos(st_data["TMIA"], st_data["pesos"]) if vol_final > 0 else "--:--",
                "TMIC": calcular_media_tempos(st_data["TMIC"], st_data["pesos"]) if vol_final > 0 else "--:--",
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
    
    # Adicionado method='min' para não penalizar empates puros.
    df_calc['Rank_TMA'] = df_calc['TMA_Seg'].rank(ascending=True, method='min')
    df_calc['Rank_TMIA'] = df_calc['TMIA_Seg'].rank(ascending=True, method='min')
    df_calc['Rank_CSAT'] = df_calc['CSAT Score'].rank(ascending=False, method='min')
    
    df_calc['Score_Final'] = df_calc['Rank_TMA'] + df_calc['Rank_TMIA'] + df_calc['Rank_CSAT']
    
    min_score = df_calc['Score_Final'].min()
    mvps = df_calc[df_calc['Score_Final'] == min_score]
    
    nomes = mvps['Agente'].tolist()
    if len(nomes) > 1:
        return "Empate: " + " | ".join(nomes)
    return nomes[0]

# ==============================================================================
# 5.1 FUNÇÕES MULTISETOR (ADICIONADAS PARA NÃO ALTERAR AS ANTIGAS)
# ==============================================================================

def buscar_agentes_online_filtrado_setor(token, setor_nome):
    headers = {"Authorization": f"Bearer {token}"}
    agentes_online_filtrados = []
    
    lista_alvo = SETORES_AGENTES.get(setor_nome, [])
    lista_upper = [x.strip().upper() for x in lista_alvo]
    
    try:
        r = requests.get(f"{BASE_URL}/agentesOnline", headers=headers)
        time.sleep(0.3)
        if r.status_code == 200:
            todos_online = r.json()
            for agente in todos_online:
                nome_full = str(agente.get("nom_agente", "")).strip().upper()
                partes_nome = nome_full.split()
                if not partes_nome: continue
                
                match_encontrado = False
                for alvo in lista_upper:
                    if alvo in partes_nome: match_encontrado = True; break
                    if alvo in NOMES_COMUNS_PRIMEIRO and alvo == partes_nome[0]: match_encontrado = True; break
                    if " " in alvo and alvo in nome_full: match_encontrado = True; break
                
                if match_encontrado:
                    agentes_online_filtrados.append(agente)
    except: pass
    return agentes_online_filtrados

@st.cache_data(ttl=300)
def buscar_dados_supervisor_multisetor(token, data_ini, data_fim, setor_nome, contas_selecionadas, excluir_plantao=False):
    headers = {"Authorization": f"Bearer {token}"}
    ids_agentes = []
    mapa_agentes = {}
    
    lista_nomes_alvo = SETORES_AGENTES.get(setor_nome, [])
    lista_servicos_alvo = SETORES_SERVICOS.get(setor_nome, [])
    
    nomes_upper = [x.strip().upper() for x in lista_nomes_alvo]
    
    pagina = 1
    while True:
        try:
            r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": pagina, "bol_cancelado": 0})
            time.sleep(0.5)
            if r.status_code != 200: break
            data = r.json()
            rows = data.get("result", [])
            if not rows: break
            for agente in rows:
                nome_raw = str(agente.get("nome_exibicao") or agente.get("agente")).strip()
                nome_upper = nome_raw.upper() 
                partes_nome = nome_upper.split()
                if not partes_nome: continue
                match_encontrado = False
                for alvo in nomes_upper:
                    if alvo in partes_nome: match_encontrado = True; break
                    if alvo in NOMES_COMUNS_PRIMEIRO and alvo == partes_nome[0]: match_encontrado = True; break
                    if " " in alvo and alvo in nome_upper: match_encontrado = True; break
                    
                if match_encontrado: 
                    cod = str(agente.get("cod_agente"))
                    ids_agentes.append(cod)
                    mapa_agentes[cod] = nome_upper
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break

    ids_canais = buscar_ids_canais(token)
    
    ids_agentes_stats = []
    nomes_plantao_upper = [x.strip().upper() for x in LISTA_PLANTAO]
    nomes_aprendizes = [x.strip().upper() for x in (JOVENS_APRENDIZES_NRC if setor_nome == "NRC" else JOVENS_APRENDIZES_SUPORTE)]
    
    # EXCLUSÃO ISOLADA DE JOVEM APRENDIZ E PLANTÃO DOS DADOS GERAIS
    for cod, nome_upper in mapa_agentes.items():
        is_excluido = False
        
        if excluir_plantao and setor_nome == "SUPORTE":
            partes_nome = nome_upper.split()
            for p_alvo in nomes_plantao_upper:
                if p_alvo in partes_nome or (" " in p_alvo and p_alvo in nome_upper): 
                    is_excluido = True
                    break
                    
        partes_nome_ap = nome_upper.split()
        for p_alvo in nomes_aprendizes:
            if p_alvo in partes_nome_ap or (" " in p_alvo and p_alvo in nome_upper): 
                is_excluido = True
                break

        if not is_excluido:
            ids_agentes_stats.append(cod)
    
    resultados = {
        s: {
            "num_qtd": 0, "tma": "--:--", "tme": "--:--", 
            "tmia": "--:--", "tmic": "--:--", "csat_pos": 0, "csat_total": 0
        } 
        for s in lista_servicos_alvo
    }

    if not ids_agentes_stats: return resultados, 0.0, 0, mapa_agentes, {"tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--"}, 0
    
    dados_globais = {"tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--"}
    tempos_globais_agregados = {"tma": [], "tme": [], "tmia": [], "tmic": []}
    pesos_globais = []
    volume_total_setor = 0
    
    for conta in contas_selecionadas:
        try:
            params_globais = {
                "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
                "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
                "agrupador": "agente", 
                "agente[]": ids_agentes_stats, # Apenas Agentes Regulares
                "canal[]": ids_canais,
                "id_conta": conta
            }
            r_global = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_globais)
            time.sleep(0.5)
            if r_global.status_code == 200:
                lista_global = r_global.json()
                if lista_global and isinstance(lista_global, list):
                    for item in lista_global:
                        nome_api = str(item.get("agrupador", "")).upper()
                        cod_match = None
                        for cod in ids_agentes_stats:
                            nome_ag = mapa_agentes[cod]
                            if nome_ag == nome_api or nome_ag in nome_api:
                                cod_match = cod
                                break
                                
                        if cod_match:
                            vol = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                            if vol > 0:
                                volume_total_setor += vol
                                pesos_globais.append(vol)
                                tempos_globais_agregados["tma"].append(item.get("tma", "00:00:00"))
                                tempos_globais_agregados["tme"].append(item.get("tme", "00:00:00"))
                                tempos_globais_agregados["tmia"].append(item.get("tmia", "00:00:00"))
                                tempos_globais_agregados["tmic"].append(item.get("tmic", "00:00:00"))
        except: pass

    if pesos_globais:
        dados_globais["tma"] = calcular_media_tempos(tempos_globais_agregados["tma"], pesos_globais)
        dados_globais["tme"] = calcular_media_tempos(tempos_globais_agregados["tme"], pesos_globais)
        dados_globais["tmia"] = calcular_media_tempos(tempos_globais_agregados["tmia"], pesos_globais)
        dados_globais["tmic"] = calcular_media_tempos(tempos_globais_agregados["tmic"], pesos_globais)

    tempos_srv_agregados = {s: {"vols": [], "tma": [], "tme": [], "tmia": [], "tmic": []} for s in lista_servicos_alvo}
    
    for conta in contas_selecionadas:
        params_srv = {
            "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
            "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
            "agrupador": "servico",
            "agente[]": ids_agentes_stats, # Apenas Agentes Regulares
            "canal[]": ids_canais,
            "id_conta": conta
        }
        try:
            r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_srv)
            time.sleep(0.5)
            if r.status_code == 200:
                lista = r.json()
                if lista and isinstance(lista, list):
                    for item in lista:
                        nome_servico = str(item.get("agrupador", "")).upper()
                        match_srv = None
                        
                        for s_alvo in lista_servicos_alvo:
                            if " ".join(s_alvo.upper().split()) == " ".join(nome_servico.split()):
                                match_srv = s_alvo
                                break
                        
                        if match_srv:
                            qtd_bruta = int(item.get("num_qtd", 0))
                            qtd_aband = int(item.get("num_qtd_abandonado", 0))
                            qtd_liquida = qtd_bruta - qtd_aband
                            if qtd_liquida > 0:
                                resultados[match_srv]["num_qtd"] += qtd_liquida
                                tempos_srv_agregados[match_srv]["vols"].append(qtd_liquida)
                                tempos_srv_agregados[match_srv]["tma"].append(item.get("tma", "00:00:00"))
                                tempos_srv_agregados[match_srv]["tme"].append(item.get("tme", "00:00:00"))
                                tempos_srv_agregados[match_srv]["tmia"].append(item.get("tmia", "00:00:00"))
                                tempos_srv_agregados[match_srv]["tmic"].append(item.get("tmic", "00:00:00"))
        except: pass
        
    for s_alvo in lista_servicos_alvo:
        vols = tempos_srv_agregados[s_alvo]["vols"]
        if sum(vols) > 0:
            resultados[s_alvo]["tma"] = calcular_media_tempos(tempos_srv_agregados[s_alvo]["tma"], vols)
            resultados[s_alvo]["tme"] = calcular_media_tempos(tempos_srv_agregados[s_alvo]["tme"], vols)
            resultados[s_alvo]["tmia"] = calcular_media_tempos(tempos_srv_agregados[s_alvo]["tmia"], vols)
            resultados[s_alvo]["tmic"] = calcular_media_tempos(tempos_srv_agregados[s_alvo]["tmic"], vols)

    csat_geral_pos = 0; csat_geral_total = 0
    for p_id in PESQUISAS_IDS:
        for conta in contas_selecionadas:
            p_page = 1
            while True:
                p_params = {
                    "data_inicial": data_ini.strftime("%Y-%m-%d"), 
                    "data_final": data_fim.strftime("%Y-%m-%d"), 
                    "pesquisa": p_id, "id_conta": conta, "limit": 1000, 
                    "page": p_page, "agente[]": ids_agentes_stats # Apenas Agentes Regulares
                }
                try:
                    r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=p_params)
                    time.sleep(0.5)
                    if r.status_code != 200: break
                    data = r.json()
                    if not data or not isinstance(data, list): break
                    total_api = 0
                    for bloco in data:
                        if str(bloco.get("id_pergunta", "")) in IDS_PERGUNTAS_VALIDAS:
                            if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                            for resp in bloco.get("respostas", []):
                                try:
                                    nom_ag = str(resp.get("nom_agente", "")).upper()
                                    cod_match = None
                                    for cod in ids_agentes_stats:
                                        nome_ag = mapa_agentes[cod]
                                        if nome_ag == nom_ag or nome_ag in nom_ag:
                                            cod_match = cod
                                            break
                                    
                                    if cod_match:
                                        servico_resp = str(resp.get("nom_servico", "")).upper().strip()
                                        val_raw = resp.get("nom_valor")
                                        if val_raw and val_raw != "": nota = int(float(val_raw))
                                        else: nota = -1
                                        
                                        if nota >= 0: 
                                            csat_geral_total += 1
                                            if nota >= 8: csat_geral_pos += 1
                                            
                                            for s_alvo in lista_servicos_alvo:
                                                if " ".join(s_alvo.upper().split()) == " ".join(servico_resp.split()):
                                                    resultados[s_alvo]["csat_total"] += 1
                                                    if nota >= 8: resultados[s_alvo]["csat_pos"] += 1
                                                    break
                                except: pass
                    if (p_page * 1000) >= total_api: break
                    if len(data) < 2: break
                    p_page += 1
                except: break

    score_geral = (csat_geral_pos / csat_geral_total * 100) if csat_geral_total > 0 else 0.0
    return resultados, score_geral, csat_geral_total, mapa_agentes, dados_globais, volume_total_setor

# ==============================================================================
# 5.2 FUNÇÕES DE PRÉ-PAUSA
# ==============================================================================

@st.cache_data(ttl=60)
def buscar_pre_pausas_detalhado(token, id_agente, data_ini, data_fim):
    url = f"{BASE_URL}/relPausasAgendadas"
    headers = {"Authorization": f"Bearer {token}"}
    todas_pre_pausas = []
    page = 1
    while True:
        params = {
            "data_inicial": data_ini.strftime("%Y-%m-%d"),
            "data_final": data_fim.strftime("%Y-%m-%d"),
            "agente": id_agente,
            "page": page,
            "limit": 100
        }
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            time.sleep(0.5)
            if r.status_code != 200: break
            data = r.json()
            rows = data.get("rows", [])
            if not rows: break
            todas_pre_pausas.extend(rows)
            if len(rows) < 100: break
            page += 1
        except: break
    return todas_pre_pausas

def processar_dados_pre_pausas_geral(token, data_ini, data_fim, mapa_agentes):
    resultados = []
    def _fetch_pre_pausa(cod, nome):
        raw_data = buscar_pre_pausas_detalhado(token, cod, data_ini, data_fim)
        if not raw_data: return []
        
        lista_retorno = []
        for p in raw_data:
            data_ini_str = p.get("data_pre", "")
            data_fim_str = p.get("data_fim", "")
            duracao_str = p.get("tempo_pre_pausado", "00:00:00")
            motivo_str = p.get("pausa", "Agendada")
            
            inicio_fmt = data_ini_str
            fim_fmt = data_fim_str
            try:
                if data_ini_str:
                    inicio_fmt = datetime.strptime(data_ini_str, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M:%S")
                if data_fim_str:
                    fim_fmt = datetime.strptime(data_fim_str, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M:%S")
            except: pass

            lista_retorno.append({
                "Agente": nome,
                "Início": inicio_fmt,
                "Término": fim_fmt,
                "Duração": duracao_str,
                "Motivo": motivo_str
            })
        return lista_retorno

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(_fetch_pre_pausa, cod, nome): nome for cod, nome in mapa_agentes.items()}
        for f in concurrent.futures.as_completed(futures):
            try:
                res = f.result()
                if res: resultados.extend(res)
            except: pass
            
    return resultados

# ==============================================================================
# 5.3 FUNÇÕES ESPECIAIS SUPORTE E JOVEM APRENDIZ
# ==============================================================================

@st.cache_data(ttl=300)
def buscar_dados_plantao(token, data_ini, data_fim, contas_selecionadas):
    headers = {"Authorization": f"Bearer {token}"}
    ids_plantao = []
    mapa_plantao = {}
    
    pagina = 1
    nomes_upper = [x.strip().upper() for x in LISTA_PLANTAO]
    
    while True:
        try:
            r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": pagina, "bol_cancelado": 0})
            time.sleep(0.5)
            if r.status_code != 200: break
            data = r.json()
            rows = data.get("result", [])
            if not rows: break
            for agente in rows:
                nome_raw = str(agente.get("nome_exibicao") or agente.get("agente")).strip().upper()
                partes = nome_raw.split()
                match = False
                for alvo in nomes_upper:
                    if alvo in partes: match = True; break
                if match:
                    ids_plantao.append(str(agente.get("cod_agente")))
                    mapa_plantao[str(agente.get("cod_agente"))] = nome_raw
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break
        
    ids_canais = buscar_ids_canais(token)
    
    if not ids_plantao: return pd.DataFrame(), {}, {}

    lista_stats_agente = []
    agentes_agregados = {ag: {"Vol": 0, "pesos": [], "TMA": [], "TMIA": []} for ag in mapa_plantao.values()}
    
    for conta in contas_selecionadas:
        params_agente = {
            "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
            "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
            "agrupador": "agente", 
            "agente[]": ids_plantao, 
            "canal[]": ids_canais,
            "id_conta": conta
        }
        try:
            r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_agente)
            time.sleep(0.5)
            if r.status_code == 200:
                for item in r.json():
                    nome_api = str(item.get("agrupador", "")).upper()
                    agente_nome = next((n for c, n in mapa_plantao.items() if n == nome_api or n in nome_api), nome_api)
                    
                    if agente_nome in agentes_agregados:
                        qtd = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                        if qtd > 0:
                            agentes_agregados[agente_nome]["Vol"] += qtd
                            agentes_agregados[agente_nome]["pesos"].append(qtd)
                            agentes_agregados[agente_nome]["TMA"].append(item.get("tma", "00:00:00"))
                            agentes_agregados[agente_nome]["TMIA"].append(item.get("tmia", "00:00:00"))
        except: pass
        
    for ag, st_data in agentes_agregados.items():
        if st_data["Vol"] > 0:
            lista_stats_agente.append({
                "Agente": ag,
                "Volume": st_data["Vol"],
                "TMA": calcular_media_tempos(st_data["TMA"], st_data["pesos"]),
                "TMIA": calcular_media_tempos(st_data["TMIA"], st_data["pesos"]),
                "CSAT": 0.0, 
                "Qtd CSAT": 0
            })
    
    stats_por_servico = {}
    for conta in contas_selecionadas:
        params_servico = {
            "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
            "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
            "agrupador": "servico", 
            "agente[]": ids_plantao, 
            "canal[]": ids_canais,
            "id_conta": conta
        }
        try:
            r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_servico)
            time.sleep(0.5)
            if r.status_code == 200:
                for item in r.json():
                    serv = item.get("agrupador", "Outros")
                    if serv not in stats_por_servico:
                        stats_por_servico[serv] = {"num_qtd": 0, "pesos": [], "tma": [], "tme": [], "tmia": [], "tmic": []}
                    
                    qtd = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                    if qtd > 0:
                        stats_por_servico[serv]["num_qtd"] += qtd
                        stats_por_servico[serv]["pesos"].append(qtd)
                        stats_por_servico[serv]["tma"].append(item.get("tma", "00:00:00"))
                        stats_por_servico[serv]["tme"].append(item.get("tme", "00:00:00"))
                        stats_por_servico[serv]["tmia"].append(item.get("tmia", "00:00:00"))
                        stats_por_servico[serv]["tmic"].append(item.get("tmic", "00:00:00"))
        except: pass

    csat_scores = {}
    csat_servico = {}
    for pid in PESQUISAS_IDS:
        for conta in contas_selecionadas:
            pg = 1
            while True:
                pars = {"data_inicial": data_ini.strftime("%Y-%m-%d"), "data_final": data_fim.strftime("%Y-%m-%d"), "pesquisa": pid, "id_conta": conta, "limit": 1000, "page": pg, "agente[]": ids_plantao}
                try:
                    rr = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=pars)
                    time.sleep(0.5)
                    if rr.status_code != 200: break
                    dd = rr.json()
                    if not dd or not isinstance(dd, list): break
                    total_k = 0
                    for b in dd:
                        if str(b.get("id_pergunta","")) in IDS_PERGUNTAS_VALIDAS:
                            if b.get("sintetico"): total_k += sum(int(x.get("num_quantidade", 0)) for x in b["sintetico"])
                            for rsp in b.get("respostas", []):
                                nom_ag = str(rsp.get("nom_agente","")).upper()
                                nome_match = next((n for c, n in mapa_plantao.items() if n == nom_ag or n in nom_ag), nom_ag)
                                
                                if nome_match:
                                    serv_resp = str(rsp.get("nom_servico", "")).upper().strip()
                                    val = float(rsp.get("nom_valor", -1))
                                    if val >= 0:
                                        if nome_match not in csat_scores: csat_scores[nome_match] = {"pos": 0, "tot": 0}
                                        csat_scores[nome_match]["tot"] += 1
                                        if val >= 8: csat_scores[nome_match]["pos"] += 1
                                        
                                        if serv_resp not in csat_servico: csat_servico[serv_resp] = {"pos": 0, "tot": 0}
                                        csat_servico[serv_resp]["tot"] += 1
                                        if val >= 8: csat_servico[serv_resp]["pos"] += 1
                    if (pg * 1000) >= total_k: break
                    if len(dd) < 2: break
                    pg += 1
                except: break
                
    stats_servico_finais = {}
    for serv, st_data in stats_por_servico.items():
        if st_data["num_qtd"] > 0:
            c_pos = 0; c_tot = 0
            for s_k, s_v in csat_servico.items():
                if " ".join(s_k.split()) == " ".join(serv.upper().split()):
                    c_pos += s_v["pos"]
                    c_tot += s_v["tot"]
            
            stats_servico_finais[serv] = {
                "num_qtd": st_data["num_qtd"],
                "tma": calcular_media_tempos(st_data["tma"], st_data["pesos"]),
                "tme": calcular_media_tempos(st_data["tme"], st_data["pesos"]),
                "tmia": calcular_media_tempos(st_data["tmia"], st_data["pesos"]),
                "tmic": calcular_media_tempos(st_data["tmic"], st_data["pesos"]),
                "csat_pos": c_pos,
                "csat_tot": c_tot
            }
            
    for row in lista_stats_agente:
        ag = row["Agente"]
        if ag in csat_scores:
            d = csat_scores[ag]
            row["Qtd CSAT"] = d["tot"]
            row["CSAT"] = (d["pos"] / d["tot"] * 100) if d["tot"] > 0 else 0.0
            
    return pd.DataFrame(lista_stats_agente), stats_servico_finais, {}

@st.cache_data(ttl=300)
def buscar_dados_cliente_interno(token, data_ini, data_fim, nomes_suporte_validos):
    headers = {"Authorization": f"Bearer {token}"}
    ids_canais = buscar_ids_canais(token) 
    nomes_validos_upper = [x.strip().upper() for x in nomes_suporte_validos]
    
    stats_globais = {"TMA": "--:--", "TME": "--:--", "TMIA": "--:--"}
    try:
        params = {
            "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
            "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
            "agrupador": "conta", 
            "canal[]": ids_canais,
            "id_conta": ID_CONTA_CLIENTE_INTERNO 
        }
        r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params)
        time.sleep(0.3)
        if r.status_code == 200:
            lista = r.json()
            if lista and isinstance(lista, list):
                item = lista[0]
                stats_globais["TMA"] = item.get("tma", "--:--")
                stats_globais["TME"] = item.get("tme", "--:--")
                stats_globais["TMIA"] = item.get("tmia", "--:--")
    except: pass
    
    lista_pesquisas = []
    csat_pos = 0; csat_total = 0
    
    for p_id in PESQUISAS_IDS:
        pg = 1
        while True:
            params_p = {
                "data_inicial": data_ini.strftime("%Y-%m-%d"), 
                "data_final": data_fim.strftime("%Y-%m-%d"), 
                "pesquisa": p_id, 
                "id_conta": ID_CONTA_CLIENTE_INTERNO, 
                "limit": 1000, 
                "page": pg
            }
            try:
                r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=params_p)
                time.sleep(0.5)
                if r.status_code != 200: break
                data = r.json()
                if not data or not isinstance(data, list): break
                
                total_api = 0
                for bloco in data:
                    if str(bloco.get("id_pergunta", "")) in IDS_PERGUNTAS_VALIDAS:
                        if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                        for resp in bloco.get("respostas", []):
                            nome_agente_resp = str(resp.get("nom_agente", "")).strip().upper()
                            partes_nome = nome_agente_resp.split()
                            eh_do_suporte = False
                            if partes_nome:
                                for alvo in nomes_validos_upper:
                                    if alvo in partes_nome: eh_do_suporte = True; break
                                    if " " in alvo and alvo in nome_agente_resp: eh_do_suporte = True; break
                            
                            if eh_do_suporte:
                                val_raw = resp.get("nom_valor")
                                if val_raw and val_raw != "": 
                                    nota = int(float(val_raw))
                                    csat_total += 1
                                    if nota >= 8: csat_pos += 1
                                    
                                    lista_pesquisas.append({
                                        "Data": resp.get("dat_resposta"),
                                        "Cliente": resp.get("nom_contato"),
                                        "Agente": nome_agente_resp,
                                        "Nota": nota,
                                        "Comentario": resp.get("nom_resposta"),
                                        "Protocolo": resp.get("num_protocolo")
                                    })
                
                if (pg * 1000) >= total_api: break
                if len(data) < 2: break
                pg += 1
            except: break
            
    df_pesquisas = pd.DataFrame(lista_pesquisas)
    score = (csat_pos / csat_total * 100) if csat_total > 0 else 0.0
    
    return stats_globais, score, csat_total, df_pesquisas

@st.cache_data(ttl=300)
def buscar_dados_jovem_aprendiz(token, data_ini, data_fim, setor_nome, contas_selecionadas):
    headers = {"Authorization": f"Bearer {token}"}
    ids_ja = []
    mapa_ja = {}
    
    lista_nomes_alvo = JOVENS_APRENDIZES_NRC if setor_nome == "NRC" else JOVENS_APRENDIZES_SUPORTE
    nomes_upper = [x.strip().upper() for x in lista_nomes_alvo]
    
    pagina = 1
    while True:
        try:
            r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": pagina, "bol_cancelado": 0})
            time.sleep(0.5)
            if r.status_code != 200: break
            data = r.json()
            rows = data.get("result", [])
            if not rows: break
            for agente in rows:
                nome_raw = str(agente.get("nome_exibicao") or agente.get("agente")).strip()
                nome_upper = nome_raw.upper() 
                partes_nome = nome_upper.split()
                if not partes_nome: continue
                match_encontrado = False
                for alvo in nomes_upper:
                    if alvo in partes_nome: match_encontrado = True; break
                    if " " in alvo and alvo in nome_upper: match_encontrado = True; break
                if match_encontrado: 
                    cod = str(agente.get("cod_agente"))
                    ids_ja.append(cod)
                    mapa_ja[cod] = nome_upper
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break

    ids_canais = buscar_ids_canais(token)

    stats_globais = {"Volume": 0, "TMA": "--:--", "TME": "--:--", "TMIA": "--:--", "TMIC": "--:--"}
    ranking_dict = {cod: {"Volume": 0, "pesos": [], "TMA": [], "TMIA": [], "TME": [], "TMIC": [], "CSAT_Pos": 0, "CSAT_Tot": 0} for cod in ids_ja}
    
    if not ids_ja: return stats_globais, [], pd.DataFrame(), 0.0

    tempos_globais = {"tma": [], "tme": [], "tmia": [], "tmic": []}
    pesos_globais = []
    
    for conta in contas_selecionadas:
        try:
            params_g = {
                "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
                "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
                "agrupador": "conta", 
                "agente[]": ids_ja,
                "canal[]": ids_canais,
                "id_conta": conta
            }
            r_global = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_g)
            time.sleep(0.5)
            if r_global.status_code == 200:
                lista_g = r_global.json()
                if lista_g and isinstance(lista_g, list):
                    item = lista_g[0]
                    vol = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                    if vol > 0:
                        stats_globais["Volume"] += vol
                        pesos_globais.append(vol)
                        tempos_globais["tma"].append(item.get("tma", "00:00:00"))
                        tempos_globais["tme"].append(item.get("tme", "00:00:00"))
                        tempos_globais["tmia"].append(item.get("tmia", "00:00:00"))
                        tempos_globais["tmic"].append(item.get("tmic", "00:00:00"))
        except: pass

    if pesos_globais:
        stats_globais["TMA"] = calcular_media_tempos(tempos_globais["tma"], pesos_globais)
        stats_globais["TME"] = calcular_media_tempos(tempos_globais["tme"], pesos_globais)
        stats_globais["TMIA"] = calcular_media_tempos(tempos_globais["tmia"], pesos_globais)
        stats_globais["TMIC"] = calcular_media_tempos(tempos_globais["tmic"], pesos_globais)

    for conta in contas_selecionadas:
        try:
            params_a = {
                "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
                "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
                "agrupador": "agente", 
                "agente[]": ids_ja,
                "canal[]": ids_canais,
                "id_conta": conta
            }
            r_a = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_a)
            time.sleep(0.5)
            if r_a.status_code == 200:
                for item in r_a.json():
                    nome_api = str(item.get("agrupador", "")).upper()
                    cod_match = next((c for c, n in mapa_ja.items() if n == nome_api or n in nome_api), None)
                    if cod_match:
                        vol = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                        if vol > 0:
                            ranking_dict[cod_match]["Volume"] += vol
                            ranking_dict[cod_match]["pesos"].append(vol)
                            ranking_dict[cod_match]["TMA"].append(item.get("tma", "00:00:00"))
                            ranking_dict[cod_match]["TME"].append(item.get("tme", "00:00:00"))
                            ranking_dict[cod_match]["TMIA"].append(item.get("tmia", "00:00:00"))
                            ranking_dict[cod_match]["TMIC"].append(item.get("tmic", "00:00:00"))
        except: pass

    lista_pesquisas = []
    csat_geral_pos = 0
    csat_geral_tot = 0
    
    for p_id in PESQUISAS_IDS:
        for conta in contas_selecionadas:
            pg = 1
            while True:
                params_p = {
                    "data_inicial": data_ini.strftime("%Y-%m-%d"), 
                    "data_final": data_fim.strftime("%Y-%m-%d"), 
                    "pesquisa": p_id, "id_conta": conta, "limit": 1000, 
                    "page": pg, "agente[]": ids_ja
                }
                try:
                    r_p = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=params_p)
                    time.sleep(0.5)
                    if r_p.status_code != 200: break
                    data = r_p.json()
                    if not data or not isinstance(data, list): break
                    total_api = 0
                    for bloco in data:
                        if str(bloco.get("id_pergunta", "")) in IDS_PERGUNTAS_VALIDAS:
                            if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                            for resp in bloco.get("respostas", []):
                                nom_ag = str(resp.get("nom_agente","")).upper()
                                cod_match = next((c for c, n in mapa_ja.items() if n == nom_ag or n in nom_ag), None)
                                
                                if cod_match:
                                    val_raw = resp.get("nom_valor")
                                    if val_raw and val_raw != "": 
                                        nota = int(float(val_raw))
                                        csat_geral_tot += 1
                                        ranking_dict[cod_match]["CSAT_Tot"] += 1
                                        if nota >= 8: 
                                            csat_geral_pos += 1
                                            ranking_dict[cod_match]["CSAT_Pos"] += 1
                                        
                                        lista_pesquisas.append({
                                            "Data": resp.get("dat_resposta"),
                                            "Agente": mapa_ja[cod_match],
                                            "Cliente": resp.get("nom_contato"),
                                            "Nota": nota,
                                            "Comentario": resp.get("nom_resposta"),
                                            "Protocolo": resp.get("num_protocolo")
                                        })
                    if (pg * 1000) >= total_api: break
                    if len(data) < 2: break
                    pg += 1
                except: break

    ranking_final = []
    for cod, d in ranking_dict.items():
        if d["Volume"] > 0 or d["CSAT_Tot"] > 0:
            tma_str = calcular_media_tempos(d["TMA"], d["pesos"]) if d["Volume"] > 0 else "00:00:00"
            tmia_str = calcular_media_tempos(d["TMIA"], d["pesos"]) if d["Volume"] > 0 else "00:00:00"
            score = (d["CSAT_Pos"] / d["CSAT_Tot"] * 100) if d["CSAT_Tot"] > 0 else 0.0
            
            tmia_seg = time_str_to_seconds(tmia_str)
            alerta = False
            motivos_alerta = []
            
            if tmia_seg > 60:
                alerta = True
                motivos_alerta.append("TMIA Alto")
            if d["CSAT_Tot"] > 0 and score < 90.0:
                alerta = True
                motivos_alerta.append("CSAT Baixo")
            
            status_alerta = "🔴 " + " e ".join(motivos_alerta) if alerta else "🟢 OK"

            ranking_final.append({
                "Agente": mapa_ja[cod],
                "Volume": d["Volume"],
                "TMA": tma_str,
                "TMIA": tmia_str,
                "CSAT": score,
                "Qtd CSAT": d["CSAT_Tot"],
                "Status": status_alerta
            })

    score_global = (csat_geral_pos / csat_geral_tot * 100) if csat_geral_tot > 0 else 0.0
    return stats_globais, ranking_final, pd.DataFrame(lista_pesquisas), score_global

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
        with st.expander("📜 Versão Platinum 16.4"):
            st.markdown("""
            **v16.4 - Ajustes de Bugs e Refinamento de Lógicas**
            - **Global Inteligente:** A aba Geral não pede mais totais aglutinados, garantindo o descarte real dos Aprendizes.
            - **MVP Empatado:** Novo cálculo do Melhor do Mês cruza somente TMA, TMIA e CSAT. Em caso de empate matemático, exibe todos os empatados.
            - **Atrasos (Rankings):** Adicionada coluna visual (⚠️) apontando o volume de atrasos detectados no cruzamento de logs.
            - **CSAT Plantão Geral:** Corrigido render para mostrar nota média no bloco de visualização geral de serviços.
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
                    st.session_state.auth_status = True
                    st.session_state.user_role = "supervisor"
                    st.session_state.user_setor = "NRC"
                    st.session_state.user_data = {"nome": "Supervisor NRC", "id": "SUPERVISOR"}
                    st.rerun()
                elif usuario == SUPERVISOR_CANCELAMENTO_LOGIN and senha == SUPERVISOR_CANCELAMENTO_PASS:
                    st.session_state.auth_status = True
                    st.session_state.user_role = "supervisor"
                    st.session_state.user_setor = "CANCELAMENTO"
                    st.session_state.user_data = {"nome": "Supervisor Cancelamento", "id": "SUPERVISOR_CANC"}
                    st.rerun()
                elif usuario == SUPERVISOR_SUPORTE_LOGIN and senha == SUPERVISOR_SUPORTE_PASS:
                    st.session_state.auth_status = True
                    st.session_state.user_role = "supervisor"
                    st.session_state.user_setor = "SUPORTE"
                    st.session_state.user_data = {"nome": "Supervisor Suporte", "id": "SUPERVISOR_SUP"}
                    st.rerun()
                elif usuario == SUPERVISOR_NEGOCIACAO_LOGIN and senha == SUPERVISOR_NEGOCIACAO_PASS:
                    st.session_state.auth_status = True
                    st.session_state.user_role = "supervisor"
                    st.session_state.user_setor = "NEGOCIACAO"
                    st.session_state.user_data = {"nome": "Supervisor Negociação", "id": "SUPERVISOR_NEG"}
                    st.rerun()
                elif usuario == MASTER_LOGIN and senha == MASTER_PASS:
                    st.session_state.auth_status = True
                    st.session_state.user_role = "master"
                    st.session_state.user_setor = "NRC" 
                    st.session_state.user_data = {"nome": "Gestão Geral", "id": "MASTER"}
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

    if st.session_state.user_role == "master":
        st.sidebar.markdown("---")
        st.sidebar.header("👑 Painel Master")
        setor_selecionado = st.sidebar.selectbox("Visualizar Setor:", list(SETORES_AGENTES.keys()), index=0)
        setor_atual = setor_selecionado

    contas_selecionadas = [str(ID_CONTA)]
    if st.session_state.user_role in ["supervisor", "master"]:
        st.sidebar.markdown("---")
        st.sidebar.header("🏢 Filtro de Contas")
        opcoes_contas = [f"{cod} - {nome}" for cod, nome in CONTAS_DISPONIVEIS.items()]
        default_conta = next((f"{cod} - {nome}" for cod, nome in CONTAS_DISPONIVEIS.items() if cod == str(ID_CONTA)), None)
        if not default_conta: default_conta = opcoes_contas[0]

        contas_selecionadas_str = st.sidebar.multiselect(
            "Contas a Monitorar:",
            options=opcoes_contas,
            default=[default_conta]
        )
        contas_selecionadas = [s.split(" - ")[0] for s in contas_selecionadas_str]
        if not contas_selecionadas: 
            contas_selecionadas = [str(ID_CONTA)]
    
    contas_tuple = tuple(contas_selecionadas)
    
    modo_visao_supervisor = "Geral"
    id_alvo = None
    nome_alvo = None

    if st.session_state.user_role == "supervisor" or st.session_state.user_role == "master":
        with st.spinner("Carregando equipe..."):
            if setor_atual == "NRC":
                _, _, _, mapa_agentes_sidebar, _, _ = buscar_dados_completos_supervisor(token, d_inicial, d_final, contas_tuple)
            else:
                _, _, _, mapa_agentes_sidebar, _, _ = buscar_dados_supervisor_multisetor(token, d_inicial, d_final, setor_atual, contas_tuple, excluir_plantao=False)
        
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
    if st.session_state.user_role == "agente" or ((st.session_state.user_role == "supervisor" or st.session_state.user_role == "master") and modo_visao_supervisor == "Individual"):
        
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
                
                detratores = df_csat[df_csat['Nota_Num'] < 8].copy()
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
    # PAINEL SUPERVISOR GERAL (ADAPTADO PARA MULTISETOR)
    # --------------------------------------------------------------------------
    elif (st.session_state.user_role == "supervisor" or st.session_state.user_role == "master") and modo_visao_supervisor == "Geral":
        st.markdown(f"## 🏢 Painel de Gestão - Setor {setor_atual}")
        
        lista_abas = ["👁️ Visão Geral", "🏆 Rankings", "⏸️ Pausas", "⚡ Tempo Real", "🆘 Solicitações", "📝 Diário de Bordo"]
        if setor_atual in ["NRC", "SUPORTE"]:
            lista_abas.append("👶 Jovem Aprendiz")
            
        if setor_atual == "SUPORTE":
            lista_abas.extend(["🌙 Plantão", "🏢 Cliente Interno"])
            
        abas_sup = st.tabs(lista_abas)
        
        # Criação do mapa isolado (Sem Jovem Aprendiz) para ser passado para as próximas abas (Ranking, Pausas, Tempo Real)
        mapa_agentes_filtrado = {}
        if 'mapa_agentes_sidebar' in locals():
            lista_exclusao = [x.strip().upper() for x in (JOVENS_APRENDIZES_NRC if setor_atual == "NRC" else JOVENS_APRENDIZES_SUPORTE)]
            for cod, nome in mapa_agentes_sidebar.items():
                is_excluido = False
                for alvo in lista_exclusao:
                    if alvo in nome.split() or (" " in alvo and alvo in nome): 
                        is_excluido = True
                        break
                if not is_excluido:
                    mapa_agentes_filtrado[cod] = nome

        # ABA 1: VISÃO GERAL
        with abas_sup[0]:
            if token:
                with st.spinner("Sincronizando estatísticas..."):
                    
                    if setor_atual == "NRC":
                        dados_servicos, csat_geral, base_geral, mapa_agentes, dados_globais, vol_total_setor = buscar_dados_completos_supervisor(token, d_inicial, d_final, contas_tuple)
                        lista_servicos_exibir = SERVICOS_ALVO
                    else:
                        excluir_plantao = (setor_atual == "SUPORTE")
                        dados_servicos, csat_geral, base_geral, mapa_agentes, dados_globais, vol_total_setor = buscar_dados_supervisor_multisetor(token, d_inicial, d_final, setor_atual, contas_tuple, excluir_plantao=excluir_plantao)
                        lista_servicos_exibir = SETORES_SERVICOS.get(setor_atual, [])

                    st.markdown(f"#### ⭐ Visão Global da Equipe ({setor_atual})")
                    
                    if setor_atual in ["NRC", "SUPORTE"]:
                        c_vol1, c_vol2, c_vol3 = st.columns([1, 1, 1])
                        with c_vol1:
                            texto_vol = "Volumetria Geral NRC" if setor_atual == "NRC" else "Volumetria Geral Suporte (Diurno)"
                            render_kpi_card(texto_vol, str(vol_total_setor), "Soma de todos os serviços", "#6366f1")

                    col_kpi1, col_kpi2 = st.columns([1, 2])
                    with col_kpi1:
                        cor_geral = "#10b981" if csat_geral >= 85 else ("#f59e0b" if csat_geral >= 75 else "#ef4444")
                        render_kpi_card("CSAT Global (Setor)", f"{csat_geral:.2f}%", f"Base Total: {base_geral}", cor_geral)
                    with col_kpi2:
                        if setor_atual == "NRC":
                            render_link_card("Ferramenta Externa", "https://fideliza-nator-live.streamlit.app/", "FIDELIZA-NATOR")
                        elif setor_atual == "CANCELAMENTO":
                            render_link_card("Acesso Rápido", "https://docs.google.com/spreadsheets/d/1y-7_w8RuzE2SSWatbdZj0SjsIa-aJyZCV0_1OxwD7bs/edit?gid=0#gid=0", "CLIENTE CRITICO", cor_borda="#ef4444")

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
                        
                        total_s = dado.get("csat_total", 0)
                        pos_s = dado.get("csat_pos", 0)
                        score_s = (pos_s / total_s * 100) if total_s > 0 else 0.0
                        
                        col1, col2, col3, col4, col5, col6 = st.columns(6)
                        
                        with col1: render_kpi_card("Volume", str(dado.get("num_qtd", 0)), "Atendimentos", "#8b5cf6")
                        cor_s = "#10b981" if score_s >= 85 else ("#f59e0b" if score_s >= 75 else "#ef4444")
                        with col2: render_kpi_card("Satisfação", f"{score_s:.2f}%", f"Base: {total_s}", cor_s)
                        with col3: render_kpi_card("T.M.A", str(dado.get("tma", "--:--")), "Tempo Médio", "#3b82f6")
                        with col4: render_kpi_card("T.M.E", str(dado.get("tme", "--:--")), "Fila/Espera", "#ef4444")
                        with col5: render_kpi_card("T.M.I.A", str(dado.get("tmia", "--:--")), "Inatividade Agt", "#f59e0b")
                        with col6: render_kpi_card("T.M.I.C", str(dado.get("tmic", "--:--")), "Inatividade Cli", "#6366f1")

                    if setor_atual == "SUPORTE":
                        st.markdown("---")
                        st.markdown("#### 🌙 Visão Global do Plantão (Madrugada)")
                        st.info("A equipe do plantão atende múltiplos serviços (Comercial, Financeiro, etc.). Abaixo estão os dados consolidados exclusivamente deles.")
                        
                        df_plantao, stats_servico_plantao, _ = buscar_dados_plantao(token, d_inicial, d_final, contas_tuple)
                        vol_total_plantao = df_plantao['Volume'].sum() if not df_plantao.empty else 0
                        
                        c_p1, c_p2, c_p3 = st.columns([1, 1, 1])
                        with c_p1:
                            render_kpi_card("Volumetria Geral Plantão", str(vol_total_plantao), "Soma de todos os serviços", "#8b5cf6")
                            
                        for servico, dado in stats_servico_plantao.items():
                            st.markdown(f"<div class='service-header'>{servico} (Plantão)</div>", unsafe_allow_html=True)
                            
                            tot_p = dado.get("csat_tot", 0)
                            pos_p = dado.get("csat_pos", 0)
                            score_p = (pos_p / tot_p * 100) if tot_p > 0 else 0.0
                            cor_p = "#10b981" if score_p >= 85 else ("#f59e0b" if score_p >= 75 else "#ef4444")
                            
                            col1, col2, col3, col4, col5, col6 = st.columns(6)
                            with col1: render_kpi_card("Volume", str(dado.get("num_qtd", 0)), "Atendimentos", "#8b5cf6")
                            with col2: render_kpi_card("Satisfação", f"{score_p:.2f}%", f"Base: {tot_p}", cor_p)
                            with col3: render_kpi_card("T.M.A", str(dado.get("tma", "--:--")), "Tempo Médio", "#3b82f6")
                            with col4: render_kpi_card("T.M.E", str(dado.get("tme", "--:--")), "Fila/Espera", "#ef4444")
                            with col5: render_kpi_card("T.M.I.A", str(dado.get("tmia", "--:--")), "Inatividade Agt", "#f59e0b")
                            with col6: render_kpi_card("T.M.I.C", str(dado.get("tmic", "--:--")), "Inatividade Cli", "#6366f1")
        
        # ABA 2: RANKINGS
        with abas_sup[1]:
            if token and 'mapa_agentes_filtrado' in locals():
                with st.spinner("Calculando o MVP do Mês..."):
                    lista_rank = processar_ranking_geral(token, d_inicial, d_final, mapa_agentes_filtrado, contas_tuple)
                    _, _, lista_logins, _ = processar_dados_pausas_supervisor(token, d_inicial, d_final, mapa_agentes_filtrado)
                    
                    if lista_rank:
                        df_rank = pd.DataFrame(lista_rank)
                        mvp_nome = eleger_melhor_do_mes(df_rank)
                        
                        atrasos_dict = defaultdict(int)
                        if lista_logins:
                            for l in lista_logins:
                                atrasos_dict[l['Agente']] += 1
                                
                        df_rank['Atrasos (Informativo)'] = df_rank['Agente'].map(lambda x: atrasos_dict[x]).fillna(0).astype(int)
                        df_rank['Atrasos (Informativo)'] = df_rank['Atrasos (Informativo)'].apply(lambda x: f"⚠️ {x} atrasos" if x > 0 else "🟢 OK")

                        if mvp_nome:
                            st.markdown(f"""<div class="mvp-card"><div style="font-size: 1rem; opacity: 0.8; text-transform: uppercase;">⭐ Destaque do Período ⭐</div><div style="font-size: 2.5rem; font-weight: 800; margin: 10px 0;">{mvp_nome}</div><div style="font-size: 0.9rem;">Melhor equilíbrio rigoroso entre TMA, TMIA e Satisfação</div></div>""", unsafe_allow_html=True)

                        st.markdown("### 🚀 Top Produtividade (Volume)")
                        render_podium("Campeões de Volume", lista_rank, "Volume", "")
                        
                        st.markdown("---")
                        
                        st.markdown("### ⭐ Top Qualidade (CSAT)")
                        render_podium("Campeões de Nota", lista_rank, "CSAT Score", "%")
                        
                        st.markdown("---")
                        
                        st.markdown("#### 📊 Tabela Geral de Desempenho (Todos os Tempos)")
                        df_display = df_rank[['Agente', 'Volume', 'TMA', 'TME', 'TMIA', 'TMIC', 'CSAT Score', 'CSAT Qtd', 'Atrasos (Informativo)']].sort_values(by="Volume", ascending=False)
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
            if token and 'mapa_agentes_filtrado' in locals():
                lista_curtas, lista_almoco, lista_logins, lista_ranking = processar_dados_pausas_supervisor(token, d_inicial, d_final, mapa_agentes_filtrado)
                c_p1, c_p2 = st.columns(2)
                with c_p1:
                    st.subheader("1. 🚨 Risco de Estouro (Manhã/Tarde)")
                    if lista_curtas:
                        df_c = pd.DataFrame(lista_curtas).sort_values(by="Valor Num", ascending=False)
                        st.dataframe(df_c[['Agente', 'Excesso Acumulado', 'Status']], use_container_width=True, hide_index=True)
                    else: st.success("Ninguém estourou!")
                with c_p2:
                    st.subheader("2. 🍽️ Atrasos de Almoço")
                    if lista_almoco:
                        st.dataframe(pd.DataFrame(lista_almoco), use_container_width=True, hide_index=True)
                    else: st.success("Sem atrasos.")
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
                    lista_pre_pausas = processar_dados_pre_pausas_geral(token, d_inicial, d_final, mapa_agentes_filtrado)
                    if lista_pre_pausas:
                        df_pre = pd.DataFrame(lista_pre_pausas)
                        contagem = df_pre['Agente'].value_counts()
                        agentes_ordenados = contagem.index.tolist()
                        
                        for agente in agentes_ordenados:
                            qtd = contagem[agente]
                            df_filtrado = df_pre[df_pre['Agente'] == agente].sort_values(by="Início", ascending=False)
                            with st.expander(f"➕ {agente} ({qtd} pré-pausas)"):
                                st.dataframe(df_filtrado[['Início', 'Término', 'Duração', 'Motivo']], use_container_width=True, hide_index=True)
                    else:
                        st.info("Nenhuma pré-pausa registrada no período.")
            else: st.info("Aguarde o carregamento da Visão Geral.")
            
        # ABA 4: TEMPO REAL
        with abas_sup[3]:
            if token:
                if st.button("🔄 Atualizar Lista Online"): st.rerun()
                
                if setor_atual == "NRC":
                    lista_online_bruta = buscar_agentes_online_filtrado_nrc(token)
                else:
                    lista_online_bruta = buscar_agentes_online_filtrado_setor(token, setor_atual)
                
                lista_online = []
                lista_exclusao = [x.strip().upper() for x in (JOVENS_APRENDIZES_NRC if setor_atual == "NRC" else JOVENS_APRENDIZES_SUPORTE)]
                
                for agente_online in lista_online_bruta:
                    nome_online = str(agente_online.get("nom_agente", "")).upper()
                    is_excluido = False
                    for alvo in lista_exclusao:
                        if alvo in nome_online.split() or (" " in alvo and alvo in nome_online): 
                            is_excluido = True
                            break
                    if not is_excluido:
                        lista_online.append(agente_online)

                if lista_online:
                    st.markdown(f"### 🟢 {len(lista_online)} Agentes Online ({setor_atual})")
                    for agente_online in lista_online:
                        aid = agente_online.get("cod")
                        nome_online = agente_online.get("nom_agente", "Desconhecido")
                        status_online = agente_online.get("status", "Online")
                        tempo_online = agente_online.get("tempo_status", "--:--")
                        
                        cor_status_online = "#10b981"
                        if "Pausa" in status_online: cor_status_online = "#f59e0b"
                        
                        st.markdown(f"""
                        <div class="realtime-card">
                            <div style="flex:1;">
                                <div style="font-weight:bold; color:white; font-size:1.1rem;">{nome_online}</div>
                                <div style="color:#9ca3af; font-size:0.8rem;">ID: {aid}</div>
                            </div>
                            <div style="flex:1; text-align:center;">
                                <span style="background-color:{cor_status_online}; color:black; padding:2px 10px; border-radius:12px; font-weight:bold; font-size:0.8rem;">{status_online}</span>
                                <div style="margin-top:5px; color:#e5e7eb; font-family:monospace;">⏱ {tempo_online}</div>
                            </div>
                            <div style="flex:1; text-align:right;"></div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        col_btn = st.columns([4, 1])[1]
                        with col_btn:
                            if st.button("🔴 Deslogar", key=f"btn_logout_{aid}"):
                                with st.spinner(f"Deslogando {nome_online}..."):
                                    sucesso_logout, msg_logout = forcar_logout(token, aid)
                                    if sucesso_logout: 
                                        st.success(f"{nome_online} deslogado!")
                                        time.sleep(1); st.rerun()
                                    else: st.error(msg_logout)
                else:
                    st.warning(f"Nenhum agente regular da equipe {setor_atual} está online no momento.")
            else: st.error("Erro de conexão.")

        # ABA 5: SOLICITAÇÕES
        with abas_sup[4]:
            df_gsheets = ler_solicitacoes_gsheets()
            if not df_gsheets.empty: st.dataframe(df_gsheets, use_container_width=True)
            else: st.warning("Nenhuma solicitação encontrada na planilha.")

        # ABA 6: DIÁRIO DE BORDO
        with abas_sup[5]:
            st.markdown("### 📝 Diário de Bordo da Supervisão")
            with st.form("form_diario_bordo"):
                lista_agentes_diario = ["Geral (Equipe)"] + sorted(list(mapa_agentes_sidebar.values())) if 'mapa_agentes_sidebar' in locals() else ["Geral (Equipe)"]
                col_d1, col_d2 = st.columns(2)
                with col_d1: agente_selecionado = st.selectbox("Agente Relacionado", lista_agentes_diario)
                with col_d2: tipo_ponto = st.selectbox("Tipo de Registro", ["Advertência", "Atestado/Falta", "Feedback Comportamental", "Feedback Técnico", "Elogio/Destaque", "Problema Sistêmico", "Outros"])
                texto_diario = st.text_area("Descrição detalhada do ponto:", height=120)
                btn_diario = st.form_submit_button("💾 Registrar no Diário", use_container_width=True)
                if btn_diario:
                    if texto_diario:
                        with st.spinner("Salvando..."):
                            sucesso_db, msg_db = salvar_diario_bordo(st.session_state.user_data['nome'], setor_atual, agente_selecionado, tipo_ponto, texto_diario)
                            if sucesso_db: st.success(msg_db); time.sleep(1); st.rerun()
                            else: st.error(msg_db)
            
            st.markdown("---")
            df_diario = ler_diario_bordo(setor_atual if st.session_state.user_role != "master" else None)
            if not df_diario.empty:
                st.dataframe(df_diario, use_container_width=True, hide_index=True)
            else: st.warning("Nenhum registro encontrado.")

        # ABA 7: JOVEM APRENDIZ
        if setor_atual in ["NRC", "SUPORTE"]:
            idx_jovem = 6
            with abas_sup[idx_jovem]:
                st.markdown(f"### 👶 Painel Jovem Aprendiz - {setor_atual}")
                lista_ja = JOVENS_APRENDIZES_NRC if setor_atual == "NRC" else JOVENS_APRENDIZES_SUPORTE
                
                if token:
                    with st.spinner("Analisando dados dos Jovens Aprendizes..."):
                        stats_ja, ranking_ja, df_pesquisas_ja, score_ja_global = buscar_dados_jovem_aprendiz(token, d_inicial, d_final, setor_atual, contas_tuple)
                        
                        st.markdown("#### 📊 Visão Geral da Equipe Jovem Aprendiz")
                        c1, c2, c3, c4 = st.columns(4)
                        with c1: render_kpi_card("Volume Total", str(stats_ja["Volume"]), "Atendimentos", "#8b5cf6")
                        cor_csat_ja = "#10b981" if score_ja_global >= 90 else "#ef4444"
                        with c2: render_kpi_card("CSAT Geral", f"{score_ja_global:.2f}%", "Satisfação Media", cor_csat_ja)
                        with c3: render_kpi_card("T.M.A Equipe", stats_ja["TMA"], "Tempo Médio", "#3b82f6")
                        with c4: render_kpi_card("T.M.I.A Equipe", stats_ja["TMIA"], "Ociosidade", "#f59e0b")
                        
                        st.markdown("---")
                        
                        st.markdown("#### 🏆 Desempenho Individual e Alertas")
                        st.info("🚨 **Regra de Alerta:** Status Vermelho 🔴 disparado se TMIA for maior que 1 minuto (00:01:00) ou CSAT geral ficar menor que 90%.")
                        
                        if ranking_ja:
                            df_rank_ja = pd.DataFrame(ranking_ja).sort_values(by="Volume", ascending=False)
                            st.dataframe(
                                df_rank_ja, 
                                column_config={
                                    "CSAT": st.column_config.NumberColumn("CSAT Score", format="%.2f%%"),
                                    "Status": st.column_config.TextColumn("Status do Aprendiz")
                                }, 
                                use_container_width=True, hide_index=True
                            )
                        else:
                            st.warning("Nenhum atendimento registrado pelos jovens aprendizes no período.")
                            
                        st.markdown("---")
                        
                        st.markdown("#### ⭐ Avaliações Individuais (Feedbacks)")
                        if not df_pesquisas_ja.empty:
                            df_pesquisas_ja['Acesso'] = df_pesquisas_ja['Protocolo'].apply(gerar_link_protocolo)
                            st.dataframe(
                                df_pesquisas_ja[['Data', 'Agente', 'Cliente', 'Nota', 'Comentario', 'Acesso']], 
                                column_config={
                                    "Acesso": st.column_config.LinkColumn("Link", display_text="Abrir Atendimento"),
                                    "Nota": st.column_config.NumberColumn("Nota", format="%d ⭐")
                                }, 
                                use_container_width=True, hide_index=True
                            )
                        else:
                            st.info("Sem pesquisas de satisfação registradas para os jovens aprendizes neste período.")

        # ABAS EXTRAS SUPORTE
        if setor_atual == "SUPORTE":
            with abas_sup[7]: # Plantão
                df_plantao, stats_servico_plantao, _ = buscar_dados_plantao(token, d_inicial, d_final, contas_tuple)
                if not df_plantao.empty:
                    st.dataframe(df_plantao, use_container_width=True, hide_index=True)
                else: st.warning("Sem dados.")

            with abas_sup[8]: # Cliente Interno
                stats_ci, score_ci, total_ci, df_ci = buscar_dados_cliente_interno(token, d_inicial, d_final, SETORES_AGENTES["SUPORTE"])
                render_kpi_card("CSAT Interno", f"{score_ci:.2f}%", f"Base: {total_ci}", "#10b981")
                if not df_ci.empty: st.dataframe(df_ci, use_container_width=True, hide_index=True)
