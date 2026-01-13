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
    
    # Credenciais Supervisors
    SUPERVISOR_CANCELAMENTO_LOGIN = st.secrets["auth"].get("SUPERVISOR_CANCELAMENTO_LOGIN", "admin_cancel")
    SUPERVISOR_CANCELAMENTO_PASS = st.secrets["auth"].get("SUPERVISOR_CANCELAMENTO_PASS", "senha_cancel")
    
    SUPERVISOR_SUPORTE_LOGIN = st.secrets["auth"].get("SUPERVISOR_SUPORTE_LOGIN", "admin_sup")
    SUPERVISOR_SUPORTE_PASS = st.secrets["auth"].get("SUPERVISOR_SUPORTE_PASS", "senha_sup")
    
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
# 1.1 CONFIGURAÇÃO DE NOVOS SETORES (ADICIONADO - NÃO ALTERA O ORIGINAL)
# ==============================================================================
SETORES_AGENTES = {
    "NRC": LISTA_NRC, 
    "CANCELAMENTO": ['BARBOSA', 'ELOISA', 'LARISSA', 'EDUARDO', 'CAMILA', 'SAMARA'],
    "NEGOCIACAO": ['Carla', 'Lenk', 'Ana Luiza', 'JULIETTI', 'RODRIGO', 'Monalisa', 'Ramom', 'Ednael', 'Leticia', 'Rita', 'Mariana', 'Flavia s', 'Uri', 'Clara', 'Wanderson', 'Aparecida', 'Cristina', 'Caio', 'LUKAS'],
    "SUPORTE": ['VALERIO', 'TARCISIO', 'GRANJA', 'ALICE', 'FERNANDO', 'SANTOS', 'RENAN', 'FERREIRA', 'HUEMILLY', 'LOPES', 'LAUDEMILSON', 'RAYANE', 'LAYS', 'JORGE', 'LIGIA', 'ALESSANDRO', 'GEIBSON', 'ROBERTO', 'OLIVEIRA', 'MAURÍCIO', 'AVOLO', 'CLEBER', 'ROMERIO', 'JUNIOR', 'ISABELA', 'RENAN', 'WAGNER', 'CLAUDIA', 'ANTONIO', 'JOSE', 'LEONARDO', 'KLEBSON', 'OZENAIDE']
}

SETORES_SERVICOS = {
    "NRC": SERVICOS_ALVO,
    "CANCELAMENTO": ['CANCELAMENTO'], 
    "NEGOCIACAO": ['NEGOCIACAO', 'RENEGOCIACAO'],
    "SUPORTE": ['SUPORTE', 'LIBERAÇÃO'] # Filtra apenas esses dois serviços no painel geral
}

# Configurações Especiais Suporte
LISTA_PLANTAO = ['TARCISIO', 'GEIBSON', 'LEONARDO', 'FERNANDO', 'RENAN']
ID_CONTA_CLIENTE_INTERNO = "5"

# REGRAS DE PAUSA
LIMITES_PAUSA = { "CURTA": 15.0, "LONGA": 120.0 }
TOLERANCIA_MENSAL_EXCESSO = 20.0 
TOLERANCIA_VISUAL_ALMOCO = 2.0

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
    """
    Conecta a uma aba específica da planilha Google Sheets.
    Usado para o Diário de Bordo (aba Feedback_Gestao).
    """
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        # Abre a planilha pelo nome do arquivo principal
        spreadsheet = client.open("solicitacoes_nrc")
        # Seleciona a aba específica
        worksheet = spreadsheet.worksheet(nome_aba)
        return worksheet
    except Exception as erro:
        return None

def salvar_solicitacao_gsheets(nome_agente, id_agente, motivo, mensagem):
    sheet = conectar_gsheets()
    if sheet:
        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        try:
            # Adiciona linha: Data, ID, Nome, Motivo, Mensagem
            sheet.append_row([data_hora, id_agente, nome_agente, motivo, mensagem])
            return True, "Solicitação salva na nuvem com sucesso!"
        except Exception as erro:
            return False, f"Erro ao escrever na planilha: {erro}"
    else:
        return False, "Erro de conexão com Google Sheets. Verifique o compartilhamento."

def salvar_diario_bordo(supervisor_nome, setor_atual, nome_agente, tipo_ponto, descricao):
    """
    Salva o feedback do supervisor na aba Feedback_Gestao.
    """
    sheet = conectar_gsheets_aba("Feedback_Gestao")
    if sheet:
        data_hora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        try:
            # Colunas esperadas na aba Feedback_Gestao:
            # A: Data | B: Supervisor | C: Setor | D: Agente | E: Tipo | F: Descrição | G: Validação Gestão
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
    """
    Lê os registros do Diário de Bordo para exibir ao supervisor.
    Filtra pelo setor se necessário.
    """
    sheet = conectar_gsheets_aba("Feedback_Gestao")
    if sheet:
        try:
            data = sheet.get_all_records()
            df = pd.DataFrame(data)
            if not df.empty and setor_filtro:
                # Filtra apenas os registros do setor do supervisor logado
                # Assume que a coluna se chama 'Setor' no Google Sheets
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
    """Busca agentes online e filtra apenas os do NRC."""
    headers = {"Authorization": f"Bearer {token}"}
    agentes_online_nrc = []
    
    nrc_upper = [x.strip().upper() for x in LISTA_NRC]
    
    try:
        r = requests.get(f"{BASE_URL}/agentesOnline", headers=headers)
        if r.status_code == 200:
            todos_online = r.json()
            for agente in todos_online:
                nome_full = str(agente.get("nom_agente", "")).strip().upper()
                partes_nome = nome_full.split()
                if not partes_nome: continue
                
                match_encontrado = False
                for alvo in nrc_upper:
                    if alvo in NOMES_COMUNS_PRIMEIRO:
                        if alvo == partes_nome[0]: match_encontrado = True; break
                    else:
                        if alvo in partes_nome: match_encontrado = True; break
                
                if match_encontrado:
                    agentes_online_nrc.append(agente)
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
    mapa_agentes = {}
    nrc_upper = [x.strip().upper() for x in LISTA_NRC]
    
    # 1. Mapeamento de Agentes
    pagina = 1
    while True:
        try:
            r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": pagina, "bol_cancelado": 0})
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
                    if alvo in NOMES_COMUNS_PRIMEIRO:
                        if alvo == partes_nome[0]: match_encontrado = True; break
                    else:
                        if alvo in partes_nome: match_encontrado = True; break
                if match_encontrado: 
                    cod = str(agente.get("cod_agente"))
                    ids_agentes.append(cod)
                    mapa_agentes[cod] = nome_upper
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break

    ids_canais = buscar_ids_canais(token)
    
    # Estrutura expandida para os 6 cards
    resultados = {
        s: {
            "num_qtd": 0, 
            "tma": "--:--", 
            "tme": "--:--", 
            "tmia": "--:--", 
            "tmic": "--:--", 
            "csat_pos": 0, 
            "csat_total": 0
        } 
        for s in SERVICOS_ALVO
    }

    if not ids_agentes: return resultados, 0.0, 0, mapa_agentes, dados_globais
    
    # 2. Dados Globais NRC (Estatísticas Agrupadas por Conta filtradas pelos Agentes NRC)
    # Importante: Como queremos os totais dos agentes do NRC, podemos usar o agrupador 'conta'
    # mas filtrando pelos IDs dos agentes. A API deve retornar o somatório.
    dados_globais = {"tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--"}
    
    try:
        params_globais = {
            "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
            "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
            "agrupador": "conta", 
            "agente[]": ids_agentes,
            "canal[]": ids_canais,
            "id_conta": ID_CONTA
        }
        r_global = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_globais)
        if r_global.status_code == 200:
            lista_global = r_global.json()
            if lista_global and isinstance(lista_global, list):
                item_global = lista_global[0]
                dados_globais["tma"] = item_global.get("tma", "--:--")
                dados_globais["tme"] = item_global.get("tme", "--:--")
                dados_globais["tmia"] = item_global.get("tmia", "--:--")
                dados_globais["tmic"] = item_global.get("tmic", "--:--")
    except: pass

    # 3. Estatísticas (Por Serviço)
    for servico in SERVICOS_ALVO:
        params = {
            "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
            "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
            "agrupador": "servico",
            "agente[]": ids_agentes,
            "canal[]": ids_canais,
            "id_conta": ID_CONTA,
            "servico": servico
        }
        try:
            r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params)
            if r.status_code == 200:
                lista = r.json()
                if lista and isinstance(lista, list):
                    item = lista[0]
                    qtd_bruta = int(item.get("num_qtd", 0))
                    qtd_aband = int(item.get("num_qtd_abandonado", 0))
                    resultados[servico]["num_qtd"] = qtd_bruta - qtd_aband
                    resultados[servico]["tma"] = item.get("tma", "--:--")
                    resultados[servico]["tme"] = item.get("tme", "--:--")
                    resultados[servico]["tmia"] = item.get("tmia", "--:--")
                    resultados[servico]["tmic"] = item.get("tmic", "--:--")
        except: pass

    # 4. Satisfação (CSAT)
    csat_geral_pos = 0; csat_geral_total = 0
    for p_id in PESQUISAS_IDS:
        p_page = 1
        while True:
            p_params = {
                "data_inicial": data_ini.strftime("%Y-%m-%d"), 
                "data_final": data_fim.strftime("%Y-%m-%d"), 
                "pesquisa": p_id, "id_conta": ID_CONTA, "limit": 1000, 
                "page": p_page, "agente[]": ids_agentes
            }
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
                                val_raw = resp.get("nom_valor")
                                if val_raw and val_raw != "": nota = int(float(val_raw))
                                else: nota = -1
                                
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
    return resultados, score_geral, csat_geral_total, mapa_agentes, dados_globais

def _processar_agente_pausas(token, cod_agente, nome_agente, data_ini, data_fim):
    headers = {"Authorization": f"Bearer {token}"}
    local_curtas, local_almoco, local_logins, local_ranking = [], [], [], []
    
    # 1. PAUSAS
    pausas_agente = []
    pagina = 1
    while True:
        if pagina > 5: break
        params = {"dat_inicial": data_ini.strftime("%Y-%m-%d"), "dat_final": data_fim.strftime("%Y-%m-%d"), "cod_agente": cod_agente, "limit": 100, "pagina": pagina}
        try:
            r = requests.get(f"{BASE_URL}/relAgentePausa", headers=headers, params=params, timeout=10)
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
        
        # Pausa Curta
        if any(x in motivo for x in ["MANHA", "MANHÃ", "TARDE", "NOITE"]):
            if minutos > LIMITES_PAUSA["CURTA"]:
                acumulado_excesso_curta += (minutos - LIMITES_PAUSA["CURTA"])
                
        # Almoço
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

    # 2. LOGINS
    page_log = 1
    logins_raw = []
    while page_log <= 2:
        params_log = {"data_inicial": data_ini.strftime("%Y-%m-%d"), "data_final": data_fim.strftime("%Y-%m-%d"), "agente": cod_agente, "page": page_log, "limit": 100}
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
            dt = datetime.strptime(d_str, "%Y-%m-%d %H:%M:%S")
            d_key = dt.strftime("%Y-%m-%d")
            if d_key not in min_logins: min_logins[d_key] = dt
            else:
                if dt < min_logins[d_key]: min_logins[d_key] = dt
        except: pass
        
    for d, dt in min_logins.items():
        mins = dt.minute
        # Regra Pontualidade: 02 a 55 = Atraso (Ajustado conforme solicitação).
        if 1 < mins <= 55:
            local_logins.append({
                "Agente": nome_agente,
                "Data": d,
                "Hora Entrada": dt.strftime("%H:%M:%S"),
                "Atraso": f"{mins}m"
            })
            
    return local_curtas, local_almoco, local_logins, local_ranking

@st.cache_data(ttl=300)
def processar_dados_pausas_supervisor(token, data_ini, data_fim, mapa_agentes):
    """Processamento Paralelo: Pausas e Logins"""
    curtas, almoco, logins, ranking = [], [], [], []
    barra_progresso = st.progress(0, text="Auditando pausas e horários...")
    total = len(mapa_agentes)
    concluidos = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
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
def processar_ranking_geral(token, data_ini, data_fim, mapa_agentes):
    """Gera dados completos: Volume, TMA, TME, TMIA, TMIC e CSAT"""
    headers = {"Authorization": f"Bearer {token}"}
    lista_rank = []
    ids_validos = list(mapa_agentes.keys())
    ids_canais = buscar_ids_canais(token)
    
    # 1. Volume e Tempos
    dados_stats = {cod: {"Vol": 0, "TMA": "--:--", "TME": "--:--", "TMIA": "--:--", "TMIC": "--:--"} for cod in ids_validos}
    
    params_stats = {
        "data_inicial": f"{data_ini} 00:00:00",
        "data_final": f"{data_fim} 23:59:59",
        "agrupador": "agente",
        "agente[]": ids_validos,
        "canal[]": ids_canais,
        "id_conta": ID_CONTA
    }
    try:
        r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_stats)
        if r.status_code == 200:
            for item in r.json():
                nome_api = str(item.get("agrupador", "")).upper()
                cod_match = next((c for c, n in mapa_agentes.items() if n == nome_api or n in nome_api), None)
                if cod_match:
                    qtd = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                    dados_stats[cod_match]["Vol"] += qtd
                    dados_stats[cod_match]["TMA"] = item.get("tma", "--:--")
                    dados_stats[cod_match]["TME"] = item.get("tme", "--:--")
                    dados_stats[cod_match]["TMIA"] = item.get("tmia", "--:--")
                    dados_stats[cod_match]["TMIC"] = item.get("tmic", "--:--")
    except: pass

    # 2. CSAT Individual (Paralelo)
    def _fetch_csat_agente(cod_ag):
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
        futs = {executor.submit(_fetch_csat_agente, cod): cod for cod in ids_validos}
        for f in concurrent.futures.as_completed(futs):
            cod = futs[f]
            try:
                p, t = f.result()
                dados_csat[cod] = (p, t)
            except: dados_csat[cod] = (0, 0)
            
    # Monta Lista Final
    for cod, nome in mapa_agentes.items():
        st_data = dados_stats[cod]
        pos, tot = dados_csat.get(cod, (0, 0))
        score = (pos/tot*100) if tot > 0 else 0.0
        
        if st_data["Vol"] > 0 or tot > 0:
            lista_rank.append({
                "Agente": nome,
                "Volume": st_data["Vol"],
                "TMA": st_data["TMA"],
                "TME": st_data["TME"],
                "TMIA": st_data["TMIA"],
                "TMIC": st_data["TMIC"],
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
# 5.1 FUNÇÕES MULTISETOR (NOVAS - ADICIONADAS PARA NÃO ALTERAR AS ANTIGAS)
# ==============================================================================

def buscar_agentes_online_filtrado_setor(token, setor_nome):
    """Versão genérica do monitoramento online para qualquer setor."""
    headers = {"Authorization": f"Bearer {token}"}
    agentes_online_filtrados = []
    
    # Obtém a lista de agentes do setor especificado
    lista_alvo = SETORES_AGENTES.get(setor_nome, [])
    lista_upper = [x.strip().upper() for x in lista_alvo]
    
    try:
        r = requests.get(f"{BASE_URL}/agentesOnline", headers=headers)
        if r.status_code == 200:
            todos_online = r.json()
            for agente in todos_online:
                nome_full = str(agente.get("nom_agente", "")).strip().upper()
                partes_nome = nome_full.split()
                if not partes_nome: continue
                
                match_encontrado = False
                for alvo in lista_upper:
                    # Verifica nome exato ou contido nas partes
                    if alvo in partes_nome: match_encontrado = True; break
                    # Verifica logica de primeiro nome se necessario (adaptado do original)
                    if alvo in NOMES_COMUNS_PRIMEIRO and alvo == partes_nome[0]: match_encontrado = True; break
                
                if match_encontrado:
                    agentes_online_filtrados.append(agente)
    except: pass
    return agentes_online_filtrados

@st.cache_data(ttl=300)
def buscar_dados_supervisor_multisetor(token, data_ini, data_fim, setor_nome):
    """Versão genérica da busca de dados do supervisor para qualquer setor."""
    headers = {"Authorization": f"Bearer {token}"}
    ids_agentes = []
    mapa_agentes = {}
    
    # Define as listas baseadas no setor escolhido
    lista_nomes_alvo = SETORES_AGENTES.get(setor_nome, [])
    lista_servicos_alvo = SETORES_SERVICOS.get(setor_nome, [])
    
    nomes_upper = [x.strip().upper() for x in lista_nomes_alvo]
    
    # 1. Mapeamento de Agentes (Igual ao original, mas usando a lista dinâmica)
    pagina = 1
    while True:
        try:
            r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": pagina, "bol_cancelado": 0})
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
                if match_encontrado: 
                    cod = str(agente.get("cod_agente"))
                    ids_agentes.append(cod)
                    mapa_agentes[cod] = nome_upper
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break

    ids_canais = buscar_ids_canais(token)
    
    # Inicializa resultados com os serviços deste setor
    resultados = {
        s: {
            "num_qtd": 0, "tma": "--:--", "tme": "--:--", 
            "tmia": "--:--", "tmic": "--:--", "csat_pos": 0, "csat_total": 0
        } 
        for s in lista_servicos_alvo
    }

    if not ids_agentes: return resultados, 0.0, 0, mapa_agentes, {}
    
    # 2. Dados Globais do Setor
    dados_globais = {"tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--"}
    try:
        params_globais = {
            "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
            "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
            "agrupador": "conta", 
            "agente[]": ids_agentes,
            "canal[]": ids_canais,
            "id_conta": ID_CONTA
        }
        r_global = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_globais)
        if r_global.status_code == 200:
            lista_global = r_global.json()
            if lista_global and isinstance(lista_global, list):
                item_global = lista_global[0]
                dados_globais["tma"] = item_global.get("tma", "--:--")
                dados_globais["tme"] = item_global.get("tme", "--:--")
                dados_globais["tmia"] = item_global.get("tmia", "--:--")
                dados_globais["tmic"] = item_global.get("tmic", "--:--")
    except: pass

    # 3. Estatísticas por Serviço (Usando lista_servicos_alvo)
    for servico in lista_servicos_alvo:
        params = {
            "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
            "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
            "agrupador": "servico",
            "agente[]": ids_agentes,
            "canal[]": ids_canais,
            "id_conta": ID_CONTA,
            "servico": servico
        }
        try:
            r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params)
            if r.status_code == 200:
                lista = r.json()
                if lista and isinstance(lista, list):
                    item = lista[0]
                    qtd_bruta = int(item.get("num_qtd", 0))
                    qtd_aband = int(item.get("num_qtd_abandonado", 0))
                    resultados[servico]["num_qtd"] = qtd_bruta - qtd_aband
                    resultados[servico]["tma"] = item.get("tma", "--:--")
                    resultados[servico]["tme"] = item.get("tme", "--:--")
                    resultados[servico]["tmia"] = item.get("tmia", "--:--")
                    resultados[servico]["tmic"] = item.get("tmic", "--:--")
        except: pass

    # 4. Satisfação (CSAT)
    csat_geral_pos = 0; csat_geral_total = 0
    for p_id in PESQUISAS_IDS:
        p_page = 1
        while True:
            p_params = {
                "data_inicial": data_ini.strftime("%Y-%m-%d"), 
                "data_final": data_fim.strftime("%Y-%m-%d"), 
                "pesquisa": p_id, "id_conta": ID_CONTA, "limit": 1000, 
                "page": p_page, "agente[]": ids_agentes
            }
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
                                val_raw = resp.get("nom_valor")
                                if val_raw and val_raw != "": nota = int(float(val_raw))
                                else: nota = -1
                                
                                if nota >= 0: 
                                    csat_geral_total += 1
                                    if nota >= 8: csat_geral_pos += 1
                                    # Verifica se o serviço pertence ao setor atual
                                    if servico_resp in lista_servicos_alvo:
                                        resultados[servico_resp]["csat_total"] += 1
                                        if nota >= 8: resultados[servico_resp]["csat_pos"] += 1
                            except: pass
                if (p_page * 1000) >= total_api: break
                if len(data) < 2: break
                p_page += 1
            except: break

    score_geral = (csat_geral_pos / csat_geral_total * 100) if csat_geral_total > 0 else 0.0
    return resultados, score_geral, csat_geral_total, mapa_agentes, dados_globais

# ==============================================================================
# 5.2 FUNÇÕES DE PRÉ-PAUSA
# ==============================================================================

@st.cache_data(ttl=60)
def buscar_pre_pausas_detalhado(token, id_agente, data_ini, data_fim):
    """Busca dados brutos de pré-pausas agendadas via endpoint dedicado."""
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
    """Processa pré-pausas para todos os agentes do mapa e lista individualmente."""
    resultados = []
    
    # Função interna para thread
    def _fetch_pre_pausa(cod, nome):
        raw_data = buscar_pre_pausas_detalhado(token, cod, data_ini, data_fim)
        if not raw_data: return []
        
        lista_retorno = []
        for p in raw_data:
            data_ini_str = p.get("data_pre", "")
            data_fim_str = p.get("data_fim", "")
            duracao_str = p.get("tempo_pre_pausado", "00:00:00")
            motivo_str = p.get("pausa", "Agendada") # Novo: Captura o motivo
            
            # Formatação de datas
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

    # Execução Paralela
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_fetch_pre_pausa, cod, nome): nome for cod, nome in mapa_agentes.items()}
        for f in concurrent.futures.as_completed(futures):
            try:
                res = f.result()
                if res: resultados.extend(res)
            except: pass
            
    return resultados

# ==============================================================================
# 5.3 FUNÇÕES ESPECIAIS SUPORTE
# ==============================================================================

@st.cache_data(ttl=300)
def buscar_dados_plantao(token, data_ini, data_fim):
    """Busca dados específicos para os agentes do plantão (LISTA_PLANTAO) com estatísticas por serviço e CSAT."""
    headers = {"Authorization": f"Bearer {token}"}
    ids_plantao = []
    mapa_plantao = {}
    
    # 1. Identificar IDs dos agentes do plantão
    pagina = 1
    nomes_upper = [x.strip().upper() for x in LISTA_PLANTAO]
    
    while True:
        try:
            r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": pagina, "bol_cancelado": 0})
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

    # 2. Buscar estatísticas GERAIS (agrupado por agente)
    lista_stats_agente = []
    params_agente = {
        "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
        "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
        "agrupador": "agente", 
        "agente[]": ids_plantao,
        "canal[]": ids_canais,
        "id_conta": ID_CONTA
    }
    try:
        r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_agente)
        if r.status_code == 200:
            for item in r.json():
                nome_api = str(item.get("agrupador", "")).upper()
                agente_nome = next((n for c, n in mapa_plantao.items() if n == nome_api or n in nome_api), nome_api)
                
                # Inicializa stats do agente
                lista_stats_agente.append({
                    "Agente": agente_nome,
                    "Volume": int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0)),
                    "TMA": item.get("tma", "--:--"),
                    "TMIA": item.get("tmia", "--:--"),
                    "CSAT": 0.0, # Placeholder
                    "Qtd CSAT": 0
                })
    except: pass
    
    # 3. Buscar estatísticas por SERVIÇO (Agrupado por serviço, filtrado pelos agentes)
    stats_por_servico = {}
    params_servico = {
        "data_inicial": f"{data_ini.strftime('%Y-%m-%d')} 00:00:00",
        "data_final": f"{data_fim.strftime('%Y-%m-%d')} 23:59:59",
        "agrupador": "servico", 
        "agente[]": ids_plantao, # Filtro Crucial
        "canal[]": ids_canais,
        "id_conta": ID_CONTA
    }
    try:
        r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_servico)
        if r.status_code == 200:
            for item in r.json():
                serv = item.get("agrupador", "Outros")
                stats_por_servico[serv] = {
                    "num_qtd": int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0)),
                    "tma": item.get("tma", "--:--"),
                    "tme": item.get("tme", "--:--"),
                    "tmia": item.get("tmia", "--:--"),
                    "tmic": item.get("tmic", "--:--")
                }
    except: pass

    # 4. Calcular CSAT (Satisfação) dos agentes
    csat_scores = {}
    for pid in PESQUISAS_IDS:
        pg = 1
        while True:
            pars = {"data_inicial": data_ini.strftime("%Y-%m-%d"), "data_final": data_fim.strftime("%Y-%m-%d"), "pesquisa": pid, "id_conta": ID_CONTA, "limit": 1000, "page": pg, "agente[]": ids_plantao}
            try:
                rr = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=pars)
                if rr.status_code != 200: break
                dd = rr.json()
                if not dd or not isinstance(dd, list): break
                total_k = 0
                for b in dd:
                    if str(b.get("id_pergunta","")) in IDS_PERGUNTAS_VALIDAS:
                        if b.get("sintetico"): total_k += sum(int(x.get("num_quantidade", 0)) for x in b["sintetico"])
                        for rsp in b.get("respostas", []):
                            nom_ag = str(rsp.get("nom_agente","")).upper()
                            # Normaliza nome
                            nome_match = next((n for c, n in mapa_plantao.items() if n == nom_ag or n in nom_ag), nom_ag)
                            
                            val = float(rsp.get("nom_valor", -1))
                            if val >= 0:
                                if nome_match not in csat_scores: csat_scores[nome_match] = {"pos": 0, "tot": 0}
                                csat_scores[nome_match]["tot"] += 1
                                if val >= 8: csat_scores[nome_match]["pos"] += 1
                if (pg * 1000) >= total_k: break
                if len(dd) < 2: break
                pg += 1
            except: break
            
    # 5. Unificar CSAT na tabela
    for row in lista_stats_agente:
        ag = row["Agente"]
        if ag in csat_scores:
            d = csat_scores[ag]
            row["Qtd CSAT"] = d["tot"]
            row["CSAT"] = (d["pos"] / d["tot"] * 100) if d["tot"] > 0 else 0.0
            
    return pd.DataFrame(lista_stats_agente), stats_por_servico, {}

@st.cache_data(ttl=300)
def buscar_dados_cliente_interno(token, data_ini, data_fim, nomes_suporte_validos):
    """Busca dados da conta ID 5 (Cliente Interno) filtrando por agentes do Suporte."""
    headers = {"Authorization": f"Bearer {token}"}
    ids_canais = buscar_ids_canais(token) 
    
    # Prepara lista para filtro
    nomes_validos_upper = [x.strip().upper() for x in nomes_suporte_validos]
    
    # 1. Estatísticas Gerais (Card de Tempo)
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
        if r.status_code == 200:
            lista = r.json()
            if lista and isinstance(lista, list):
                item = lista[0]
                stats_globais["TMA"] = item.get("tma", "--:--")
                stats_globais["TME"] = item.get("tme", "--:--")
                stats_globais["TMIA"] = item.get("tmia", "--:--")
    except: pass
    
    # 2. Pesquisas (Tabela e CSAT)
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
                if r.status_code != 200: break
                data = r.json()
                if not data or not isinstance(data, list): break
                
                total_api = 0
                for bloco in data:
                    if str(bloco.get("id_pergunta", "")) in IDS_PERGUNTAS_VALIDAS:
                        if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                        for resp in bloco.get("respostas", []):
                            # FILTRO DE AGENTE (Regra de Ouro: Adicionando validação)
                            nome_agente_resp = str(resp.get("nom_agente", "")).strip().upper()
                            partes_nome = nome_agente_resp.split()
                            eh_do_suporte = False
                            if partes_nome:
                                for alvo in nomes_validos_upper:
                                    if alvo in partes_nome: eh_do_suporte = True; break
                            
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
        with st.expander("📜 Versão Platinum 15.6"):
            st.markdown("""
            **v15.6 - Visual Otimizado**
            - **Pré-Pausas:** Lista ordenada por quantidade em formato 'Sanfona'.
            - **Plantão:** Cards detalhados com todas as métricas de tempo.
            - **Monitoramento:** Aviso visual sobre pontualidade em turnos da madrugada.
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
    st.session_state.user_setor = "NRC" # Padrão

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
                # 1. Login Supervisor NRC (Original)
                if usuario == SUPERVISOR_LOGIN and senha == SUPERVISOR_PASS:
                    st.session_state.auth_status = True
                    st.session_state.user_role = "supervisor"
                    st.session_state.user_setor = "NRC"
                    st.session_state.user_data = {"nome": "Supervisor NRC", "id": "SUPERVISOR"}
                    st.rerun()
                
                # 2. Login Supervisor CANCELAMENTO (Novo)
                elif usuario == SUPERVISOR_CANCELAMENTO_LOGIN and senha == SUPERVISOR_CANCELAMENTO_PASS:
                    st.session_state.auth_status = True
                    st.session_state.user_role = "supervisor"
                    st.session_state.user_setor = "CANCELAMENTO"
                    st.session_state.user_data = {"nome": "Supervisor Cancelamento", "id": "SUPERVISOR_CANC"}
                    st.rerun()

                # 3. Login Supervisor SUPORTE (Novo)
                elif usuario == SUPERVISOR_SUPORTE_LOGIN and senha == SUPERVISOR_SUPORTE_PASS:
                    st.session_state.auth_status = True
                    st.session_state.user_role = "supervisor"
                    st.session_state.user_setor = "SUPORTE"
                    st.session_state.user_data = {"nome": "Supervisor Suporte", "id": "SUPERVISOR_SUP"}
                    st.rerun()
                    
                # 4. Login Agente (Genérico)
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
    
    # Define o setor atual com base no login
    setor_atual = st.session_state.get("user_setor", "NRC")
    
    # LOGICA DE VISÃO SUPERVISOR (GERAL OU INDIVIDUAL)
    modo_visao_supervisor = "Geral"
    id_alvo = None
    nome_alvo = None

    if st.session_state.user_role == "supervisor":
        # Carrega lista apenas para o selectbox
        # SE FOR NRC, USA LOGICA ANTIGA. SE FOR OUTRO, USA NOVA.
        with st.spinner("Carregando equipe..."):
            if setor_atual == "NRC":
                _, _, _, mapa_agentes_sidebar, _ = buscar_dados_completos_supervisor(token, d_inicial, d_final)
            else:
                _, _, _, mapa_agentes_sidebar, _ = buscar_dados_supervisor_multisetor(token, d_inicial, d_final, setor_atual)
        
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
    # PAINEL SUPERVISOR GERAL (ADAPTADO PARA MULTISETOR)
    # --------------------------------------------------------------------------
    elif st.session_state.user_role == "supervisor" and modo_visao_supervisor == "Geral":
        st.markdown(f"## 🏢 Painel de Gestão - Setor {setor_atual}")
        
        # Definição das abas (Condicional para SUPORTE)
        lista_abas = ["👁️ Visão Geral", "🏆 Rankings", "⏸️ Pausas", "⚡ Tempo Real", "🆘 Solicitações", "📝 Diário de Bordo"]
        if setor_atual == "SUPORTE":
            lista_abas.extend(["🌙 Plantão", "🏢 Cliente Interno"])
            
        abas_sup = st.tabs(lista_abas)
        
        # ABA 1: VISÃO GERAL
        with abas_sup[0]:
            if token:
                with st.spinner("Sincronizando estatísticas..."):
                    
                    if setor_atual == "NRC":
                        # LÓGICA ORIGINAL INTACTA PARA NRC
                        dados_servicos, csat_geral, base_geral, mapa_agentes, dados_globais = buscar_dados_completos_supervisor(token, d_inicial, d_final)
                        lista_servicos_exibir = SERVICOS_ALVO
                    else:
                        # LÓGICA NOVA PARA OUTROS SETORES (INCLUINDO SUPORTE)
                        dados_servicos, csat_geral, base_geral, mapa_agentes, dados_globais = buscar_dados_supervisor_multisetor(token, d_inicial, d_final, setor_atual)
                        lista_servicos_exibir = SETORES_SERVICOS.get(setor_atual, [])

                    st.markdown(f"#### ⭐ Visão Global da Equipe ({setor_atual})")
                    
                    # LINHA 1: CSAT + Cards Especiais
                    col_kpi1, col_kpi2 = st.columns([1, 2])
                    with col_kpi1:
                        cor_geral = "#10b981" if csat_geral >= 85 else ("#f59e0b" if csat_geral >= 75 else "#ef4444")
                        render_kpi_card("CSAT Global (Setor)", f"{csat_geral:.2f}%", f"Base Total: {base_geral}", cor_geral)
                    with col_kpi2:
                        if setor_atual == "NRC":
                            render_link_card("Ferramenta Externa", "https://fideliza-nator-live.streamlit.app/", "FIDELIZA-NATOR")
                        elif setor_atual == "CANCELAMENTO":
                            render_link_card("Acesso Rápido", "https://docs.google.com/spreadsheets/d/1y-7_w8RuzE2SSWatbdZj0SjsIa-aJyZCV0_1OxwD7bs/edit?gid=0#gid=0", "CLIENTE CRITICO", cor_borda="#ef4444")
                        # SUPORTE não exibe card aqui

                    # LINHA 2: CARDS DE TEMPO GLOBAIS
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
                        
                        # LAYOUT EXPANDIDO (6 CARDS)
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
            if token and 'mapa_agentes' in locals():
                lista_curtas, lista_almoco, lista_logins, lista_ranking = processar_dados_pausas_supervisor(token, d_inicial, d_final, mapa_agentes)
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
                        st.caption("ℹ️ Nota: Turnos que iniciam no dia anterior (madrugada) podem apresentar falsos atrasos aqui.")
                    else: st.success("Todos pontuais!")
                with c_p4:
                    st.subheader("4. 🏆 Ranking Pausas (Qtd)")
                    if lista_ranking:
                        st.dataframe(pd.DataFrame(lista_ranking).sort_values(by="Qtd Pausas", ascending=False), use_container_width=True, hide_index=True)
                
                # SEÇÃO DE PRÉ-PAUSAS (SANFONA)
                st.markdown("---")
                st.subheader("5. ⏳ Monitoramento de Pré-Pausas (Agendadas)")
                with st.spinner("Buscando pré-pausas..."):
                    lista_pre_pausas = processar_dados_pre_pausas_geral(token, d_inicial, d_final, mapa_agentes)
                    if lista_pre_pausas:
                        df_pre = pd.DataFrame(lista_pre_pausas)
                        # Ordenar agentes por quantidade decrescente
                        contagem = df_pre['Agente'].value_counts()
                        agentes_ordenados = contagem.index.tolist()
                        
                        for agente in agentes_ordenados:
                            qtd = contagem[agente]
                            # Filtra as pausas deste agente
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
                
                # Seleção da função correta baseada no setor
                if setor_atual == "NRC":
                    lista_online = buscar_agentes_online_filtrado_nrc(token)
                else:
                    lista_online = buscar_agentes_online_filtrado_setor(token, setor_atual)
                
                if lista_online:
                    st.markdown(f"### 🟢 {len(lista_online)} Agentes Online ({setor_atual})")
                    
                    for agente_online in lista_online:
                        aid = agente_online.get("cod")
                        nome_online = agente_online.get("nom_agente", "Desconhecido")
                        status_online = agente_online.get("status", "Online")
                        tempo_online = agente_online.get("tempo_status", "--:--")
                        
                        cor_status_online = "#10b981" # Verde
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
                            <div style="flex:1; text-align:right;">
                                </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Botão de Ação (Streamlit)
                        col_btn = st.columns([4, 1])[1]
                        with col_btn:
                            if st.button("🔴 Deslogar", key=f"btn_logout_{aid}"):
                                with st.spinner(f"Deslogando {nome_online}..."):
                                    sucesso_logout, msg_logout = forcar_logout(token, aid)
                                    if sucesso_logout: 
                                        st.success(f"{nome_online} deslogado!")
                                        time.sleep(1)
                                        st.rerun()
                                    else: st.error(msg_logout)
                else:
                    st.warning(f"Nenhum agente da equipe {setor_atual} está online no momento.")
            else: st.error("Erro de conexão.")

        # ABA 5: SOLICITAÇÕES
        with abas_sup[4]:
            st.info("Visualização das solicitações registradas no Google Sheets.")
            df_gsheets = ler_solicitacoes_gsheets()
            if not df_gsheets.empty:
                st.dataframe(df_gsheets, use_container_width=True)
            else: st.warning("Nenhuma solicitação encontrada na planilha.")

        # ABA 6: DIÁRIO DE BORDO (NOVA)
        with abas_sup[5]:
            st.markdown("### 📝 Diário de Bordo da Supervisão")
            st.info("Utilize este espaço para registrar ocorrências semanais, advertências, elogios e feedbacks da equipe.")
            
            # --- FORMULÁRIO ---
            with st.form("form_diario_bordo"):
                # Lista de agentes para o supervisor selecionar
                lista_agentes_diario = ["Geral (Equipe)"] + sorted(list(mapa_agentes.values())) if 'mapa_agentes' in locals() else ["Geral (Equipe)"]
                
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    agente_selecionado = st.selectbox("Agente Relacionado", lista_agentes_diario)
                with col_d2:
                    tipo_ponto = st.selectbox("Tipo de Registro", [
                        "Advertência", 
                        "Atestado/Falta", 
                        "Feedback Comportamental", 
                        "Feedback Técnico", 
                        "Elogio/Destaque", 
                        "Problema Sistêmico", 
                        "Outros"
                    ])
                
                texto_diario = st.text_area("Descrição detalhada do ponto:", height=120, placeholder="Descreva o ocorrido, a tratativa realizada e o resultado esperado...")
                
                btn_diario = st.form_submit_button("💾 Registrar no Diário", use_container_width=True)
                
                if btn_diario:
                    if texto_diario:
                        with st.spinner("Salvando registro..."):
                            supervisor_logado = st.session_state.user_data['nome']
                            sucesso_db, msg_db = salvar_diario_bordo(supervisor_logado, setor_atual, agente_selecionado, tipo_ponto, texto_diario)
                            if sucesso_db:
                                st.success(msg_db)
                                time.sleep(1) # Pequeno delay para garantir o reload
                                st.rerun()
                            else:
                                st.error(msg_db)
                    else:
                        st.warning("Por favor, preencha a descrição.")
            
            # --- TABELA DE ACOMPANHAMENTO ---
            st.markdown("---")
            st.markdown("#### 🗂️ Histórico de Registros")
            
            # Lê os dados filtrando pelo setor atual
            df_diario = ler_diario_bordo(setor_atual)
            
            if not df_diario.empty:
                # Ordena por data
                try:
                    df_diario['Data_Sort'] = pd.to_datetime(df_diario['Data'], format="%d/%m/%Y %H:%M:%S", errors='coerce')
                    df_diario = df_diario.sort_values(by='Data_Sort', ascending=False).drop(columns=['Data_Sort'])
                except:
                    pass

                # Função para adicionar emoji visual e cor no Pandas Styler
                def highlight_status(val):
                    status_str = str(val).strip().lower()
                    if "pendente" in status_str:
                        return 'color: #ef4444; font-weight: bold;' # Vermelho
                    elif "validado" in status_str or "visto" in status_str or "ok" in status_str:
                        return 'color: #10b981; font-weight: bold;' # Verde
                    return ''

                # Adiciona coluna visual de emoji para ficar mais bonito ainda
                def add_icon(val):
                    status_str = str(val).strip().lower()
                    if "pendente" in status_str:
                        return f"🔴 {val}"
                    elif "validado" in status_str or "visto" in status_str or "ok" in status_str:
                        return f"🟢 {val}"
                    return val

                # Aplica o ícone apenas para visualização
                df_view = df_diario.copy()
                if "Validação Gestão" in df_view.columns:
                    df_view["Validação Gestão"] = df_view["Validação Gestão"].apply(add_icon)

                # Renderiza com estilo
                st.dataframe(
                    df_view.style.applymap(highlight_status, subset=["Validação Gestão"]),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Validação Gestão": st.column_config.TextColumn(
                            "Status Gestão",
                            help="Validação feita pela gestão geral na planilha (🔴 Pendente / 🟢 Validado)",
                            width="medium"
                        ),
                        "Descrição": st.column_config.TextColumn(
                            "Descrição",
                            width="large"
                        )
                    }
                )
            else:
                st.warning("Nenhum registro encontrado para este setor.")

        # ABA 7 (PLANTÃO) e 8 (CLIENTE INTERNO) - SE FOREM DO SETOR SUPORTE
        # Nota: Os índices das abas mudaram porque inserimos o Diário antes
        if setor_atual == "SUPORTE":
            with abas_sup[6]: # Era 5
                if token:
                    with st.spinner("Carregando dados do Plantão..."):
                        df_plantao, stats_servico_plantao, _ = buscar_dados_plantao(token, d_inicial, d_final)
                        
                        st.markdown("### 🌙 Equipe de Plantão (Madrugada)")
                        
                        if not df_plantao.empty:
                            # 1. Tabela com Satisfação
                            st.dataframe(df_plantao, column_config={"CSAT": st.column_config.NumberColumn("CSAT (%)", format="%.2f%%")}, use_container_width=True, hide_index=True)
                            
                            # 2. Resumo Geral
                            total_vol_plantao = df_plantao['Volume'].sum()
                            st.markdown(f"**Volume Total Plantão:** {total_vol_plantao} atendimentos")
                            
                            st.markdown("---")
                            st.markdown("#### 📊 Métricas por Setor (Plantão)")
                            
                            # 3. Cards Detalhados
                            if len(stats_servico_plantao) > 0:
                                cols_plantao = st.columns(len(stats_servico_plantao))
                                for i, (servico, dados) in enumerate(stats_servico_plantao.items()):
                                    with cols_plantao[i % len(cols_plantao)]:
                                        st.markdown(f"**{servico}**")
                                        st.metric("Volume", dados["num_qtd"])
                                        st.caption(f"TMA: {dados['tma']} | TME: {dados['tme']}")
                                        st.caption(f"TMIA: {dados['tmia']} | TMIC: {dados['tmic']}")
                        else:
                            st.warning("Sem dados para a equipe de plantão neste período.")

            with abas_sup[7]: # Era 6
                if token:
                    # Passa a lista de nomes do SUPORTE para filtrar
                    nomes_suporte = SETORES_AGENTES["SUPORTE"]
                    with st.spinner("Conectando à Conta 5 (Cliente Interno)..."):
                        stats_ci, score_ci, total_ci, df_ci = buscar_dados_cliente_interno(token, d_inicial, d_final, nomes_suporte)
                        
                        st.markdown("### 🏢 Cliente Interno (Conta 5)")
                        
                        # Cards de Tempo e Satisfação
                        c1, c2, c3, c4 = st.columns(4)
                        cor_ci = "#10b981" if score_ci >= 85 else "#f59e0b"
                        with c1: render_kpi_card("CSAT Interno", f"{score_ci:.2f}%", f"Base: {total_ci}", cor_ci)
                        with c2: render_kpi_card("T.M.A", stats_ci["TMA"], "Tempo Médio", "#3b82f6")
                        with c3: render_kpi_card("T.M.E", stats_ci["TME"], "Espera", "#ef4444")
                        with c4: render_kpi_card("T.M.I.A", stats_ci["TMIA"], "Inatividade", "#f59e0b")
                        
                        st.markdown("---")
                        
                        # Tabela de Detalhes
                        if not df_ci.empty:
                            st.markdown("#### 📋 Histórico de Chamados Internos (Filtrado: Equipe Suporte)")
                            df_ci['Acesso'] = df_ci['Protocolo'].apply(gerar_link_protocolo)
                            
                            # Tabela Principal
                            st.dataframe(
                                df_ci[['Data', 'Agente', 'Cliente', 'Nota', 'Comentario', 'Acesso']], 
                                column_config={
                                    "Acesso": st.column_config.LinkColumn("Link", display_text="Abrir"),
                                    "Nota": st.column_config.NumberColumn("Nota", format="%d ⭐")
                                }, 
                                use_container_width=True, hide_index=True
                            )
                            
                            # Ranking de Detratores Internos
                            detratores_ci = df_ci[df_ci['Nota'] < 7].copy()
                            st.markdown("<br>", unsafe_allow_html=True)
                            with st.expander(f"🔻 Detratores Internos ({len(detratores_ci)})", expanded=True):
                                if not detratores_ci.empty:
                                    st.dataframe(
                                        detratores_ci[['Data', 'Agente', 'Cliente', 'Nota', 'Comentario', 'Acesso']], 
                                        column_config={
                                            "Acesso": st.column_config.LinkColumn("Link", display_text="Verificar"),
                                            "Nota": st.column_config.NumberColumn("Nota", format="%d 🔴")
                                        }, 
                                        use_container_width=True, hide_index=True
                                    )
                                else: st.success("Nenhum detrator interno no período!")
                        else:
                            st.info("Nenhum chamado de cliente interno atendido pela equipe de Suporte no período.")
