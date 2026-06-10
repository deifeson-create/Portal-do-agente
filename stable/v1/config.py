import streamlit as st

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

# ==============================================================================
# 1.1 CONFIGURAÇÃO DE NOVOS SETORES (REFATORADO PARA IDs ESTABELECIDOS)
# ==============================================================================
# Dicionários de IDs para garantir precisão absoluta e evitar roubo de volumetria
JOVENS_APRENDIZES_NRC_IDS = ['330', '331', '333', '332']
JOVENS_APRENDIZES_SUPORTE_IDS = ['336', '337', '342', '334']

SETORES_AGENTES_IDS = {
    "NRC": ['202', '200', '196', '161', '193', '192', '203', '190', '180', '205', '209', '211', '253', '201', '281', '283', '285', '181', '301', '176', '175', '172', '174'] + JOVENS_APRENDIZES_NRC_IDS,
    "CANCELAMENTO": ['163', '159', '80', '245', '299', '297'],
    "NEGOCIACAO": ['27', '263', '275', '73', '265', '100', '109', '44', '309', '350', '167', '184', '311', '315', '313', '269', '267', '34', '26', '89'],
    "SUPORTE": ['162', '221', '217', '341', '307', '231', '233', '239', '197', '243', '259', '271', '289', '291', '293', '295', '303', '305', '219', '319', '321', '255', '177', '33', '69', '140', '125', '19', '68', '151', '183', '103', '340', '124', '344', '345', '346', '338'] + JOVENS_APRENDIZES_SUPORTE_IDS
}

SETORES_SERVICOS = {
    "NRC": SERVICOS_ALVO,
    "CANCELAMENTO": ['CANCELAMENTO'], 
    "NEGOCIACAO": ['NEGOCIAÇÃO ATIVA', 'NEGOCIAÇÃO  PASSIVA'], 
    "SUPORTE": ['SUPORTE', 'LIBERAÇÃO'] 
}

# Configurações Especiais Suporte
LISTA_PLANTAO_IDS = ['221', '219', '151', '233', '197']
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