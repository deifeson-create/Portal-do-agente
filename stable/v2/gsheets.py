import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# ==============================================================================
# CONEXÃO GOOGLE SHEETS (BANCO DE DADOS)
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