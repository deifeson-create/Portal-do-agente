import streamlit as st
import pandas as pd
import requests
import time
import json
import concurrent.futures
from datetime import datetime, timedelta
from collections import defaultdict

# Importa o novo módulo de banco de dados
from database import get_cache, set_cache, is_cacheable

# Importando as variáveis de configuração e funções úteis
from config import (
    BASE_URL, ADMIN_USER, ADMIN_PASS, ID_CONTA,
    PESQUISAS_IDS, IDS_PERGUNTAS_VALIDAS, CANAIS_ALVO,
    SERVICOS_ALVO, SETORES_AGENTES_IDS, SETORES_SERVICOS,
    JOVENS_APRENDIZES_NRC_IDS, JOVENS_APRENDIZES_SUPORTE_IDS,
    LIMITES_PAUSA, TOLERANCIA_VISUAL_ALMOCO, TOLERANCIA_MENSAL_EXCESSO,
    LISTA_PLANTAO_IDS, ID_CONTA_CLIENTE_INTERNO,
    PESQUISA_SATISFACAO, PERGUNTA_NOTA, PERGUNTA_TEXTO
)
from utils import calcular_media_tempos, formatar_tempo_humano, time_str_to_seconds

# ==============================================================================
# DICIONÁRIO DE APELIDOS (CORREÇÃO DE DIVERGÊNCIA MATRIX)
# ==============================================================================
# Se a Matrix mandar na pesquisa um nome diferente do 'Nome de Exibição',
# basta adicionar aqui para que o sistema consiga cruzar com o ID correto.
MAPA_APELIDOS = {
    "ANA L.": "ANINHA",
    # "NOME DA PESQUISA": "NOME DE EXIBIÇÃO NO PAINEL", (Pode adicionar mais se precisar no futuro)
}

def normalizar_nome_pesquisa(nome_bruto):
    """Traduz o nome vindo da pesquisa caso exista um apelido mapeado."""
    nome_limpo = str(nome_bruto).strip().upper()
    if nome_limpo in MAPA_APELIDOS:
        return MAPA_APELIDOS[nome_limpo]
    return nome_limpo

# ==============================================================================
# FUNÇÕES GERAIS DE BACKEND (MATRIX API)
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
                return { "id": str(agente.get("cod_agente")), "nome": str(agente.get("nome_exibicao") or agente.get("agente")).strip().upper(), "email": agente.get("email", "").lower() }
    except: pass
    return None

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

def forcar_logout(token, id_agente):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/deslogarAgente"
    payload = {"id_agente": int(id_agente)}
    try:
        r = requests.post(url, headers=headers, json=payload)
        time.sleep(0.3)
        if r.status_code == 200: return True, "Sucesso"
        else: return False, f"Erro API: {r.text}"
    except Exception as e: return False, str(e)

# ==============================================================================
# FUNÇÕES ESPECÍFICAS DO AGENTE (VISÃO INDIVIDUAL COM CACHE DIÁRIO INTELIGENTE)
# ==============================================================================

@st.cache_data(ttl=60)
def buscar_historico_login(token, id_agente, data_ini, data_fim):
    url = f"{BASE_URL}/relAgenteLogin"
    headers = {"Authorization": f"Bearer {token}"}
    logins_por_dia = {}
    
    delta = data_fim - data_ini
    for i in range(delta.days + 1):
        dia = data_ini + timedelta(days=i)
        dia_str = dia.strftime("%Y-%m-%d")
        cacheable = is_cacheable(dia_str)
        
        page = 1
        while page <= 3: 
            params_cache = {"limit": 100}
            cached = get_cache(dia_str, "relAgenteLogin", "ALL", json.dumps(params_cache, sort_keys=True), page) if cacheable else None
            
            if cached is not None:
                data = cached
            else:
                params_req = {"data_inicial": dia_str, "data_final": dia_str, "page": page, "limit": 100}
                try:
                    r = requests.get(url, headers=headers, params=params_req, timeout=10)
                    time.sleep(0.5)
                    if r.status_code != 200: break
                    data = r.json()
                    if cacheable: set_cache(dia_str, "relAgenteLogin", "ALL", json.dumps(params_cache, sort_keys=True), page, data)
                except: break
                
            rows = data.get("rows", [])
            if not rows: break
            for row in rows:
                if str(row.get("id_agente")) == str(id_agente):
                    data_str = row.get("data_login")
                    if data_str:
                        dt_obj = datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S")
                        d_str_log = dt_obj.strftime("%Y-%m-%d")
                        if d_str_log not in logins_por_dia: logins_por_dia[d_str_log] = dt_obj
                        else:
                            if dt_obj < logins_por_dia[d_str_log]: logins_por_dia[d_str_log] = dt_obj
            if len(rows) < 100: break
            page += 1
            
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
    
    tempos = {"tma": [], "tme": [], "tmia": [], "tmic": []}
    pesos = []
    vol_total = 0
    
    delta = data_fim - data_ini
    for i in range(delta.days + 1):
        dia = data_ini + timedelta(days=i)
        dia_str = dia.strftime("%Y-%m-%d")
        cacheable = is_cacheable(dia_str)
        
        params_cache = {"agrupador": "agente", "agente[]": [id_agente], "canal[]": ids_canais}
        cached = get_cache(dia_str, "relAtEstatistico", ID_CONTA, json.dumps(params_cache, sort_keys=True), 1) if cacheable else None
        
        if cached is not None:
            data = cached
        else:
            params_req = {
                "data_inicial": f"{dia_str} 00:00:00", "data_final": f"{dia_str} 23:59:59",
                "agrupador": "agente", "agente[]": [id_agente], "canal[]": ids_canais, "id_conta": ID_CONTA
            }
            try:
                r = requests.get(url, headers=headers, params=params_req, timeout=15)
                time.sleep(0.3)
                if r.status_code == 200:
                    data = r.json()
                    if cacheable and isinstance(data, list): set_cache(dia_str, "relAtEstatistico", ID_CONTA, json.dumps(params_cache, sort_keys=True), 1, data)
                else: continue
            except: continue
            
        if data and isinstance(data, list) and len(data) > 0:
            item = data[0]
            qtd = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
            if qtd > 0:
                vol_total += qtd
                pesos.append(qtd)
                tempos["tma"].append(item.get("tma", "00:00:00"))
                tempos["tme"].append(item.get("tme", "00:00:00"))
                tempos["tmia"].append(item.get("tmia", "00:00:00"))
                tempos["tmic"].append(item.get("tmic", "00:00:00"))
                
    if pesos:
        return {
            "num_qtd": vol_total, "tma": calcular_media_tempos(tempos["tma"], pesos),
            "tme": calcular_media_tempos(tempos["tme"], pesos), "tmia": calcular_media_tempos(tempos["tmia"], pesos),
            "tmic": calcular_media_tempos(tempos["tmic"], pesos)
        }
    return None

@st.cache_data(ttl=600)
def buscar_csat_nrc(token, id_agente, data_ini, data_fim):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/RelPesqAnalitico"
    todas_respostas = []
    
    delta = data_fim - data_ini
    for p_id in PESQUISAS_IDS:
        for i in range(delta.days + 1):
            dia = data_ini + timedelta(days=i)
            dia_str = dia.strftime("%Y-%m-%d")
            cacheable = is_cacheable(dia_str)
            
            page = 1
            while True:
                params_cache = {"pesquisa": p_id, "limit": 1000, "agente[]": [id_agente]}
                cached = get_cache(dia_str, "RelPesqAnalitico", ID_CONTA, json.dumps(params_cache, sort_keys=True), page) if cacheable else None
                
                if cached is not None:
                    data = cached
                else:
                    params_req = {"data_inicial": dia_str, "data_final": dia_str, "pesquisa": p_id, "id_conta": ID_CONTA, "limit": 1000, "page": page, "agente[]": [id_agente]}
                    try:
                        r = requests.get(url, headers=headers, params=params_req, timeout=30)
                        time.sleep(0.5)
                        if r.status_code != 200: break
                        data = r.json()
                        if cacheable and isinstance(data, list): set_cache(dia_str, "RelPesqAnalitico", ID_CONTA, json.dumps(params_cache, sort_keys=True), page, data)
                    except: break
                    
                if not data or not isinstance(data, list): break
                total_api = 0
                for bloco in data:
                    if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                    id_perg = str(bloco.get("id_pergunta", ""))
                    if id_perg not in IDS_PERGUNTAS_VALIDAS: continue
                    if id_perg in PERGUNTA_TEXTO: continue 
                    for resp in bloco.get("respostas", []):
                        todas_respostas.append({"Nota": resp.get("nom_valor"), "Comentario": resp.get("nom_resposta"), "Data": resp.get("dat_resposta"), "Cliente": resp.get("nom_contato"), "Protocolo": resp.get("num_protocolo")})
                if (page * 1000) >= total_api: break
                if len(data) < 2: break
                page += 1
                
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
    
    delta = data_fim - data_ini
    for i in range(delta.days + 1):
        dia = data_ini + timedelta(days=i)
        dia_str = dia.strftime("%Y-%m-%d")
        cacheable = is_cacheable(dia_str)
        
        page = 1
        while True:
            params_cache = {"limit": 100}
            cached = get_cache(dia_str, "relAgentePausa", "ALL", json.dumps(params_cache, sort_keys=True), page) if cacheable else None
            
            if cached is not None:
                data = cached
            else:
                params_req = {"dat_inicial": dia_str, "dat_final": dia_str, "limit": 100, "pagina": page}
                try:
                    r = requests.get(url, headers=headers, params=params_req, timeout=10)
                    time.sleep(0.5)
                    if r.status_code != 200: break
                    data = r.json()
                    if cacheable: set_cache(dia_str, "relAgentePausa", "ALL", json.dumps(params_cache, sort_keys=True), page, data)
                except: break
                
            rows = data.get("rows", [])
            if not rows: break
            for row in rows:
                if str(row.get("id_agente")) == str(id_agente):
                    todas_pausas.append(row)
            if len(rows) < 100: break
            page += 1
            
    return pd.DataFrame(todas_pausas)

# ==============================================================================
# FUNÇÕES DO SUPERVISOR (DADOS EM LOTE E ESTATÍSTICAS)
# ==============================================================================

def buscar_agentes_online_filtrado_nrc(token):
    headers = {"Authorization": f"Bearer {token}"}
    agentes_online_nrc = []
    ids_alvo = SETORES_AGENTES_IDS["NRC"]
    try:
        r = requests.get(f"{BASE_URL}/agentesOnline", headers=headers)
        time.sleep(0.3)
        if r.status_code == 200:
            todos_online = r.json()
            for agente in todos_online:
                if str(agente.get("cod")) in ids_alvo:
                    agentes_online_nrc.append(agente)
    except: pass
    return agentes_online_nrc

def buscar_agentes_online_filtrado_setor(token, setor_nome):
    headers = {"Authorization": f"Bearer {token}"}
    agentes_online_filtrados = []
    ids_alvo = SETORES_AGENTES_IDS.get(setor_nome, [])
    try:
        r = requests.get(f"{BASE_URL}/agentesOnline", headers=headers)
        time.sleep(0.3)
        if r.status_code == 200:
            todos_online = r.json()
            for agente in todos_online:
                if str(agente.get("cod")) in ids_alvo:
                    agentes_online_filtrados.append(agente)
    except: pass
    return agentes_online_filtrados

@st.cache_data(ttl=300)
def buscar_dados_completos_supervisor(token, data_ini, data_fim, contas_selecionadas):
    headers = {"Authorization": f"Bearer {token}"}
    ids_agentes = []
    mapa_agentes = {}
    ids_alvo = SETORES_AGENTES_IDS["NRC"]
    
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
                cod = str(agente.get("cod_agente"))
                if cod in ids_alvo:
                    nome_raw = str(agente.get("nome_exibicao") or agente.get("agente")).strip().upper()
                    ids_agentes.append(cod)
                    mapa_agentes[cod] = nome_raw
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break

    ids_canais = buscar_ids_canais(token)
    resultados = { s: { "num_qtd": 0, "tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--", "csat_pos": 0, "csat_total": 0 } for s in SERVICOS_ALVO }
    ids_agentes_stats = [cod for cod in ids_agentes if cod not in JOVENS_APRENDIZES_NRC_IDS]
    if not ids_agentes_stats: return resultados, 0.0, 0, mapa_agentes, {"tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--"}, 0
    
    dados_globais = {"tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--"}
    tempos_globais_agregados = {"tma": [], "tme": [], "tmia": [], "tmic": []}
    pesos_globais = []
    volume_total_setor = 0
    delta = data_fim - data_ini
    
    for conta in contas_selecionadas:
        for i in range(delta.days + 1):
            dia = data_ini + timedelta(days=i)
            dia_str = dia.strftime("%Y-%m-%d")
            cacheable = is_cacheable(dia_str)
            
            params_cache = {"agrupador": "agente", "canal[]": ids_canais}
            cached = get_cache(dia_str, "relAtEstatistico", conta, json.dumps(params_cache, sort_keys=True), 1) if cacheable else None
            
            if cached is not None:
                lista_global = cached
            else:
                params_req = {"data_inicial": f"{dia_str} 00:00:00", "data_final": f"{dia_str} 23:59:59", "agrupador": "agente", "canal[]": ids_canais, "id_conta": conta}
                try:
                    r_global = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_req)
                    time.sleep(0.3)
                    if r_global.status_code == 200:
                        lista_global = r_global.json()
                        if cacheable and isinstance(lista_global, list): set_cache(dia_str, "relAtEstatistico", conta, json.dumps(params_cache, sort_keys=True), 1, lista_global)
                    else: continue
                except: continue
                
            if lista_global and isinstance(lista_global, list):
                for item in lista_global:
                    nome_api = str(item.get("agrupador", "")).strip().upper()
                    cod_match = next((c for c in ids_agentes_stats if mapa_agentes.get(c) == nome_api), None)
                    
                    if cod_match:
                        vol = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                        if vol > 0:
                            volume_total_setor += vol
                            pesos_globais.append(vol)
                            tempos_globais_agregados["tma"].append(item.get("tma", "00:00:00"))
                            tempos_globais_agregados["tme"].append(item.get("tme", "00:00:00"))
                            tempos_globais_agregados["tmia"].append(item.get("tmia", "00:00:00"))
                            tempos_globais_agregados["tmic"].append(item.get("tmic", "00:00:00"))

    if pesos_globais:
        dados_globais["tma"] = calcular_media_tempos(tempos_globais_agregados["tma"], pesos_globais)
        dados_globais["tme"] = calcular_media_tempos(tempos_globais_agregados["tme"], pesos_globais)
        dados_globais["tmia"] = calcular_media_tempos(tempos_globais_agregados["tmia"], pesos_globais)
        dados_globais["tmic"] = calcular_media_tempos(tempos_globais_agregados["tmic"], pesos_globais)

    tempos_srv_agregados = {s: {"vols": [], "tma": [], "tme": [], "tmia": [], "tmic": []} for s in SERVICOS_ALVO}
    for conta in contas_selecionadas:
        for i in range(delta.days + 1):
            dia = data_ini + timedelta(days=i)
            dia_str = dia.strftime("%Y-%m-%d")
            cacheable = is_cacheable(dia_str)
            
            params_cache = {"agrupador": "servico", "canal[]": ids_canais}
            cached = get_cache(dia_str, "relAtEstatisticoServico", conta, json.dumps(params_cache, sort_keys=True), 1) if cacheable else None
            
            if cached is not None:
                lista = cached
            else:
                params_srv = {"data_inicial": f"{dia_str} 00:00:00", "data_final": f"{dia_str} 23:59:59", "agrupador": "servico", "canal[]": ids_canais, "id_conta": conta}
                try:
                    r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_srv)
                    time.sleep(0.3)
                    if r.status_code == 200:
                        lista = r.json()
                        if cacheable and isinstance(lista, list): set_cache(dia_str, "relAtEstatisticoServico", conta, json.dumps(params_cache, sort_keys=True), 1, lista)
                    else: continue
                except: continue
                
            if lista and isinstance(lista, list):
                for item in lista:
                    nome_servico = str(item.get("agrupador", "")).upper()
                    match_srv = None
                    for s_alvo in SERVICOS_ALVO:
                        if " ".join(s_alvo.upper().split()) == " ".join(nome_servico.split()):
                            match_srv = s_alvo; break
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
            for i in range(delta.days + 1):
                dia = data_ini + timedelta(days=i)
                dia_str = dia.strftime("%Y-%m-%d")
                cacheable = is_cacheable(dia_str)
                
                p_page = 1
                while True:
                    params_cache = {"pesquisa": p_id, "limit": 1000}
                    cached = get_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), p_page) if cacheable else None
                    
                    if cached is not None:
                        data = cached
                    else:
                        p_params = {"data_inicial": dia_str, "data_final": dia_str, "pesquisa": p_id, "id_conta": conta, "limit": 1000, "page": p_page}
                        try:
                            r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=p_params)
                            time.sleep(0.3)
                            if r.status_code != 200: break
                            data = r.json()
                            if cacheable and isinstance(data, list): set_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), p_page, data)
                        except: break
                        
                    if not data or not isinstance(data, list): break
                    total_api = 0
                    for bloco in data:
                        if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                        id_perg = str(bloco.get("id_pergunta", ""))
                        if id_perg in IDS_PERGUNTAS_VALIDAS:
                            if id_perg in PERGUNTA_TEXTO: continue 
                            for resp in bloco.get("respostas", []):
                                try:
                                    nom_ag = normalizar_nome_pesquisa(resp.get("nom_agente", ""))
                                    cod_match = next((c for c in ids_agentes_stats if mapa_agentes.get(c) == nom_ag), None)
                                    
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

    score_geral = (csat_geral_pos / csat_geral_total * 100) if csat_geral_total > 0 else 0.0
    return resultados, score_geral, csat_geral_total, mapa_agentes, dados_globais, volume_total_setor

@st.cache_data(ttl=300)
def buscar_dados_supervisor_multisetor(token, data_ini, data_fim, setor_nome, contas_selecionadas, excluir_plantao=False):
    headers = {"Authorization": f"Bearer {token}"}
    ids_agentes = []
    mapa_agentes = {}
    
    ids_alvo = SETORES_AGENTES_IDS.get(setor_nome, [])
    lista_servicos_alvo = SETORES_SERVICOS.get(setor_nome, [])
    
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
                cod = str(agente.get("cod_agente"))
                if cod in ids_alvo:
                    nome_raw = str(agente.get("nome_exibicao") or agente.get("agente")).strip().upper()
                    ids_agentes.append(cod)
                    mapa_agentes[cod] = nome_raw
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break

    ids_canais = buscar_ids_canais(token)
    ids_agentes_stats = []
    ids_aprendizes = JOVENS_APRENDIZES_NRC_IDS if setor_nome == "NRC" else JOVENS_APRENDIZES_SUPORTE_IDS
    
    for cod in ids_agentes:
        if excluir_plantao and setor_nome == "SUPORTE" and cod in LISTA_PLANTAO_IDS: continue
        if cod in ids_aprendizes: continue
        ids_agentes_stats.append(cod)
    
    resultados = { s: { "num_qtd": 0, "tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--", "csat_pos": 0, "csat_total": 0 } for s in lista_servicos_alvo }
    if not ids_agentes_stats: return resultados, 0.0, 0, mapa_agentes, {"tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--"}, 0
    
    dados_globais = {"tma": "--:--", "tme": "--:--", "tmia": "--:--", "tmic": "--:--"}
    tempos_globais_agregados = {"tma": [], "tme": [], "tmia": [], "tmic": []}
    pesos_globais = []
    volume_total_setor = 0
    delta = data_fim - data_ini
    
    for conta in contas_selecionadas:
        for i in range(delta.days + 1):
            dia = data_ini + timedelta(days=i)
            dia_str = dia.strftime("%Y-%m-%d")
            cacheable = is_cacheable(dia_str)
            
            params_cache = {"agrupador": "agente", "canal[]": ids_canais}
            cached = get_cache(dia_str, "relAtEstatistico", conta, json.dumps(params_cache, sort_keys=True), 1) if cacheable else None
            
            if cached is not None:
                lista_global = cached
            else:
                params_globais = {"data_inicial": f"{dia_str} 00:00:00", "data_final": f"{dia_str} 23:59:59", "agrupador": "agente", "canal[]": ids_canais, "id_conta": conta}
                try:
                    r_global = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_globais)
                    time.sleep(0.3)
                    if r_global.status_code == 200:
                        lista_global = r_global.json()
                        if cacheable and isinstance(lista_global, list): set_cache(dia_str, "relAtEstatistico", conta, json.dumps(params_cache, sort_keys=True), 1, lista_global)
                    else: continue
                except: continue
                
            if lista_global and isinstance(lista_global, list):
                for item in lista_global:
                    nome_api = str(item.get("agrupador", "")).strip().upper()
                    cod_match = next((c for c in ids_agentes_stats if mapa_agentes.get(c) == nome_api), None)
                    
                    if cod_match:
                        vol = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                        if vol > 0:
                            volume_total_setor += vol
                            pesos_globais.append(vol)
                            tempos_globais_agregados["tma"].append(item.get("tma", "00:00:00"))
                            tempos_globais_agregados["tme"].append(item.get("tme", "00:00:00"))
                            tempos_globais_agregados["tmia"].append(item.get("tmia", "00:00:00"))
                            tempos_globais_agregados["tmic"].append(item.get("tmic", "00:00:00"))

    if pesos_globais:
        dados_globais["tma"] = calcular_media_tempos(tempos_globais_agregados["tma"], pesos_globais)
        dados_globais["tme"] = calcular_media_tempos(tempos_globais_agregados["tme"], pesos_globais)
        dados_globais["tmia"] = calcular_media_tempos(tempos_globais_agregados["tmia"], pesos_globais)
        dados_globais["tmic"] = calcular_media_tempos(tempos_globais_agregados["tmic"], pesos_globais)

    tempos_srv_agregados = {s: {"vols": [], "tma": [], "tme": [], "tmia": [], "tmic": []} for s in lista_servicos_alvo}
    
    for conta in contas_selecionadas:
        for i in range(delta.days + 1):
            dia = data_ini + timedelta(days=i)
            dia_str = dia.strftime("%Y-%m-%d")
            cacheable = is_cacheable(dia_str)
            
            params_cache = {"agrupador": "servico", "canal[]": ids_canais}
            cached = get_cache(dia_str, "relAtEstatisticoServico", conta, json.dumps(params_cache, sort_keys=True), 1) if cacheable else None
            
            if cached is not None:
                lista = cached
            else:
                params_srv = {"data_inicial": f"{dia_str} 00:00:00", "data_final": f"{dia_str} 23:59:59", "agrupador": "servico", "canal[]": ids_canais, "id_conta": conta}
                try:
                    r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_srv)
                    time.sleep(0.3)
                    if r.status_code == 200:
                        lista = r.json()
                        if cacheable and isinstance(lista, list): set_cache(dia_str, "relAtEstatisticoServico", conta, json.dumps(params_cache, sort_keys=True), 1, lista)
                    else: continue
                except: continue
                
            if lista and isinstance(lista, list):
                for item in lista:
                    nome_servico = str(item.get("agrupador", "")).upper()
                    match_srv = None
                    for s_alvo in lista_servicos_alvo:
                        if " ".join(s_alvo.upper().split()) == " ".join(nome_servico.split()):
                            match_srv = s_alvo; break
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
            for i in range(delta.days + 1):
                dia = data_ini + timedelta(days=i)
                dia_str = dia.strftime("%Y-%m-%d")
                cacheable = is_cacheable(dia_str)
                
                p_page = 1
                while True:
                    params_cache = {"pesquisa": p_id, "limit": 1000}
                    cached = get_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), p_page) if cacheable else None
                    
                    if cached is not None:
                        data = cached
                    else:
                        p_params = {"data_inicial": dia_str, "data_final": dia_str, "pesquisa": p_id, "id_conta": conta, "limit": 1000, "page": p_page}
                        try:
                            r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=p_params)
                            time.sleep(0.3)
                            if r.status_code != 200: break
                            data = r.json()
                            if cacheable and isinstance(data, list): set_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), p_page, data)
                        except: break
                        
                    if not data or not isinstance(data, list): break
                    total_api = 0
                    for bloco in data:
                        if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                        id_perg = str(bloco.get("id_pergunta", ""))
                        if id_perg in IDS_PERGUNTAS_VALIDAS:
                            if id_perg in PERGUNTA_TEXTO: continue 
                            for resp in bloco.get("respostas", []):
                                try:
                                    nom_ag = normalizar_nome_pesquisa(resp.get("nom_agente", ""))
                                    cod_match = next((c for c in ids_agentes_stats if mapa_agentes.get(c) == nom_ag), None)
                                    
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

    score_geral = (csat_geral_pos / csat_geral_total * 100) if csat_geral_total > 0 else 0.0
    return resultados, score_geral, csat_geral_total, mapa_agentes, dados_globais, volume_total_setor

def _processar_agente_pausas(token, cod_agente, nome_agente, data_ini, data_fim):
    headers = {"Authorization": f"Bearer {token}"}
    local_curtas, local_almoco, local_logins, local_ranking = [], [], [], []
    
    pausas_agente = []
    delta = data_fim - data_ini
    for i in range(delta.days + 1):
        dia = data_ini + timedelta(days=i)
        dia_str = dia.strftime("%Y-%m-%d")
        cacheable = is_cacheable(dia_str)
        
        pagina = 1
        while pagina <= 5:
            params_cache = {"limit": 100}
            cached = get_cache(dia_str, "relAgentePausa", "ALL", json.dumps(params_cache, sort_keys=True), pagina) if cacheable else None
            
            if cached is not None:
                data = cached
            else:
                params = {"dat_inicial": dia_str, "dat_final": dia_str, "limit": 100, "pagina": pagina}
                try:
                    r = requests.get(f"{BASE_URL}/relAgentePausa", headers=headers, params=params, timeout=10)
                    time.sleep(0.3)
                    if r.status_code != 200: break
                    data = r.json()
                    if cacheable: set_cache(dia_str, "relAgentePausa", "ALL", json.dumps(params_cache, sort_keys=True), pagina, data)
                except: break
            
            rows = data.get("rows", [])
            if not rows: break
            for row in rows:
                if str(row.get("id_agente")) == str(cod_agente):
                    pausas_agente.append(row)
            if len(rows) < 100: break
            pagina += 1
            
    acumulado_excesso_curta = 0.0
    for p in pausas_agente:
        motivo = str(p.get("pausa", "")).upper()
        try: seg = float(p.get("seg_pausado", 0))
        except: seg = 0
        minutos = seg / 60
        limite_aplicado = 0
        tipo_pausa = None
        
        if any(x in motivo for x in ["MANHA", "MANHÃ", "TARDE", "NOITE"]):
            limite_aplicado = LIMITES_PAUSA["CURTA_ANTIGA"]; tipo_pausa = "CURTA"
        elif any(x in motivo for x in ["PAUSA 1", "PAUSA 3"]):
            limite_aplicado = LIMITES_PAUSA["PAUSA_1_3"]; tipo_pausa = "CURTA"
        elif any(x in motivo for x in ["PAUSA 2"]):
            limite_aplicado = LIMITES_PAUSA["PAUSA_2"]; tipo_pausa = "LONGA" 
        elif any(x in motivo for x in ["ALMOÇO", "ALMOCO", "PLANTÃO", "PLANTAO"]):
            limite_aplicado = LIMITES_PAUSA["LONGA_ANTIGA"]; tipo_pausa = "LONGA"
            
        if tipo_pausa == "CURTA":
            if minutos > limite_aplicado: acumulado_excesso_curta += (minutos - limite_aplicado)
        elif tipo_pausa == "LONGA":
            if minutos > (limite_aplicado + TOLERANCIA_VISUAL_ALMOCO):
                excesso = minutos - limite_aplicado
                local_almoco.append({"Agente": nome_agente, "Data": p.get("data_pausa", "")[:10], "Duração": formatar_tempo_humano(minutos), "Status": f"Estourou {formatar_tempo_humano(excesso)}"})
    
    status_curta = "Normal"
    if acumulado_excesso_curta > TOLERANCIA_MENSAL_EXCESSO: status_curta = "ADVERTÊNCIA"
    if acumulado_excesso_curta > 0:
        local_curtas.append({"Agente": nome_agente, "Excesso Acumulado": formatar_tempo_humano(acumulado_excesso_curta), "Valor Num": acumulado_excesso_curta, "Status": status_curta})
        
    qtd_pausas = len([p for p in pausas_agente if "TERMINO" not in str(p.get("pausa")).upper() and "EXPEDIENTE" not in str(p.get("pausa")).upper()])
    if qtd_pausas > 0:
        local_ranking.append({"Agente": nome_agente, "Qtd Pausas": qtd_pausas})

    logins_raw = []
    for i in range(delta.days + 1):
        dia = data_ini + timedelta(days=i)
        dia_str = dia.strftime("%Y-%m-%d")
        cacheable = is_cacheable(dia_str)
        
        page_log = 1
        while page_log <= 2:
            params_cache = {"limit": 100}
            cached = get_cache(dia_str, "relAgenteLogin", "ALL", json.dumps(params_cache, sort_keys=True), page_log) if cacheable else None
            
            if cached is not None:
                data = cached
            else:
                params_log = {"data_inicial": dia_str, "data_final": dia_str, "page": page_log, "limit": 100}
                try:
                    r = requests.get(f"{BASE_URL}/relAgenteLogin", headers=headers, params=params_log, timeout=10)
                    time.sleep(0.3)
                    if r.status_code != 200: break
                    data = r.json()
                    if cacheable: set_cache(dia_str, "relAgenteLogin", "ALL", json.dumps(params_cache, sort_keys=True), page_log, data)
                except: break
                
            rows = data.get("rows", [])
            if not rows: break
            for row in rows:
                if str(row.get("id_agente")) == str(cod_agente):
                    logins_raw.append(row)
            if len(rows) < 100: break
            page_log += 1
        
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
            local_logins.append({"Agente": nome_agente, "Data": d, "Hora Entrada": dt.strftime("%H:%M:%S"), "Atraso": f"{mins}m"})
    
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
    for i, o in logins_ordenados: mapa_dias[i.date()].append((i, o))
        
    for dia, periodos in mapa_dias.items():
        for i in range(len(periodos) - 1):
            logout_atual = periodos[i][1]
            login_proximo = periodos[i+1][0]
            delta_min = (login_proximo - logout_atual).total_seconds() / 60.0
            
            if delta_min > 30:
                 limite_longa = LIMITES_PAUSA["LONGA_ANTIGA"]
                 if delta_min > (limite_longa + TOLERANCIA_VISUAL_ALMOCO):
                     excesso = delta_min - limite_longa
                     local_almoco.append({"Agente": nome_agente, "Data": dia.strftime("%d/%m/%Y"), "Duração": formatar_tempo_humano(delta_min), "Status": f"Gap Deslogue: Estourou {formatar_tempo_humano(excesso)}"})
            
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
    delta = data_fim - data_ini
    
    for conta in contas_selecionadas:
        for i in range(delta.days + 1):
            dia = data_ini + timedelta(days=i)
            dia_str = dia.strftime("%Y-%m-%d")
            cacheable = is_cacheable(dia_str)
            
            params_cache = {"agrupador": "agente", "canal[]": ids_canais}
            cached = get_cache(dia_str, "relAtEstatistico", conta, json.dumps(params_cache, sort_keys=True), 1) if cacheable else None
            
            if cached is not None:
                data = cached
            else:
                params_stats = {"data_inicial": f"{dia_str} 00:00:00", "data_final": f"{dia_str} 23:59:59", "agrupador": "agente", "canal[]": ids_canais, "id_conta": conta}
                try:
                    r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_stats)
                    time.sleep(0.3)
                    if r.status_code == 200:
                        data = r.json()
                        if cacheable and isinstance(data, list): set_cache(dia_str, "relAtEstatistico", conta, json.dumps(params_cache, sort_keys=True), 1, data)
                    else: continue
                except: continue
                
            if data and isinstance(data, list):
                for item in data:
                    nome_api = str(item.get("agrupador", "")).strip().upper()
                    cod_match = next((c for c in ids_validos if mapa_agentes[c] == nome_api), None)
                    
                    if cod_match:
                        qtd = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                        if qtd > 0:
                            dados_stats[cod_match]["Vol"] += qtd
                            dados_stats[cod_match]["pesos"].append(qtd)
                            dados_stats[cod_match]["TMA"].append(item.get("tma", "--:--"))
                            dados_stats[cod_match]["TME"].append(item.get("tme", "--:--"))
                            dados_stats[cod_match]["TMIA"].append(item.get("tmia", "--:--"))
                            dados_stats[cod_match]["TMIC"].append(item.get("tmic", "--:--"))

    dados_csat = {cod: {"pos": 0, "tot": 0} for cod in ids_validos}
    for pid in PESQUISAS_IDS:
        for conta in contas_selecionadas:
            for i in range(delta.days + 1):
                dia = data_ini + timedelta(days=i)
                dia_str = dia.strftime("%Y-%m-%d")
                cacheable = is_cacheable(dia_str)
                
                pg = 1
                while True:
                    params_cache = {"pesquisa": pid, "limit": 1000}
                    cached = get_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), pg) if cacheable else None
                    
                    if cached is not None:
                        dd = cached
                    else:
                        pars = {"data_inicial": dia_str, "data_final": dia_str, "pesquisa": pid, "id_conta": conta, "limit": 1000, "page": pg}
                        try:
                            rr = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=pars)
                            time.sleep(0.5)
                            if rr.status_code != 200: break
                            dd = rr.json()
                            if cacheable and isinstance(dd, list): set_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), pg, dd)
                        except: break
                        
                    if not dd or not isinstance(dd, list): break
                    
                    found_pg = False
                    total_k = 0
                    for b in dd:
                        if b.get("sintetico"): total_k += sum(int(x.get("num_quantidade", 0)) for x in b["sintetico"])
                        id_perg = str(b.get("id_pergunta",""))
                        if id_perg in IDS_PERGUNTAS_VALIDAS:
                            if id_perg in PERGUNTA_TEXTO: continue 
                            found_pg = True
                            for rsp in b.get("respostas", []):
                                try:
                                    nom_ag = normalizar_nome_pesquisa(rsp.get("nom_agente", ""))
                                    cod_match = next((c for c in ids_validos if mapa_agentes[c] == nom_ag), None)
                                    
                                    if cod_match:
                                        val = float(rsp.get("nom_valor", -1))
                                        if val >= 0:
                                            dados_csat[cod_match]["tot"] += 1
                                            if val >= 8: dados_csat[cod_match]["pos"] += 1
                                except: pass
                                
                    if (pg * 1000) >= total_k: break
                    if not found_pg and len(dd) < 5: break
                    if len(dd) < 100: break
                    pg += 1
            
    for cod, nome in mapa_agentes.items():
        st_data = dados_stats[cod]
        d_csat = dados_csat.get(cod, {"pos": 0, "tot": 0})
        pos = d_csat["pos"]
        tot = d_csat["tot"]
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

@st.cache_data(ttl=60)
def buscar_pre_pausas_detalhado(token, id_agente, data_ini, data_fim):
    url = f"{BASE_URL}/relPausasAgendadas"
    headers = {"Authorization": f"Bearer {token}"}
    todas_pre_pausas = []
    
    delta = data_fim - data_ini
    for i in range(delta.days + 1):
        dia = data_ini + timedelta(days=i)
        dia_str = dia.strftime("%Y-%m-%d")
        cacheable = is_cacheable(dia_str)
        
        page = 1
        while True:
            params_cache = {"limit": 100}
            cached = get_cache(dia_str, "relPausasAgendadas", "ALL", json.dumps(params_cache, sort_keys=True), page) if cacheable else None
            
            if cached is not None:
                data = cached
            else:
                params_req = {"data_inicial": dia_str, "data_final": dia_str, "page": page, "limit": 100}
                try:
                    r = requests.get(url, headers=headers, params=params_req, timeout=10)
                    time.sleep(0.3)
                    if r.status_code != 200: break
                    data = r.json()
                    if cacheable: set_cache(dia_str, "relPausasAgendadas", "ALL", json.dumps(params_cache, sort_keys=True), page, data)
                except: break
                
            rows = data.get("rows", [])
            if not rows: break
            for row in rows:
                if str(row.get("id_agene")) == str(id_agente):  # Nota: A API retorna 'id_agene' com erro de digitação original
                    todas_pre_pausas.append(row)
            if len(rows) < 100: break
            page += 1
            
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
                if data_ini_str: inicio_fmt = datetime.strptime(data_ini_str, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M:%S")
                if data_fim_str: fim_fmt = datetime.strptime(data_fim_str, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M:%S")
            except: pass
            lista_retorno.append({"Agente": nome, "Início": inicio_fmt, "Término": fim_fmt, "Duração": duracao_str, "Motivo": motivo_str})
        return lista_retorno

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(_fetch_pre_pausa, cod, nome): nome for cod, nome in mapa_agentes.items()}
        for f in concurrent.futures.as_completed(futures):
            try:
                res = f.result()
                if res: resultados.extend(res)
            except: pass
    return resultados

@st.cache_data(ttl=300)
def buscar_dados_plantao(token, data_ini, data_fim, contas_selecionadas):
    headers = {"Authorization": f"Bearer {token}"}
    ids_plantao = []
    mapa_plantao = {}
    ids_alvo = LISTA_PLANTAO_IDS
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
                cod = str(agente.get("cod_agente"))
                if cod in ids_alvo:
                    nome_raw = str(agente.get("nome_exibicao") or agente.get("agente")).strip().upper()
                    ids_plantao.append(cod)
                    mapa_plantao[cod] = nome_raw
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break
        
    ids_canais = buscar_ids_canais(token)
    if not ids_plantao: return pd.DataFrame(), {}, {}

    lista_stats_agente = []
    agentes_agregados = {ag: {"Vol": 0, "pesos": [], "TMA": [], "TMIA": []} for ag in mapa_plantao.values()}
    delta = data_fim - data_ini
    
    for conta in contas_selecionadas:
        for i in range(delta.days + 1):
            dia = data_ini + timedelta(days=i)
            dia_str = dia.strftime("%Y-%m-%d")
            cacheable = is_cacheable(dia_str)
            
            params_cache = {"agrupador": "agente", "canal[]": ids_canais}
            cached = get_cache(dia_str, "relAtEstatistico", conta, json.dumps(params_cache, sort_keys=True), 1) if cacheable else None
            
            if cached is not None:
                data = cached
            else:
                params_agente = {"data_inicial": f"{dia_str} 00:00:00", "data_final": f"{dia_str} 23:59:59", "agrupador": "agente", "canal[]": ids_canais, "id_conta": conta}
                try:
                    r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_agente)
                    time.sleep(0.3)
                    if r.status_code == 200:
                        data = r.json()
                        if cacheable and isinstance(data, list): set_cache(dia_str, "relAtEstatistico", conta, json.dumps(params_cache, sort_keys=True), 1, data)
                    else: continue
                except: continue
                
            if data and isinstance(data, list):
                for item in data:
                    nome_api = str(item.get("agrupador", "")).strip().upper()
                    cod_match = next((c for c in ids_plantao if mapa_plantao[c] == nome_api), None)
                    
                    if cod_match:
                        agente_nome = mapa_plantao[cod_match]
                        qtd = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                        if qtd > 0:
                            agentes_agregados[agente_nome]["Vol"] += qtd
                            agentes_agregados[agente_nome]["pesos"].append(qtd)
                            agentes_agregados[agente_nome]["TMA"].append(item.get("tma", "00:00:00"))
                            agentes_agregados[agente_nome]["TMIA"].append(item.get("tmia", "00:00:00"))
        
    for ag, st_data in agentes_agregados.items():
        if st_data["Vol"] > 0:
            lista_stats_agente.append({
                "Agente": ag, "Volume": st_data["Vol"],
                "TMA": calcular_media_tempos(st_data["TMA"], st_data["pesos"]),
                "TMIA": calcular_media_tempos(st_data["TMIA"], st_data["pesos"]),
                "CSAT": 0.0, "Qtd CSAT": 0
            })
    
    stats_por_servico = {}
    for conta in contas_selecionadas:
        for i in range(delta.days + 1):
            dia = data_ini + timedelta(days=i)
            dia_str = dia.strftime("%Y-%m-%d")
            cacheable = is_cacheable(dia_str)
            
            params_cache = {"agrupador": "servico", "canal[]": ids_canais}
            cached = get_cache(dia_str, "relAtEstatisticoServico", conta, json.dumps(params_cache, sort_keys=True), 1) if cacheable else None
            
            if cached is not None:
                data = cached
            else:
                params_servico = {"data_inicial": f"{dia_str} 00:00:00", "data_final": f"{dia_str} 23:59:59", "agrupador": "servico", "canal[]": ids_canais, "id_conta": conta}
                try:
                    r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_servico)
                    time.sleep(0.3)
                    if r.status_code == 200:
                        data = r.json()
                        if cacheable and isinstance(data, list): set_cache(dia_str, "relAtEstatisticoServico", conta, json.dumps(params_cache, sort_keys=True), 1, data)
                    else: continue
                except: continue
                
            if data and isinstance(data, list):
                for item in data:
                    serv = item.get("agrupador", "Outros")
                    if serv not in stats_por_servico: stats_por_servico[serv] = {"num_qtd": 0, "pesos": [], "tma": [], "tme": [], "tmia": [], "tmic": []}
                    qtd = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                    if qtd > 0:
                        stats_por_servico[serv]["num_qtd"] += qtd
                        stats_por_servico[serv]["pesos"].append(qtd)
                        stats_por_servico[serv]["tma"].append(item.get("tma", "00:00:00"))
                        stats_por_servico[serv]["tme"].append(item.get("tme", "00:00:00"))
                        stats_por_servico[serv]["tmia"].append(item.get("tmia", "00:00:00"))
                        stats_por_servico[serv]["tmic"].append(item.get("tmic", "00:00:00"))

    csat_scores = {}
    csat_servico = {}
    for pid in PESQUISAS_IDS:
        for conta in contas_selecionadas:
            for i in range(delta.days + 1):
                dia = data_ini + timedelta(days=i)
                dia_str = dia.strftime("%Y-%m-%d")
                cacheable = is_cacheable(dia_str)
                
                pg = 1
                while True:
                    params_cache = {"pesquisa": pid, "limit": 1000}
                    cached = get_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), pg) if cacheable else None
                    
                    if cached is not None:
                        dd = cached
                    else:
                        pars = {"data_inicial": dia_str, "data_final": dia_str, "pesquisa": pid, "id_conta": conta, "limit": 1000, "page": pg}
                        try:
                            rr = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=pars)
                            time.sleep(0.5)
                            if rr.status_code != 200: break
                            dd = rr.json()
                            if cacheable and isinstance(dd, list): set_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), pg, dd)
                        except: break
                        
                    if not dd or not isinstance(dd, list): break
                    total_k = 0
                    for b in dd:
                        if b.get("sintetico"): total_k += sum(int(x.get("num_quantidade", 0)) for x in b["sintetico"])
                        id_perg = str(b.get("id_pergunta",""))
                        if id_perg in IDS_PERGUNTAS_VALIDAS:
                            if id_perg in PERGUNTA_TEXTO: continue 
                            for rsp in b.get("respostas", []):
                                nom_ag = normalizar_nome_pesquisa(rsp.get("nom_agente",""))
                                cod_match = next((c for c in ids_plantao if mapa_plantao[c] == nom_ag), None)
                                
                                if cod_match:
                                    nome_match = mapa_plantao[cod_match]
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
                
    stats_servico_finais = {}
    for serv, st_data in stats_por_servico.items():
        if st_data["num_qtd"] > 0:
            c_pos = 0; c_tot = 0
            for s_k, s_v in csat_servico.items():
                if " ".join(s_k.split()) == " ".join(serv.upper().split()):
                    c_pos += s_v["pos"]; c_tot += s_v["tot"]
            stats_servico_finais[serv] = {
                "num_qtd": st_data["num_qtd"],
                "tma": calcular_media_tempos(st_data["tma"], st_data["pesos"]), "tme": calcular_media_tempos(st_data["tme"], st_data["pesos"]),
                "tmia": calcular_media_tempos(st_data["tmia"], st_data["pesos"]), "tmic": calcular_media_tempos(st_data["tmic"], st_data["pesos"]),
                "csat_pos": c_pos, "csat_tot": c_tot
            }
    for row in lista_stats_agente:
        ag = row["Agente"]
        if ag in csat_scores:
            d = csat_scores[ag]
            row["Qtd CSAT"] = d["tot"]
            row["CSAT"] = (d["pos"] / d["tot"] * 100) if d["tot"] > 0 else 0.0
    return pd.DataFrame(lista_stats_agente), stats_servico_finais, {}

@st.cache_data(ttl=300)
def buscar_dados_cliente_interno(token, data_ini, data_fim, ids_suporte_validos):
    headers = {"Authorization": f"Bearer {token}"}
    ids_canais = buscar_ids_canais(token) 
    nomes_suporte = []
    pagina = 1
    while True:
        try:
            r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": pagina, "bol_cancelado": 0})
            if r.status_code != 200: break
            data = r.json()
            rows = data.get("result", [])
            if not rows: break
            for agente in rows:
                if str(agente.get("cod_agente")) in ids_suporte_validos:
                    nomes_suporte.append(str(agente.get("nome_exibicao") or agente.get("agente")).strip().upper())
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break

    stats_globais = {"TMA": "--:--", "TME": "--:--", "TMIA": "--:--"}
    delta = data_fim - data_ini
    
    # Processo Estatístico do Cliente Interno
    tempos_ci = {"tma": [], "tme": [], "tmia": []}
    pesos_ci = []
    
    for i in range(delta.days + 1):
        dia = data_ini + timedelta(days=i)
        dia_str = dia.strftime("%Y-%m-%d")
        cacheable = is_cacheable(dia_str)
        
        params_cache = {"agrupador": "conta", "canal[]": ids_canais}
        cached = get_cache(dia_str, "relAtEstatistico", ID_CONTA_CLIENTE_INTERNO, json.dumps(params_cache, sort_keys=True), 1) if cacheable else None
        
        if cached is not None:
            lista = cached
        else:
            params = {"data_inicial": f"{dia_str} 00:00:00", "data_final": f"{dia_str} 23:59:59", "agrupador": "conta", "canal[]": ids_canais, "id_conta": ID_CONTA_CLIENTE_INTERNO}
            try:
                r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params)
                time.sleep(0.3)
                if r.status_code == 200:
                    lista = r.json()
                    if cacheable and isinstance(lista, list): set_cache(dia_str, "relAtEstatistico", ID_CONTA_CLIENTE_INTERNO, json.dumps(params_cache, sort_keys=True), 1, lista)
                else: continue
            except: continue
            
        if lista and isinstance(lista, list) and len(lista) > 0:
            item = lista[0]
            vol = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
            if vol > 0:
                pesos_ci.append(vol)
                tempos_ci["tma"].append(item.get("tma", "00:00:00"))
                tempos_ci["tme"].append(item.get("tme", "00:00:00"))
                tempos_ci["tmia"].append(item.get("tmia", "00:00:00"))
                
    if pesos_ci:
        stats_globais["TMA"] = calcular_media_tempos(tempos_ci["tma"], pesos_ci)
        stats_globais["TME"] = calcular_media_tempos(tempos_ci["tme"], pesos_ci)
        stats_globais["TMIA"] = calcular_media_tempos(tempos_ci["tmia"], pesos_ci)
    
    lista_pesquisas = []
    csat_pos = 0; csat_total = 0
    for p_id in PESQUISAS_IDS:
        for i in range(delta.days + 1):
            dia = data_ini + timedelta(days=i)
            dia_str = dia.strftime("%Y-%m-%d")
            cacheable = is_cacheable(dia_str)
            
            pg = 1
            while True:
                params_cache = {"pesquisa": p_id, "limit": 1000}
                cached = get_cache(dia_str, "RelPesqAnalitico", ID_CONTA_CLIENTE_INTERNO, json.dumps(params_cache, sort_keys=True), pg) if cacheable else None
                
                if cached is not None:
                    data = cached
                else:
                    params_p = {"data_inicial": dia_str, "data_final": dia_str, "pesquisa": p_id, "id_conta": ID_CONTA_CLIENTE_INTERNO, "limit": 1000, "page": pg}
                    try:
                        r = requests.get(url=f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=params_p)
                        time.sleep(0.5)
                        if r.status_code != 200: break
                        data = r.json()
                        if cacheable and isinstance(data, list): set_cache(dia_str, "RelPesqAnalitico", ID_CONTA_CLIENTE_INTERNO, json.dumps(params_cache, sort_keys=True), pg, data)
                    except: break
                    
                if not data or not isinstance(data, list): break
                total_api = 0
                for bloco in data:
                    if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                    id_perg = str(bloco.get("id_pergunta", ""))
                    if id_perg in IDS_PERGUNTAS_VALIDAS:
                        if id_perg in PERGUNTA_TEXTO: continue 
                        for resp in bloco.get("respostas", []):
                            nome_agente_resp = normalizar_nome_pesquisa(resp.get("nom_agente", ""))
                            eh_do_suporte = any(alvo == nome_agente_resp for alvo in nomes_suporte)
                            
                            if eh_do_suporte:
                                val_raw = resp.get("nom_valor")
                                if val_raw and val_raw != "": 
                                    nota = int(float(val_raw))
                                    csat_total += 1
                                    if nota >= 8: csat_pos += 1
                                    lista_pesquisas.append({"Data": resp.get("dat_resposta"), "Cliente": resp.get("nom_contato"), "Agente": nome_agente_resp, "Nota": nota, "Comentario": resp.get("nom_resposta"), "Protocolo": resp.get("num_protocolo")})
                if (pg * 1000) >= total_api: break
                if len(data) < 2: break
                pg += 1
                
    df_pesquisas = pd.DataFrame(lista_pesquisas)
    score = (csat_pos / csat_total * 100) if csat_total > 0 else 0.0
    return stats_globais, score, csat_total, df_pesquisas

@st.cache_data(ttl=300)
def buscar_dados_jovem_aprendiz(token, data_ini, data_fim, setor_nome, contas_selecionadas):
    headers = {"Authorization": f"Bearer {token}"}
    ids_ja_alvo = JOVENS_APRENDIZES_NRC_IDS if setor_nome == "NRC" else JOVENS_APRENDIZES_SUPORTE_IDS
    ids_ja = []
    mapa_ja = {}
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
                cod = str(agente.get("cod_agente"))
                if cod in ids_ja_alvo:
                    nome_raw = str(agente.get("nome_exibicao") or agente.get("agente")).strip().upper()
                    ids_ja.append(cod)
                    mapa_ja[cod] = nome_raw
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break

    ids_canais = buscar_ids_canais(token)
    stats_globais = {"Volume": 0, "TMA": "--:--", "TME": "--:--", "TMIA": "--:--", "TMIC": "--:--"}
    ranking_dict = {cod: {"Volume": 0, "pesos": [], "TMA": [], "TMIA": [], "TME": [], "TMIC": [], "CSAT_Pos": 0, "CSAT_Tot": 0} for cod in ids_ja}
    if not ids_ja: return stats_globais, [], pd.DataFrame(), 0.0

    tempos_globais = {"tma": [], "tme": [], "tmia": [], "tmic": []}
    pesos_globais = []
    delta = data_fim - data_ini
    
    for conta in contas_selecionadas:
        for i in range(delta.days + 1):
            dia = data_ini + timedelta(days=i)
            dia_str = dia.strftime("%Y-%m-%d")
            cacheable = is_cacheable(dia_str)
            
            params_cache = {"agrupador": "agente", "canal[]": ids_canais}
            cached = get_cache(dia_str, "relAtEstatistico", conta, json.dumps(params_cache, sort_keys=True), 1) if cacheable else None
            
            if cached is not None:
                lista = cached
            else:
                params_g = {"data_inicial": f"{dia_str} 00:00:00", "data_final": f"{dia_str} 23:59:59", "agrupador": "agente", "canal[]": ids_canais, "id_conta": conta}
                try:
                    r = requests.get(f"{BASE_URL}/relAtEstatistico", headers=headers, params=params_g)
                    time.sleep(0.3)
                    if r.status_code == 200:
                        lista = r.json()
                        if cacheable and isinstance(lista, list): set_cache(dia_str, "relAtEstatistico", conta, json.dumps(params_cache, sort_keys=True), 1, lista)
                    else: continue
                except: continue
                
            if lista and isinstance(lista, list):
                for item in lista:
                    nome_api = str(item.get("agrupador", "")).strip().upper()
                    cod_match = next((c for c in ids_ja if mapa_ja[c] == nome_api), None)
                    
                    if cod_match:
                        vol = int(item.get("num_qtd", 0)) - int(item.get("num_qtd_abandonado", 0))
                        if vol > 0:
                            stats_globais["Volume"] += vol
                            pesos_globais.append(vol)
                            tempos_globais["tma"].append(item.get("tma", "00:00:00"))
                            tempos_globais["tme"].append(item.get("tme", "00:00:00"))
                            tempos_globais["tmia"].append(item.get("tmia", "00:00:00"))
                            tempos_globais["tmic"].append(item.get("tmic", "00:00:00"))
                            
                            ranking_dict[cod_match]["Volume"] += vol
                            ranking_dict[cod_match]["pesos"].append(vol)
                            ranking_dict[cod_match]["TMA"].append(item.get("tma", "00:00:00"))
                            ranking_dict[cod_match]["TME"].append(item.get("tme", "00:00:00"))
                            ranking_dict[cod_match]["TMIA"].append(item.get("tmia", "00:00:00"))
                            ranking_dict[cod_match]["TMIC"].append(item.get("tmic", "00:00:00"))

    if pesos_globais:
        stats_globais["TMA"] = calcular_media_tempos(tempos_globais["tma"], pesos_globais)
        stats_globais["TME"] = calcular_media_tempos(tempos_globais["tme"], pesos_globais)
        stats_globais["TMIA"] = calcular_media_tempos(tempos_globais["tmia"], pesos_globais)
        stats_globais["TMIC"] = calcular_media_tempos(tempos_globais["tmic"], pesos_globais)

    lista_pesquisas = []
    csat_geral_pos = 0; csat_geral_tot = 0
    for p_id in PESQUISAS_IDS:
        for conta in contas_selecionadas:
            for i in range(delta.days + 1):
                dia = data_ini + timedelta(days=i)
                dia_str = dia.strftime("%Y-%m-%d")
                cacheable = is_cacheable(dia_str)
                
                pg = 1
                while True:
                    params_cache = {"pesquisa": p_id, "limit": 1000}
                    cached = get_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), pg) if cacheable else None
                    
                    if cached is not None:
                        data = cached
                    else:
                        params_p = {"data_inicial": dia_str, "data_final": dia_str, "pesquisa": p_id, "id_conta": conta, "limit": 1000, "page": pg}
                        try:
                            r_p = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=params_p)
                            time.sleep(0.5)
                            if r_p.status_code != 200: break
                            data = r_p.json()
                            if cacheable and isinstance(data, list): set_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), pg, data)
                        except: break
                        
                    if not data or not isinstance(data, list): break
                    total_api = 0
                    for bloco in data:
                        if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                        id_perg = str(bloco.get("id_pergunta", ""))
                        if id_perg in IDS_PERGUNTAS_VALIDAS:
                            if id_perg in PERGUNTA_TEXTO: continue 
                            for resp in bloco.get("respostas", []):
                                nom_ag = normalizar_nome_pesquisa(resp.get("nom_agente",""))
                                cod_match = next((c for c in ids_ja if mapa_ja[c] == nom_ag), None)
                                
                                if cod_match:
                                    val_raw = resp.get("nom_valor")
                                    if val_raw and val_raw != "": 
                                        nota = int(float(val_raw))
                                        csat_geral_tot += 1
                                        ranking_dict[cod_match]["CSAT_Tot"] += 1
                                        if nota >= 8: 
                                            csat_geral_pos += 1
                                            ranking_dict[cod_match]["CSAT_Pos"] += 1
                                        lista_pesquisas.append({"Data": resp.get("dat_resposta"), "Agente": mapa_ja[cod_match], "Cliente": resp.get("nom_contato"), "Nota": nota, "Comentario": resp.get("nom_resposta"), "Protocolo": resp.get("num_protocolo")})
                    if (pg * 1000) >= total_api: break
                    if len(data) < 2: break
                    pg += 1

    ranking_final = []
    for cod, d in ranking_dict.items():
        if d["Volume"] > 0 or d["CSAT_Tot"] > 0:
            tma_str = calcular_media_tempos(d["TMA"], d["pesos"]) if d["Volume"] > 0 else "00:00:00"
            tmia_str = calcular_media_tempos(d["TMIA"], d["pesos"]) if d["Volume"] > 0 else "00:00:00"
            score = (d["CSAT_Pos"] / d["CSAT_Tot"] * 100) if d["CSAT_Tot"] > 0 else 0.0
            tmia_seg = time_str_to_seconds(tmia_str)
            alerta = False; motivos_alerta = []
            if tmia_seg > 60:
                alerta = True; motivos_alerta.append("TMIA Alto")
            if d["CSAT_Tot"] > 0 and score < 90.0:
                alerta = True; motivos_alerta.append("CSAT Baixo")
            status_alerta = "🔴 " + " e ".join(motivos_alerta) if alerta else "🟢 OK"
            ranking_final.append({"Agente": mapa_ja[cod], "Volume": d["Volume"], "TMA": tma_str, "TMIA": tmia_str, "CSAT": score, "Qtd CSAT": d["CSAT_Tot"], "Status": status_alerta})

    score_global = (csat_geral_pos / csat_geral_tot * 100) if csat_geral_tot > 0 else 0.0
    return stats_globais, ranking_final, pd.DataFrame(lista_pesquisas), score_global

@st.cache_data(ttl=300)
def buscar_dados_satisfacao(token, data_ini, data_fim, contas_selecionadas, mapa_agentes):
    """Puxa a pesquisa informada no secrets, cruzando Nota e Justificativa."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/RelPesqAnalitico"
    ids_validos = list(mapa_agentes.keys())
    protocolos = defaultdict(dict)
    
    delta = data_fim - data_ini
    for p_id in PESQUISA_SATISFACAO:
        for conta in contas_selecionadas:
            for i in range(delta.days + 1):
                dia = data_ini + timedelta(days=i)
                dia_str = dia.strftime("%Y-%m-%d")
                cacheable = is_cacheable(dia_str)
                
                page = 1
                while True:
                    params_cache = {"pesquisa": p_id, "limit": 1000}
                    cached = get_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), page) if cacheable else None
                    
                    if cached is not None:
                        data = cached
                    else:
                        params = {"data_inicial": dia_str, "data_final": dia_str, "pesquisa": p_id, "id_conta": conta, "limit": 1000, "page": page}
                        try:
                            r = requests.get(url, headers=headers, params=params, timeout=30)
                            time.sleep(0.5)
                            if r.status_code != 200: break
                            data = r.json()
                            if cacheable and isinstance(data, list): set_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), page, data)
                        except: break
                        
                    if not data or not isinstance(data, list): break
                    total_api = 0
                    for bloco in data:
                        if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                        id_perg = str(bloco.get("id_pergunta", ""))
                        if id_perg not in (PERGUNTA_NOTA + PERGUNTA_TEXTO): continue
                        respostas = bloco.get("respostas", [])
                        for resp in respostas:
                            proto = str(resp.get("num_protocolo", "")).strip()
                            if not proto: continue
                            nom_ag = normalizar_nome_pesquisa(resp.get("nom_agente", ""))
                            cod_match = next((c for c in ids_validos if mapa_agentes[c] == nom_ag), None)
                            
                            if cod_match:
                                nome_agente_limpo = mapa_agentes[cod_match]
                                if proto not in protocolos:
                                     protocolos[proto] = {"Protocolo": proto, "Data": resp.get("dat_resposta", ""), "Cliente": resp.get("nom_contato", "Desconhecido"), "Agente": nome_agente_limpo, "Servico": resp.get("nom_servico", "Geral").upper().strip(), "Nota": None, "Comentario": ""}
                                if id_perg in PERGUNTA_NOTA:
                                    val_raw = resp.get("nom_valor")
                                    if val_raw and val_raw != "":
                                        try: protocolos[proto]["Nota"] = int(float(val_raw))
                                        except: pass
                                elif id_perg in PERGUNTA_TEXTO:
                                    coment = resp.get("nom_resposta") or resp.get("nom_valor") or ""
                                    protocolos[proto]["Comentario"] = str(coment).replace("##1F621##", "").replace("##1F603##", "").strip()
                    if (page * 1000) >= total_api: break
                    if len(data) < 2: break
                    page += 1
    return list(protocolos.values())

@st.cache_data(ttl=300)
def buscar_csat_unificado_suporte(token, data_ini, data_fim, contas_selecionadas):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/RelPesqAnalitico"
    todos_ids_suporte = list(set(SETORES_AGENTES_IDS.get("SUPORTE", []) + LISTA_PLANTAO_IDS + JOVENS_APRENDIZES_SUPORTE_IDS))
    
    nomes_autorizados = []
    pagina = 1
    while True:
        try:
            r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": pagina, "bol_cancelado": 0})
            time.sleep(0.3)
            if r.status_code != 200: break
            data = r.json()
            if not data.get("result"): break
            for ag in data["result"]:
                if str(ag.get("cod_agente")) in todos_ids_suporte:
                    nomes_autorizados.append(str(ag.get("nome_exibicao") or ag.get("agente")).strip().upper())
            if pagina * 100 >= data.get("total", 0): break
            pagina += 1
        except: break

    csat_pos = 0; csat_total = 0
    delta = data_fim - data_ini
    
    for p_id in PESQUISAS_IDS:
        for conta in contas_selecionadas:
            for i in range(delta.days + 1):
                dia = data_ini + timedelta(days=i)
                dia_str = dia.strftime("%Y-%m-%d")
                cacheable = is_cacheable(dia_str)
                
                page = 1
                while True:
                    params_cache = {"pesquisa": p_id, "limit": 1000, "agente[]": todos_ids_suporte}
                    cached = get_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), page) if cacheable else None
                    
                    if cached is not None:
                        data = cached
                    else:
                        params = {
                            "data_inicial": dia_str, "data_final": dia_str,
                            "pesquisa": p_id, "id_conta": conta, "limit": 1000, "page": page, "agente[]": todos_ids_suporte
                        }
                        try:
                            r = requests.get(url, headers=headers, params=params, timeout=30)
                            time.sleep(0.5)
                            if r.status_code != 200: break
                            data = r.json()
                            if cacheable and isinstance(data, list): set_cache(dia_str, "RelPesqAnalitico", conta, json.dumps(params_cache, sort_keys=True), page, data)
                        except: break
                        
                    if not data or not isinstance(data, list): break
                    total_api = 0
                    for bloco in data:
                        if bloco.get("sintetico"): total_api += sum(int(x.get("num_quantidade", 0)) for x in bloco["sintetico"])
                        id_perg = str(bloco.get("id_pergunta", ""))
                        if id_perg in IDS_PERGUNTAS_VALIDAS:
                            if id_perg in PERGUNTA_TEXTO: continue 
                            for resp in bloco.get("respostas", []):
                                nom_ag = normalizar_nome_pesquisa(resp.get("nom_agente", ""))
                                
                                eh_da_equipe = False
                                for nome_auth in nomes_autorizados:
                                    if nom_ag == nome_auth:
                                        eh_da_equipe = True
                                        break
                                
                                if eh_da_equipe:
                                    val_raw = resp.get("nom_valor")
                                    if val_raw and val_raw != "":
                                        try:
                                            nota = int(float(val_raw))
                                            if nota >= 0:
                                                csat_total += 1
                                                if nota >= 8: csat_pos += 1
                                        except: pass
                    if (page * 1000) >= total_api: break
                    if len(data) < 2: break
                    page += 1
                    
    score = (csat_pos / csat_total * 100) if csat_total > 0 else 0.0
    return score, csat_total
