import streamlit as st
import pandas as pd
import time
from datetime import datetime, timedelta
from collections import defaultdict

# ==============================================================================
# IMPORTAÇÃO DOS MÓDULOS (ARQUITETURA SEGMENTADA)
# ==============================================================================
from config import (
    SUPERVISOR_LOGIN, SUPERVISOR_PASS,
    SUPERVISOR_CANCELAMENTO_LOGIN, SUPERVISOR_CANCELAMENTO_PASS,
    SUPERVISOR_SUPORTE_LOGIN, SUPERVISOR_SUPORTE_PASS,
    SUPERVISOR_NEGOCIACAO_LOGIN, SUPERVISOR_NEGOCIACAO_PASS,
    MASTER_LOGIN, MASTER_PASS,
    ID_CONTA, CONTAS_DISPONIVEIS, SETORES_AGENTES_IDS, SERVICOS_ALVO, SETORES_SERVICOS,
    LIMITES_PAUSA, TOLERANCIA_MENSAL_EXCESSO, TOLERANCIA_VISUAL_ALMOCO,
    JOVENS_APRENDIZES_NRC_IDS, JOVENS_APRENDIZES_SUPORTE_IDS
)
from gsheets import (
    salvar_solicitacao_gsheets, salvar_diario_bordo,
    ler_solicitacoes_gsheets, ler_diario_bordo
)
from utils import (
    time_str_to_seconds, formatar_tempo_humano, gerar_link_protocolo, eleger_melhor_do_mes
)
from components import (
    inject_custom_css, render_podium, render_kpi_card,
    render_link_card, render_top_bar
)
from api import (
    get_admin_token, validar_agente_api, buscar_historico_login,
    buscar_estatisticas_agente, buscar_csat_nrc, buscar_pausas_detalhado,
    buscar_dados_completos_supervisor, buscar_dados_supervisor_multisetor,
    processar_dados_pausas_supervisor, processar_ranking_geral,
    processar_dados_pre_pausas_geral, buscar_dados_plantao,
    buscar_dados_cliente_interno, buscar_dados_jovem_aprendiz,
    buscar_agentes_online_filtrado_nrc, buscar_agentes_online_filtrado_setor,
    forcar_logout
)

# ==============================================================================
# CONFIGURAÇÃO VISUAL E BLOQUEIO DE SEGURANÇA
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
                if senha_digitada == st.secrets["security"]["MASTER_PASSWORD"]:
                    st.session_state.app_unlocked = True
                    st.rerun()
                else:
                    st.error("Senha incorreta.")
            except:
                st.error("Erro: Configure [security] MASTER_PASSWORD no Secrets.")
    st.stop()

check_master_password()
inject_custom_css()

# ==============================================================================
# MENU LATERAL E COMPONENTES DE TELA
# ==============================================================================
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
        with st.expander("📜 Versão Platinum 17.0"):
            st.markdown("""
            **v17.0 - Refatoração Completa de Autenticidade (IDs) e Modularização**
            - **Segurança de Volumetria:** A coleta de dados do sistema não se baseia mais em nomes comuns ou fragmentos. A integração foi refeita utilizando a **Chave Primária (cod_agente)** para 100% dos setores.
            - **Prevenção de Roubo de Volume:** Resolução definitiva para conflitos de homônimos (como os casos da Maria, Leticia, Alice, etc.).
            - **Arquitetura Modular:** O sistema foi dividido em módulos para facilitar a manutenção futura.
            """)
        if st.button("🚪 Sair", use_container_width=True):
            st.session_state.auth_status = False; st.session_state.user_data = None; st.rerun()
        return d_ini, d_fim

# ==============================================================================
# ROTEAMENTO E AUTENTICAÇÃO
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
        setor_selecionado = st.sidebar.selectbox("Visualizar Setor:", list(SETORES_AGENTES_IDS.keys()), index=0)
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
            ids_exclusao = JOVENS_APRENDIZES_NRC_IDS if setor_atual == "NRC" else JOVENS_APRENDIZES_SUPORTE_IDS
            for cod, nome in mapa_agentes_sidebar.items():
                if cod not in ids_exclusao:
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
                ids_ja = JOVENS_APRENDIZES_NRC_IDS if setor_atual == "NRC" else JOVENS_APRENDIZES_SUPORTE_IDS
                
                for agente_online in lista_online_bruta:
                    aid = str(agente_online.get("cod"))
                    if aid not in ids_ja:
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
                stats_ci, score_ci, total_ci, df_ci = buscar_dados_cliente_interno(token, d_inicial, d_final, SETORES_AGENTES_IDS["SUPORTE"])
                render_kpi_card("CSAT Interno", f"{score_ci:.2f}%", f"Base: {total_ci}", "#10b981")
                if not df_ci.empty: st.dataframe(df_ci, use_container_width=True, hide_index=True)