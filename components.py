import streamlit as st
from utils import time_str_to_seconds

# ==============================================================================
# CSS E ESTILIZAÇÃO GERAL
# ==============================================================================

def inject_custom_css():
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
        
        /* Cards KPI */
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

# ==============================================================================
# FUNÇÕES DE RENDERIZAÇÃO
# ==============================================================================

def render_podium(titulo, dados, metrica, formato, inverso=False):
    st.markdown(f"##### {titulo}")
    if not dados: return st.info("Sem dados.")
    c1, c2, c3 = st.columns(3)
    
    # Ordenação. Se for tempo (TMA/TMIA), converte para segundos para ordenar
    if metrica in ["TMA", "TMIA"]:
        rev = True if inverso else False
        top = sorted(dados, key=lambda x: time_str_to_seconds(x[metrica]), reverse=rev)[:3]
    else:
        rev = False if inverso else True
        top = sorted(dados, key=lambda x: x[metrica], reverse=rev)[:3]
    
    emojis = ["🥇", "🥈", "🥉"] if not inverso else ["🔻", "🔻", "🔻"]
    cols = [c1, c2, c3]
    
    for i, item in enumerate(top):
        if i >= 3: break
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
        <div><span style="color:#6366f1; font-weight:bold;">PORTAL</span> <span style="color:white;">DO CALLCENTER</span></div>
        <div style="display:flex; align-items:center; gap:15px;">
            <div style="text-align:right; line-height:1.2;">
                <div style="color:white; font-weight:600; font-size:0.9rem;">{nome}</div>
                <div style="color:#9ca3af; font-size:0.75rem;">ID: {display_id}</div>
            </div>
            <div style="background:#374151; width:35px; height:35px; border-radius:50%; display:flex; align-items:center; justify-content:center; color:white; font-weight:bold; border:2px solid #6366f1;">{iniciais}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)