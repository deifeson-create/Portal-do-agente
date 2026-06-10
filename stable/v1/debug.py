import streamlit as st
import requests
from datetime import datetime, timedelta

# ==============================================================================
# 1. CREDENCIAIS (Puxando direto do seu secrets)
# ==============================================================================
try:
    BASE_URL = st.secrets["api"]["BASE_URL"]
    ADMIN_USER = st.secrets["api"]["ADMIN_USER"]
    ADMIN_PASS = st.secrets["api"]["ADMIN_PASS"]
    ID_CONTA = st.secrets["api"]["ID_CONTA"]
except Exception as e:
    st.error(f"Erro ao carregar o secrets: {e}")
    st.stop()

st.set_page_config(page_title="Raio-X Matrix API", page_icon="🔎")

# ==============================================================================
# 2. FUNÇÕES DE REQUISIÇÃO
# ==============================================================================
def get_token():
    r = requests.post(f"{BASE_URL}/authuser", json={"login": ADMIN_USER, "chave": ADMIN_PASS})
    if r.status_code == 200:
        return r.json().get("result", {}).get("token")
    return None

def puxar_raio_x(token, pesquisa_id, dias):
    headers = {"Authorization": f"Bearer {token}"}
    hoje = datetime.now()
    data_inicio = hoje - timedelta(days=dias)
    
    params = {
        "data_inicial": data_inicio.strftime("%Y-%m-%d"),
        "data_final": hoje.strftime("%Y-%m-%d"),
        "pesquisa": pesquisa_id,
        "id_conta": ID_CONTA,
        "limit": 50, # Puxa uma amostra para análise
        "page": 1
    }
    
    r = requests.get(f"{BASE_URL}/RelPesqAnalitico", headers=headers, params=params)
    if r.status_code == 200:
        return r.json()
    return None

# ==============================================================================
# 3. INTERFACE DO RAIO-X
# ==============================================================================
st.title("🔎 Raio-X da Matrix API")
st.markdown("Use esta ferramenta para descobrir os IDs reais das perguntas e os nomes exatos dos serviços.")

col1, col2 = st.columns(2)
with col1:
    pesq_alvo = st.number_input("ID da Pesquisa", value=67, step=1)
with col2:
    dias_alvo = st.number_input("Puxar dados dos últimos (dias)", value=15, step=1)

if st.button("Executar Scanner 🚀", type="primary"):
    with st.spinner("Autenticando e extraindo dados da Matrix..."):
        token = get_token()
        if not token:
            st.error("Falha na autenticação com a API.")
        else:
            dados = puxar_raio_x(token, pesq_alvo, dias_alvo)
            
            if not dados or not isinstance(dados, list):
                st.warning("Nenhum dado encontrado para essa pesquisa neste período. Tente aumentar o número de dias.")
            else:
                st.success(f"✅ Scanner concluído! Encontramos {len(dados)} blocos de perguntas.")
                st.markdown("---")
                
                # Exibindo os IDs das perguntas mapeados
                st.subheader("📌 IDs Mapeados nesta Pesquisa:")
                for bloco in dados:
                    id_perg = str(bloco.get("id_pergunta", "SEM_ID"))
                    nome_perg = str(bloco.get("nom_pergunta", "Sem Título"))
                    
                    st.markdown(f"**ID da Pergunta:** `{id_perg}` | **Texto:** {nome_perg}")
                    
                    # Mostra a estrutura exata da primeira resposta para validar os Serviços
                    respostas = bloco.get("respostas", [])
                    if respostas:
                        amostra = respostas[0]
                        servico_encontrado = amostra.get("nom_servico", "Vazio")
                        st.info(f"Exemplo de Serviço atrelado a esta pergunta: **{servico_encontrado}**")
                        
                    with st.expander(f"Ver JSON Completo do ID {id_perg}"):
                        st.json(bloco)