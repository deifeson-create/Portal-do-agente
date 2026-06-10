import requests
from config import BASE_URL
from api import get_admin_token

def buscar_id_da_ana():
    print("=====================================================")
    print("🔍 RASTREADOR DE ID - BUSCANDO A 'ANA L.'")
    print("=====================================================\n")
    
    token = get_admin_token()
    if not token:
        print("❌ Erro de conexão com a API.")
        return

    headers = {"Authorization": f"Bearer {token}"}
    nome_procurado = "ANA L"
    
    print(f"⏳ Procurando por '{nome_procurado}' na base da Matrix...\n")
    
    pagina = 1
    encontrou = False
    
    while True:
        r = requests.get(f"{BASE_URL}/agentes", headers=headers, params={"limit": 100, "page": pagina})
        if r.status_code != 200: break
        
        data = r.json()
        if not data.get("result"): break
        
        for ag in data["result"]:
            nome_api = str(ag.get("nome_exibicao") or ag.get("agente")).strip().upper()
            
            # Se tiver "ANA L" no nome, ele dedura na tela
            if nome_procurado in nome_api:
                cod = ag.get("cod_agente")
                email = ag.get("email", "Sem email")
                print(f"✅ ACHOU! -> Nome: {nome_api}")
                print(f"🎯 O ID DELA É: '{cod}'")
                print(f"📧 E-mail: {email}\n")
                encontrou = True
                
        if pagina * 100 >= data.get("total", 0): break
        pagina += 1

    if not encontrou:
        print("❌ Não encontramos ninguém com esse nome. Ela pode estar inativa ou com um nome de exibição muito diferente.")
        
    print("=====================================================")
    print("💡 PRÓXIMO PASSO:")
    print("Pegue o ID numérico que apareceu acima, abra o seu arquivo 'config.py',")
    print("e adicione esse número dentro da lista SETORES_AGENTES_IDS['SUPORTE'].")
    print("=====================================================")

if __name__ == "__main__":
    buscar_id_da_ana()