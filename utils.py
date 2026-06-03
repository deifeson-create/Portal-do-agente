import pandas as pd

def time_str_to_seconds(t_str):
    """Converte 'HH:MM:SS' ou 'MM:SS' para segundos."""
    if not t_str or t_str in ["--:--", "--:--:--", "0", 0]:
        return 0.0
    parts = str(t_str).split(':')
    try:
        if len(parts) == 3:
            return float(parts[0])*3600 + float(parts[1])*60 + float(parts[2])
        elif len(parts) == 2:
            return float(parts[0])*60 + float(parts[1])
    except:
        pass
    return 0.0

def formatar_tempo_humano(minutos):
    """Formatador de minutos para string amigável."""
    if minutos <= 0:
        return "0s"
    if minutos < 1:
        return f"{int(minutos * 60)}s"
    horas = int(minutos // 60)
    mins_restantes = int(minutos % 60)
    segundos_restantes = int((minutos * 60) % 60)
    
    if horas > 0:
        return f"{horas}h {mins_restantes}m"
    if mins_restantes > 0 and segundos_restantes > 0:
        return f"{mins_restantes}m {segundos_restantes}s"
    return f"{mins_restantes}m"

def calcular_media_tempos(lista_tempos, lista_pesos):
    """Calcula a média ponderada de tempos no formato HH:MM:SS."""
    tempos_seg = [time_str_to_seconds(t) for t in lista_tempos]
    total_pesos = sum(lista_pesos)
    if total_pesos == 0:
        return "--:--"
    
    soma_ponderada = sum(t * p for t, p in zip(tempos_seg, lista_pesos))
    media_seg = soma_ponderada / total_pesos
    
    hrs = int(media_seg // 3600)
    mins = int((media_seg % 3600) // 60)
    segs = int(media_seg % 60)
    return f"{hrs:02d}:{mins:02d}:{segs:02d}"

def gerar_link_protocolo(protocolo):
    """Gera o link dinâmico para auditoria na Matrix."""
    proto_limpo = str(protocolo).strip()
    return f"https://atel.matrixdobrasil.ai/#/atendimento-view?protocolo={proto_limpo}"

def eleger_melhor_do_mes(df_rank):
    """
    Elege o MVP utilizando um Torneio de Confronto Direto (Pairwise).
    Garante peso perfeitamente igual: vence o confronto quem for melhor em 2 de 3 indicadores.
    Trata empates exibindo múltiplos agentes se acumularem o mesmo número de vitórias.
    """
    if df_rank.empty or len(df_rank) < 1:
        return None
        
    df = df_rank.copy()
    
    # Converte os tempos para segundos para permitir comparações diretas
    df['TMA_seg'] = df['TMA'].apply(time_str_to_seconds)
    df['TMIA_seg'] = df['TMIA'].apply(time_str_to_seconds)
    
    # Filtra apenas os agentes com dados ativos no período
    df = df[(df['Volume'] > 0) & (df['TMA_seg'] > 0)].copy()
    if df.empty:
        return None

    agentes = df['Agente'].tolist()
    vitorias = {ag: 0 for ag in agentes}

    # Torneio todos contra todos (Confronto Direto)
    for i in range(len(agentes)):
        for j in range(i + 1, len(agentes)):
            ag_a = agentes[i]
            ag_b = agentes[j]
            
            row_a = df[df['Agente'] == ag_a].iloc[0]
            row_b = df[df['Agente'] == ag_b].iloc[0]
            
            pontos_a = 0
            pontos_b = 0
            
            # 1. Indicador CSAT (Maior é melhor)
            if row_a['CSAT Score'] > row_b['CSAT Score']: pontos_a += 1
            elif row_a['CSAT Score'] < row_b['CSAT Score']: pontos_b += 1
            
            # 2. Indicador TMA (Menor é melhor)
            if row_a['TMA_seg'] < row_b['TMA_seg']: pontos_a += 1
            elif row_a['TMA_seg'] > row_b['TMA_seg']: pontos_b += 1
            
            # 3. Indicador TMIA (Menor é melhor)
            if row_a['TMIA_seg'] < row_b['TMIA_seg']: pontos_a += 1
            elif row_a['TMIA_seg'] > row_b['TMIA_seg']: pontos_b += 1
            
            # Vence o confronto quem pontuar em pelo menos 2 de 3
            if pontos_a > pontos_b:
                vitorias[ag_a] += 1
            elif pontos_b > pontos_a:
                vitorias[ag_b] += 1

    # Identifica o número máximo de vitórias obtido no torneio
    max_vitorias = max(vitorias.values())
    
    # Filtra todos os que alcançaram este topo (Suporta múltiplos vencedores em caso de empate)
    mvps = [ag for ag, vit in vitorias.items() if vit == max_vitorias]
    
    if not mvps:
        return None
        
    return " e ".join(mvps)