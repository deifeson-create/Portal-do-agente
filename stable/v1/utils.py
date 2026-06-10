import pandas as pd

# ==============================================================================
# FUNÇÕES DE FORMATAÇÃO E CÁLCULOS (FERRAMENTAS)
# ==============================================================================

def time_str_to_seconds(tempo_str):
    """Converte string 'HH:MM:SS' para segundos inteiros."""
    if not tempo_str or not isinstance(tempo_str, str): return 0
    try:
        parts = list(map(int, tempo_str.split(':')))
        if len(parts) == 3: return parts[0]*3600 + parts[1]*60 + parts[2]
    except: pass
    return 0

def seconds_to_hms(seconds):
    """Converte segundos inteiros para string 'HH:MM:SS'."""
    if not seconds or seconds < 0: return "00:00:00"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def formatar_tempo_humano(minutos_float):
    """Converte minutos decimais para um formato legível (Ex: 1h 30m)."""
    if not minutos_float: return "0m"
    minutos_int = int(minutos_float)
    horas, mins = divmod(minutos_int, 60)
    if horas > 0: return f"{horas}h {mins:02d}m"
    else: return f"{mins}m"

def calcular_media_tempos(lista_tempos, pesos):
    """Calcula a média ponderada de uma lista de tempos em formato string."""
    total_seg = 0
    total_peso = 0
    for t, p in zip(lista_tempos, pesos):
        if p > 0:
            total_seg += time_str_to_seconds(t) * p
            total_peso += p
    return seconds_to_hms(total_seg / total_peso) if total_peso > 0 else "--:--"

def gerar_link_protocolo(protocolo):
    """Gera o link direto para o atendimento na Matrix com base no número de protocolo."""
    if not protocolo: return None
    s_proto = str(protocolo).strip()
    if len(s_proto) < 7: suffix = s_proto
    else: suffix = s_proto[-7:]
    return f"https://ateltelecom.matrixdobrasil.ai/atendimento/view/cod_atendimento/{suffix}/readonly/true#atendimento-div"

def eleger_melhor_do_mes(df_rank):
    """Lógica que cruza TMA, TMIA e CSAT para encontrar o melhor operador."""
    if df_rank.empty: return None
    
    df_calc = df_rank.copy()
    df_calc['TMA_Seg'] = df_calc['TMA'].apply(time_str_to_seconds)
    df_calc['TMIA_Seg'] = df_calc['TMIA'].apply(time_str_to_seconds)
    
    # Filtra quem tem volume e respondentes de pesquisa
    df_calc = df_calc[(df_calc['Volume'] > 0) & (df_calc['CSAT Qtd'] > 0)].copy()
    if df_calc.empty: return None
    
    # Rankeamento (método min preserva empates reais)
    df_calc['Rank_TMA'] = df_calc['TMA_Seg'].rank(ascending=True, method='min')
    df_calc['Rank_TMIA'] = df_calc['TMIA_Seg'].rank(ascending=True, method='min')
    df_calc['Rank_CSAT'] = df_calc['CSAT Score'].rank(ascending=False, method='min')
    
    # A menor soma de posições define o vencedor
    df_calc['Score_Final'] = df_calc['Rank_TMA'] + df_calc['Rank_TMIA'] + df_calc['Rank_CSAT']
    
    min_score = df_calc['Score_Final'].min()
    mvps = df_calc[df_calc['Score_Final'] == min_score]
    
    nomes = mvps['Agente'].tolist()
    if len(nomes) > 1:
        return "Empate: " + " | ".join(nomes)
    return nomes[0]