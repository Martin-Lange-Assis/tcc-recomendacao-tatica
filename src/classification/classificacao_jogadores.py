import pandas as pd
import numpy as np
import json
from sklearn.metrics.pairwise import cosine_similarity
from src.database import database as db

# Configurações de exibição do Pandas
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 20000)

caminho_json = r"caminho"


def extrair_primeira_posicao(valor):
    # Verifica se é nulo (NaN)
    if pd.isna(valor):
        return 'Desconhecida'

    # Converte para string e remove espaços em branco nas extremidades
    texto = str(valor).strip()

    # Verifica se a string ficou vazia após o strip
    if not texto:
        return 'Desconhecida'

    # Divide pela vírgula e pega o primeiro elemento
    partes = texto.split(',')
    primeira_posicao = partes[0].strip()

    return primeira_posicao


def calcular_similaridade_arquetipos(caminho_json):
    """
    Função que busca os dados no banco, realiza os cálculos P90,
    normalização, similaridade de cosseno filtrando pelas posições alvo.
    """

    with open(caminho_json, 'r', encoding='utf-8') as arquivo_json:
        arquivo_json_carregado = json.load(arquivo_json)
    dict_pesos = arquivo_json_carregado.get("arquetipo", {})

    stats_list = [
        "goals", "bigChancesCreated",
        "bigChancesMissed", "assists", "goalsAssistsSum", "accuratePasses",
        "inaccuratePasses", "totalPasses", "accurateOwnHalfPasses",
        "accurateOppositionHalfPasses", "accurateFinalThirdPasses", "keyPasses",
        "successfulDribbles", "tackles", "interceptions", "yellowCards",
        "directRedCards", "redCards", "accurateCrosses", "totalShots",
        "shotsOnTarget", "shotsOffTarget", "groundDuelsWon", "aerialDuelsWon",
        "totalDuelsWon", "penaltiesTaken", "penaltyGoals",
        "penaltyWon", "penaltyConceded", "shotFromSetPiece", "freeKickGoal",
        "goalsFromInsideTheBox", "goalsFromOutsideTheBox", "shotsFromInsideTheBox",
        "shotsFromOutsideTheBox", "headedGoals", "leftFootGoals", "rightFootGoals",
        "accurateLongBalls", "clearances", "errorLeadToGoal", "errorLeadToShot",
        "dispossessed", "possessionLost", "possessionWonAttThird", "totalChippedPasses",
        "accurateChippedPasses", "touches", "wasFouled", "fouls", "hitWoodwork",
        "ownGoals", "dribbledPast", "offsides", "blockedShots", "passToAssist",
        "saves", "cleanSheet", "penaltyFaced", "penaltySave", "savedShotsFromInsideTheBox",
        "savedShotsFromOutsideTheBox", "goalsConcededInsideTheBox",
        "goalsConcededOutsideTheBox", "punches", "runsOut", "successfulRunsOut",
        "highClaims", "crossesNotClaimed", "matchesStarted", "totalAttemptAssist",
        "totalContest", "totalCross", "duelLost", "aerialLost", "attemptPenaltyMiss",
        "attemptPenaltyPost", "attemptPenaltyTarget", "totalLongBalls",
        "goalsConceded", "tacklesWon", "yellowRedCards", "savesCaught",
        "savesParried", "totalOwnHalfPasses", "totalOppositionHalfPasses",
        "totwAppearances", "goalKicks", "ballRecovery", "outfielderBlocks",
        "appearances", "goalsPrevented"
    ]

    colunas_formatadas = []
    for col in stats_list:
        item = f"e.{col}"
        colunas_formatadas.append(item)

    columns_str = ", ".join(colunas_formatadas)

    # Traz posicoes_detalhadas via LEFT JOIN
    query_jogadores_reais = f"""
            SELECT e.player_id, e.minutesPlayed, ct.posicoes_detalhadas, {columns_str} 
            FROM estatisticas_2025 e
            JOIN jogadores j ON e.player_id = j.player_id
            LEFT JOIN caracteristicas_taticas ct ON e.player_id = ct.player_id
        """

    # Traz a nova coluna posicoes_alvo da tabela de arquétipos
    query_deuses = """
            SELECT d.*, a.posicao_alvo 
            FROM deuses_arquetipos d
            JOIN arquetipos_ref a ON d.id_arquetipo = a.id_arquetipo
        """

    df_jogadores = pd.read_sql_query(query_jogadores_reais, con=db.engine)
    df_deuses = pd.read_sql_query(query_deuses, con=db.engine)

    # Extrai a Posição Primária (pega o primeiro elemento antes da vírgula)
    df_jogadores['posicao_primaria'] = df_jogadores['posicoes_detalhadas'].apply(extrair_primeira_posicao)

    # --- 1. CÁLCULO P90 ---
    colunas_p90 = []
    for coluna in stats_list:
        coluna_nova = coluna + '_p90'
        colunas_p90.append(coluna_nova)

    df_jogadores[colunas_p90] = (df_jogadores[stats_list].div(df_jogadores['minutesPlayed'], axis=0) * 90)

    # Tratamento de divisões por zero
    df_jogadores = df_jogadores.replace([np.inf, -np.inf], 0).fillna(0)

    # --- 2. NORMALIZAÇÃO MIN-MAX ---
    escala_min_max = {}

    for coluna in colunas_p90:
        min_val = df_jogadores[coluna].min()
        max_val = df_jogadores[coluna].max()

        escala_min_max[coluna] = {'min': min_val, 'max': max_val}

        if max_val - min_val != 0:
            df_jogadores[coluna] = (df_jogadores[coluna] - min_val) / (max_val - min_val)
        else:
            df_jogadores[coluna] = 0.0

    # --- 3. LIMPEZA GLOBAL DE ZEROS NOS DEUSES ---
    colunas_so_zeros = []
    for coluna in df_deuses.columns:
        if (df_deuses[coluna] == 0).all():
            colunas_so_zeros.append(coluna)

    df_deuses = df_deuses.drop(columns=colunas_so_zeros)
    print(f"Número de colunas irrelevantes para TODOS os deuses removidas: {len(colunas_so_zeros)}")

    # --- 4. ALGORITMO DE SIMILARIDADE ---
    print("Calculando similaridades (Cosseno)...")

    classificacoes_finais = []

    for index, deus in df_deuses.iterrows():
        id_arquetipo = str(int(deus['id_arquetipo']))

        # Lê a string de posições do Sheets, separa por vírgula e transforma em lista
        string_posicoes = str(deus['posicao_alvo'])

        # Inicializamos a lista vazia
        lista_posicoes_alvo = []

        # Verificamos se o valor não é nulo (string 'nan') e se não está vazio
        if string_posicoes != 'nan' and string_posicoes.strip() != '':
            # Dividimos a string pela vírgula
            partes = string_posicoes.split(',')

            # Limpamos os espaços de cada item e adicionamos à lista final
            for pos in partes:
                item_limpo = pos.strip()
                lista_posicoes_alvo.append(item_limpo)

        pesos_dos_arquetipos = dict_pesos.get(id_arquetipo, {})
        colunas_relevantes = list(pesos_dos_arquetipos.keys())

        # Se não tiver peso ou não tiver posição alvo, ignora o arquétipo
        if not colunas_relevantes or not lista_posicoes_alvo:
            continue

        # FILTRO PRINCIPAL: Pega apenas os jogadores cuja posição primária esteja dentro da lista de alvos do arquétipo
        df_jogadores_setor = df_jogadores[df_jogadores['posicao_primaria'].isin(lista_posicoes_alvo)].copy()
        df_jogadores_setor.reset_index(drop=True, inplace=True)

        if df_jogadores_setor.empty:
            continue

        vetor_deus_completo = []
        vetor_pesos_completo = []
        colunas_p90_relevantes = []

        for col in colunas_relevantes:
            peso_atual = pesos_dos_arquetipos[col]
            valor_deus = float(deus.get(col, 0))

            colunas_p90_relevantes.append(col + '_p90')
            vetor_pesos_completo.append(peso_atual)

            # Normalização do Deus
            coluna_p90_ref = col + '_p90'
            min_camp = escala_min_max[coluna_p90_ref]['min']
            max_camp = escala_min_max[coluna_p90_ref]['max']

            if max_camp - min_camp == 0:
                valor_norm = 0.0
            else:
                valor_norm = (valor_deus - min_camp) / (max_camp - min_camp)

            valor_norm = max(0.0, min(1.0, valor_norm))
            vetor_deus_completo.append(valor_norm)

        # Calculando para os jogadores filtrados
        vetores_jogadores_filtrados = df_jogadores_setor[colunas_p90_relevantes].values
        soma_acoes_jogador = vetores_jogadores_filtrados.sum(axis=1)

        vetor_deus = np.array(vetor_deus_completo).reshape(1, -1)
        vetor_pesos = np.array(vetor_pesos_completo)

        vetores_jogadores_pesados = vetores_jogadores_filtrados * vetor_pesos
        vetor_deus_pesado = vetor_deus * vetor_pesos

        similaridades = cosine_similarity(vetores_jogadores_pesados, vetor_deus_pesado)

        for i, sim in enumerate(similaridades):
            score_final = float(sim[0])

            if score_final > 0.1 and soma_acoes_jogador[i] > 0.1:
                classificacoes_finais.append({
                    'player_id': int(df_jogadores_setor.loc[i, 'player_id']),
                    'id_arquetipo': int(id_arquetipo),
                    'score_similaridade': round(score_final * 100, 2)
                })

    df_classificacao_final = pd.DataFrame(classificacoes_finais)

    print("Visualização dos resultados (Dentro da função):")
    print(df_classificacao_final.head(100))

    return df_classificacao_final


if __name__ == "__main__":
    df_teste = calcular_similaridade_arquetipos(caminho_json)
    print("Processamento finalizado com sucesso!")