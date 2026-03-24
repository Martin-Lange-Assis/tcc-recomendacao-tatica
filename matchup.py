import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from src.database import database as db
from scipy.optimize import linear_sum_assignment

# --- NEUTRALIZAÇÃO ---

MATCHUP_ATACANTE_VS_ZAGUEIRO = [
    {'stat_adversario': 'goals', 'stat_meu_jogador': 'goalsConceded', 'sentido': 'inverso'},
    {'stat_adversario': 'bigChancesCreated', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
    {'stat_adversario': 'totalShots', 'stat_meu_jogador': 'blockedShots', 'sentido': 'direto'},
    {'stat_adversario': 'totalShots', 'stat_meu_jogador': 'outfielderBlocks', 'sentido': 'direto'},
    {'stat_adversario': 'shotsOnTarget', 'stat_meu_jogador': 'clearances', 'sentido': 'direto'},
    {'stat_adversario': 'successfulDribbles', 'stat_meu_jogador': 'dribbledPast', 'sentido': 'inverso'},
    {'stat_adversario': 'aerialDuelsWon', 'stat_meu_jogador': 'aerialDuelsWon', 'sentido': 'direto'},
    {'stat_adversario': 'groundDuelsWon', 'stat_meu_jogador': 'groundDuelsWon', 'sentido': 'direto'},
    {'stat_adversario': 'goalsFromInsideTheBox', 'stat_meu_jogador': 'clearances', 'sentido': 'direto'},
    {'stat_adversario': 'headedGoals', 'stat_meu_jogador': 'aerialDuelsWon', 'sentido': 'direto'},
    {'stat_adversario': 'accurateFinalThirdPasses', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
    {'stat_adversario': 'possessionWonAttThird', 'stat_meu_jogador': 'tackles', 'sentido': 'direto'},
    {'stat_adversario': 'keyPasses', 'stat_meu_jogador': 'tackles', 'sentido': 'direto'},
    {'stat_adversario': 'keyPasses', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
]

MATCHUP_PONTA_VS_LATERAL = [
    {'stat_adversario': 'successfulDribbles', 'stat_meu_jogador': 'dribbledPast', 'sentido': 'inverso'},
    {'stat_adversario': 'accurateCrosses', 'stat_meu_jogador': 'clearances', 'sentido': 'direto'},
    {'stat_adversario': 'totalCross', 'stat_meu_jogador': 'clearances', 'sentido': 'direto'},
    {'stat_adversario': 'keyPasses', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
    {'stat_adversario': 'groundDuelsWon', 'stat_meu_jogador': 'groundDuelsWon', 'sentido': 'direto'},
    {'stat_adversario': 'aerialDuelsWon', 'stat_meu_jogador': 'aerialDuelsWon', 'sentido': 'direto'},
    {'stat_adversario': 'assists', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
    {'stat_adversario': 'assists', 'stat_meu_jogador': 'tackles', 'sentido': 'direto'},
    {'stat_adversario': 'accurateFinalThirdPasses', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
    {'stat_adversario': 'shotsOnTarget', 'stat_meu_jogador': 'blockedShots', 'sentido': 'direto'},
    {'stat_adversario': 'possessionWonAttThird', 'stat_meu_jogador': 'tackles', 'sentido': 'direto'},
    {'stat_adversario': 'totalShots', 'stat_meu_jogador': 'blockedShots', 'sentido': 'direto'},
    {'stat_adversario': 'goals', 'stat_meu_jogador': 'goalsConceded', 'sentido': 'inverso'},
]

MATCHUP_MEIA_VS_MEIA = [
    {'stat_adversario': 'keyPasses', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
    {'stat_adversario': 'accuratePasses', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
    {'stat_adversario': 'accuratePasses', 'stat_meu_jogador': 'tackles', 'sentido': 'direto'},
    {'stat_adversario': 'accurateFinalThirdPasses', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
    {'stat_adversario': 'accurateOwnHalfPasses', 'stat_meu_jogador': 'tackles', 'sentido': 'direto'},
    {'stat_adversario': 'accurateOppositionHalfPasses', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
    {'stat_adversario': 'assists', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
    {'stat_adversario': 'successfulDribbles', 'stat_meu_jogador': 'dribbledPast', 'sentido': 'inverso'},
    {'stat_adversario': 'groundDuelsWon', 'stat_meu_jogador': 'groundDuelsWon', 'sentido': 'direto'},
    {'stat_adversario': 'aerialDuelsWon', 'stat_meu_jogador': 'aerialDuelsWon', 'sentido': 'direto'},
    {'stat_adversario': 'totalShots', 'stat_meu_jogador': 'blockedShots', 'sentido': 'direto'},
    {'stat_adversario': 'shotsOnTarget', 'stat_meu_jogador': 'blockedShots', 'sentido': 'direto'},
    {'stat_adversario': 'possessionWonAttThird', 'stat_meu_jogador': 'tackles', 'sentido': 'direto'},
    {'stat_adversario': 'bigChancesCreated', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
    {'stat_adversario': 'bigChancesCreated', 'stat_meu_jogador': 'tackles', 'sentido': 'direto'},
    {'stat_adversario': 'accurateLongBalls', 'stat_meu_jogador': 'interceptions', 'sentido': 'direto'},
]

# --- EXPLORAÇÃO ---

MATCHUP_ZAGUEIRO_ADV_VS_MEU_ATACANTE = [
    {'stat_adversario': 'dribbledPast', 'stat_meu_jogador': 'successfulDribbles', 'sentido': 'direto'},
    {'stat_adversario': 'aerialDuelsWon', 'stat_meu_jogador': 'aerialDuelsWon', 'sentido': 'inverso'},
    {'stat_adversario': 'groundDuelsWon', 'stat_meu_jogador': 'groundDuelsWon', 'sentido': 'inverso'},
    {'stat_adversario': 'clearances', 'stat_meu_jogador': 'totalShots', 'sentido': 'direto'},
    {'stat_adversario': 'tackles', 'stat_meu_jogador': 'successfulDribbles', 'sentido': 'direto'},
    {'stat_adversario': 'interceptions', 'stat_meu_jogador': 'keyPasses', 'sentido': 'direto'},
    {'stat_adversario': 'blockedShots', 'stat_meu_jogador': 'shotsOnTarget', 'sentido': 'direto'},
    {'stat_adversario': 'goalsConceded', 'stat_meu_jogador': 'goals', 'sentido': 'direto'},
]

MATCHUP_LATERAL_ADV_VS_MINHA_PONTA = [
    {'stat_adversario': 'dribbledPast', 'stat_meu_jogador': 'successfulDribbles', 'sentido': 'direto'},
    {'stat_adversario': 'groundDuelsWon', 'stat_meu_jogador': 'groundDuelsWon', 'sentido': 'inverso'},
    {'stat_adversario': 'aerialDuelsWon', 'stat_meu_jogador': 'aerialDuelsWon', 'sentido': 'inverso'},
    {'stat_adversario': 'tackles', 'stat_meu_jogador': 'successfulDribbles', 'sentido': 'direto'},
    {'stat_adversario': 'interceptions', 'stat_meu_jogador': 'keyPasses', 'sentido': 'direto'},
    {'stat_adversario': 'clearances', 'stat_meu_jogador': 'accurateCrosses', 'sentido': 'direto'},
    {'stat_adversario': 'blockedShots', 'stat_meu_jogador': 'shotsOnTarget', 'sentido': 'direto'},
    {'stat_adversario': 'accurateCrosses', 'stat_meu_jogador': 'interceptions', 'sentido': 'inverso'},
    {'stat_adversario': 'goalsConceded', 'stat_meu_jogador': 'goals', 'sentido': 'direto'},
]

MATCHUP_POR_ARQUETIPO = {
    # Laterais adversários → minhas pontas/meias largos exploram
    4: MATCHUP_LATERAL_ADV_VS_MINHA_PONTA,
    5: MATCHUP_LATERAL_ADV_VS_MINHA_PONTA,
    6: MATCHUP_LATERAL_ADV_VS_MINHA_PONTA,
    7: MATCHUP_LATERAL_ADV_VS_MINHA_PONTA,
    8: MATCHUP_LATERAL_ADV_VS_MINHA_PONTA,
    # Zagueiros adversários → meus atacantes exploram
    9:  MATCHUP_ZAGUEIRO_ADV_VS_MEU_ATACANTE,
    10: MATCHUP_ZAGUEIRO_ADV_VS_MEU_ATACANTE,
    11: MATCHUP_ZAGUEIRO_ADV_VS_MEU_ATACANTE,
    12: MATCHUP_ZAGUEIRO_ADV_VS_MEU_ATACANTE,
    # Volantes adversários → meus volantes/MCs neutralizam
    13: MATCHUP_MEIA_VS_MEIA, 14: MATCHUP_MEIA_VS_MEIA,
    15: MATCHUP_MEIA_VS_MEIA, 16: MATCHUP_MEIA_VS_MEIA,
    17: MATCHUP_MEIA_VS_MEIA, 18: MATCHUP_MEIA_VS_MEIA,
    # Meio-Campistas adversários → meus MCs/meias ofensivos neutralizam
    19: MATCHUP_MEIA_VS_MEIA, 20: MATCHUP_MEIA_VS_MEIA,
    21: MATCHUP_MEIA_VS_MEIA, 22: MATCHUP_MEIA_VS_MEIA,
    # Meias Ofensivos adversários → meus MCs/meias ofensivos neutralizam
    23: MATCHUP_MEIA_VS_MEIA, 24: MATCHUP_MEIA_VS_MEIA,  # 24 = Atacante Sombra (MEI)
    25: MATCHUP_MEIA_VS_MEIA, 26: MATCHUP_MEIA_VS_MEIA,
    # Meias Abertos adversários → meus laterais neutralizam
    27: MATCHUP_PONTA_VS_LATERAL, 28: MATCHUP_PONTA_VS_LATERAL,
    29: MATCHUP_PONTA_VS_LATERAL, 30: MATCHUP_PONTA_VS_LATERAL,
    # Pontas adversárias → meus laterais neutralizam
    31: MATCHUP_PONTA_VS_LATERAL, 32: MATCHUP_PONTA_VS_LATERAL,
    33: MATCHUP_PONTA_VS_LATERAL,
    # Atacantes adversários → meus zagueiros neutralizam
    34: MATCHUP_ATACANTE_VS_ZAGUEIRO, 35: MATCHUP_ATACANTE_VS_ZAGUEIRO,
    36: MATCHUP_ATACANTE_VS_ZAGUEIRO, 37: MATCHUP_ATACANTE_VS_ZAGUEIRO,
}

# ==============================================================================
# MAPA DE ARQUÉTIPO ADVERSÁRIO → IDs DOS ARQUÉTIPOS COUNTER (time da casa)
# As posicoes_alvo são lidas do banco (arquetipos_ref) em tempo de execução.
# ==============================================================================

ARQUETIPO_COUNTER_IDS = {
    # Laterais adversários (LD/LE) → pontas + meias largos da casa (PD, PE, MD, ME)
    4: [31, 32, 33, 27, 28, 29, 30],
    5: [31, 32, 33, 27, 28, 29, 30],
    6: [31, 32, 33, 27, 28, 29, 30],
    7: [31, 32, 33, 27, 28, 29, 30],
    8: [31, 32, 33, 27, 28, 29, 30],
    # Zagueiros adversários (ZAG) → atacantes da casa (ATA)
    9:  [34, 35, 36, 37],
    10: [34, 35, 36, 37],
    11: [34, 35, 36, 37],
    12: [34, 35, 36, 37],
    # Volantes adversários (VOL) → volantes + MCs da casa (VOL, MC)
    13: [13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
    14: [13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
    15: [13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
    16: [13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
    17: [13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
    18: [13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
    # Meio-Campistas adversários (MC) → MCs + meias ofensivos da casa (MC, MEI)
    19: [19, 20, 21, 22, 23, 24, 25, 26],
    20: [19, 20, 21, 22, 23, 24, 25, 26],
    21: [19, 20, 21, 22, 23, 24, 25, 26],
    22: [19, 20, 21, 22, 23, 24, 25, 26],
    # Meias Ofensivos adversários (MEI) → MCs + meias ofensivos da casa (MC, MEI)
    23: [19, 20, 21, 22, 23, 24, 25, 26],
    24: [19, 20, 21, 22, 23, 24, 25, 26],
    25: [19, 20, 21, 22, 23, 24, 25, 26],
    26: [19, 20, 21, 22, 23, 24, 25, 26],
    # Meias Abertos adversários (MD/ME) → laterais da casa (LD, LE)
    27: [4, 5, 6, 7, 8],
    28: [4, 5, 6, 7, 8],
    29: [4, 5, 6, 7, 8],
    30: [4, 5, 6, 7, 8],
    # Pontas adversárias (PD/PE) → laterais da casa (LD, LE)
    31: [4, 5, 6, 7, 8],
    32: [4, 5, 6, 7, 8],
    33: [4, 5, 6, 7, 8],
    # Atacantes adversários (ATA) → zagueiros da casa (ZAG)
    34: [9, 10, 11, 12],
    35: [9, 10, 11, 12],
    36: [9, 10, 11, 12],
    37: [9, 10, 11, 12],
}

# IDs de arquétipos de goleiro
ARQUETIPOS_GOLEIRO = {1, 2, 3}


def extrair_primeira_posicao(valor):
    # 1. Verifica se o valor é nulo (NaN) usando a função nativa do Pandas
    if pd.isna(valor):
        return 'N/A'

    # 2. Converte para string e limpa espaços nas extremidades
    texto = str(valor).strip()

    # 3. Verifica se a string resultante está vazia
    if texto == '':
        return 'N/A'

    # 4. Divide a string pela vírgula e pega apenas o primeiro elemento
    partes = texto.split(',')
    primeira_posicao = partes[0].strip()

    return primeira_posicao


def calcular_matchup(time_casa: str, time_fora: str, escalacao_adversario: list = None):

    # --- 1. BUSCA DOS DADOS ---
    query = """
        SELECT e.player_id, j.name AS player_name, j.posicao_bruta, j.time_id, j.time_nome, e.minutesPlayed,
               ct.posicoes_detalhadas,
               e.goals, e.bigChancesCreated, e.bigChancesMissed, e.assists,
               e.goalsAssistsSum, e.accuratePasses, e.inaccuratePasses,
               e.totalPasses, e.accurateOwnHalfPasses, e.accurateOppositionHalfPasses,
               e.accurateFinalThirdPasses, e.keyPasses, e.successfulDribbles,
               e.tackles, e.interceptions, e.accurateCrosses, e.totalShots,
               e.shotsOnTarget, e.groundDuelsWon, e.aerialDuelsWon, e.totalDuelsWon,
               e.goalsFromInsideTheBox, e.headedGoals, e.accurateLongBalls,
               e.clearances, e.possessionWonAttThird, e.blockedShots,
               e.dribbledPast, e.outfielderBlocks, e.goalsConceded,
               e.totalCross, e.ballRecovery,
               c.id_arquetipo, c.score_similaridade,
               a.nome_arquetipo
        FROM estatisticas_2025 e
        JOIN jogadores j ON e.player_id = j.player_id
        LEFT JOIN classificacao_jogadores c ON e.player_id = c.player_id
        LEFT JOIN arquetipos_ref a ON c.id_arquetipo = a.id_arquetipo
        LEFT JOIN caracteristicas_taticas ct ON e.player_id = ct.player_id
        WHERE c.score_similaridade = (
            SELECT MAX(c2.score_similaridade)
            FROM classificacao_jogadores c2
            WHERE c2.player_id = e.player_id
        )
    """

    # Busca posições granulares dos arquétipos — mesma fonte do classificador
    query_posicoes = "SELECT id_arquetipo, posicao_alvo FROM arquetipos_ref"

    df = pd.read_sql_query(query, con=db.engine)
    df_posicoes_ref = pd.read_sql_query(query_posicoes, con=db.engine)

    # Monta dicionário: id_arquetipo → set de posições válidas
    posicao_alvo_por_arquetipo = {}
    for _, row in df_posicoes_ref.iterrows():
        arq_id = int(row['id_arquetipo'])
        valor_bruto = str(row['posicao_alvo']).strip()
        posicoes = set()
        if valor_bruto != '' and valor_bruto.lower() != 'nan':
            valor_bruto_dividido = valor_bruto.split(',')

            for parte_do_valor in valor_bruto_dividido:
                valor_limpo = parte_do_valor.strip()

                if valor_limpo and valor_limpo.lower() != 'nan':
                    posicoes.add(valor_limpo)

        posicao_alvo_por_arquetipo[arq_id] = posicoes

    # Extrai posição primária (primeiro elemento de posicoes_detalhadas)
    df['posicao_primaria'] = df['posicoes_detalhadas'].apply(extrair_primeira_posicao)

    df_casa = df[df['time_nome'] == time_casa].copy()
    df_fora = df[df['time_nome'] == time_fora].copy()

    if escalacao_adversario:
        df_fora = df_fora[df_fora['player_name'].isin(escalacao_adversario)].copy()
        if df_fora.empty:
            print("Nenhum jogador encontrado na escalação adversária.")
            return pd.DataFrame()

    if df_casa.empty or df_fora.empty:
        print(f"Time não encontrado. Verifique os nomes: '{time_casa}' e '{time_fora}'")
        return pd.DataFrame()

    stats_usados = list({'goals', 'bigChancesCreated', 'assists', 'accuratePasses', 'accurateOwnHalfPasses',
                         'accurateOppositionHalfPasses', 'accurateFinalThirdPasses', 'keyPasses', 'successfulDribbles',
                         'tackles', 'interceptions', 'accurateCrosses', 'totalShots', 'shotsOnTarget', 'groundDuelsWon',
                         'aerialDuelsWon', 'goalsFromInsideTheBox', 'headedGoals', 'accurateLongBalls', 'clearances',
                         'possessionWonAttThird', 'blockedShots', 'dribbledPast', 'outfielderBlocks', 'goalsConceded',
                         'totalCross', 'ballRecovery', 'accurateOppositionHalfPasses'})

    # --- 2. CÁLCULO P90 ---
    for df_temp in [df_casa, df_fora]:
        for stat in stats_usados:
            if stat in df_temp.columns:
                df_temp[stat + '_p90'] = (
                    df_temp[stat] / df_temp['minutesPlayed'].replace(0, np.nan) * 90
                ).fillna(0)

    # --- 3. NORMALIZAÇÃO MIN-MAX ---
    for stat in stats_usados:
        col_p90 = stat + '_p90'
        todos_valores = pd.concat([df_casa[col_p90], df_fora[col_p90]])
        min_val = todos_valores.min()
        max_val = todos_valores.max()

        for df_temp in [df_casa, df_fora]:
            if max_val - min_val != 0:
                df_temp[col_p90] = (df_temp[col_p90] - min_val) / (max_val - min_val)
            else:
                df_temp[col_p90] = 0.0

    # --- 4. MATCHUP ---
    recomendacoes = []

    for _, jogador_fora in df_fora.iterrows():
        arquetipo = jogador_fora.get('id_arquetipo')

        if arquetipo in ARQUETIPOS_GOLEIRO or arquetipo not in MATCHUP_POR_ARQUETIPO:
            continue

        # Une as posicoes_alvo de todos os arquétipos counter
        counter_ids = ARQUETIPO_COUNTER_IDS.get(arquetipo, [])
        posicoes_validas = set()
        for cid in counter_ids:
            posicoes_validas.update(posicao_alvo_por_arquetipo.get(cid, set()))

        if not posicoes_validas:
            print(f"Sem posições válidas para arquétipo adversário {arquetipo}. Pulando.")
            continue

        # Filtra jogadores da casa pelas posições granulares
        df_casa_filtrado = df_casa[df_casa['posicao_primaria'].isin(posicoes_validas)].copy()
        df_casa_filtrado.reset_index(drop=True, inplace=True)

        if df_casa_filtrado.empty:
            print(f"Nenhum jogador da casa em {posicoes_validas} para arquétipo {arquetipo}. Pulando.")
            continue

        matriz = MATCHUP_POR_ARQUETIPO[arquetipo]

        vetor_reflexo = {}
        for mapeamento in matriz:
            stat_adv = mapeamento['stat_adversario'] + '_p90'
            stat_meu = mapeamento['stat_meu_jogador'] + '_p90'
            sentido = mapeamento['sentido']

            valor_adversario = jogador_fora.get(stat_adv, 0)
            if sentido == 'direto':
                vetor_reflexo[stat_meu] = valor_adversario
            else:
                vetor_reflexo[stat_meu] = 1 - valor_adversario

        colunas_disponiveis = []

        colunas_do_dataframe = df_casa.columns

        for col in vetor_reflexo.keys():
            if col in colunas_do_dataframe:
                colunas_disponiveis.append(col)

        if not colunas_disponiveis:
            continue

        lista_valores = []

        for col in colunas_disponiveis:
            valor = vetor_reflexo[col]

            lista_valores.append(valor)
        array_reflexo = np.array(lista_valores).reshape(1, -1)

        array_casa = df_casa_filtrado[colunas_disponiveis].values
        similaridades = cosine_similarity(array_casa, array_reflexo)

        for i, sim in enumerate(similaridades):
            recomendacoes.append({
                'adversario_player_id': int(jogador_fora['player_id']),
                'adversario_nome': str(jogador_fora['player_name']),
                'adversario_posicao': str(jogador_fora['posicao_primaria']),
                'adversario_arquetipo': str(jogador_fora['nome_arquetipo']),

                'meu_player_id': int(df_casa_filtrado.iloc[i]['player_id']),
                'meu_player_nome': str(df_casa_filtrado.iloc[i]['player_name']),
                'minha_posicao': str(df_casa_filtrado.iloc[i]['posicao_primaria']),
                'meu_arquetipo': str(df_casa_filtrado.iloc[i]['nome_arquetipo']),

                'score_matchup': round(float(sim[0]) * 100, 2)
            })

    df_rec = pd.DataFrame(recomendacoes)

    if df_rec.empty:
        print("Nenhuma recomendação gerada.")
        return pd.DataFrame()

    # --- 5. SELEÇÃO ÓTIMA GLOBAL ---
    matriz_matchup = df_rec.pivot(
        index='meu_player_id',
        columns='adversario_player_id',
        values='score_matchup'
    ).fillna(0)

    custo_matriz = -matriz_matchup.values
    meus_indices, adv_indices = linear_sum_assignment(custo_matriz)

    titulares = []
    for i, j in zip(meus_indices, adv_indices):
        meu_id = matriz_matchup.index[i]
        adv_id = matriz_matchup.columns[j]
        score_obtido = matriz_matchup.iloc[i, j]

        if score_obtido > 0:
            linha_original = df_rec[
                (df_rec['meu_player_id'] == meu_id) &
                (df_rec['adversario_player_id'] == adv_id)
            ].iloc[0]
            titulares.append(linha_original)

            if len(titulares) == 10:
                break

    df_titulares = pd.DataFrame(titulares)

    # --- 6. GOLEIRO ---
    goleiros_casa = df_casa[df_casa['id_arquetipo'].isin(ARQUETIPOS_GOLEIRO)].copy()
    soma_jogadores = 0

    for t in titulares:
        soma_jogadores += t['score_matchup']

    if not goleiros_casa.empty:
        goleiros_casa = goleiros_casa.sort_values('score_similaridade', ascending=False)
        melhor_goleiro = goleiros_casa.iloc[0]
        soma_jogadores += melhor_goleiro['score_similaridade']

        linha_goleiro = pd.DataFrame([{
            'adversario_player_id': None,
            'adversario_nome': '-',
            'adversario_posicao': '-',
            'adversario_arquetipo': '-',
            'meu_player_id': int(melhor_goleiro['player_id']),
            'meu_player_nome': str(melhor_goleiro['player_name']),
            'minha_posicao': str(melhor_goleiro['posicao_primaria']),
            'meu_arquetipo': str(melhor_goleiro['nome_arquetipo']),
            'score_matchup': round(float(melhor_goleiro['score_similaridade']), 2)
        }])
        df_titulares = pd.concat([linha_goleiro, df_titulares], ignore_index=True)

    print(f'A eficiencia maxima global foi de {soma_jogadores:.2f}')
    return df_titulares


if __name__ == "__main__":
    jogadores_adversarios = [
        "Agustín Rossi",
        "Emerson Royal", "João Victor", "Léo Pereira", "Ayrton Lucas",
        "Erick Pulgar", "Saúl Ñíguez", "Luiz Araújo", "Jorge Carrascal", "Samuel Lino",
        "Pedro"
    ]


    df_resultado = calcular_matchup(
        time_casa="Fluminense",
        time_fora="Flamengo",
        escalacao_adversario=jogadores_adversarios
    )

    print("\n========== 11 TITULARES RECOMENDADOS ==========")

    colunas_finais = [
        'meu_player_nome', 'minha_posicao', 'meu_arquetipo',
        'adversario_nome', 'adversario_posicao', 'adversario_arquetipo',
        'score_matchup'
    ]

    print(df_resultado[colunas_finais].to_string(index=False))
    print("===============================================")