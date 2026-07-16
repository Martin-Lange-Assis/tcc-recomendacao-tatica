# src/recommendation/engine.py
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from src.domain.regras_matchup import MATCHUP_POR_ARQUETIPO, ARQUETIPOS_GOLEIRO, ARQUETIPO_COUNTER_IDS


def calcular_matriz_similaridade(df_casa: pd.DataFrame, df_fora: pd.DataFrame,
                                 posicao_alvo_por_arquetipo: dict) -> pd.DataFrame:
    """
    Cruza o time da casa com o time de fora, calcula os vetores reflexos
    e retorna o DataFrame com todos os scores de matchup válidos.
    """
    recomendacoes = []

    for _, jogador_fora in df_fora.iterrows():
        arquetipo = jogador_fora.get('id_arquetipo')

        # Ignora goleiros e arquétipos sem mapeamento definido
        if arquetipo in ARQUETIPOS_GOLEIRO or arquetipo not in MATCHUP_POR_ARQUETIPO:
            continue

        # Identifica as posições aptas a marcar o jogador adversário
        counter_ids = ARQUETIPO_COUNTER_IDS.get(arquetipo, [])
        posicoes_validas = set()
        for cid in counter_ids:
            posicoes_validas.update(posicao_alvo_por_arquetipo.get(cid, set()))

        if not posicoes_validas:
            continue

        # Filtra os candidatos do time da casa compatíveis com as posições válidas
        df_casa_filtrado = df_casa[df_casa['posicao_primaria'].isin(posicoes_validas)].copy()
        df_casa_filtrado.reset_index(drop=True, inplace=True)

        if df_casa_filtrado.empty:
            continue

        # Constrói o vetor reflexo baseado nos atributos do adversário
        matriz = MATCHUP_POR_ARQUETIPO[arquetipo]
        vetor_reflexo = {}
        for mapeamento in matriz:
            stat_adv = mapeamento['stat_adversario'] + '_p90'
            stat_meu = mapeamento['stat_meu_jogador'] + '_p90'
            sentido = mapeamento['sentido']
            valor_adversario = jogador_fora.get(stat_adv, 0)

            # Aplica o mapeamento direto ou inverso das estatísticas
            vetor_reflexo[stat_meu] = valor_adversario if sentido == 'direto' else 1 - valor_adversario

        colunas_disponiveis = []
        for col in vetor_reflexo:
            if col in df_casa.columns:
                colunas_disponiveis.append(col)

        if not colunas_disponiveis:
            continue

        # Estruturação dos arrays para o cálculo vetorial
        lista_valores = []
        for col in colunas_disponiveis:
            valor = vetor_reflexo[col]
            lista_valores.append(valor)

        array_reflexo = np.array(lista_valores)
        array_reflexo = array_reflexo.reshape(1, -1)

        array_casa = df_casa_filtrado[colunas_disponiveis].values

        # Aplicação da Similaridade de Cosseno
        similaridades = cosine_similarity(array_casa, array_reflexo)

        # Estruturação e armazenamento dos resultados do matchup
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
                'score_matchup': round(float(sim[0]) * 100, 2),
            })

    return pd.DataFrame(recomendacoes)
