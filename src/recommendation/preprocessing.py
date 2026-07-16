# src/recommendation/preprocessing.py
import pandas as pd
import numpy as np

# Lista de estatísticas utilizadas nas regras de pré-processamento
stats_usados = list({'goals', 'bigChancesCreated', 'assists', 'accuratePasses', 'accurateOwnHalfPasses',
                     'accurateOppositionHalfPasses', 'accurateFinalThirdPasses', 'keyPasses', 'successfulDribbles',
                     'tackles', 'interceptions', 'accurateCrosses', 'totalShots', 'shotsOnTarget', 'groundDuelsWon',
                     'aerialDuelsWon', 'goalsFromInsideTheBox', 'headedGoals', 'accurateLongBalls', 'clearances',
                     'possessionWonAttThird', 'blockedShots', 'dribbledPast', 'outfielderBlocks', 'goalsConceded',
                     'totalCross', 'ballRecovery', 'accurateOppositionHalfPasses'})


def _extrair_primeira_posicao(valor):
    """Extrai apenas a primeira posição da string de posições detalhadas."""
    if pd.isna(valor) or str(valor).strip() == '':
        return 'N/A'
    return str(valor).split(',')[0].strip()


def _extrair_todas_posicoes(valor):
    """Converte a string de posições separadas por vírgula em uma lista limpa."""
    if pd.isna(valor) or str(valor).strip() == '':
        return []

    partes = str(valor).split(',')
    resultado = []

    for p in partes:
        item_limpo = p.strip()
        resultado.append(item_limpo)

    return resultado


def processar_posicoes_alvo(df_posicoes_ref: pd.DataFrame) -> dict:
    """Mapeia o DataFrame do banco de dados para um dicionário de posições-alvo por arquétipo."""
    posicao_alvo_por_arquetipo = {}

    for _, row in df_posicoes_ref.iterrows():
        arq_id = int(row['id_arquetipo'])
        valor_bruto = str(row['posicao_alvo']).strip()
        posicoes = set()

        if valor_bruto and valor_bruto.lower() != 'nan':
            for parte in valor_bruto.split(','):
                valor_limpo = parte.strip()
                if valor_limpo and valor_limpo.lower() != 'nan':
                    posicoes.add(valor_limpo)

        posicao_alvo_por_arquetipo[arq_id] = posicoes

    return posicao_alvo_por_arquetipo


def aplicar_filtros_e_improvisacoes(df_casa, df_fora, escalacao_adversario,
                                    jogadores_indisponiveis, adversario_improvisado):
    """Aplica filtros de elenco e improvisações manuais antes do processamento."""

    # Trata indisponibilidades (lesões/suspensões) no time da casa
    if jogadores_indisponiveis:
        df_casa = df_casa[~df_casa['player_id'].isin(jogadores_indisponiveis)].copy()

    # Filtra os jogadores adversários escalados no contexto atual
    if escalacao_adversario:
        primeiro_elemento = escalacao_adversario[0]

        # Verifica o tipo de dado recebido (Nomes via string ou IDs via int)
        if isinstance(primeiro_elemento, str) and not primeiro_elemento.isdigit():
            coluna_filtro = 'player_name'
        else:
            coluna_filtro = 'player_id'
            escalacao_adversario = [int(x) for x in escalacao_adversario]

        df_fora = df_fora[df_fora[coluna_filtro].isin(escalacao_adversario)].copy()

    # Aplica alterações manuais de posição ou arquétipo no time adversário
    if adversario_improvisado:
        for nome_jogador, infos in adversario_improvisado.items():
            idx = df_fora['player_name'] == nome_jogador
            if idx.any():
                if 'posicao_primaria' in infos:
                    df_fora.loc[idx, 'posicao_primaria'] = infos['posicao_primaria']
                if 'id_arquetipo' in infos:
                    df_fora.loc[idx, 'id_arquetipo'] = infos['id_arquetipo']
                if 'nome_arquetipo' in infos and 'arquetipo' in df_fora.columns:
                    df_fora.loc[idx, 'arquetipo'] = infos['nome_arquetipo']

    return df_casa, df_fora


def calcular_p90_e_normalizar(df_casa: pd.DataFrame, df_fora: pd.DataFrame):
    """Calcula as estatísticas por 90 minutos e aplica a normalização Min-Max Scaler global."""

    # 1. Cálculo por 90 minutos (P90)
    for df_temp in [df_casa, df_fora]:
        for stat in stats_usados:
            if stat in df_temp.columns:
                df_temp[stat + '_p90'] = (
                        df_temp[stat] / df_temp['minutesPlayed'].replace(0, np.nan) * 90
                ).fillna(0)

    # 2. Normalização Min-Max Scaler
    for stat in stats_usados:
        col_p90 = stat + '_p90'

        # Consolida os valores para estabelecer os limites da partida
        todos_valores = pd.concat([df_casa[col_p90], df_fora[col_p90]])
        min_val, max_val = todos_valores.min(), todos_valores.max()

        for df_temp in [df_casa, df_fora]:
            if max_val - min_val != 0:
                df_temp[col_p90] = (df_temp[col_p90] - min_val) / (max_val - min_val)
            else:
                df_temp[col_p90] = 0.0

    return df_casa, df_fora


def calcular_fator_titularidade(df_casa, time_casa, jogo_id, repository):
    """Define o peso de titularidade com base no histórico recente de partidas."""
    df_historico = repository.buscar_frequencia_titularidade(time_casa, jogo_id, limite=7)

    # Atribui peso neutro se não houver histórico suficiente no campeonato
    if df_historico.empty:
        df_casa['fator_titularidade'] = 0.5
        return df_casa

    max_jogos = 7
    df_historico['fator'] = (df_historico['partidas_titular'] / max_jogos).clip(0, 1)

    fator_map = dict(zip(df_historico['player_id'], df_historico['fator']))
    df_casa['fator_titularidade'] = df_casa['player_id'].map(fator_map).fillna(0.0)

    return df_casa


def preparar_dados_matchup(df_jogadores, time_casa, time_fora,
                           jogo_id, repository,
                           escalacao_adversario=None,
                           jogadores_indisponiveis=None,
                           adversario_improvisado=None):
    """Pipeline principal que consolida o pré-processamento de dados para o matchup."""

    df_jogadores['posicao_primaria'] = df_jogadores['posicoes_detalhadas'].apply(_extrair_primeira_posicao)
    df_jogadores['todas_posicoes'] = df_jogadores['posicoes_detalhadas'].apply(_extrair_todas_posicoes)

    df_casa = df_jogadores[df_jogadores['time_nome'] == time_casa].copy()
    df_fora = df_jogadores[df_jogadores['time_nome'] == time_fora].copy()

    df_casa, df_fora = aplicar_filtros_e_improvisacoes(
        df_casa, df_fora, escalacao_adversario, jogadores_indisponiveis, adversario_improvisado
    )

    df_casa = calcular_fator_titularidade(df_casa, time_casa, jogo_id, repository)
    df_casa, df_fora = calcular_p90_e_normalizar(df_casa, df_fora)

    return df_casa, df_fora


def calcular_hierarquia_por_minutos(df_casa, repository, jogo_id_atual, time_casa, janela=5):
    """
    Calcula a proporção de minutos jogados na janela de jogos anterior,
    garantindo que não haja data leakage para a partida atual.
    """
    ultimos_jogos_ids = repository.buscar_ultimos_jogos_id(time_casa, jogo_id_atual, limite=janela)

    if not ultimos_jogos_ids:
        df_casa['fator_titularidade'] = 0.5
        return df_casa

    historico_minutos = repository.buscar_minutos_jogadores_nos_jogos(ultimos_jogos_ids)

    minutos_por_jogador = historico_minutos.groupby('player_id')['minutesPlayed'].sum()
    max_minutos_possiveis = len(ultimos_jogos_ids) * 90

    fator_map = (minutos_por_jogador / max_minutos_possiveis).to_dict()
    df_casa['fator_titularidade'] = df_casa['player_id'].map(fator_map).fillna(0.0)

    return df_casa
