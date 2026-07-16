import pandas as pd
import numpy as np
import json
from sklearn.metrics.pairwise import cosine_similarity
from src.database import database as db

# Deixa o Pandas mostrar todas as linhas e colunas no console sem quebrar a visualização
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 20000)

caminho_json = r"caminho_json"


def extrair_primeira_posicao(valor):
    """Pega apenas a primeira posição quando há várias separadas por vírgula."""
    if pd.isna(valor):
        return 'Desconhecida'

    texto = str(valor).strip()
    if not texto:
        return 'Desconhecida'

    # Divide o texto na vírgula e devolve apenas o primeiro pedaço limpo
    partes = texto.split(',')
    primeira_posicao = partes[0].strip()

    return primeira_posicao


def calcular_similaridade_arquetipos(caminho_json):
    """
    Faz a mágica acontecer: busca os dados no banco, calcula a média por 90 minutos (P90),
    nivela tudo na mesma régua (Min-Max) e vê qual jogador dá "match" com o estilo de cada arquétipo.
    """

    # Traz os pesos das estatísticas de dentro do seu JSON
    with open(caminho_json, 'r', encoding='utf-8') as arquivo_json:
        arquivo_json_carregado = json.load(arquivo_json)

    dict_pesos = arquivo_json_carregado.get("arquetipo", {})

    # Lista de todas as estatísticas que vamos puxar do banco
    stats_list = [
        "goals", "bigChancesCreated", "bigChancesMissed", "assists", "goalsAssistsSum",
        "accuratePasses", "inaccuratePasses", "totalPasses", "accurateOwnHalfPasses",
        "accurateOppositionHalfPasses", "accurateFinalThirdPasses", "keyPasses",
        "successfulDribbles", "tackles", "interceptions", "yellowCards",
        "directRedCards", "redCards", "accurateCrosses", "totalShots",
        "shotsOnTarget", "shotsOffTarget", "groundDuelsWon", "aerialDuelsWon",
        "totalDuelsWon", "penaltiesTaken", "penaltyGoals", "penaltyWon",
        "penaltyConceded", "shotFromSetPiece", "freeKickGoal",
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

    # Prepara o formato "e.coluna" para rodar certinho na query SQL
    colunas_formatadas = []
    for col in stats_list:
        item = f"e.{col}"
        colunas_formatadas.append(item)

    columns_str = ", ".join(colunas_formatadas)

    # Monta as queries para pegar os jogadores e os dados de referência (deuses/arquétipos)
    query_jogadores_reais = f"""
        SELECT e.player_id, j.name, e.minutesPlayed, ct.posicoes_detalhadas, {columns_str} 
        FROM estatisticas_2025 e
        JOIN jogadores j ON e.player_id = j.player_id
        LEFT JOIN caracteristicas_taticas ct ON e.player_id = ct.player_id
    """

    query_deuses = """
        SELECT d.*, a.posicao_alvo, a.nome_arquetipo 
        FROM deuses_arquetipos d
        JOIN arquetipos_ref a ON d.id_arquetipo = a.id_arquetipo
    """

    # Bate no banco e traz as tabelas pro Pandas
    df_jogadores = pd.read_sql_query(query_jogadores_reais, con=db.engine)
    df_deuses = pd.read_sql_query(query_deuses, con=db.engine)

    # Corta a galera que não tem nem 90 minutos em campo
    df_jogadores = df_jogadores[df_jogadores['minutesPlayed'] >= 90].copy()

    # Guarda só a posição principal do jogador pra facilitar os filtros
    df_jogadores['posicao_primaria'] = df_jogadores['posicoes_detalhadas'].apply(extrair_primeira_posicao)

    # Pega as estatísticas brutas e transforma em proporção por 90 minutos (P90)
    colunas_p90 = []
    for coluna in stats_list:
        coluna_nova = coluna + '_p90'
        colunas_p90.append(coluna_nova)

    df_jogadores[colunas_p90] = (df_jogadores[stats_list].div(df_jogadores['minutesPlayed'], axis=0) * 90)
    df_jogadores = df_jogadores.replace([np.inf, -np.inf], 0).fillna(0)

    # Nivela tudo (Min-Max) para colocar as métricas na mesma balança (valores de 0 a 1)
    escala_min_max = {}

    for coluna in colunas_p90:
        min_val = df_jogadores[coluna].min()
        max_val = df_jogadores[coluna].max()

        escala_min_max[coluna] = {'min': min_val, 'max': max_val}

        if max_val - min_val != 0:
            df_jogadores[coluna] = (df_jogadores[coluna] - min_val) / (max_val - min_val)
        else:
            df_jogadores[coluna] = 0.0

    # Dá uma limpada nas colunas do arquétipo que vieram zeradas e não serão usadas
    colunas_so_zeros = []
    for coluna in df_deuses.columns:
        if (df_deuses[coluna] == 0).all():
            colunas_so_zeros.append(coluna)

    df_deuses = df_deuses.drop(columns=colunas_so_zeros)

    # Lista que vai guardar o resultado final do nosso "match"
    classificacoes_finais = []

    # Passa por cada arquétipo para comparar com os jogadores
    for index, deus in df_deuses.iterrows():
        id_arquetipo = str(int(deus['id_arquetipo']))
        string_posicoes = str(deus['posicao_alvo'])
        lista_posicoes_alvo = []

        # Limpa as posições que o arquétipo exige e joga numa lista
        if string_posicoes != 'nan' and string_posicoes.strip() != '':
            partes = string_posicoes.split(',')
            for pos in partes:
                item_limpo = pos.strip()
                lista_posicoes_alvo.append(item_limpo)

        pesos_dos_arquetipos = dict_pesos.get(id_arquetipo, {})
        colunas_relevantes = list(pesos_dos_arquetipos.keys())

        # Se der ruim na configuração (sem pesos ou posição alvo), pula pro próximo
        if not colunas_relevantes or not lista_posicoes_alvo:
            continue

        # Filtra os jogadores que realmente atuam na posição que o arquétipo procura
        df_jogadores_setor = df_jogadores[df_jogadores['posicao_primaria'].isin(lista_posicoes_alvo)].copy()
        df_jogadores_setor.reset_index(drop=True, inplace=True)

        if df_jogadores_setor.empty:
            continue

        vetor_deus_completo = []
        vetor_pesos_completo = []
        colunas_p90_relevantes = []

        # Normaliza os dados do arquétipo usando a mesma régua (escala Min-Max) dos jogadores
        for col in colunas_relevantes:
            peso_atual = pesos_dos_arquetipos[col]
            valor_deus = float(deus.get(col, 0))

            colunas_p90_relevantes.append(col + '_p90')
            vetor_pesos_completo.append(peso_atual)

            coluna_p90_ref = col + '_p90'
            min_camp = escala_min_max[coluna_p90_ref]['min']
            max_camp = escala_min_max[coluna_p90_ref]['max']

            if max_camp - min_camp == 0:
                valor_norm = 0.0
            else:
                valor_norm = (valor_deus - min_camp) / (max_camp - min_camp)

            # Trava os limites entre 0 e 1 pra garantir que nada passe do teto
            valor_norm = max(0.0, min(1.0, valor_norm))
            vetor_deus_completo.append(valor_norm)

        # Prepara as matrizes para cruzar os dados
        vetores_jogadores_filtrados = df_jogadores_setor[colunas_p90_relevantes].values
        soma_acoes_jogador = vetores_jogadores_filtrados.sum(axis=1)

        vetor_deus = np.array(vetor_deus_completo).reshape(1, -1)
        vetor_pesos = np.array(vetor_pesos_completo)

        # Multiplica os dados do jogador e do arquétipo pelos pesos definidos no JSON
        vetores_jogadores_pesados = vetores_jogadores_filtrados * vetor_pesos
        vetor_deus_pesado = vetor_deus * vetor_pesos

        # Calcula a compatibilidade (Similaridade de Cosseno)
        similaridades = cosine_similarity(vetores_jogadores_pesados, vetor_deus_pesado)

        # Salva o "match" na lista final, ignorando distorções matemáticas ou jogadores inativos
        for i, sim in enumerate(similaridades):
            score_final = float(sim[0])

            if score_final > 0.1 and soma_acoes_jogador[i] > 0.1:
                classificacoes_finais.append({
                    'ID do jogador': int(df_jogadores_setor.loc[i, 'player_id']),
                    'Nome do Jogador': str(df_jogadores_setor.loc[i, 'name']),
                    'ID do Arquetipo': int(deus['id_arquetipo']),
                    'Nome do Arquétipo': str(deus['nome_arquetipo']),
                    'Score de Similaridade': round(score_final * 100, 2)
                })

    # Transforma a lista de volta em DataFrame e organiza o ranking do maior pro menor
    df_classificacao_final = pd.DataFrame(classificacoes_finais)
    df_classificacao_final = df_classificacao_final.sort_values(by='Score de Similaridade',
                                                                ascending=False).reset_index(drop=True)

    return df_classificacao_final


if __name__ == "__main__":
    df_teste = calcular_similaridade_arquetipos(caminho_json)
    print(df_teste.head(10))