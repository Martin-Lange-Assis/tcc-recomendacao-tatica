# src/recommendation/solvers.py
import pulp
import pandas as pd
from scipy.optimize import linear_sum_assignment
from src.domain.regras_matchup import ARQUETIPOS_GOLEIRO
import math
import numpy as np

MAPA_SIGLAS = {
    'Atacante': 'ATA', 'Volante': 'VOL', 'Meia Ofensivo': 'MEI',
    'Zagueiro': 'ZAG', 'Goleiro': 'GL', 'Lateral Dir': 'LD',
    'Lateral Esq': 'LE', 'Meia Central': 'MC', 'Meia Dir': 'MD',
    'Meia Esq': 'ME', 'Ponta Dir': 'PD', 'Ponta Esq': 'PE'
}

# Simplificado em formato de dicionário direto (X, Y) para busca rápida
COORDENADAS = {
    'GL': (7, 50), 'ZAG': (20, 50), 'LD': (25, 15), 'LE': (25, 85),
    'VOL': (40, 50), 'MC': (50, 50), 'MD': (65, 15), 'MEI': (70, 50),
    'ME': (65, 85), 'PD': (80, 15), 'PE': (80, 85), 'ATA': (85, 50)
}


def obter_coordenada(posicao_nome, is_adv=False):
    """Busca a coordenada X,Y baseada no nome da posição e espelha o adversário."""
    sigla = MAPA_SIGLAS.get(posicao_nome, posicao_nome)
    coord = COORDENADAS.get(sigla, (50, 50))  # Meio campo como fallback

    if is_adv:
        # Espelha o campo para o adversário (X e Y)
        return (100 - coord[0], 100 - coord[1])
    return coord


# ==============================================================================
# ESTRATÉGIA 1: COM FORMAÇÃO (ILP / PuLP)
# ==============================================================================
def resolver_com_formacao_ilp(df_rec: pd.DataFrame, df_casa: pd.DataFrame,
                              df_fora: pd.DataFrame,
                              formacao_slots: list) -> pd.DataFrame:
    """
    Seleciona os 11 titulares respeitando as cotas de formação via ILP.
    Aplica bônus para forçar a escalação completa e multas pesadas por improvisação.
    """
    home_ids = df_casa['player_id'].unique().tolist()

    pos_map = dict(zip(df_casa['player_id'], df_casa['posicao_primaria']))
    todas_posicoes_map = dict(zip(df_casa['player_id'], df_casa['todas_posicoes']))
    arq_map = dict(zip(df_casa['player_id'], df_casa['id_arquetipo']))
    qualidade_map = dict(zip(df_casa['player_id'], df_casa['score_similaridade']))

    # ======================================================================
    # MAPEAMENTO DA HIERARQUIA (JANELA DESLIZANTE)
    # ======================================================================
    if 'fator_titularidade' in df_casa.columns:
        titularidade_map = dict(zip(df_casa['player_id'], df_casa['fator_titularidade']))
    else:
        titularidade_map = {pid: 0.0 for pid in home_ids}

    away_ids = df_fora['player_id'].tolist()

    print("\n" + "=" * 50)
    print("[MODELAGEM TCC] PASSO 1: CONJUNTOS BASE")
    print(f"-> Conjunto de mandantes disponíveis: {len(home_ids)} jogadores.")
    print(f"-> Conjunto de adversários: {len(away_ids)} jogadores.")
    print("-> Slots da formação tática:")

    for s in formacao_slots:
        print(f"   - {s['slot']}")

    print("=" * 50 + "\n")

    away_info = {
        int(row['player_id']): {
            'nome': str(row['player_name']),
            'posicao': str(row['posicao_primaria']),
            'arquetipo': str(row['nome_arquetipo']),
            'id_arquetipo': row['id_arquetipo'] if pd.notna(row['id_arquetipo']) else -1
        }
        for _, row in df_fora.iterrows()
    }

    score_natural = {}
    for _, row in df_rec.iterrows():
        h = int(row['meu_player_id'])
        a = int(row['adversario_player_id'])
        score_natural[(h, a)] = float(row['score_matchup'])

    todos_pares = []

    for h in home_ids:
        for a in away_ids:
            par = (h, a)
            todos_pares.append(par)

        # PESO_DISTANCIA controla a penalidade espacial.
        # Ex: 0.05 significa que a cada 1 unidade de distância no campo,
        # o jogador perde 0.05 de "score de similaridade".
        PESO_DISTANCIA = 0.05

        score_efetivo = {}
        for h, a in todos_pares:
            pos_h = pos_map.get(h, '')
            pos_a = away_info[a]['posicao']

            coord_h = obter_coordenada(pos_h, is_adv=False)
            coord_a = obter_coordenada(pos_a, is_adv=True)
            distancia = math.dist(coord_h, coord_a)

            if (h, a) in score_natural:
                score_base = score_natural[(h, a)]
            else:
                qualidade_jogador = float(qualidade_map.get(h, 0) or 0)
                score_base = 0.001 + (qualidade_jogador / 1000)

            score_efetivo[(h, a)] = score_base - (distancia * PESO_DISTANCIA)

    prob = pulp.LpProblem("matchup_formacao", pulp.LpMaximize)

    # ======================================================================
    # VARIÁVEIS DE DECISÃO
    # ======================================================================
    # x: Se o jogador 'h' está escalado (1) ou não (0)
    x = {h: pulp.LpVariable(f"x_{h}", cat='Binary') for h in home_ids}

    # y: Se o jogador 'h' está marcando o adversário 'a' (1) ou não (0)
    y = {(h, a): pulp.LpVariable(f"y_{h}_{a}", cat='Binary') for h, a in todos_pares}

    print("[MODELAGEM TCC] PASSO 2: VARIÁVEIS DE DECISÃO BÁSICAS")
    print(f"-> Variável X (Escalação): Criadas {len(x)} variáveis binárias.")
    print(f"-> Variável Y (Marcação): Criadas {len(y)} variáveis binárias (todos os pares).")
    print("-" * 50)

    z = {}
    print("[MODELAGEM TCC] PASSO 3: SUBCONJUNTOS DE APTIDÃO E VARIÁVEL Z")

    # z: Se o jogador 'h' ocupa o slot tático específico 's_name'
    for slot in formacao_slots:
        s_name = slot['slot']
        jogadores_aptos_no_slot = 0
        for h in home_ids:
            posicoes_do_jogador = todas_posicoes_map.get(h, [])
            if any(p in slot['posicoes'] for p in posicoes_do_jogador):
                z[(h, s_name)] = pulp.LpVariable(f"z_{h}_{s_name}", cat='Binary')
                jogadores_aptos_no_slot += 1
        print(f"-> Slot '{s_name}': {jogadores_aptos_no_slot} jogadores do plantel aptos. Variáveis Z criadas.")
    print("=" * 50 + "\n")

    # ======================================================================
    # FUNÇÃO OBJETIVO: Matchup + Bônus de Campo + Multa de Improvisação + Hierarquia
    # ======================================================================
    objetivo = 0

    # Maximizar os scores efetivos dos confrontos
    for h, a in todos_pares:
        objetivo += score_efetivo[(h, a)] * y[(h, a)]

    for slot in formacao_slots:
        s_name = slot['slot']
        posicao_primaria_slot = slot['posicoes'][0]

        for h in home_ids:
            if (h, s_name) in z:
                # Bônus alto para garantir a escalação de 11 jogadores
                bonus_presenca = 10000

                # Sistema de penalidades para evitar improvisações
                if pos_map[h] == posicao_primaria_slot:
                    penalidade = 0
                elif pos_map[h] in slot['posicoes']:
                    penalidade = -200
                else:
                    penalidade = -1000

                # Bônus baseado na titularidade consolidada do jogador
                PESO_HIERARQUIA = 800
                bonus_hierarquia = titularidade_map.get(h, 0.0) * PESO_HIERARQUIA

                objetivo += (bonus_presenca + penalidade + bonus_hierarquia) * z[(h, s_name)]

    prob += objetivo

    # ======================================================================
    # RESTRIÇÕES DO FUTEBOL E DO SISTEMA
    # ======================================================================

    # REGRA 1: O time deve ter NO MÁXIMO 11 jogadores em campo
    variaveis_para_soma = []
    for h in home_ids:
        variaveis_para_soma.append(x[h])
    prob += pulp.lpSum(variaveis_para_soma) <= 11, "teto_11_jogadores"

    # REGRA 2: Um jogador só entra em campo (x=1) se ocupar EXATAMENTE um slot (z=1)
    for h in home_ids:
        slots_h = []
        for slot in formacao_slots:
            nome_slot = slot['slot']
            if (h, nome_slot) in z:
                slots_h.append(nome_slot)

        if slots_h:
            variaveis_z_selecionadas = []
            for s in slots_h:
                var = z[(h, s)]
                variaveis_z_selecionadas.append(var)

            soma_z_slots = pulp.lpSum(variaveis_z_selecionadas)
            prob += (soma_z_slots == x[h]), f"link_x_z_{h}"
            prob += (soma_z_slots <= 1), f"max_1_slot_{h}"
        else:
            prob += x[h] == 0, f"fora_do_esquema_{h}"

    # REGRA 3: O número de jogadores num slot NÃO PODE ultrapassar a cota da formação
    for slot in formacao_slots:
        s_name = slot['slot']
        candidatos = []

        for h in home_ids:
            if (h, s_name) in z:
                candidatos.append(h)

        if candidatos:
            variaveis_cota = []
            for h in candidatos:
                var = z[(h, s_name)]
                variaveis_cota.append(var)

            limite = slot['count']
            nome_restricao = f"cota_{s_name}"
            prob += pulp.lpSum(variaveis_cota) <= limite, nome_restricao

    # REGRA 4: Um jogador mandante marca no máximo 1 adversário e apenas se estiver em campo
    for h in home_ids:
        confrontos_mandante = []
        for a in away_ids:
            var_y = y[(h, a)]
            confrontos_mandante.append(var_y)

        limite_participacao = x[h]
        nome_restricao = f"home_exato_{h}"
        prob += pulp.lpSum(confrontos_mandante) <= limite_participacao, nome_restricao

    # REGRA 5: Um jogador adversário é marcado por no máximo 1 jogador mandante
    for a in away_ids:
        jogos_do_visitante = []
        for h in home_ids:
            var_y = y[(h, a)]
            jogos_do_visitante.append(var_y)

        nome_restricao = f"adv_exato_{a}"
        prob += pulp.lpSum(jogos_do_visitante) <= 1, nome_restricao

    # REGRA 6: Goleiros não marcam jogadores de linha
    goleiros_casa = []
    for h in home_ids:
        arquetipo = arq_map.get(h)
        if arquetipo in ARQUETIPOS_GOLEIRO:
            goleiros_casa.append(h)

    goleiros_fora = []
    for a in away_ids:
        info_jogador = away_info[a]
        id_arquetipo_fora = info_jogador['id_arquetipo']
        if id_arquetipo_fora in ARQUETIPOS_GOLEIRO:
            goleiros_fora.append(a)

    if goleiros_fora:
        for g_casa in goleiros_casa:
            for a in away_ids:
                if a not in goleiros_fora:
                    prob += y[(g_casa, a)] == 0, f"goleiro_casa_nao_marca_linha_{g_casa}_{a}"

    # REGRA 7: Jogadores de linha não marcam o goleiro adversário
    if goleiros_fora:
        for h in home_ids:
            if h not in goleiros_casa:
                for g_fora in goleiros_fora:
                    prob += y[(h, g_fora)] == 0, f"linha_nao_marca_goleiro_{h}_{g_fora}"

    # ======================================================================
    # RESOLUÇÃO E PARSING DOS RESULTADOS
    # ======================================================================
    prob.solve(pulp.PULP_CBC_CMD(msg=0))

    if prob.status != 1:
        print(f"[ERRO CRÍTICO] ILP falhou. Status: {pulp.LpStatus[prob.status]}")
        return pd.DataFrame()

    # Mapeamento dos jogadores do time da casa que foram ativados pelo solver
    selecionados = []
    for h in home_ids:
        valor_variavel = pulp.value(x[h])
        if valor_variavel is not None and valor_variavel > 0.5:
            selecionados.append(h)

    # Identifica os confrontos definidos pelo modelo
    assign_adv = {}
    for h, a in todos_pares:
        if pulp.value(y[(h, a)]) is not None and pulp.value(y[(h, a)]) > 0.5:
            assign_adv[h] = a

    # ======================================================================
    # CASAMENTO INTELIGENTE DAS SOBRAS TÁTICAS
    # ======================================================================
    # Trata os jogadores que ficaram sem marcação direta designada pelo solver
    adversarios_pareados = list(assign_adv.values())
    adversarios_sobra = [a for a in away_ids if a not in adversarios_pareados]
    meus_sem_marca = [h for h in selecionados if h not in assign_adv]

    # Prioriza jogadores de linha para assumir as sobras, deixando o goleiro por último
    def prioridade_goleiro(h_id):
        return 1 if arq_map.get(h_id) in ARQUETIPOS_GOLEIRO else 0

    meus_sem_marca.sort(key=prioridade_goleiro)

    # Aproximação heurística para casar as sobras minimizando prejuízos táticos
    for h in meus_sem_marca:
        if not adversarios_sobra:
            break

        minha_pos = str(pos_map.get(h, '')).upper()
        melhor_idx = 0
        melhor_score = -9999

        for i, a in enumerate(adversarios_sobra):
            adv_pos = str(away_info[a]['posicao']).upper()
            score = 0

            if 'GL' in minha_pos and 'GL' in adv_pos:
                score = 100
            elif 'GL' in minha_pos or 'GL' in adv_pos:
                score = -1000
            elif ('ZAG' in minha_pos and 'ATA' in adv_pos) or ('ATA' in minha_pos and 'ZAG' in adv_pos):
                score = 50
            elif 'ZAG' in minha_pos and 'ZAG' in adv_pos:
                score = 40

            if score > melhor_score:
                melhor_score = score
                melhor_idx = i

        adv_escolhido = adversarios_sobra.pop(melhor_idx)
        assign_adv[h] = adv_escolhido

    # ======================================================================
    # FORMATAÇÃO DO DATAFRAME DE SAÍDA
    # ======================================================================
    linhas = []
    soma_scores = 0.0

    for h in selecionados:
        row_home = df_casa[df_casa['player_id'] == h].iloc[0]
        slot_atribuido = "Desconhecido"

        for slot in formacao_slots:
            s_name = slot['slot']
            if (h, s_name) in z:
                valor_z = pulp.value(z[(h, s_name)])
                if valor_z is not None and valor_z > 0.5:
                    slot_atribuido = s_name
                    break

        if h in assign_adv:
            adv_id = assign_adv[h]
            info_adv = away_info[adv_id]
            score = score_efetivo[(h, adv_id)]
            matchup_natural = (h, adv_id) in score_natural

            adv_id_final = adv_id
            adv_nome = info_adv['nome']
            adv_pos = info_adv['posicao']
            adv_arq = info_adv['arquetipo']

            if matchup_natural:
                score_exib_final = round(score, 2)
            else:
                score_exib_final = 'Sobra/Isolado'

        else:
            adv_id_final = 'icone_cobertura'
            adv_nome = 'Sem Jogador'
            adv_pos = 'Livre'
            adv_arq = '-'
            score = 0
            matchup_natural = False
            score_exib_final = 'Atuando na Cobertura'

        linhas.append({
            'adversario_player_id': adv_id_final,
            'adversario_nome': adv_nome,
            'adversario_posicao': adv_pos,
            'adversario_arquetipo': adv_arq,
            'meu_player_id': h,
            'meu_player_nome': str(row_home['player_name']),
            'minha_posicao': str(row_home['posicao_primaria']),
            'meu_slot': slot_atribuido,
            'meu_arquetipo': str(row_home['nome_arquetipo']),
            'score_matchup': score_exib_final,
            'matchup_tatico': matchup_natural,
        })
        soma_scores += score

    print(f'A eficiência global foi de {soma_scores:.2f}')
    return pd.DataFrame(linhas)


# ==============================================================================
# ESTRATÉGIA 2: SEM FORMAÇÃO (ALGORITMO HÚNGARO)
# ==============================================================================
def resolver_sem_formacao_hungarian(df_rec: pd.DataFrame, df_casa: pd.DataFrame) -> pd.DataFrame:
    """
    Seleciona os 11 titulares maximizando puramente o score de matchup direto
    utilizando o Algoritmo Húngaro (linear_sum_assignment), sem restrições táticas de formação.
    """
    print("\n" + "=" * 50)
    print("[MODELAGEM TCC] INICIANDO ALGORITMO HÚNGARO (SEM FORMAÇÃO)")

    # Extrair listas únicas de jogadores
    home_ids = df_casa['player_id'].unique().tolist()

    # Pegamos os adversários disponíveis nas recomendações
    away_ids = df_rec['adversario_player_id'].unique().tolist()

    # O time adversário deve ter 11 jogadores. Se tiver mais, limitamos.
    away_ids = away_ids[:11]

    print(f"-> Mandantes disponíveis: {len(home_ids)}")
    print(f"-> Vagas/Adversários a marcar: {len(away_ids)}")

    # Dicionários para mapear IDs para índices da matriz (0 a N)
    home_idx = {pid: i for i, pid in enumerate(home_ids)}
    away_idx = {pid: j for j, pid in enumerate(away_ids)}

    # Inicializar matriz de custos com penalidade alta para confrontos inexistentes
    # float padrão para evitar problemas de tipagem
    cost_matrix = np.full((len(home_ids), len(away_ids)), 9999.0)

    # Preencher a matriz com o negativo do score (pois o algoritmo minimiza o valor)
    for _, row in df_rec.iterrows():
        h_id = row['meu_player_id']
        a_id = row['adversario_player_id']

        if h_id in home_idx and a_id in away_idx:
            # Multiplica por -1 para que o Húngaro encontre o score MÁXIMO
            cost_matrix[home_idx[h_id], away_idx[a_id]] = -float(row['score_matchup'])

    # Executar o Algoritmo Húngaro
    # row_ind = índices dos mandantes escolhidos
    # col_ind = índices dos adversários correspondentes
    row_ind, col_ind = linear_sum_assignment(cost_matrix)

    linhas = []
    soma_scores = 0.0

    for r, c in zip(row_ind, col_ind):
        # Se o custo continuou 9999, não havia matchup mapeado para esse par
        if cost_matrix[r, c] == 9999.0:
            continue

        h_id = home_ids[r]
        a_id = away_ids[c]

        # Recuperar os dados originais
        matchup_rows = df_rec[(df_rec['meu_player_id'] == h_id) & (df_rec['adversario_player_id'] == a_id)]

        if not matchup_rows.empty:
            match_row = matchup_rows.iloc[0]
            player_row = df_casa[df_casa['player_id'] == h_id].iloc[0]

            score_real = float(match_row['score_matchup'])
            soma_scores += score_real

            linhas.append({
                'adversario_player_id': a_id,
                'adversario_nome': match_row.get('adversario_nome', str(a_id)),
                'adversario_posicao': match_row.get('adversario_posicao', 'Livre'),
                'adversario_arquetipo': match_row.get('adversario_arquetipo', '-'),
                'meu_player_id': h_id,
                'meu_player_nome': str(player_row['player_name']),
                'minha_posicao': str(player_row['posicao_primaria']),
                'meu_slot': 'Livre (Sem Formação)',
                'meu_arquetipo': str(player_row['nome_arquetipo']),
                'score_matchup': round(score_real, 2),
                'matchup_tatico': True
            })

    df_resultado = pd.DataFrame(linhas)

    # Ordenar pelos maiores scores para ficar bonito no print do terminal
    if not df_resultado.empty:
        df_resultado = df_resultado.sort_values(by='score_matchup', ascending=False)

    print(f"-> Escalação montada! Eficiência global: {soma_scores:.2f}")
    print("=" * 50 + "\n")

    return df_resultado
