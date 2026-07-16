import pandas as pd
from collections import defaultdict
from src.database import repository
from src.recommendation.matchup import calcular_matchup

# Mapeamento de coordenadas espaciais (X, Y) no campo por posição base
DICIONARIO_COORDENADAS = pd.DataFrame([
    {'Posicao_Base': 'GL', 'Eixo_X': 7, 'Eixo_Y': 50},
    {'Posicao_Base': 'ZAG', 'Eixo_X': 15, 'Eixo_Y': 50},
    {'Posicao_Base': 'LD', 'Eixo_X': 20, 'Eixo_Y': 20},
    {'Posicao_Base': 'LE', 'Eixo_X': 20, 'Eixo_Y': 85},
    {'Posicao_Base': 'VOL', 'Eixo_X': 40, 'Eixo_Y': 50},
    {'Posicao_Base': 'MC', 'Eixo_X': 50, 'Eixo_Y': 50},
    {'Posicao_Base': 'MD', 'Eixo_X': 55, 'Eixo_Y': 15},
    {'Posicao_Base': 'MEI', 'Eixo_X': 65, 'Eixo_Y': 50},
    {'Posicao_Base': 'ME', 'Eixo_X': 55, 'Eixo_Y': 85},
    {'Posicao_Base': 'PD', 'Eixo_X': 70, 'Eixo_Y': 20},
    {'Posicao_Base': 'PE', 'Eixo_X': 70, 'Eixo_Y': 85},
    {'Posicao_Base': 'ATA', 'Eixo_X': 75, 'Eixo_Y': 50}
])

# Dicionário de conversão de nomenclatura para siglas padronizadas
MAPA_SIGLAS = {
    'Atacante': 'ATA',
    'Volante': 'VOL',
    'Meia Ofensivo': 'MEI',
    'Zagueiro': 'ZAG',
    'Goleiro': 'GL',
    'Lateral Dir': 'LD',
    'Lateral Esq': 'LE',
    'Meia Central': 'MC',
    'Meia Dir': 'MD',
    'Meia Esq': 'ME',
    'Ponta Dir': 'PD',
    'Ponta Esq': 'PE'
}


def rodar_avaliacao_global():
    print("Iniciando avaliação global usando as FORMAÇÕES REAIS (Apenas Mandantes)")

    lista_jogos = repository.buscar_todos_jogos_ids()

    total_acertos = 0
    total_jogadores_avaliados = 0
    jogos_processados = 0

    acertos_por_posicao = defaultdict(int)
    avaliados_por_posicao = defaultdict(int)

    avaliados_regulares = 0
    acertos_regulares = 0

    avaliados_improvisos = 0
    acertos_improvisos = 0

    stats_por_desfalque = {}
    lista_exportacao_pbi = []

    stats_por_resultado = {
        "Vitória": {"acertos": 0, "avaliados": 0, "jogos": 0},
        "Empate": {"acertos": 0, "avaliados": 0, "jogos": 0},
        "Derrota": {"acertos": 0, "avaliados": 0, "jogos": 0},
        "Desconhecido": {"acertos": 0, "avaliados": 0, "jogos": 0}
    }

    for jogo_id in lista_jogos:
        for lado in ['home']:

            formacao_real = repository.buscar_formacao_real(jogo_id, lado)

            if not formacao_real:
                continue

            contexto = repository.buscar_contexto_partida(jogo_id=jogo_id, meu_lado=lado)

            if not contexto or not contexto.get('meus_titulares_reais'):
                continue

            titulares_reais = set(contexto['meus_titulares_reais'])
            if len(titulares_reais) == 0:
                continue

            meu_time = contexto['time_casa']
            adversario = contexto['time_fora']
            resultado_partida = repository.buscar_resultado_partida(jogo_id, lado)

            try:
                df_resultado = calcular_matchup(
                    time_casa=meu_time,
                    time_fora=adversario,
                    jogo_id=jogo_id,
                    repository=repository,
                    escalacao_adversario=contexto['jogadores_adversarios'],
                    formacao=formacao_real,
                    adversario_improvisado=contexto['improvisacoes'],
                    jogadores_indisponiveis=contexto['desfalques']
                )
            except Exception as e:
                print(f"Erro ao calcular Jogo {jogo_id} ({formacao_real}): {e}")
                continue

            if not df_resultado.empty:
                df_jogo_pbi = df_resultado.copy()
                df_jogo_pbi['Jogo_ID'] = jogo_id
                df_jogo_pbi['Time'] = meu_time
                df_jogo_pbi['Adversario'] = adversario
                df_jogo_pbi['Formacao'] = formacao_real

                # Processamento e padronização das posições do time mandante
                if 'minha_posicao' in df_jogo_pbi.columns:
                    df_jogo_pbi = df_jogo_pbi.rename(columns={'minha_posicao': 'Posicao'})

                    pos_improviso_extenso = df_jogo_pbi['Posicao'].str.extract(r'Improvisado de (.*?)\)', expand=False)
                    posicao_improvisada = pos_improviso_extenso.map(MAPA_SIGLAS).fillna(pos_improviso_extenso)
                    posicao_original = df_jogo_pbi['Posicao'].str.split(r' \(').str[0]

                    df_jogo_pbi['Posicao_Base'] = posicao_improvisada.fillna(posicao_original).str.strip()

                # Processamento e padronização das posições do time visitante
                if 'adversario_posicao' in df_jogo_pbi.columns:
                    pos_adv_improviso = df_jogo_pbi['adversario_posicao'].str.extract(r'Improvisado de (.*?)\)',
                                                                                      expand=False)
                    pos_adv_improvisada = pos_adv_improviso.map(MAPA_SIGLAS).fillna(pos_adv_improviso)
                    pos_adv_original = df_jogo_pbi['adversario_posicao'].str.split(r' \(').str[0]

                    df_jogo_pbi['Posicao_Base_Adv'] = pos_adv_improvisada.fillna(pos_adv_original).str.strip()

                # Associação das coordenadas no campo (time mandante)
                df_merged = pd.merge(df_jogo_pbi, DICIONARIO_COORDENADAS, on='Posicao_Base', how='left')

                # Associação das coordenadas e inversão do eixo X para o adversário atacar no sentido oposto
                if 'Posicao_Base_Adv' in df_merged.columns:
                    dict_adv = DICIONARIO_COORDENADAS.rename(columns={
                        'Posicao_Base': 'Posicao_Base_Adv',
                        'Eixo_X': 'Eixo_X_Adv',
                        'Eixo_Y': 'Eixo_Y_Adv'
                    })
                    df_merged = pd.merge(df_merged, dict_adv, on='Posicao_Base_Adv', how='left')
                    df_merged['Eixo_X_Adv'] = 100 - df_merged['Eixo_X_Adv']

                # Cálculo de métricas de acerto para exportação
                titulares_modelo_temp = set(df_resultado['meu_player_nome'].tolist())
                acertos_temp = titulares_modelo_temp.intersection(titulares_reais)

                df_merged['Resultado'] = resultado_partida
                df_merged['Acertos_Modelo'] = len(acertos_temp)
                df_merged['Total_Titulares'] = len(titulares_reais)

                lista_exportacao_pbi.append(df_merged)

                # Atualização das estatísticas gerais da iteração
                for _, row in df_resultado.iterrows():
                    jogador_modelo = row['meu_player_nome']
                    posicao_modelo = row.get('minha_posicao', 'Desconhecida')

                    avaliados_por_posicao[posicao_modelo] += 1
                    is_acerto = jogador_modelo in titulares_reais

                    if is_acerto:
                        acertos_por_posicao[posicao_modelo] += 1

                    if "Improvisado" in posicao_modelo:
                        avaliados_improvisos += 1
                        if is_acerto:
                            acertos_improvisos += 1
                    else:
                        avaliados_regulares += 1
                        if is_acerto:
                            acertos_regulares += 1

                titulares_modelo = set(df_resultado['meu_player_nome'].tolist())
                acertos = titulares_modelo.intersection(titulares_reais)
                qtd_acertos = len(acertos)
                qtd_titulares = len(titulares_reais)

                total_acertos += qtd_acertos
                total_jogadores_avaliados += qtd_titulares
                jogos_processados += 1

                if resultado_partida in stats_por_resultado:
                    stats_por_resultado[resultado_partida]["acertos"] += qtd_acertos
                    stats_por_resultado[resultado_partida]["avaliados"] += qtd_titulares
                    stats_por_resultado[resultado_partida]["jogos"] += 1

                qtd_desfalques = len(contexto['desfalques'])
                if qtd_desfalques not in stats_por_desfalque:
                    stats_por_desfalque[qtd_desfalques] = {"acertos": 0, "avaliados": 0, "jogos": 0}

                stats_por_desfalque[qtd_desfalques]["acertos"] += qtd_acertos
                stats_por_desfalque[qtd_desfalques]["avaliados"] += qtd_titulares
                stats_por_desfalque[qtd_desfalques]["jogos"] += 1

                print(
                    f"Jogo {jogo_id} [{meu_time} x {adversario}] ({lado} - {formacao_real}) | Resultado: {resultado_partida} | {qtd_acertos}/{qtd_titulares} acertos.")

    print("\n" + "=" * 50)
    print("RESUMO DA AVALIAÇÃO GLOBAL")
    print("=" * 50)

    # Consolidação e formatação dos dados para exportação (Dashboard)
    if lista_exportacao_pbi:
        df_final_pbi = pd.concat(lista_exportacao_pbi, ignore_index=True)

        df_final_pbi['Eixo_X'] = df_final_pbi['Eixo_X'].fillna(0)
        df_final_pbi['Eixo_Y'] = df_final_pbi['Eixo_Y'].fillna(0)

        # Lógica de espalhamento (offset) para evitar sobreposição de jogadores na mesma posição (Time Mandante)
        df_unicos_casa = df_final_pbi[['Jogo_ID', 'meu_player_id', 'Posicao_Base']].drop_duplicates()
        df_unicos_casa['total_na_posicao'] = df_unicos_casa.groupby(['Jogo_ID', 'Posicao_Base'])[
            'Posicao_Base'].transform('count')
        df_unicos_casa['ocorrencia'] = df_unicos_casa.groupby(['Jogo_ID', 'Posicao_Base']).cumcount()
        df_unicos_casa['Offset_Y'] = 0

        mask_2 = df_unicos_casa['total_na_posicao'] == 2
        df_unicos_casa.loc[mask_2 & (df_unicos_casa['ocorrencia'] == 0), 'Offset_Y'] = 14
        df_unicos_casa.loc[mask_2 & (df_unicos_casa['ocorrencia'] == 1), 'Offset_Y'] = -14

        mask_3 = df_unicos_casa['total_na_posicao'] == 3
        df_unicos_casa.loc[mask_3 & (df_unicos_casa['ocorrencia'] == 0), 'Offset_Y'] = 20
        df_unicos_casa.loc[mask_3 & (df_unicos_casa['ocorrencia'] == 2), 'Offset_Y'] = -20

        mask_4 = df_unicos_casa['total_na_posicao'] == 4
        df_unicos_casa.loc[mask_4 & (df_unicos_casa['ocorrencia'] == 0), 'Offset_Y'] = 24
        df_unicos_casa.loc[mask_4 & (df_unicos_casa['ocorrencia'] == 1), 'Offset_Y'] = 8
        df_unicos_casa.loc[mask_4 & (df_unicos_casa['ocorrencia'] == 2), 'Offset_Y'] = -8
        df_unicos_casa.loc[mask_4 & (df_unicos_casa['ocorrencia'] == 3), 'Offset_Y'] = -24

        df_final_pbi = df_final_pbi.merge(
            df_unicos_casa[['Jogo_ID', 'meu_player_id', 'Posicao_Base', 'Offset_Y']],
            on=['Jogo_ID', 'meu_player_id', 'Posicao_Base'], how='left'
        )

        df_final_pbi['Eixo_Y'] = (df_final_pbi['Eixo_Y'] + df_final_pbi['Offset_Y'].fillna(0)).clip(0, 100)
        df_final_pbi = df_final_pbi.drop(columns=['Offset_Y', 'total_na_posicao', 'ocorrencia'], errors='ignore')

        # Lógica de espalhamento (offset) para o Time Visitante
        if 'Posicao_Base_Adv' in df_final_pbi.columns:
            df_unicos_adv = df_final_pbi[['Jogo_ID', 'adversario_nome', 'Posicao_Base_Adv']].drop_duplicates()
            df_unicos_adv['total_na_posicao_adv'] = df_unicos_adv.groupby(['Jogo_ID', 'Posicao_Base_Adv'])[
                'Posicao_Base_Adv'].transform('count')
            df_unicos_adv['ocorrencia_adv'] = df_unicos_adv.groupby(['Jogo_ID', 'Posicao_Base_Adv']).cumcount()
            df_unicos_adv['Offset_Y_Adv'] = 0

            mask_2_adv = df_unicos_adv['total_na_posicao_adv'] == 2
            df_unicos_adv.loc[mask_2_adv & (df_unicos_adv['ocorrencia_adv'] == 0), 'Offset_Y_Adv'] = 14
            df_unicos_adv.loc[mask_2_adv & (df_unicos_adv['ocorrencia_adv'] == 1), 'Offset_Y_Adv'] = -14

            mask_3_adv = df_unicos_adv['total_na_posicao_adv'] == 3
            df_unicos_adv.loc[mask_3_adv & (df_unicos_adv['ocorrencia_adv'] == 0), 'Offset_Y_Adv'] = 20
            df_unicos_adv.loc[mask_3_adv & (df_unicos_adv['ocorrencia_adv'] == 2), 'Offset_Y_Adv'] = -20

            mask_4_adv = df_unicos_adv['total_na_posicao_adv'] == 4
            df_unicos_adv.loc[mask_4_adv & (df_unicos_adv['ocorrencia_adv'] == 0), 'Offset_Y_Adv'] = 24
            df_unicos_adv.loc[mask_4_adv & (df_unicos_adv['ocorrencia_adv'] == 1), 'Offset_Y_Adv'] = 8
            df_unicos_adv.loc[mask_4_adv & (df_unicos_adv['ocorrencia_adv'] == 2), 'Offset_Y_Adv'] = -8
            df_unicos_adv.loc[mask_4_adv & (df_unicos_adv['ocorrencia_adv'] == 3), 'Offset_Y_Adv'] = -24

            df_final_pbi = df_final_pbi.merge(
                df_unicos_adv[['Jogo_ID', 'adversario_nome', 'Posicao_Base_Adv', 'Offset_Y_Adv']],
                on=['Jogo_ID', 'adversario_nome', 'Posicao_Base_Adv'], how='left'
            )

            df_final_pbi['Eixo_X_Adv'] = df_final_pbi['Eixo_X_Adv'].fillna(0)
            df_final_pbi['Eixo_Y_Adv'] = (
                    df_final_pbi['Eixo_Y_Adv'].fillna(0) + df_final_pbi['Offset_Y_Adv'].fillna(0)).clip(0, 100)
            df_final_pbi = df_final_pbi.drop(columns=['Offset_Y_Adv'], errors='ignore')

        # Verificação de integridade das colunas obrigatórias
        colunas_adv_necessarias = [
            'adversario_player_id', 'adversario_nome', 'adversario_posicao',
            'Eixo_X_Adv', 'Eixo_Y_Adv'
        ]
        for col in colunas_adv_necessarias:
            if col not in df_final_pbi.columns:
                df_final_pbi[col] = None

        # Seleção final e ordenação de colunas
        colunas_exportar = [
            'Jogo_ID', 'Time', 'Adversario', 'Formacao',
            'Resultado', 'Acertos_Modelo', 'Total_Titulares',
            'meu_player_id', 'meu_player_nome', 'Posicao', 'Eixo_X', 'Eixo_Y',
            'score_matchup',
            'adversario_player_id', 'adversario_nome', 'adversario_posicao',
            'Eixo_X_Adv', 'Eixo_Y_Adv'
        ]

        df_final_pbi = df_final_pbi[colunas_exportar]
        df_final_pbi.to_csv('taticas_powerbi.csv', index=False, encoding='utf-8')
        print("✅ Arquivo 'taticas_powerbi.csv' exportado com sucesso para o Power BI!\n")

    print("=" * 50)


if __name__ == "__main__":
    rodar_avaliacao_global()
