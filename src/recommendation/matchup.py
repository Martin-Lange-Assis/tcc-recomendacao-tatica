# src/recommendation/matchup.py
import pandas as pd
from src.domain.formacoes import FORMACOES
from src.database import repository
from src.recommendation import preprocessing, engine, solvers


def calcular_matchup(time_casa: str, time_fora: str, jogo_id: int, repository,
                     escalacao_adversario: list = None,
                     formacao: str = None,
                     adversario_improvisado: dict = None,
                     jogadores_indisponiveis: list = None) -> pd.DataFrame:
    """
    Orquestra o pipeline de recomendação tática, integrando busca de dados,
    pré-processamento, cálculo de similaridade e otimização da escalação.
    """

    if formacao is not None and formacao not in FORMACOES:
        raise ValueError(
            f"Formação '{formacao}' não reconhecida. "
            f"Opções disponíveis: {list(FORMACOES.keys())}"
        )

    # 1. Busca de Dados
    df_jogadores = repository.buscar_dados_times(time_casa, time_fora)
    df_posicoes_ref = repository.buscar_posicoes_alvo_arquetipos()

    if df_jogadores.empty:
        print(f"[ERRO] Times não encontrados. Verifique os nomes: '{time_casa}' e '{time_fora}'")
        return pd.DataFrame()

    posicao_alvo_por_arquetipo = preprocessing.processar_posicoes_alvo(df_posicoes_ref)

    # 2. Pré-processamento
    df_casa, df_fora = preprocessing.preparar_dados_matchup(
        df_jogadores=df_jogadores,
        time_casa=time_casa,
        time_fora=time_fora,
        jogo_id=jogo_id,
        repository=repository,
        escalacao_adversario=escalacao_adversario,
        jogadores_indisponiveis=jogadores_indisponiveis,
        adversario_improvisado=adversario_improvisado
    )

    if df_casa.empty or df_fora.empty:
        print("[ERRO] Elenco insuficiente após a aplicação dos filtros.")
        return pd.DataFrame()

    # 3. Motor de Similaridade
    df_rec = engine.calcular_matriz_similaridade(
        df_casa=df_casa,
        df_fora=df_fora,
        posicao_alvo_por_arquetipo=posicao_alvo_por_arquetipo
    )

    if df_rec.empty:
        print("[ERRO] Nenhuma recomendação gerada pelo motor de similaridade.")
        return pd.DataFrame()

    # 4. Otimização Global e Resolução de Escalação
    if formacao:
        formacao_slots = FORMACOES[formacao]
        df_titulares = solvers.resolver_com_formacao_ilp(
            df_rec=df_rec,
            df_casa=df_casa,
            df_fora=df_fora,
            formacao_slots=formacao_slots
        )

        # Pós-processamento: Sinalização de improvisações táticas
        if not df_titulares.empty:
            posicao_primaria_por_slot = {
                item['slot']: item['posicoes'][0]
                for item in formacao_slots
            }

            if 'meu_slot' in df_titulares.columns:
                def marcar_improvisacao(row):
                    posicao_real = row['minha_posicao']
                    slot_ocupado = row['meu_slot']

                    if slot_ocupado in posicao_primaria_por_slot:
                        if posicao_real != posicao_primaria_por_slot[slot_ocupado]:
                            return f"{posicao_real} (Improvisado de {slot_ocupado.replace('_', ' ').title()})"
                    return posicao_real

                df_titulares['minha_posicao'] = df_titulares.apply(marcar_improvisacao, axis=1)
            else:
                print("[AVISO] Coluna 'meu_slot' indisponível. Validação de improvisações ignorada.")
    else:
        df_titulares = solvers.resolver_sem_formacao_hungarian(
            df_rec=df_rec,
            df_casa=df_casa
        )

    return df_titulares


if __name__ == "__main__":
    JOGO_ANALISADO_ID = 13472781
    MEU_TIME_JOGA_EM = 'home'
    FORMACAO_ESCOLHIDA = '4-2-3-1'
    # para testar o hungaro troque por None

    contexto = repository.buscar_contexto_partida(jogo_id=JOGO_ANALISADO_ID, meu_lado=MEU_TIME_JOGA_EM)

    if not contexto:
        print(f"[ERRO] Partida não encontrada.")
        exit()

    # Incorporação de improvisações manuais (scouting visual) com as detectadas via banco
    improvisacoes_manuais = {}
    improvisacoes_finais = contexto['improvisacoes'].copy()
    improvisacoes_finais.update(improvisacoes_manuais)

    if improvisacoes_finais:
        print(f"Improvisações mapeadas no adversário: {list(improvisacoes_finais.keys())}")

    print("\nIniciando cálculo de matchup...")

    df_resultado = calcular_matchup(
        time_casa=contexto['time_casa'],
        time_fora=contexto['time_fora'],
        jogo_id=JOGO_ANALISADO_ID,
        repository=repository,
        escalacao_adversario=contexto['jogadores_adversarios'],
        formacao=FORMACAO_ESCOLHIDA,
        adversario_improvisado=improvisacoes_finais,
        jogadores_indisponiveis=contexto['desfalques']
    )

    if not df_resultado.empty:
        print("\n========== 11 TITULARES RECOMENDADOS ==========")

        colunas_finais = [
            'meu_player_nome', 'minha_posicao',
            'adversario_nome', 'adversario_posicao',
            'score_matchup'
        ]

        colunas_exibicao = [col for col in colunas_finais if col in df_resultado.columns]
        print(df_resultado[colunas_exibicao].to_string(index=False))
        print("===============================================")

        # Validação do modelo: Titulares Recomendados vs Titulares Reais
        titulares_modelo = set(df_resultado['meu_player_nome'].tolist())
        titulares_reais = set(contexto['meus_titulares_reais'])

        acertos = titulares_modelo.intersection(titulares_reais)

        if acertos:
            print(f"Acertos exatos: {', '.join(acertos)}")

        erros_modelo = titulares_modelo - titulares_reais
        if erros_modelo:
            print(f"Escalados pelo modelo, mas fora na vida real: {', '.join(erros_modelo)}")

        escolhas_treinador = titulares_reais - titulares_modelo
        if escolhas_treinador:
            print(f"Jogaram na vida real, barrados pelo modelo: {', '.join(escolhas_treinador)}")
        print("============================================")

    else:
        print("\n[FALHA] Não foi possível gerar a escalação.")
