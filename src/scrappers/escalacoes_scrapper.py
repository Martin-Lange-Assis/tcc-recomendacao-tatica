from curl_cffi import requests
import time
from src.scrappers import conectar_google_api

def coletar_escalacoes_sofascore(id_planilha="id_planilha"):
    """
    Coleta dados de jogos e escalações do SofaScore e salva em uma planilha do Google Sheets.
    """

    # Verifica o último ID processado para retomar de onde parou em caso de interrupção
    ultimo_id_processado = conectar_google_api.ler_ultimo_id(id_planilha)

    # Inicializa o cabeçalho caso seja a primeira execução do script
    if ultimo_id_processado == 0:
        cabecalho = ["rodada", "jogo_id", "time_lado", "resultado_time", "formacao", "player_id", "nome_jogador",
                     "posicao_jogo", "camisa", "status_jogo"]
        conectar_google_api.adicionar_linha(id_planilha, "Escalacoes", cabecalho)
        print("Cabeçalho adicionado na aba 'Escalacoes'.")

    headers = {
        'accept': '*/*',
        'accept-language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0',

    }

    # Itera sobre as rodadas desejadas
    for rodada in range(20, 39):
        print(f"\n--- Iniciando Rodada {rodada} ---")

        url_rodada = f"https://www.sofascore.com/api/v1/unique-tournament/325/season/72034/events/round/{rodada}"

        try:
            resposta_rodada = requests.get(url_rodada, headers=headers, impersonate="chrome")
            resposta_rodada.raise_for_status()
            dados_rodada = resposta_rodada.json()

            # Mapeia os jogos da rodada e seus respectivos placares
            jogos_da_rodada = {}
            for evento in dados_rodada.get('events', []):
                jogo_id = evento['id']
                home_score = evento.get('homeScore', {}).get('current', 0)
                away_score = evento.get('awayScore', {}).get('current', 0)
                jogos_da_rodada[jogo_id] = {'home_score': home_score, 'away_score': away_score}

        except Exception as e:
            print(f"Erro ao buscar a lista da rodada {rodada}: {e}")
            time.sleep(5)
            continue

        # Processa cada jogo retornado na rodada
        for jogo_id, placar in jogos_da_rodada.items():
            if ultimo_id_processado > 0 and jogo_id <= ultimo_id_processado:
                print(f"Jogo {jogo_id} já processado anteriormente. Pulando...")
                continue

            url_escalacao = f"https://www.sofascore.com/api/v1/event/{jogo_id}/lineups"
            sucesso_jogo = False
            erro_final = ""

            gols_casa = placar['home_score']
            gols_fora = placar['away_score']

            # Mecanismo de tentativas em caso de instabilidade da API
            for tentativa in range(1, 6):
                try:
                    print(f"Buscando jogo {jogo_id} (Tentativa {tentativa}/5)...")
                    resposta_escalacao = requests.get(url_escalacao, headers=headers, impersonate="chrome")
                    resposta_escalacao.raise_for_status()
                    dados_escalacao = resposta_escalacao.json()

                    linhas_do_jogo = []

                    # Itera sobre os dados do time da casa (home) e visitante (away)
                    for time_lado in ['home', 'away']:
                        time_dados = dados_escalacao.get(time_lado, {})

                        # Determina o resultado da partida para o time atual
                        if gols_casa == gols_fora:
                            resultado_time = "Empate"
                        elif time_lado == 'home':
                            resultado_time = "Vitória" if gols_casa > gols_fora else "Derrota"
                        else:
                            resultado_time = "Vitória" if gols_fora > gols_casa else "Derrota"

                        formacao_time = time_dados.get('formation', 'N/A')
                        jogadores = time_dados.get('players', [])

                        # Processa os jogadores relacionados para a partida
                        for info in jogadores:
                            jogador = info.get('player', {})
                            player_id_sofa = jogador.get('id', 0)
                            nome = jogador.get('name', 'N/A')
                            posicao = jogador.get('position', 'N/A')
                            camisa = info.get('shirtNumber', 'N/A')

                            comecou_no_banco = info.get('substitute', False)
                            minutos_jogados = info.get('statistics', {}).get('minutesPlayed', 0)

                            if not comecou_no_banco:
                                status = "Titular"
                            elif comecou_no_banco and minutos_jogados > 0:
                                status = "Entrou no Jogo"
                            else:
                                status = "Ficou no Banco"

                            linha = [rodada, jogo_id, time_lado, resultado_time, formacao_time, player_id_sofa, nome,
                                     posicao, camisa, status]
                            linhas_do_jogo.append(linha)

                        desfalques = time_dados.get('missingPlayers', [])

                        # Processa os desfalques confirmados
                        for info in desfalques:
                            jogador = info.get('player', {})
                            player_id_sofa = jogador.get('id', 0)
                            nome = jogador.get('name', 'N/A')
                            posicao = jogador.get('position', 'N/A')
                            camisa = "N/A"
                            status = "Desfalque"

                            linha = [rodada, jogo_id, time_lado, resultado_time, formacao_time, player_id_sofa, nome,
                                     posicao, camisa, status]
                            linhas_do_jogo.append(linha)

                    # Salva os dados processados e atualiza o estado de controle
                    conectar_google_api.adicionar_multiplas_linhas(id_planilha, "Escalacoes", linhas_do_jogo)
                    conectar_google_api.atualizar_ultimo_id(id_planilha, jogo_id)

                    print(
                        f"Sucesso no jogo {jogo_id}! Formações: {dados_escalacao.get('home', {}).get('formation')} x {dados_escalacao.get('away', {}).get('formation')} | Placar: {gols_casa}x{gols_fora}")
                    sucesso_jogo = True
                    break

                except Exception as e:
                    erro_final = str(e)
                    print(f"Tentativa {tentativa} falhou para o jogo {jogo_id}: {erro_final}")
                    time.sleep(10)

            # Registra o erro se todas as tentativas falharem
            if not sucesso_jogo:
                conectar_google_api.adicionar_linha(id_planilha, "Erros_Escalacoes",
                                                    [rodada, jogo_id, url_escalacao, erro_final])
                print(f"Jogo {jogo_id} falhou após 5 tentativas. Registrado em Erros.")

            time.sleep(5)