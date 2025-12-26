def modulo_extracao():
    from curl_cffi import requests
    import pandas as pd
    import time
    import random
    import os
    import json
    from datetime import datetime
    from conectar_google_api import salvar_dataframe
    from conversor_jsonl import processar_conversao

    # --- CONFIGURAÇÃO ---
    ARQUIVO_ENTRADA = 'C:/Users/marti/PycharmProjects/tcc-recomendacao-tatica/data/raw/jogadores_brasileirao_2025.csv'
    ARQUIVO_SAIDA = 'C:/Users/marti/PycharmProjects/tcc-recomendacao-tatica/data/raw/dados_brutos_stats.jsonl'
    SPREADSHEET_ID = "1hiO9C0AKr3ALx74B2pvXAMKsWcWySbkm2Wi3x2Xu2Yg"

    ID_TORNEIO = 325
    ID_SEASON = 72034

    headers = {
        'authority': 'www.sofascore.com',
        'accept': '*/*',
        'referer': 'https://www.sofascore.com/pt/torneio/futebol/brazil/brasileirao-serie-a/325',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'x-requested-with': '6213f1',
        'cookie': 'cookie',
    }

    # --- SISTEMA DE LOGS ---
    lista_erros = []

    def registrar_erro(tipo, mensagem, alvo):
        """
        Padroniza o registro de erros

        Args:
            tipo (str): tipo do erro.
            mensagem (str): mensagem do erro.
            alvo (str, opcional): alvo do erro. Por padrão pode ser "N/A".
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"⚠️ [{tipo}] {alvo}: {mensagem}")
        lista_erros.append({
            "Data_Hora": timestamp,
            "Script": "Extrator de Stats",
            "Contexto": tipo,
            "Alvo": alvo,
            "Mensagem": str(mensagem)
        })

    def salvar_logs_nuvem():
        """Envia os erros acumulados para o Google Sheets"""
        if lista_erros and salvar_dataframe:
            print("\nEnviando relatório de erros para o Google Sheets...")
            df_erros = pd.DataFrame(lista_erros)
            salvar_dataframe(df_erros, SPREADSHEET_ID, "Log_Erros_Estatísticas_dos_Atletas")

    # --- NOVO: FUNÇÃO DE RETRY (Blindada) ---
    def faz_requisicao(url, headers_req, tentativas=5, espera=3):
        """
        Tenta fazer a requisição X vezes antes de desistir

        Args:
            url: url da requisição
            headers_req: cabeçalhos da requisição
            tentativas: nº de vezes que o algoritmo tentará requisitar antes de desistir
            espera: o algoritmo espera 3 segundos antes de fazer uma nova tentativa

        Returns:
            resposta_da_requisicao: A requisição foi um sucesso e o dado pôde ser raspado
            None: A requisição não funcionou e mesmo após 5 tentativas o algoritmo não retornou com sucesso.
            Nesse caso, ele desiste e parte para o próximo item.

        """

        for i in range(tentativas):
            try:
                # Importante: Passamos os headers dinâmicos aqui
                resposta_da_requisicao = requests.get(url, headers=headers_req, impersonate="chrome")

                # Se for sucesso (200) ou Não Encontrado (404 - erro definitivo), retorna logo
                if resposta_da_requisicao.status_code in [200, 404, 403]:
                    return resposta_da_requisicao

                # Se for erro 5xx ou outro, tenta de novo
                print(f"Tentativa {i + 1}/{tentativas} falhou (Status {resposta_da_requisicao.status_code})...", end=" ")

            except Exception as e:
                print(f"Tentativa {i + 1}/{tentativas} falhou (Erro {e})...", end=" ")

            # Aguarda antes da próxima, menos na última
            if i < tentativas - 1:
                time.sleep(espera)

        # Se saiu do loop, é porque falhou todas
        return None

        # --- 1. CARREGAR OS JOGADORES ---

    print(f"Lendo {ARQUIVO_ENTRADA}...")
    try:
        df_jogadores = pd.read_csv(ARQUIVO_ENTRADA, sep=';')
        if 'slug' not in df_jogadores.columns:
            registrar_erro("Setup", "CSV sem a coluna 'slug'", ARQUIVO_ENTRADA)
            exit()
    except FileNotFoundError:
        print("Erro: Arquivo de entrada não encontrado.")
        exit()

    # Descobrir onde paramos
    ids_processados = set()
    if os.path.exists(ARQUIVO_SAIDA):
        with open(ARQUIVO_SAIDA, 'r', encoding='utf-8') as f:
            for linha in f:
                try:
                    dado = json.loads(linha)
                    ids_processados.add(dado['player_id'])
                except:
                    pass
        print(f"Retomando: {len(ids_processados)} jogadores já coletados.")

    # --- 2. LOOP DE MINERAÇÃO ---
    print(">>> Iniciando coleta...")

    try:
        with open(ARQUIVO_SAIDA, 'a', encoding='utf-8') as f_saida:
            for index, row in df_jogadores.iterrows():
                player_id = row['id']
                player_name = row['name']
                player_slug = row['slug']

                if player_id in ids_processados:
                    continue

                print(f"[{index + 1}/{len(df_jogadores)}] {player_name}...", end="")

                # Referer Dinâmico (Muda a cada jogador)
                url_perfil_fake = f"https://www.sofascore.com/pt/football/player/{player_slug}/{player_id}"
                headers['referer'] = url_perfil_fake

                url_stats = f"https://www.sofascore.com/api/v1/player/{player_id}/unique-tournament/{ID_TORNEIO}/season/{ID_SEASON}/statistics/overall"

                resp = faz_requisicao(url_stats, headers, tentativas=5, espera=3)

                if resp:  # Se houve resposta (mesmo que erro)
                    if resp.status_code == 200:
                        try:
                            data = resp.json()
                            stats = data.get('statistics', {})

                            # Metadados
                            stats['player_id'] = player_id
                            stats['player_name'] = player_name
                            stats['team_id'] = row['time_id']

                            # Salva JSONL Local
                            json.dump(stats, f_saida, ensure_ascii=False)
                            f_saida.write('\n')
                            f_saida.flush()
                            print(" Sucesso!")
                        except Exception as e:
                            print(f" Erro JSON: {e}")
                            registrar_erro("Parse JSON", f"Erro ao ler JSON: {e}", player_name)

                    elif resp.status_code == 404:
                        print(" Sem dados (404).")
                        # Opcional: talvez ainda seja feito um registrar_erro("Aviso 404", "Sem dados na temporada", player_name)

                    elif resp.status_code == 403:
                        msg = "ERRO 403: Bloqueio detectado! Pare e troque o Cookie."
                        registrar_erro("FATAL", msg, player_name)
                        break  # BLOQUEIO É FATAL -> PARE O SCRIPT

                    else:
                        registrar_erro("HTTP Error", f"Falha após 5 tentativas. Status {resp.status_code}", player_name)

                else:
                    # Se resp for None, significa que deu exceção nas 5 tentativas
                    print(" Falha Total.")
                    registrar_erro("Conexão", "Falha de conexão após 5 tentativas", player_name)

                time.sleep(random.uniform(2.0, 4.0))

    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário.")

    finally:
        # --- BLOCO FINAL ---
        print("\nProcessando conversão final...")
        try:
            processar_conversao()
        except Exception as e:
            print(f"Erro na conversão automática: {e}")

        time.sleep(5)
        salvar_logs_nuvem()
        print("Script finalizado.")