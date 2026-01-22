def especificador_posicoes():
    from curl_cffi import requests
    import pandas as pd
    import time
    import random
    from datetime import datetime
    from conectar_google_api import salvar_dataframe

    # --- CONFIGURAÇÃO ---
    PASTA_PROJETO = 'caminho'
    ARQUIVO_ENTRADA = f'{PASTA_PROJETO}/jogadores_brasileirao_2025.csv'
    ARQUIVO_SAIDA = f'{PASTA_PROJETO}/jogadores_posicoes_detalhadas.csv'

    # Configure seu ID aqui
    SPREADSHEET_ID = "id_planilha"

    headers = {
        'authority': 'www.sofascore.com',
        'accept': '*/*',
        'referer': 'https://www.sofascore.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'x-requested-with': 'xxx',
        'cookie': 'cookie',
    }

    # --- SISTEMA DE LOGS ---
    lista_erros = []

    def registrar_erro(contexto, mensagem, alvo="N/A"):
        """
        Padroniza o registro de erros

        Args:
            contexto (str): contexto do erro.
            mensagem (str): mensagem do erro.
            alvo (str, opcional): alvo do erro. Por padrão pode ser "N/A".
        """

        erro_encontrado = {
            "Data_Hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Contexto": contexto,
            "Alvo": alvo,
            "Mensagem_Erro": str(mensagem)
        }

        lista_erros.append(erro_encontrado)  # Adiciona na lista de erros
        print(f"Foi encontrado um erro com a mensagem {mensagem}. Já adicionei na lista de erros")

    def finalizar_execucao(dados_coletados):
        """
        Salva tudo (Dados e Logs) localmente e na nuvem para finalizar a execução

        Args:
            dados_coletados: lista contendo os dados táticos que foram raspados e processados

        Returns:
            None: A função não retorna valores, apenas realiza a persistência dos dados (CSV e Google Sheets) e logs de erro
        """
        print("\nFinalizando e salvando dados...")

        # 1. Salvar Dados (CSV Local)
        if dados_coletados:
            df_out = pd.DataFrame(dados_coletados)
            df_out.to_csv(ARQUIVO_SAIDA, index=False, sep=';', encoding='utf-8-sig')
            print(f"CSV Tático salvo: {ARQUIVO_SAIDA}")

            # 2. Salvar Dados (Nuvem)
            if salvar_dataframe:
                print("Enviando Características para o Google Sheets...")
                salvar_dataframe(df_out, SPREADSHEET_ID, "Características_Táticas")
        else:
            print("Nenhum dado tático coletado.")

        # 3. Salvar Logs de Erro (Nuvem)
        if lista_erros and salvar_dataframe:
            print("Enviando Logs de erro...")
            df_erros = pd.DataFrame(lista_erros)
            salvar_dataframe(df_erros, SPREADSHEET_ID, "Log_Erros_Taticos")

    def faz_requisicao(url, headers_req, contexto="", tentativas=5, espera=3):
        """
        Tenta fazer a requisição X vezes antes de desistir

        Args:
            url: url da requisição
            headers_req: cabeçalhos para simular o navegador (ex: user-agent)
            contexto: texto para identificar nos logs o que está sendo baixado (ex: nome do jogador)
            tentativas: número máximo de vezes que o código tentará conectar
            espera: tempo em segundos de aguardo entre uma falha e a próxima tentativa

        Returns:
            resp: O objeto da resposta se o status for 200 (sucesso), 404 (não encontrado) ou 403 (bloqueado)
            None: A requisição não funcionou e mesmo após as tentativas o algoritmo não retornou.
            Nesse caso, ele desiste da URL.
        """
        for i in range(tentativas):
            try:
                resp = requests.get(url, headers=headers_req, impersonate="chrome")

                # Se for sucesso (200), 404 (não existe) ou 403 (bloqueio), retorna
                if resp.status_code in [200, 404, 403]:
                    return resp

                # Se falhar, pula linha e avisa qual jogador deu erro
                print(
                    f"\n{contexto} -> Tentativa {i + 1}/{tentativas} falhou (Status {resp.status_code})... Tentando de novo em {espera}s.")

            except Exception as e:
                # Tratamento para erro de DNS/Internet (seu erro curl 6)
                print(
                    f"\n{contexto} -> Tentativa {i + 1}/{tentativas} falhou (Erro de Conexão). Aguardando {espera}s...")

            if i < tentativas - 1:
                time.sleep(espera)

        return None  # Falhou todas

    # --- INÍCIO ---
    print(f"Lendo {ARQUIVO_ENTRADA}...")
    try:
        df_jogadores = pd.read_csv(ARQUIVO_ENTRADA, sep=';')
    except FileNotFoundError:
        print("Erro: CSV de entrada não encontrado. Rode o primeiro script antes.")
        exit()

    dados_finais = []
    print(">>> Buscando características táticas...")

    try:
        for index, row in df_jogadores.iterrows():
            pid = row['id']
            nome = row['name']
            slug = row.get('slug', 'player')

            url = f"https://www.sofascore.com/api/v1/player/{pid}/characteristics"

            # Header Referer dinâmico
            headers['referer'] = f"https://www.sofascore.com/pt/football/player/{slug}/{pid}"

            try:
                # Pausa antes de começar
                time.sleep(random.uniform(1.0, 2.0))

                # --- CHAMADA COM RETRY ---
                resp = faz_requisicao(url, headers, contexto=f"[{index}] {nome}")

                item = {
                    'player_id': pid,
                    'player_name': nome,
                    'posicoes_detalhadas': '',
                    'ids_fortes': '',
                    'ids_fracos': ''
                }

                if resp:  # Se houve resposta
                    if resp.status_code == 200:
                        data = resp.json()

                        # 1. Posições
                        lista_pos = data.get('positions', [])
                        item['posicoes_detalhadas'] = ", ".join(lista_pos)

                        # 2. Pontos Fortes e Fracos
                        fortes = []
                        positivos = data.get('positive', [])

                        for x in positivos:
                            valor = x.get('type')
                            fortes.append(str(valor))

                        fracos = []
                        negativos = data.get('negative', [])

                        for x in negativos:
                            valor = x.get('type')
                            fracos.append(str(valor))

                        item['ids_fortes'] = ", ".join(fortes)
                        item['ids_fracos'] = ", ".join(fracos)

                        print(f"[{index}] {nome}: {item['posicoes_detalhadas']} | +{len(fortes)} | -{len(fracos)}")
                        dados_finais.append(item)

                    elif resp.status_code == 404:
                        print(f"[{index}] {nome}: Sem dados táticos (404).")
                        dados_finais.append(item)  # Salva vazio

                    elif resp.status_code == 403:
                        registrar_erro("FATAL", "Bloqueio 403 detectado", nome)
                        break  # Encerra o loop para salvar o que já temos

                    else:
                        registrar_erro("HTTP", f"Status {resp.status_code}", nome)

                else:
                    # Falhou as 5 tentativas
                    print(f"[{index}] {nome}: Falha Total de Conexão.")
                    registrar_erro("Conexão", "Falha após 5 tentativas", nome)

            except Exception as e:
                registrar_erro("Exceção Loop", str(e), nome)

    except KeyboardInterrupt:
        print("\nInterrupção manual.")

    finally:
        # Garante que salva tudo mesmo se der erro no meio
        finalizar_execucao(dados_finais)