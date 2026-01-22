def modulo_discovery():
    # --- IMPORTAÇÃO DAS BIBLIOTECAS NECESSÁRIAS ---

    import time
    import random
    import pandas as pd
    import os
    from curl_cffi import requests
    from datetime import datetime
    from conectar_google_api import salvar_dataframe

    # --- CONFIGURAÇÃO DA REQUISIÇÃO ---
    headers = {
        'authority': 'www.sofascore.com',
        'accept': '*/*',
        'referer': 'https://www.sofascore.com/pt/torneio/futebol/brazil/brasileirao-serie-a/325',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'x-requested-with': 'x',
        'cookie': 'cookie',
    }

    # --- CONFIGURAÇÃO DE ARQUIVOS ---
    id_da_planilha = "id"

    pasta_destino = 'caminho'
    nome_arquivo = 'jogadores_brasileirao_2025.csv'

    # Lista para acumular os erros
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

        lista_erros.append(erro_encontrado) # Adiciona na lista de erros
        print(f"Foi encontrado um erro com a mensagem {mensagem}. Já adicionei na lista de erros")


    def faz_requisicao(url, dados_sendo_baixados):
        """
        Tenta fazer a requisição X vezes antes de desistir

        Args:
            url: url da requisição
            dados_sendo_baixados: diferenciar qual arquivo estais fazendo a raspagem dos dados

        Returns:
            resposta_da_requisicao: A requisição foi um sucesso e o dado pôde ser raspado
            None: A requisição não funcionou e mesmo após 5 tentativas o algoritmo não retornou com sucesso.
            Nesse caso, ele desiste e parte para o próximo item.

        """

        tentativas = 5
        espera = 3

        for i in range(tentativas):
            try:
                resposta_da_requisicao = requests.get(url, headers=headers, impersonate="chrome")
                if resposta_da_requisicao.status_code == 200:
                    return resposta_da_requisicao
                else:
                    # Se não for 200, avisa e tenta de novo
                    print(f"Tentativa {i + 1}/{tentativas} falhou (Status {resposta_da_requisicao.status_code})...", end=" ")

            except Exception as erro_requsicao:
                print(f"Tentativa {i + 1}/{tentativas} falhou (Erro {erro_requsicao})...", end=" ")

            # Se ainda não é a última tentativa, espera um pouco
            if i < tentativas - 1:
                print(f"Aguardando {espera}s.")
                time.sleep(espera)
            else:
                print("Desistindo.")

        return None

    # --- 1. PEGAR OS TIMES ---
    print(">>> 1. Buscando times...")
    url_tabela = "https://www.sofascore.com/api/v1/unique-tournament/325/season/72034/standings/total"
    lista_times = []

    # Usa a nova função com retry
    requisicao_times = faz_requisicao(url_tabela, "Tabela de Times")

    if requisicao_times and requisicao_times.status_code == 200:
        try:
            linhas_da_tabela_do_brasileirao = requisicao_times.json()['standings'][0]['rows']
            lista_times = []
            for linha in linhas_da_tabela_do_brasileirao:
                # Acessa o dicionário 'team' aninhado
                informacao_do_time = linha['team']

                # Extrai os dados
                id_do_time = informacao_do_time['id']
                nome_do_time = informacao_do_time['name']

                # Cria a tupla e adiciona à lista
                tupla_do_time = (id_do_time, nome_do_time)
                lista_times.append(tupla_do_time)
            print(f"Sucesso! {len(lista_times)} times encontrados.")
        except Exception as erro_time:
            registrar_erro("Parse Times", f"Erro ao ler JSON: {erro_time}", url_tabela)
            exit()
    else:
        # Se chegou aqui, é porque falhou as 5 vezes
        mensagem_de_erro = f"Falha ao obter times após 5 tentativas."
        if requisicao_times:
            mensagem_de_erro += f" Último status: {requisicao_times.status_code}"

        registrar_erro("Busca de Times", mensagem_de_erro, url_tabela)

        # Salva o log e sai
        if lista_erros:
            df_erros = pd.DataFrame(lista_erros)
            salvar_dataframe(df_erros, id_da_planilha, "Log_Erros")
        exit()

    # --- 2. PEGAR OS JOGADORES ---
    print("\n>>> 2. Buscando elencos...")
    todos_jogadores = []

    for id_do_time, nome_do_time in lista_times:
        print(f"Processando {id_do_time}...", end="")

        url_elenco = f"https://www.sofascore.com/api/v1/team/{id_do_time}/players"

        requisicao_jogadores = faz_requisicao(url_elenco, f"Elenco {nome_do_time}")

        if requisicao_jogadores and requisicao_jogadores.status_code == 200:
            try:
                data = requisicao_jogadores.json()
                jogadores = data.get('players', [])

                for jogador in jogadores:
                    if 'player' in jogador:
                        jogador_normalizado = jogador['player']
                        jogador_normalizado['time_id'] = id_do_time
                        jogador_normalizado['time_nome'] = nome_do_time
                        todos_jogadores.append(jogador_normalizado)

                print(f" Ok! (+{len(jogadores)})")
            except Exception as e:
                print(f" Erro JSON: {e}")
                registrar_erro("Parse Jogadores", f"Erro JSON: {e}", nome_do_time)
        else:
            # Falhou as 5 tentativas para este time específico
            print(f"Falha total.")
            status_final = requisicao_jogadores.status_code if requisicao_jogadores else "Erro Conexão"
            registrar_erro("Busca Jogadores", f"Falha após 5 tentativas (Status: {status_final})", nome_do_time)

        # Pausa entre times (diferente da pausa de retry)
        pausa = random.uniform(1.5, 4.0)
        time.sleep(pausa)

    # --- 3. SALVAR OS DADOS (SUCESSO) ---
    if todos_jogadores:
        df = pd.json_normalize(todos_jogadores)

        colunas_desejadas = [
            'id', 'name', 'slug', 'position', 'positionsDetailed',
            'dateOfBirthTimestamp', 'height', 'preferredFoot',
            'country.name', 'time_id', 'time_nome'
        ]

        colunas_finais = []
        for coluna in colunas_desejadas:
            if coluna in df.columns:
                colunas_finais.append(coluna)

        df = df[colunas_finais]

        # Garante que a pasta existe
        os.makedirs(pasta_destino, exist_ok=True)
        caminho_completo = os.path.join(pasta_destino, nome_arquivo)

        # 1. Backup Local
        df.to_csv(caminho_completo, index=False, sep=';', encoding='utf-8-sig')
        print(f"CSV salvo em: {caminho_completo}")

        # 2. Google Sheets - Dados
        print("Enviando dados para o Google Sheets...")
        salvar_dataframe(df, id_da_planilha, "Geral_Times_Jogadores_2025")

    else:
        print("Nenhum dado coletado de jogadores.")
        registrar_erro("Finalização", "Nenhum jogador foi coletado após o loop dos times.")

    # --- 4. SALVAR OS ERROS (SE HOUVER) ---
    if lista_erros:
        print("\nSalvando LOG DE ERROS no Sheets...")
        df_erros = pd.DataFrame(lista_erros)
        salvar_dataframe(df_erros, id_da_planilha, "Erros_Info_Gerais_dos_Times_e_Jogadores_2025")
    else:
        print("\nExecução perfeita! Nenhum erro registrado.")