def modulo_discovery():
    import time
    import random
    import pandas as pd
    import os
    from curl_cffi import requests
    from datetime import datetime
    from src.scrappers.conectar_google_api import salvar_dataframe

    headers = {
        'authority': 'www.sofascore.com',
        'accept': '*/*',
        'referer': 'https://www.sofascore.com/pt/torneio/futebol/brazil/brasileirao-serie-a/325',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',

    }

    id_da_planilha = "id_da_planilha"
    pasta_destino = "pasta_destino"
    nome_arquivo = 'jogadores_brasileirao_2025.csv'

    lista_erros = []

    def registrar_erro(contexto, mensagem, alvo="N/A"):
        """Registra falhas de execução para posterior auditoria no Google Sheets."""
        erro_encontrado = {
            "Data_Hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Contexto": contexto,
            "Alvo": alvo,
            "Mensagem_Erro": str(mensagem)
        }

        lista_erros.append(erro_encontrado)
        print(f"Erro registrado [{contexto}]: {mensagem}")

    def faz_requisicao(url, dados_sendo_baixados):
        """Gerencia as requisições HTTP com política de retry para contornar instabilidades."""
        tentativas = 5
        espera = 3

        for i in range(tentativas):
            try:
                resposta_da_requisicao = requests.get(url, headers=headers, impersonate="chrome")
                if resposta_da_requisicao.status_code == 200:
                    return resposta_da_requisicao

                print(f"Tentativa {i + 1}/{tentativas} falhou (Status {resposta_da_requisicao.status_code})...",
                      end=" ")

            except Exception as erro_requsicao:
                print(f"Tentativa {i + 1}/{tentativas} falhou (Erro {erro_requsicao})...", end=" ")

            if i < tentativas - 1:
                print(f"Aguardando {espera}s.")
                time.sleep(espera)
            else:
                print("Desistindo da requisição.")

        return None

    print(">>> Iniciando busca de times...")
    url_tabela = "https://www.sofascore.com/api/v1/unique-tournament/325/season/72034/standings/total"
    lista_times = []
    requisicao_times = faz_requisicao(url_tabela, "Tabela de Times")

    if requisicao_times and requisicao_times.status_code == 200:
        try:
            linhas_da_tabela_do_brasileirao = requisicao_times.json()['standings'][0]['rows']

            for linha in linhas_da_tabela_do_brasileirao:
                informacao_do_time = linha['team']
                id_do_time = informacao_do_time['id']
                nome_do_time = informacao_do_time['name']
                lista_times.append((id_do_time, nome_do_time))

            print(f"Sucesso! {len(lista_times)} times encontrados.")
        except Exception as erro_time:
            registrar_erro("Parse Times", f"Erro ao ler JSON: {erro_time}", url_tabela)
            exit()
    else:
        mensagem_de_erro = f"Falha ao obter times após 5 tentativas. Último status: {requisicao_times.status_code if requisicao_times else 'N/A'}"
        registrar_erro("Busca de Times", mensagem_de_erro, url_tabela)

        if lista_erros:
            df_erros = pd.DataFrame(lista_erros)
            salvar_dataframe(df_erros, id_da_planilha, "Log_Erros")
        exit()

    print("\n>>> Iniciando busca de elencos...")
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

                print(f" Ok! (+{len(jogadores)} jogadores)")
            except Exception as e:
                print(f" Erro JSON: {e}")
                registrar_erro("Parse Jogadores", f"Erro JSON: {e}", nome_do_time)
        else:
            print(f" Falha total na coleta.")
            status_final = requisicao_jogadores.status_code if requisicao_jogadores else "Erro Conexão"
            registrar_erro("Busca Jogadores", f"Falha após 5 tentativas (Status: {status_final})", nome_do_time)

        pausa = random.uniform(1.5, 4.0)
        time.sleep(pausa)

    print("\n>>> Salvando resultados...")
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

        os.makedirs(pasta_destino, exist_ok=True)
        caminho_completo = os.path.join(pasta_destino, nome_arquivo)

        df.to_csv(caminho_completo, index=False, sep=';', encoding='utf-8-sig')
        print(f"CSV local salvo em: {caminho_completo}")

        print("Enviando dados consolidados para o Google Sheets...")
        salvar_dataframe(df, id_da_planilha, "Geral_Times_Jogadores_2025")

    else:
        print("Atenção: Nenhum dado de jogador foi coletado.")
        registrar_erro("Finalização", "Loop concluído sem captura de jogadores.")

    if lista_erros:
        print("\nExportando LOG DE ERROS para o Sheets...")
        df_erros = pd.DataFrame(lista_erros)
        salvar_dataframe(df_erros, id_da_planilha, "Erros_Info_Gerais_dos_Times_e_Jogadores_2025")
    else:
        print("\nExecução finalizada sem erros!")