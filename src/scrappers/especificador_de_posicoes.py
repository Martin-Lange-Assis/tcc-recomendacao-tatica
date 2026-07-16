def especificador_posicoes():
    import time
    import random
    from datetime import datetime
    import pandas as pd
    from curl_cffi import requests
    from conectar_google_api import salvar_dataframe

    # Configurações de diretórios e arquivos locais
    PASTA_PROJETO = 'PASTA_PROJETO'
    ARQUIVO_ENTRADA = f'ARQUIVO_ENTRADA'
    ARQUIVO_SAIDA = f'ARQUIVO_SAIDA'

    # Credenciais e IDs de integração
    SPREADSHEET_ID = "SPREADSHEET_ID"

    # Cabeçalhos padrão para emulação de navegador
    headers = {
        'authority': 'www.sofascore.com',
        'accept': '*/*',
        'referer': 'https://www.sofascore.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',

    }

    # Armazenamento de logs em memória
    lista_erros = []

    def registrar_erro(contexto, mensagem, alvo="N/A"):
        """
        Registra exceções e falhas de execução em uma estrutura de dicionário para posterior exportação.
        """
        erro_encontrado = {
            "Data_Hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Contexto": contexto,
            "Alvo": alvo,
            "Mensagem_Erro": str(mensagem)
        }
        lista_erros.append(erro_encontrado)
        print(f"[LOG] Erro registrado: {mensagem} | Contexto: {contexto}")

    def finalizar_execucao(dados_coletados):
        """
        Realiza a persistência final dos dados coletados e logs gerados,
        salvando os resultados localmente em CSV e remotamente via Google Sheets.
        """
        print("\n[SISTEMA] Iniciando processo de salvamento de dados...")

        # Persistência de dados coletados
        if dados_coletados:
            df_out = pd.DataFrame(dados_coletados)
            df_out.to_csv(ARQUIVO_SAIDA, index=False, sep=';', encoding='utf-8-sig')
            print(f"[SISTEMA] CSV exportado com sucesso: {ARQUIVO_SAIDA}")

            if salvar_dataframe:
                print("[SISTEMA] Sincronizando Características Táticas com Google Sheets...")
                salvar_dataframe(df_out, SPREADSHEET_ID, "Características_Táticas")
        else:
            print("[SISTEMA] Nenhum dado tático disponível para exportação.")

        # Persistência de logs de erro
        if lista_erros and salvar_dataframe:
            print("[SISTEMA] Sincronizando Logs de Erro com Google Sheets...")
            df_erros = pd.DataFrame(lista_erros)
            salvar_dataframe(df_erros, SPREADSHEET_ID, "Log_Erros_Taticos")

    def faz_requisicao(url, headers_req, contexto="", tentativas=5, espera=3):
        """
        Executa chamadas HTTP com política de retry para mitigação de falhas de rede ou bloqueios temporários.
        """
        for i in range(tentativas):
            try:
                resp = requests.get(url, headers=headers_req, impersonate="chrome")

                # Códigos de retorno aceitos para processamento
                if resp.status_code in [200, 404, 403]:
                    return resp

                print(
                    f"[RETRY] {contexto} -> Tentativa {i + 1}/{tentativas} retornou Status {resp.status_code}. Aguardando {espera}s.")

            except Exception as e:
                # Captura falhas de resolução de DNS ou indisponibilidade de rede
                print(
                    f"[RETRY] {contexto} -> Tentativa {i + 1}/{tentativas} falhou (Erro de Conexão). Aguardando {espera}s.")

            if i < tentativas - 1:
                time.sleep(espera)

        return None

    # --- FLUXO PRINCIPAL DE EXECUÇÃO ---
    print(f"[SISTEMA] Carregando base de entrada: {ARQUIVO_ENTRADA}")
    try:
        df_jogadores = pd.read_csv(ARQUIVO_ENTRADA, sep=';')
    except FileNotFoundError:
        print("[ERRO FATAL] Arquivo CSV de entrada ausente. Verifique a execução do script anterior.")
        exit()

    dados_finais = []
    print("[SISTEMA] Iniciando coleta de características táticas...")

    try:
        # Iteração padrão mantida conforme solicitação
        for index, row in df_jogadores.iterrows():
            pid = row['id']
            nome = row['name']
            slug = row.get('slug', 'player')

            url = f"https://www.sofascore.com/api/v1/player/{pid}/characteristics"

            # Atualização dinâmica do Referer
            headers['referer'] = f"https://www.sofascore.com/pt/football/player/{slug}/{pid}"

            try:
                # Rate limiting (delay aleatório)
                time.sleep(random.uniform(1.0, 2.0))

                resp = faz_requisicao(url, headers, contexto=f"[{index}] {nome}")

                item = {
                    'player_id': pid,
                    'player_name': nome,
                    'posicoes_detalhadas': '',
                    'ids_fortes': '',
                    'ids_fracos': ''
                }

                if resp:
                    if resp.status_code == 200:
                        data = resp.json()

                        # Extração de Posições
                        lista_pos = data.get('positions', [])
                        item['posicoes_detalhadas'] = ", ".join(lista_pos)

                        # Extração de Pontos Fortes e Fracos
                        fortes = [str(x.get('type')) for x in data.get('positive', [])]
                        fracos = [str(x.get('type')) for x in data.get('negative', [])]

                        item['ids_fortes'] = ", ".join(fortes)
                        item['ids_fracos'] = ", ".join(fracos)

                        print(f"[{index}] {nome}: {item['posicoes_detalhadas']} | +{len(fortes)} | -{len(fracos)}")
                        dados_finais.append(item)

                    elif resp.status_code == 404:
                        print(f"[{index}] {nome}: Status 404 (Dados Inexistentes).")
                        dados_finais.append(item)

                    elif resp.status_code == 403:
                        registrar_erro("FATAL", "Bloqueio de acesso 403 (Forbidden) detectado", nome)
                        break

                    else:
                        registrar_erro("HTTP", f"Status não mapeado: {resp.status_code}", nome)

                else:
                    print(f"[{index}] {nome}: Timeout (Esgotamento de tentativas).")
                    registrar_erro("Conexão", "Falha de conexão persistente após retries", nome)

            except Exception as e:
                registrar_erro("Exceção Loop", str(e), nome)

    except KeyboardInterrupt:
        print("\n[SISTEMA] Processo interrompido pelo usuário (KeyboardInterrupt).")

    finally:
        # Bloco de segurança para garantia de persistência
        finalizar_execucao(dados_finais)