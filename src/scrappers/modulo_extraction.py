def modulo_extracao():
    from curl_cffi import requests
    import pandas as pd
    import time
    import random
    import os
    import json
    from datetime import datetime
    from src.scrappers.conectar_google_api import salvar_dataframe

    # Configurações de diretórios e parâmetros da API
    PASTA_PROJETO = 'PASTA_PROJETO'
    ARQUIVO_ENTRADA = f'ARQUIVO_ENTRADA'
    ARQUIVO_BRUTO = f'ARQUIVO_BRUTO'
    ARQUIVO_CSV_FINAL = f'ARQUIVO_CSV_FINAL'

    SPREADSHEET_ID = "SPREADSHEET_ID"

    ID_TORNEIO = 325
    ID_SEASON = 72034

    headers = {
        'authority': 'www.sofascore.com',
        'accept': '*/*',
        'referer': 'https://www.sofascore.com/pt/torneio/futebol/brazil/brasileirao-serie-a/325',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',

    }

    lista_erros = []

    def registrar_erro(tipo, mensagem, alvo):
        """Registra erros de execução com timestamp, contexto e mensagem detalhada."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{tipo}] {alvo}: {mensagem}")
        lista_erros.append({
            "Data_Hora": timestamp,
            "Script": "Extrator de Stats",
            "Contexto": tipo,
            "Alvo": alvo,
            "Mensagem": str(mensagem)
        })

    def salvar_logs_nuvem():
        """Exporta a lista de erros acumulados para uma planilha no Google Sheets."""
        if lista_erros and salvar_dataframe:
            print("\nExportando logs de erro para o Google Sheets...")
            df_erros = pd.DataFrame(lista_erros)
            salvar_dataframe(df_erros, SPREADSHEET_ID, "Log_Erros_Estatísticas_dos_Atletas")

    def gerar_csv_e_enviar_sheets():
        """Lê os dados brutos, organiza, salva em CSV e envia para a nuvem."""
        print("\n[Processamento Final] Gerando CSV e enviando para o Sheets...")
        if not os.path.exists(ARQUIVO_BRUTO):
            print("Aviso: Nenhum dado bruto encontrado para converter.")
            return

        try:
            # Lê o arquivo bruto montado durante a extração
            df = pd.read_json(ARQUIVO_BRUTO, lines=True, convert_dates=False)

            # Organiza as colunas principais na frente
            colunas_fixas = ['player_id', 'player_name', 'team_id']
            outras_colunas = [c for c in df.columns if c not in colunas_fixas]
            ordem_final = colunas_fixas + outras_colunas

            # Limpa, organiza e padroniza para texto
            df = df[ordem_final].fillna('').astype(str)

            # Salva o arquivo final CSV no computador
            df.to_csv(ARQUIVO_CSV_FINAL, index=False, sep=';', encoding='utf-8-sig')
            print(f"[Local] CSV salvo com sucesso em: {ARQUIVO_CSV_FINAL}")

            # Manda a tabela estruturada para o Sheets
            if salvar_dataframe:
                print(f"[Nuvem] Subindo dados para a aba 'tabela_final_stats_2025' no Sheets...")
                salvar_dataframe(df, SPREADSHEET_ID, "tabela_final_stats_2025")

        except Exception as e:
            print(f"Erro ao gerar o CSV ou enviar para o Sheets: {e}")

    def faz_requisicao(url, headers_req, tentativas=5, espera=3):
        """Realiza requisições HTTP com política de repetição (retry)."""
        for i in range(tentativas):
            try:
                resposta_da_requisicao = requests.get(url, headers=headers_req, impersonate="chrome")

                if resposta_da_requisicao.status_code in [200, 404, 403]:
                    return resposta_da_requisicao

                print(f"Tentativa {i + 1}/{tentativas} falhou (Status {resposta_da_requisicao.status_code})...",
                      end=" ")
            except Exception as e:
                print(f"Tentativa {i + 1}/{tentativas} falhou (Erro {e})...", end=" ")

            if i < tentativas - 1:
                time.sleep(espera)

        return None

    # Leitura do arquivo de entrada e verificação de integridade
    print(f"Lendo base de dados em: {ARQUIVO_ENTRADA}...")
    try:
        df_jogadores = pd.read_csv(ARQUIVO_ENTRADA, sep=';')
        if 'slug' not in df_jogadores.columns:
            registrar_erro("Setup", "Arquivo CSV não possui a coluna 'slug'", ARQUIVO_ENTRADA)
            return
    except FileNotFoundError:
        print("Erro Crítico: Arquivo de entrada não localizado.")
        return

    # Mapeamento do progresso atual para evitar duplicidade na extração
    ids_processados = set()
    if os.path.exists(ARQUIVO_BRUTO):
        with open(ARQUIVO_BRUTO, 'r', encoding='utf-8') as f:
            for linha in f:
                try:
                    dado = json.loads(linha)
                    ids_processados.add(dado['player_id'])
                except:
                    pass
        print(f"Retomando operação: {len(ids_processados)} jogadores já constam na base.")

    print("Iniciando extração de dados...")

    try:
        # Abre o arquivo bruto em modo 'append' (adiciona linhas novas sem apagar o resto)
        with open(ARQUIVO_BRUTO, 'a', encoding='utf-8') as f_saida:
            for index, row in df_jogadores.iterrows():
                player_id = row['id']
                player_name = row['name']
                player_slug = row['slug']

                # Pula jogadores que já foram raspados na execução anterior
                if player_id in ids_processados:
                    continue

                print(f"[{index + 1}/{len(df_jogadores)}] Extraindo {player_name}...", end="")

                url_perfil_fake = f"https://www.sofascore.com/pt/football/player/{player_slug}/{player_id}"
                headers['referer'] = url_perfil_fake

                url_stats = f"https://www.sofascore.com/api/v1/player/{player_id}/unique-tournament/{ID_TORNEIO}/season/{ID_SEASON}/statistics/overall"

                resp = faz_requisicao(url_stats, headers, tentativas=5, espera=3)

                if resp:
                    if resp.status_code == 200:
                        try:
                            data = resp.json()
                            stats = data.get('statistics', {})

                            # Inserção de metadados
                            stats['player_id'] = player_id
                            stats['player_name'] = player_name
                            stats['team_id'] = row['time_id']

                            # Gravação no arquivo temporário bruto
                            json.dump(stats, f_saida, ensure_ascii=False)
                            f_saida.write('\n')
                            f_saida.flush()
                            print(" Sucesso.")
                        except Exception as e:
                            print(f" Erro de parsing JSON: {e}")
                            registrar_erro("Parse JSON", f"Erro na leitura do JSON: {e}", player_name)

                    elif resp.status_code == 404:
                        print(" Dados não encontrados (Erro 404).")

                    elif resp.status_code == 403:
                        msg = "Acesso negado (Erro 403): Bloqueio detectado pela API. Atualize o Cookie."
                        registrar_erro("Erro Fatal", msg, player_name)
                        break

                    else:
                        registrar_erro("Erro HTTP", f"Falha esgotando as 5 tentativas. Status {resp.status_code}",
                                       player_name)

                else:
                    print(" Falha de conexão.")
                    registrar_erro("Conexão", "Esgotamento do limite de tentativas de requisição", player_name)

                time.sleep(random.uniform(2.0, 4.0))

    except KeyboardInterrupt:
        print("\nProcesso interrompido manualmente pelo usuário.")

    finally:
        print("\nIniciando rotina de encerramento...")

        # Aqui o script faz tudo o que o conversor fazia, direto no pipeline principal:
        gerar_csv_e_enviar_sheets()

        time.sleep(2)
        salvar_logs_nuvem()
        print("Script finalizado com sucesso.")