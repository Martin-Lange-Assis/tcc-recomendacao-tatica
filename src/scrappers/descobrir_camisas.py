import time
import random
import pandas as pd
import os
from curl_cffi import requests
from datetime import datetime
from src.scrappers.conectar_google_api import salvar_dataframe


def modulo_discovery_camisas_filtrado():
    # Configurações de cabeçalho para simular navegação real na requisição
    headers = {
        'authority': 'www.sofascore.com',
        'accept': '*/*',
        'referer': 'https://www.sofascore.com/pt/torneio/futebol/brazil/brasileirao-serie-a/325',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',

    }

    # Definição de diretórios e links das planilhas
    id_da_planilha = "id_da_planilha"
    pasta_destino = "pasta_destino"
    nome_arquivo_saida = 'nome_arquivo_saida'
    url_ids_base = "url_ids_base"

    lista_erros = []

    # Helper para registro e armazenamento de falhas durante as requisições
    def registrar_erro(contexto, mensagem, alvo="N/A"):
        erro_encontrado = {
            "Data_Hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Contexto": contexto,
            "Alvo": alvo,
            "Mensagem_Erro": str(mensagem)
        }
        lista_erros.append(erro_encontrado)
        print(f"Erro registrado: {mensagem}")

    # Helper para lidar com instabilidades na API e tentativas de reconexão
    def faz_requisicao(url, dados_sendo_baixados):
        tentativas = 5
        espera = 3
        for i in range(tentativas):
            try:
                resposta = requests.get(url, headers=headers, impersonate="chrome")
                if resposta.status_code == 200:
                    return resposta
                else:
                    print(f"Falha na tentativa {i + 1}/{tentativas} (Status {resposta.status_code})...", end=" ")
            except Exception as erro_requisicao:
                print(f"Falha na tentativa {i + 1}/{tentativas} (Erro {erro_requisicao})...", end=" ")

            if i < tentativas - 1:
                print(f"Aguardando {espera}s.")
                time.sleep(espera)
            else:
                print("Limite de tentativas atingido. Abortando requisição.")
        return None

    # Carrega os IDs validados do Google Sheets em memória usando um Set para otimizar as buscas
    print("Carregando IDs base da planilha...")
    try:
        df_base = pd.read_csv(url_ids_base)
        if 'id' not in df_base.columns:
            print("A coluna 'id' não está presente no arquivo base.")
            return

        ids_validos = set(df_base['id'].dropna().astype(int).tolist())
        print(f"{len(ids_validos)} IDs carregados com sucesso.")
    except Exception as e:
        print(f"Falha ao ler o arquivo base online: {e}")
        return

    # Mapeia as equipes do Brasileirão na temporada atual
    print("\nMapeando os times do campeonato...")
    url_tabela = "https://www.sofascore.com/api/v1/unique-tournament/325/season/72034/standings/total"
    lista_times = []

    requisicao_times = faz_requisicao(url_tabela, "Tabela de Times")

    if requisicao_times and requisicao_times.status_code == 200:
        try:
            linhas_da_tabela = requisicao_times.json()['standings'][0]['rows']
            for linha in linhas_da_tabela:
                time_info = linha['team']
                lista_times.append((time_info['id'], time_info['name']))
            print(f"{len(lista_times)} times processados.")
        except Exception as erro_time:
            registrar_erro("Parse Times", erro_time, url_tabela)
            return
    else:
        registrar_erro("Busca de Times", "Falha definitiva ao obter times.", url_tabela)
        return

    # Percorre o elenco de cada equipe buscando os números das camisas
    print("\nExtraindo informações das camisas para os IDs mapeados...")
    todos_jogadores = []

    for id_do_time, nome_do_time in lista_times:
        print(f"Lendo dados de: {nome_do_time}...", end="")
        url_elenco = f"https://www.sofascore.com/api/v1/team/{id_do_time}/players"

        requisicao_jogadores = faz_requisicao(url_elenco, f"Elenco {nome_do_time}")

        if requisicao_jogadores and requisicao_jogadores.status_code == 200:
            try:
                dados_elenco = requisicao_jogadores.json()
                jogadores = dados_elenco.get('players', [])

                contador_filtrados = 0

                for jogador in jogadores:
                    if 'player' in jogador:
                        jogador_id = jogador['player'].get('id')

                        if jogador_id in ids_validos:
                            dados_jogador = jogador['player']
                            dados_jogador['time_id'] = id_do_time
                            dados_jogador['time_nome'] = nome_do_time

                            numero_camisa = jogador.get('shirtNumber', dados_jogador.get('shirtNumber', None))
                            dados_jogador['shirtNumber'] = numero_camisa

                            todos_jogadores.append(dados_jogador)
                            contador_filtrados += 1

                print(f" {contador_filtrados} perfis atualizados.")
            except Exception as e:
                print(f" Erro de processamento: {e}")
                registrar_erro("Parse Jogadores", e, nome_do_time)
        else:
            print(" Falha de conexão.")
            registrar_erro("Busca Jogadores", "Falha de conexão com a API", nome_do_time)

        # Intervalo aleatório para evitar bloqueios por taxa de requisições
        time.sleep(random.uniform(1.5, 3.0))

    # Consolida os dados em um DataFrame e os distribui para os locais de destino
    if todos_jogadores:
        df_final = pd.json_normalize(todos_jogadores)

        colunas_desejadas = ['id', 'name', 'shirtNumber', 'time_id', 'time_nome']
        colunas_finais = [col for col in colunas_desejadas if col in df_final.columns]
        df_final = df_final[colunas_finais]

        # Salva o arquivo CSV no repositório local
        os.makedirs(pasta_destino, exist_ok=True)
        caminho_completo = os.path.join(pasta_destino, nome_arquivo_saida)
        df_final.to_csv(caminho_completo, index=False, sep=';', encoding='utf-8-sig')
        print(f"\nArquivo local gerado: {caminho_completo}")

        # Atualiza a aba principal no Google Sheets
        print("Sincronizando dados com o Google Sheets...")
        salvar_dataframe(df_final, id_da_planilha, "Camisas_Jogadores_2025")
    else:
        print("\nNenhuma atualização de camisa capturada para os IDs fornecidos.")

    # Registra o histórico de problemas encontrados durante a execução, se houver
    if lista_erros:
        print("\nSincronizando logs de erro com o Sheets...")
        df_erros = pd.DataFrame(lista_erros)
        salvar_dataframe(df_erros, id_da_planilha, "Erros_Camisas_Jogadores_2025")
    else:
        print("\nColeta finalizada com sucesso e sem erros mapeados.")