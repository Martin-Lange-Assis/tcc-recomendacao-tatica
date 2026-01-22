import pandas as pd
import os
from conectar_google_api import salvar_dataframe

# --- CONFIGURAÇÃO DE CAMINHOS ---
# Usando o mesmo caminho do script anterior para garantir que eles se encontrem
PASTA_PROJETO = 'caminho'
ARQUIVO_JSONL = f'{PASTA_PROJETO}/dados_brutos_stats.jsonl'
ARQUIVO_CSV_FINAL = f'{PASTA_PROJETO}/tabela_final_stats_2025.csv'
SPREADSHEET_ID = "id_planilha"  # <--- SEU ID


def processar_conversao():
    """
        Converte a planilha de extração dos jogadores de jsonl para CSV
    """
    print("Iniciando processamento dos dados...")

    if not os.path.exists(ARQUIVO_JSONL):
        print(f"Erro: Arquivo '{ARQUIVO_JSONL}' não encontrado.")
        return

    # 1. Lê o arquivo JSONL
    # O lines=True é a mágica que alinha Goleiros com Atacantes
    try:
        df = pd.read_json(ARQUIVO_JSONL, lines=True)
    except ValueError:
        print("Erro: O arquivo JSONL parece estar vazio ou corrompido.")
        return

    print(f"Lido com sucesso: {len(df)} registros.")

    # 2. Organização de Colunas
    # Garante que identificadores fiquem na esquerda
    colunas_fixas = ['player_id', 'player_name', 'team_id']
    # Pega o resto das colunas dinâmicas (goals, assists, saves, etc)
    outras_colunas = []

    for c in df.columns:
        if c not in colunas_fixas:
            outras_colunas.append(c)
    ordem_final = colunas_fixas + outras_colunas

    # Reordena e PREENCHE VAZIOS
    df = df[ordem_final].fillna('')

    # 3. Salva o CSV final limpo (Backup Local)
    df.to_csv(ARQUIVO_CSV_FINAL, index=False, sep=';', encoding='utf-8-sig')
    print(f"[Local] CSV salvo em: {ARQUIVO_CSV_FINAL}")

    # 4. Envia para o Google Sheets (Nuvem)
    if salvar_dataframe:
        print("[Nuvem] Enviando Tabela Final para o Google Sheets...")
        # Salva numa aba chamada "Dataset_Final_Stats"
        salvar_dataframe(df, SPREADSHEET_ID, "Dataset_Final_Stats")