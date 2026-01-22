import gspread
import os
from google.oauth2.service_account import Credentials

CAMINHO_PADRAO = os.path.join(os.getcwd(), 'credentials.json')

def conectar(json_keyfile):
    """Cria a conexão autenticada com o Google."""
    try:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(json_keyfile, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"[Sheets] Erro na autenticação: {e}")
        return None

def salvar_dataframe(dataframe, id_planilha, nome_aba, json_file=CAMINHO_PADRAO):
    """
    Converte dicionários/listas para string antes de enviar.
    """
    print(f"[Sheets] Iniciando exportação para aba '{nome_aba}'...")

    client = conectar(json_file)
    if not client:
        return

    try:
        # Tenta abrir a planilha pelo ID
        try:
            sh = client.open_by_key(id_planilha)
        except gspread.SpreadsheetNotFound:
            print(f"[Sheets] Planilha ID '{id_planilha}' não encontrada.")
            return

        # Tenta selecionar a aba ou cria uma nova
        try:
            worksheet = sh.worksheet(nome_aba)
            worksheet.clear()  # Limpa dados antigos
        except gspread.WorksheetNotFound:
            print(f"[Sheets] Criando nova aba: {nome_aba}")
            worksheet = sh.add_worksheet(title=nome_aba, rows=dataframe.shape[0] + 50, cols=dataframe.shape[1] + 5)

        # --- TRATAMENTO DE DADOS ---
        df_limpo = dataframe.fillna('')

        # Verifica cada coluna: se tiver dicionários ou listas, converte para Texto (String)
        for col in df_limpo.columns:
            # Se a coluna for do tipo 'object' (pode conter dicts), forçamos string
            if df_limpo[col].dtype == 'object':
                df_limpo[col] = df_limpo[col].astype(str)

        # Prepara os dados: Cabeçalho + Linhas
        dados = [df_limpo.columns.values.tolist()] + df_limpo.values.tolist()

        worksheet.update(range_name='A1', values=dados)

        print(f"[Sheets] Sucesso! {len(dataframe)} linhas enviadas para '{nome_aba}'.")

    except Exception as e:
        print(f"[Sheets] Erro durante o envio: {e}")