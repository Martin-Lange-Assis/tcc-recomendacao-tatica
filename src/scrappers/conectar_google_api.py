import gspread
import os
from google.oauth2.service_account import Credentials

CAMINHO_PADRAO = os.path.join(os.getcwd(),
                              r'arquivo_credenciais')


def conectar(json_keyfile):
    """Cria a conexão autenticada com o Google Drive e Sheets."""
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
    Exporta um DataFrame para uma aba do Google Sheets,
    realizando o tratamento de tipos complexos antes do envio.
    """
    print(f"[Sheets] Iniciando exportação para a aba '{nome_aba}'...")

    client = conectar(json_file)
    if not client:
        return

    try:
        # Acessa a planilha alvo pelo ID
        try:
            sh = client.open_by_key(id_planilha)
        except gspread.SpreadsheetNotFound:
            print(f"[Sheets] Planilha ID '{id_planilha}' não encontrada.")
            return

        # Seleciona a aba existente (limpando-a) ou cria uma nova
        try:
            worksheet = sh.worksheet(nome_aba)
            worksheet.clear()
        except gspread.WorksheetNotFound:
            print(f"[Sheets] Criando nova aba: {nome_aba}")
            worksheet = sh.add_worksheet(title=nome_aba, rows=dataframe.shape[0] + 50, cols=dataframe.shape[1] + 5)

        # Prepara os dados tratando valores nulos
        df_limpo = dataframe.fillna('')

        # Converte colunas com objetos (dicionários/listas) para string
        # para evitar erros de compatibilidade (ex: "struct_value") no Sheets
        for col in df_limpo.columns:
            if df_limpo[col].dtype == 'object':
                df_limpo[col] = df_limpo[col].astype(str)

        # Monta a estrutura final com cabeçalho seguido dos valores
        dados = [df_limpo.columns.values.tolist()] + df_limpo.values.tolist()

        worksheet.update(range_name='A1', values=dados)

        print(f"[Sheets] Sucesso! {len(dataframe)} linhas enviadas para '{nome_aba}'.")

    except Exception as e:
        print(f"[Sheets] Erro durante o envio: {e}")


def ler_ultimo_id(id_planilha, json_file=CAMINHO_PADRAO):
    """Retorna o último ID processado registrado na aba Controle."""
    client = conectar(json_file)
    if not client: return 0

    try:
        sh = client.open_by_key(id_planilha)
        worksheet = sh.worksheet("Controle")
        valor = worksheet.acell('A1').value
        return int(valor) if valor else 0
    except gspread.WorksheetNotFound:
        print("[Sheets] Aba 'Controle' não encontrada. Começando do zero.")
        return 0
    except Exception as e:
        print(f"[Sheets] Erro ao ler Controle: {e}")
        return 0


def adicionar_linha(id_planilha, nome_aba, dados_lista, json_file=CAMINHO_PADRAO):
    """Adiciona uma única linha ao final da aba especificada."""
    client = conectar(json_file)
    if not client:
        raise Exception("Falha na conexão com a API do Google.")

    sh = client.open_by_key(id_planilha)
    worksheet = sh.worksheet(nome_aba)
    worksheet.append_row(dados_lista)
    print(f"[Sheets] Registro adicionado na aba '{nome_aba}'.")


def adicionar_multiplas_linhas(id_planilha, nome_aba, dados_lista_de_listas, json_file=CAMINHO_PADRAO):
    """Insere múltiplas linhas de dados simultaneamente na aba especificada."""
    client = conectar(json_file)
    if not client:
        raise Exception("Falha na conexão com a API do Google.")

    sh = client.open_by_key(id_planilha)
    worksheet = sh.worksheet(nome_aba)
    worksheet.append_rows(dados_lista_de_listas)
    print(f"[Sheets] {len(dados_lista_de_listas)} registros salvos na aba '{nome_aba}'.")


def atualizar_ultimo_id(id_planilha, ultimo_id, json_file=CAMINHO_PADRAO):
    """Atualiza o registro do último ID processado na aba Controle."""
    client = conectar(json_file)
    if not client:
        raise Exception("Falha na conexão com a API do Google.")

    sh = client.open_by_key(id_planilha)
    worksheet = sh.worksheet("Controle")
    worksheet.update_acell('A1', ultimo_id)
