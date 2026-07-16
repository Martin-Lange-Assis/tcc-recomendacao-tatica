import time
from src.database import create_db
from src.database.limpeza_insercao_bd import executar_pipeline


def main_db():
    print('Iniciando processo de criar tabelas no Banco de Dados e Inserir os Registros')

    # 1. Cria as tabelas fisicamente no MariaDB
    create_db.create_tables()

    # Pausa para garantir que o banco processe a criação estrutural
    time.sleep(5)

    # 2. Caminho do JSON contendo os pesos dos arquétipos
    caminho_json = r"caminho_json"

    # 3. Executa todo o fluxo de extração, limpeza, cálculo e inserção
    executar_pipeline(caminho_json)


if __name__ == "__main__":
    main_db()