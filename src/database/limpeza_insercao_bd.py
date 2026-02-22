import pandas as pd
import numpy as np
from sqlalchemy import text
from database import engine, SessionLocal
from models import Jogador, Estatistica2025, SetorRef, PosicaoRef, ArquetipoRef, CaracteristicaTatica

# --- CONFIGURAÇÃO DE URLs ---
BASE_URL = "LINK"

URLS = {
    'geral': f"{BASE_URL}?gid=140211115&single=true&output=csv",
    'stats': f"{BASE_URL}?gid=1068403530&single=true&output=csv",
    'setores': f"{BASE_URL}?gid=1759049841&single=true&output=csv",
    'posicoes_ref': f"{BASE_URL}?gid=402997298&single=true&output=csv",
    'tatica': f"{BASE_URL}?gid=376444125&single=true&output=csv",
    'arquetipos': f"{BASE_URL}?gid=1851659119&single=true&output=csv",
    'deuses': f"{BASE_URL}?gid=946728303&single=true&output=csv"
}

# Dicionário de Tradução Explícito (Inglês -> EAFC Brasil)
TRADUCAO_POSICOES = {
    'GK': 'GL',    # Goleiro
    'DR': 'LD',    # Lateral-Direito
    'DL': 'LE',    # Lateral-Esquerdo
    'DC': 'ZAG',   # Zagueiro
    'DM': 'VOL',   # Volante
    'MC': 'MC',    # Meio-Campista
    'MR': 'MD',    # Meia-Direita
    'ML': 'ME',    # Meia-Esquerda
    'AM': 'MEI',   # Meia-Ofensivo
    'RW': 'PD',    # Ponta-Direita
    'LW': 'PE',    # Ponta-Esquerda
    'ST': 'ATA',   # Atacante
}

MAPA_SETORES = {'F': 'Ataque', 'M': 'Meio', 'D': 'Defesa', 'G': 'Gol'}


def limpar_valor(val, tipo_func):
    if pd.isna(val):
        return None

    return tipo_func(val)


def traduzir_posicao_eafc(sigla_en):
    """Traduz siglas do inglês para o padrão EAFC 26."""
    if pd.isna(sigla_en) or sigla_en == "":
        return None

    partes = []
    for parte in str(sigla_en).split(','):
        partes.append(parte.strip())

    posicoes_traduzidas = []
    for pos in partes:
        termo_traduzido = TRADUCAO_POSICOES.get(pos, pos)
        posicoes_traduzidas.append(termo_traduzido)
    return ", ".join(posicoes_traduzidas)


def sincronizar_banco_de_dados():
    db = SessionLocal()
    try:
        # --- 1. REFERÊNCIAS (Lógica Anti-Duplicata) ---
        print("Povoando tabelas de referência...")

        # Lista das tabelas de referência para limpar antes de repovoar
        tabelas_ref = ['setores_ref', 'posicoes_ref', 'arquetipos_ref', 'deuses_arquetipos']

        with engine.connect() as conn:
            # Desativa verificações de FK temporariamente para limpar sem erros
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            for tabela in tabelas_ref:
                conn.execute(text(f"TRUNCATE TABLE {tabela};"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            conn.commit()

        # Agora insere normalmente, sem risco de ID duplicado
        pd.read_csv(URLS['setores']).to_sql('setores_ref', engine, if_exists='append', index=False)
        pd.read_csv(URLS['posicoes_ref']).to_sql('posicoes_ref', engine, if_exists='append', index=False)
        pd.read_csv(URLS['arquetipos']).to_sql('arquetipos_ref', engine, if_exists='append', index=False)

        # --- 2. JOGADORES ---
        print("Cadastrando jogadores e vinculando setores...")
        df_geral = pd.read_csv(URLS['geral'])
        df_geral.columns = df_geral.columns.str.strip()
        df_geral = df_geral.dropna(subset=['position'])

        for _, row in df_geral.iterrows():
            player_id_planilha = int(row['id'])
            jogador = db.query(Jogador).filter_by(player_id=player_id_planilha).first()

            if not jogador:

                # 1. Preparação dos dados (extração e limpeza)
                nome = str(row['name'])
                slug = str(row['slug'])
                posicao = str(row['position'])
                altura = limpar_valor(row['height'], float)
                nascimento = limpar_valor(row['dateOfBirthTimestamp'], float)
                id_time = int(row['time_id'])
                nome_time = str(row['time_nome'])

                # 2. Lógica condicional explícita para o pé preferido
                if pd.notna(row['preferredFoot']):
                    pe_preferido = str(row['preferredFoot'])
                else:
                    pe_preferido = "N/A"

                # 3. Lógica condicional explícita para o país
                if 'country.name' in row:
                    pais = str(row['country.name'])
                else:
                    pais = "Brazil"

                # 4. Construção da URL de imagem
                url_foto = "https://api.sofascore.app/api/v1/player/" + str(player_id_planilha) + "/image"

                # 5. Instanciação do objeto com variáveis limpas
                jogador = Jogador(
                    player_id=player_id_planilha,
                    name=nome,
                    slug=slug,
                    posicao_bruta=posicao,
                    height=altura,
                    preferredFoot=pe_preferido,
                    dateOfBirthTimestamp=nascimento,
                    country_name=pais,
                    time_id=id_time,
                    time_nome=nome_time,
                    url_imagem=url_foto
                )
                db.add(jogador)
                db.flush()

            # Vincula apenas o SETOR (F, M, D, G)
            nome_setor = MAPA_SETORES.get(row['position'])
            if nome_setor:
                setor_obj = db.query(SetorRef).filter_by(nome_setor=nome_setor).first()
                if setor_obj and setor_obj not in jogador.setores:
                    jogador.setores.append(setor_obj)
        db.commit()

        # --- 3. CARACTERÍSTICAS TÁTICAS E ASSOCIAÇÃO DE POSIÇÕES ---
        print("Traduzindo táticas e vinculando jogador na posição...")
        df_tatica = pd.read_csv(URLS['tatica'])

        for _, row in df_tatica.iterrows():
            player_id_planilha = int(row['player_id'])
            jogador = db.query(Jogador).filter_by(player_id=player_id_planilha).first()

            if not jogador:
                continue

            # 1. Traduz a string para o CaracteristicaTatica (Ex: "ST, CAM" -> "ATA, MEI")
            pos_traduzida = traduzir_posicao_eafc(row['posicoes_detalhadas'])

            if pos_traduzida:
                # 2. Salva o texto traduzido na tabela de tática
                tatica = CaracteristicaTatica(
                    player_id=player_id_planilha,
                    posicoes_detalhadas=pos_traduzida,
                    ids_fortes=str(row.get('ids_fortes', '')),
                    ids_fracos=str(row.get('ids_fracos', ''))
                )
                db.merge(tatica)

                # 3. VÍNCULO REAL NA TABELA jogador_posicao
                # Quebra a string traduzida ("ATA, MEI") para buscar cada ID no banco
                siglas = []
                partes_da_posicao = pos_traduzida.split(',')

                for s in partes_da_posicao:
                    sigla_limpa = s.strip()
                    siglas.append(sigla_limpa)

                for sigla in siglas:
                    pos_ref = db.query(PosicaoRef).filter_by(sigla_posicao=sigla).first()
                    if pos_ref and pos_ref not in jogador.posicoes:
                        jogador.posicoes.append(pos_ref)

        db.commit()

        # --- 4. ESTATÍSTICAS (As 116 Colunas) ---
        print("Processando as 116 estatísticas para jogadores válidos...")
        df_stats = pd.read_csv(URLS['stats'], decimal=',')

        # Sincroniza os IDs
        # Realiza a consulta no banco de dados
        resultado_consulta = db.query(Jogador.player_id).all()

        # Inicializa a lista vazia
        lista_ids_validos = []

        # Percorre os resultados e adiciona apenas o ID à lista
        for registro in resultado_consulta:
            lista_ids_validos.append(registro.player_id)

        df_stats = df_stats[df_stats['player_id'].isin(lista_ids_validos)]

        # Limpeza das 116 colunas
        cols_para_ignorar = ['player_name', 'type', 'position', 'team_name', 'team_id', 'id', 'statisticsType']

        for col in df_stats.columns:
            if col not in cols_para_ignorar:
                df_stats[col] = pd.to_numeric(df_stats[col], errors='coerce').fillna(0)

        # Remove as colunas que não pertencem à tabela estatisticas_2025
        # 1. Identificar quais colunas ignorar que realmente existem no DataFrame
        colunas_para_remover = []

        for coluna in cols_para_ignorar:
            if coluna in df_stats.columns:
                colunas_para_remover.append(coluna)

        # 2. Executar a remoção das colunas identificadas
        df_final_stats = df_stats.drop(columns=colunas_para_remover)

        with engine.connect() as conexao:
            conexao.execute(text("TRUNCATE TABLE estatisticas_2025;"))
            conexao.commit()

        # Agora o player_id será a única "chave" enviada junto com as métricas
        df_final_stats.to_sql('estatisticas_2025', con=engine, if_exists='append', index=False)
        print("Sucesso! Dados limpos e 116 colunas integradas no MariaDB.")

        # --- 5. DEUSES DOS ARQUÉTIPOS ---
        print("Processando os atributos dos Deuses (Arquétipos)...")
        df_deuses = pd.read_csv(URLS['deuses'], decimal=',')

        # As colunas inúteis para ignorar
        cols_para_ignorar_deuses = ['player_name', 'type', 'position', 'team_name', 'team_id', 'id', 'statisticsType',
                                    'nome_arquetipo']

        # Limpa e converte para numérico tudo que não for a coluna de ignorar E não for o id_arquetipo
        for col in df_deuses.columns:
            if col not in cols_para_ignorar_deuses and col != 'id_arquetipo':
                df_deuses[col] = pd.to_numeric(df_deuses[col], errors='coerce').fillna(0)

        # Remove as colunas de texto/inúteis
        colunas_para_remover_deuses = []
        for coluna in cols_para_ignorar_deuses:
            if coluna in df_deuses.columns:
                colunas_para_remover_deuses.append(coluna)

        df_final_deuses = df_deuses.drop(columns=colunas_para_remover_deuses)

        # Insere na nova tabela deuses_arquetipos
        df_final_deuses.to_sql('deuses_arquetipos', con=engine, if_exists='append', index=False)
        print("Sucesso! Deuses integrados no MariaDB.")

    except Exception as e:
        print(f"Erro crítico: {e}")
        db.rollback()
    finally:
        db.close()

# --- BLOCO PRINCIPAL DE EXECUÇÃO ---
if __name__ == "__main__":
    sincronizar_banco_de_dados()