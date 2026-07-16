import pandas as pd
import numpy as np
from sqlalchemy import text
from src.database.database import engine, SessionLocal
from src.database.models import Jogador, Estatistica2025, SetorRef, PosicaoRef, ArquetipoRef, CaracteristicaTatica
from src.classification.classificacao_jogadores import calcular_similaridade_arquetipos
from src.database.repository import refinar_formacao_tatica

BASE_URL = "BASE_URL"

URLS = {
    'geral': f"{BASE_URL}?gid=geral=csv",
    'stats': f"{BASE_URL}?gid=stats=csv",
    'setores': f"{BASE_URL}?gid=setores=csv",
    'posicoes_ref': f"{BASE_URL}?gid=posicoes_ref=csv",
    'tatica': f"{BASE_URL}?gid=tatica=csv",
    'arquetipos': f"{BASE_URL}?gid=arquetipos=csv",
    'deuses': f"{BASE_URL}?gid=deuses=csv",
    'escalacoes': f"{BASE_URL}?gid=escalacoes=csv"
}

TRADUCAO_POSICOES = {
    'GK': 'GL',
    'DR': 'LD',
    'DL': 'LE',
    'DC': 'ZAG',
    'DM': 'VOL',
    'MC': 'MC',
    'MR': 'MD',
    'ML': 'ME',
    'AM': 'MEI',
    'RW': 'PD',
    'LW': 'PE',
    'ST': 'ATA',
}

MAPA_SETORES = {'F': 'Ataque', 'M': 'Meio', 'D': 'Defesa', 'G': 'Gol'}


def limpar_valor(val, tipo_func):
    """Trata nulos antes de converter o tipo."""
    if pd.isna(val):
        return None
    return tipo_func(val)


def traduzir_posicao_eafc(sigla_en):
    """Traduz siglas de posição para o padrão BR."""
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


def salvar_classificacao_jogadores(df_jogadores_classificados):
    """Salva os scores de similaridade no banco."""
    print("Salvando classificações de similaridade no banco de dados...")

    # Ajusta colunas pro formato da tabela
    df_para_banco = df_jogadores_classificados[['ID do jogador', 'ID do Arquetipo', 'Score de Similaridade']].copy()
    df_para_banco = df_para_banco.rename(columns={
        'ID do jogador': 'player_id',
        'ID do Arquetipo': 'id_arquetipo',
        'Score de Similaridade': 'score_similaridade'
    })

    try:
        with engine.connect() as conexao:
            conexao.execute(text("TRUNCATE TABLE classificacao_jogadores;"))
            conexao.commit()

        df_para_banco.to_sql('classificacao_jogadores', con=engine, if_exists='append', index=False)
        print("Sucesso! Classificação dos jogadores salva no banco.")

    except Exception as e:
        print(f"Erro ao salvar a classificação: {e}")


def sincronizar_banco_de_dados():
    """Pipeline principal de extração, limpeza e carga no banco."""
    db = SessionLocal()
    try:
        # Tabelas de referência
        print("Povoando tabelas de referência...")
        tabelas_ref = ['setores_ref', 'posicoes_ref', 'arquetipos_ref', 'deuses_arquetipos']

        with engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            for tabela in tabelas_ref:
                conn.execute(text(f"TRUNCATE TABLE {tabela};"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            conn.commit()

        pd.read_csv(URLS['setores']).to_sql('setores_ref', engine, if_exists='append', index=False)
        pd.read_csv(URLS['posicoes_ref']).to_sql('posicoes_ref', engine, if_exists='append', index=False)
        pd.read_csv(URLS['arquetipos']).to_sql('arquetipos_ref', engine, if_exists='append', index=False)

        # Upsert de jogadores
        print("Cadastrando e atualizando jogadores...")
        df_geral = pd.read_csv(URLS['geral'])
        df_geral.columns = df_geral.columns.str.strip()
        df_geral = df_geral.dropna(subset=['position'])

        for _, row in df_geral.iterrows():
            player_id_planilha = int(row['id'])
            jogador = db.query(Jogador).filter_by(player_id=player_id_planilha).first()

            # Dados base do jogador
            nome = str(row['name'])
            slug = str(row['slug'])
            posicao = str(row['position'])
            altura = limpar_valor(row['height'], float)
            nascimento = limpar_valor(row['dateOfBirthTimestamp'], float)
            id_time = int(row['time_id'])
            nome_time = str(row['time_nome'])
            pe_preferido = str(row['preferredFoot']) if pd.notna(row['preferredFoot']) else "N/A"
            pais = str(row['country.name']) if 'country.name' in row else "Brazil"
            url_foto = f"https://img.sofascore.com/api/v1/player/{player_id_planilha}/image"

            if not jogador:
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
            else:
                jogador.name = nome
                jogador.slug = slug
                jogador.posicao_bruta = posicao
                jogador.height = altura
                jogador.preferredFoot = pe_preferido
                jogador.dateOfBirthTimestamp = nascimento
                jogador.country_name = pais
                jogador.time_id = id_time
                jogador.time_nome = nome_time
                jogador.url_imagem = url_foto

            db.flush()

            # Associa jogador ao setor
            nome_setor = MAPA_SETORES.get(row['position'])
            if nome_setor:
                setor_obj = db.query(SetorRef).filter_by(nome_setor=nome_setor).first()
                if setor_obj and setor_obj not in jogador.setores:
                    jogador.setores.append(setor_obj)

        db.commit()

        # Táticas e posições
        print("Traduzindo táticas e vinculando posições aos jogadores...")
        df_tatica = pd.read_csv(URLS['tatica'])

        for _, row in df_tatica.iterrows():
            player_id_planilha = int(row['player_id'])
            jogador = db.query(Jogador).filter_by(player_id=player_id_planilha).first()

            if not jogador:
                continue

            pos_traduzida = traduzir_posicao_eafc(row['posicoes_detalhadas'])

            if pos_traduzida:
                tatica = CaracteristicaTatica(
                    player_id=player_id_planilha,
                    posicoes_detalhadas=pos_traduzida,
                    ids_fortes=str(row.get('ids_fortes', '')),
                    ids_fracos=str(row.get('ids_fracos', ''))
                )
                db.merge(tatica)

                siglas = []
                partes_da_posicao = pos_traduzida.split(',')

                for sig in partes_da_posicao:
                    sigla_limpa = sig.strip()
                    siglas.append(sigla_limpa)

                for sigla in siglas:
                    pos_ref = db.query(PosicaoRef).filter_by(sigla_posicao=sigla).first()
                    if pos_ref and pos_ref not in jogador.posicoes:
                        jogador.posicoes.append(pos_ref)

        db.commit()

        # Processa estatísticas gerais
        print("Processando o volume de estatísticas para jogadores cadastrados...")
        df_stats = pd.read_csv(URLS['stats'], decimal=',')

        resultado_consulta = db.query(Jogador.player_id).all()
        lista_ids_validos = []

        for registro in resultado_consulta:
            lista_ids_validos.append(registro.player_id)

        df_stats = df_stats[df_stats['player_id'].isin(lista_ids_validos)]

        # Limpa colunas que não são métricas de performance
        cols_para_ignorar = ['player_name', 'type', 'position', 'team_name', 'team_id', 'id', 'statisticsType']

        for col in df_stats.columns:
            if col not in cols_para_ignorar:
                df_stats[col] = pd.to_numeric(df_stats[col], errors='coerce').fillna(0)

        colunas_para_remover = []
        for coluna in cols_para_ignorar:
            if coluna in df_stats.columns:
                colunas_para_remover.append(coluna)

        df_final_stats = df_stats.drop(columns=colunas_para_remover)

        # Imprime o log das colunas selecionadas
        print("\n" + "=" * 60)
        print("--- RESUMO DA SELEÇÃO DE VARIÁVEIS ---")
        print("=" * 60)
        print(f"Colunas descartadas ({len(colunas_para_remover)}): {colunas_para_remover}")
        print(f"Total de colunas na matriz resultante: {len(df_final_stats.columns)}")

        colunas_restantes = df_final_stats.columns.tolist()
        print("\nListando as 10 primeiras colunas mantidas na estrutura:")

        for indice, coluna in enumerate(colunas_restantes[:10], start=1):
            print(f"   {indice:03d} - {coluna}")

        colunas_ocultas = len(colunas_restantes) - 10
        if colunas_ocultas > 0:
            print(f"   ... [+ {colunas_ocultas} colunas de dados do jogador]")

        print("\nConversão de dados nulos preenchidos com 0.0 finalizada.")
        print("=" * 60 + "\n")

        with engine.connect() as conexao:
            conexao.execute(text("TRUNCATE TABLE estatisticas_2025;"))
            conexao.commit()

        df_final_stats.to_sql('estatisticas_2025', con=engine, if_exists='append', index=False)
        print("Estatísticas integradas na base com sucesso.")

        # Escalações e status de jogo
        print("Processando logs de escalações e status de jogos...")
        df_escalacoes = pd.read_csv(URLS['escalacoes'], header=None)

        df_escalacoes.columns = ['rodada', 'jogo_id', 'time_lado', 'resultado_time', 'formacao', 'player_id',
                                 'nome_jogador', 'posicao_jogo', 'camisa', 'status_jogo']

        df_escalacoes['player_id'] = pd.to_numeric(df_escalacoes['player_id'], errors='coerce')
        df_escalacoes = df_escalacoes.dropna(subset=['player_id'])
        df_escalacoes = df_escalacoes[df_escalacoes['player_id'].isin(lista_ids_validos)]

        def aplicar_regras_grupo(grupo):
            """Ajusta a formação baseada na posição inicial em campo."""
            formacao_base = str(grupo['formacao'].iloc[0])
            posicoes_titulares = grupo[grupo['status_jogo'] == 'Titular']['posicao_jogo'].dropna().tolist()
            formacao_refinada = refinar_formacao_tatica(formacao_base, posicoes_titulares)
            grupo['formacao'] = formacao_refinada
            return grupo

        df_escalacoes = df_escalacoes.groupby(['jogo_id', 'time_lado'], group_keys=False).apply(aplicar_regras_grupo)

        with engine.connect() as conexao:
            conexao.execute(text("TRUNCATE TABLE escalacoes_partidas;"))
            conexao.commit()

        df_escalacoes.to_sql('escalacoes_partidas', con=engine, if_exists='append', index=False)
        print("Escalações integradas no banco com sucesso.")

        # Métricas dos deuses (arquétipos)
        print("Processando métricas de referência para os arquétipos...")
        df_deuses = pd.read_csv(URLS['deuses'], decimal=',')

        cols_para_ignorar_deuses = ['player_name', 'type', 'position', 'team_name', 'team_id', 'id', 'statisticsType',
                                    'nome_arquetipo']

        for col in df_deuses.columns:
            if col not in cols_para_ignorar_deuses and col != 'id_arquetipo':
                df_deuses[col] = pd.to_numeric(df_deuses[col], errors='coerce').fillna(0)

        colunas_para_remover_deuses = []
        for coluna in cols_para_ignorar_deuses:
            if coluna in df_deuses.columns:
                colunas_para_remover_deuses.append(coluna)

        df_final_deuses = df_deuses.drop(columns=colunas_para_remover_deuses)
        df_final_deuses.to_sql('deuses_arquetipos', con=engine, if_exists='append', index=False)
        print("Atributos de arquétipos estruturados com sucesso.")

    except Exception as e:
        print(f"Erro durante a sincronização de dados: {e}")
        db.rollback()
    finally:
        db.close()


def executar_pipeline(caminho_json):
    """Executa o pipeline completo (sincronização e cálculo de similaridade)."""
    sincronizar_banco_de_dados()
    print("\nIniciando cálculo de similaridade de arquétipos...")
    df_classificados = calcular_similaridade_arquetipos(caminho_json)

    salvar_classificacao_jogadores(df_classificados)
    print("\nPipeline de processamento finalizado com sucesso!")


if __name__ == "__main__":
    # Caminho do JSON contido externamente na raiz do script
    CAMINHO_JSON = r"CAMINHO_JSON"
    executar_pipeline(CAMINHO_JSON)