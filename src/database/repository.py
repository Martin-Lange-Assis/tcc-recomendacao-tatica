# src/database/repository.py
import pandas as pd
from sqlalchemy import text
from src.database.database import engine, SessionLocal
from src.database.models import EscalacaoPartida, Jogador


def buscar_resultado_partida(jogo_id: int, time_lado: str) -> str:
    """Recupera o resultado final da partida no banco de dados para o time especificado."""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT resultado_time 
                FROM escalacoes_partidas 
                WHERE jogo_id = :j AND time_lado = :t 
                LIMIT 1
            """)
            resultado = conn.execute(query, {"j": jogo_id, "t": time_lado}).scalar()
            return resultado if resultado else "Desconhecido"
    except Exception as e:
        print(f"Erro ao buscar resultado do jogo {jogo_id}: {e}")
        return "Desconhecido"


def buscar_dados_times(time_casa: str, time_fora: str) -> pd.DataFrame:
    """
    Consolida as estatísticas, posições detalhadas e o arquétipo de maior
    similaridade dos jogadores relacionados aos clubes do confronto.
    """
    query = """
        SELECT e.player_id, j.name AS player_name, j.posicao_bruta, j.time_id, j.time_nome, e.minutesPlayed,
               ct.posicoes_detalhadas,
               e.goals, e.bigChancesCreated, e.bigChancesMissed, e.assists,
               e.goalsAssistsSum, e.accuratePasses, e.inaccuratePasses,
               e.totalPasses, e.accurateOwnHalfPasses, e.accurateOppositionHalfPasses,
               e.accurateFinalThirdPasses, e.keyPasses, e.successfulDribbles,
               e.tackles, e.interceptions, e.accurateCrosses, e.totalShots,
               e.shotsOnTarget, e.groundDuelsWon, e.aerialDuelsWon, e.totalDuelsWon,
               e.goalsFromInsideTheBox, e.headedGoals, e.accurateLongBalls,
               e.clearances, e.possessionWonAttThird, e.blockedShots,
               e.dribbledPast, e.outfielderBlocks, e.goalsConceded,
               e.totalCross, e.ballRecovery,
               c.id_arquetipo, c.score_similaridade,
               a.nome_arquetipo
        FROM estatisticas_2025 e
        JOIN jogadores j ON e.player_id = j.player_id
        LEFT JOIN classificacao_jogadores c ON e.player_id = c.player_id
        LEFT JOIN arquetipos_ref a ON c.id_arquetipo = a.id_arquetipo
        LEFT JOIN caracteristicas_taticas ct ON e.player_id = ct.player_id
        WHERE c.score_similaridade = (
            SELECT MAX(c2.score_similaridade)
            FROM classificacao_jogadores c2
            WHERE c2.player_id = e.player_id
        )
        AND j.time_nome IN (%(time_casa)s, %(time_fora)s)
    """

    df = pd.read_sql_query(
        query,
        con=engine,
        params={"time_casa": time_casa, "time_fora": time_fora}
    )

    df = df.drop_duplicates(subset=['player_id'], keep='first')
    return df


def buscar_posicoes_alvo_arquetipos() -> pd.DataFrame:
    """Carrega o mapeamento de posições alvo dos arquétipos para cruzamento no matchup."""
    query_posicoes = "SELECT id_arquetipo, posicao_alvo FROM arquetipos_ref"
    return pd.read_sql_query(query_posicoes, con=engine)


def buscar_contexto_partida(jogo_id: int, meu_lado: str = 'home'):
    """
    Mapeia a disposição das equipes listando adversários titulares, ausências
    do time base e improvisações táticas detectadas pelo posicionamento.
    """
    db = SessionLocal()
    try:
        registros = db.query(EscalacaoPartida, Jogador) \
            .join(Jogador, EscalacaoPartida.player_id == Jogador.player_id) \
            .filter(EscalacaoPartida.jogo_id == jogo_id).all()

        if not registros:
            return None

        time_casa = ""
        time_fora = ""
        adversarios_titulares_ids = []
        adversarios_titulares_nomes = []
        meus_desfalques_ids = []
        meus_titulares_reais = []
        improvisacoes_detectadas = {}

        for escalacao, jogador in registros:
            if escalacao.time_lado == 'home':
                time_casa = jogador.time_nome
            elif escalacao.time_lado == 'away':
                time_fora = jogador.time_nome

            is_adversario = (meu_lado == 'home' and escalacao.time_lado == 'away') or \
                            (meu_lado == 'away' and escalacao.time_lado == 'home')
            is_meu_time = not is_adversario

            if is_meu_time:
                if escalacao.status_jogo == 'Desfalque':
                    meus_desfalques_ids.append(escalacao.player_id)
                elif escalacao.status_jogo == 'Titular':
                    meus_titulares_reais.append(escalacao.nome_jogador)

            if is_adversario and escalacao.status_jogo == 'Titular':
                adversarios_titulares_ids.append(escalacao.player_id)
                adversarios_titulares_nomes.append(escalacao.nome_jogador)

                if escalacao.posicao_jogo != jogador.posicao_bruta:
                    improvisacoes_detectadas[escalacao.player_id] = {
                        "id_arquetipo": 30,
                        "nome_arquetipo": f"Atuando como {escalacao.posicao_jogo} (Improvisado)",
                        "posicao_primaria": jogador.posicao_bruta
                    }

        return {
            "time_casa": time_casa,
            "time_fora": time_fora,
            "jogadores_adversarios": adversarios_titulares_ids,
            "desfalques": meus_desfalques_ids,
            "improvisacoes": improvisacoes_detectadas,
            "meus_titulares_reais": meus_titulares_reais
        }
    finally:
        db.close()


def buscar_todos_jogos_ids():
    """Retorna a relação de chaves primárias (IDs) de todos os confrontos registrados."""
    db = SessionLocal()
    try:
        jogos = db.query(EscalacaoPartida.jogo_id).distinct().all()
        return [jogo[0] for jogo in jogos]
    finally:
        db.close()


def buscar_formacao_real(jogo_id: int, time_lado: str) -> str:
    """Extrai a formação base da partida e aplica o refinamento tático pelos titulares escalados."""
    db = SessionLocal()
    try:
        jogadores = db.query(EscalacaoPartida).filter(
            EscalacaoPartida.jogo_id == jogo_id,
            EscalacaoPartida.time_lado == time_lado,
            EscalacaoPartida.status_jogo == "Titular"
        ).all()

        if not jogadores:
            return None

        formacao_base = jogadores[0].formacao
        if not formacao_base or formacao_base == "N/A":
            return None

        posicoes_titulares = [jogador.posicao_jogo for jogador in jogadores]
        return refinar_formacao_tatica(formacao_base, posicoes_titulares)
    finally:
        db.close()


def refinar_formacao_tatica(formacao_base: str, posicoes_titulares: list) -> str:
    """Classifica a variante tática real comparando a prancheta inicial com os setores preenchidos."""
    if not formacao_base:
        return None

    posicoes = [pos.upper() for pos in posicoes_titulares if pos]

    if formacao_base == "4-3-3":
        qtd_vol = posicoes.count('VOL')
        qtd_mei = posicoes.count('MEI')
        qtd_mc = posicoes.count('MC')

        if qtd_mei >= 1:
            return "4-3-3 (Ofensivo)"
        elif qtd_mc >= 3:
            return "4-3-3 (Em linha)"
        elif qtd_vol >= 2:
            return "4-3-3 (Defensivo)"
        return "4-3-3 (Conservador)"

    elif formacao_base == "4-1-2-1-2":
        posicoes_abertas = ['PD', 'PE', 'MD', 'ME']
        if any(pos in posicoes for pos in posicoes_abertas):
            return "4-1-2-1-2 (Aberto)"
        return "4-1-2-1-2 (Fechado)"

    elif formacao_base == "4-4-2":
        if "MC" in posicoes:
            return "4-4-2 (Em linha)"
        return "4-4-2 (Conservador)"

    elif formacao_base == "4-4-1-1":
        return "4-4-1-1 (Meio-Campo)"

    elif formacao_base == "4-5-1":
        if "MEI" in posicoes:
            return "4-5-1 (Ofensivo)"
        return "4-5-1 (Em Linha)"

    elif formacao_base == "3-4-3":
        return "3-4-3 (Em Linha)"

    elif formacao_base == "5-4-1":
        return "5-4-1 (Em Linha)"

    elif formacao_base == "3-5-1-1":
        return "3-5-2"

    return formacao_base


def buscar_frequencia_titularidade(time_nome: str, jogo_id_atual: int, limite: int = 5) -> pd.DataFrame:
    """
    Agrega o volume de titularidade recente dos atletas filtrando apenas
    partidas históricas para evitar data leakage no pipeline.
    """
    query_jogos = text("""
        SELECT DISTINCT ep.jogo_id
        FROM escalacoes_partidas ep
        JOIN jogadores j ON ep.player_id = j.player_id
        WHERE j.time_nome = :time AND ep.jogo_id < :jogo_id
        ORDER BY ep.jogo_id DESC
        LIMIT :limite
    """)

    with engine.connect() as conn:
        result_jogos = conn.execute(query_jogos, {
            "time": time_nome,
            "jogo_id": jogo_id_atual,
            "limite": limite
        }).fetchall()

        jogos_ids = [row[0] for row in result_jogos]

        if not jogos_ids:
            return pd.DataFrame(columns=['player_id', 'partidas_titular'])

        query_titular = text("""
            SELECT ep.player_id, COUNT(ep.jogo_id) as partidas_titular
            FROM escalacoes_partidas ep
            JOIN jogadores j ON ep.player_id = j.player_id
            WHERE j.time_nome = :time 
              AND ep.jogo_id IN :jogos_ids
              AND ep.status_jogo = 'Titular'
            GROUP BY ep.player_id
        """)

        result_titular = conn.execute(query_titular, {
            "time": time_nome,
            "jogos_ids": tuple(jogos_ids)
        }).fetchall()

        return pd.DataFrame(result_titular, columns=['player_id', 'partidas_titular'])