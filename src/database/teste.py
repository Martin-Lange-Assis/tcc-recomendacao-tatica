from sqlalchemy import text
from database import SessionLocal


def testar_motor_tcc():
    db = SessionLocal()
    try:
        # Buscando os Top 10 Atacantes (ATA) - Padrão EAFC
        query = text("""
            SELECT j.name, j.time_nome, e.rating
            FROM jogadores j
            JOIN jogador_posicao jp ON j.player_id = jp.player_id
            JOIN posicoes_ref p ON jp.id_posicao = p.id_posicao
            JOIN estatisticas_2025 e ON j.player_id = e.player_id
            WHERE p.sigla_posicao = 'ATA'
            ORDER BY e.rating DESC
            LIMIT 10
        """)

        resultados = db.execute(query).fetchall()

        print("\n🏆 TOP 10 ATACANTES (RECOMENDAÇÃO TCC) 🏆")
        print("-" * 50)
        for res in resultados:
            print(f"{res.name:.<25} | {res.time_nome:.<15} | Nota: {res.rating:.2f}")

    except Exception as e:
        print(f"❌ Erro na consulta: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    testar_motor_tcc()