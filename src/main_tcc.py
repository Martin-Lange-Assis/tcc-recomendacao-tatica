import time
from scrappers.main_scrappers import main_scrappers
from database.main_db import main_db
from recommendation.avaliar_modelo import rodar_avaliacao_global

def main_tcc():
    print('Iniciando Main TCC')
    time.sleep(1)
    #main_scrappers()
    time.sleep(2)
    main_db()
    time.sleep(3)
    rodar_avaliacao_global()
    time.sleep(1)
    print('Finalizando Main TCC')

main_tcc()
