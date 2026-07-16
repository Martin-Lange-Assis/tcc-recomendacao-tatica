from src.scrappers.modulo_discovery import modulo_discovery
from src.scrappers.modulo_extraction import modulo_extracao
from src.scrappers.especificador_de_posicoes import especificador_posicoes
from src.scrappers.descobrir_camisas import modulo_discovery_camisas_filtrado
from src.scrappers.escalacoes_scrapper import coletar_escalacoes_sofascore
import time

def main_scrappers():
    print('Começando a Coleta dos dados do Sofascore')
    time.sleep(5)
    modulo_discovery()
    time.sleep(5)
    modulo_extracao()
    time.sleep(10)
    especificador_posicoes()
    time.sleep(10)
    modulo_discovery_camisas_filtrado()
    time.sleep(5)
    coletar_escalacoes_sofascore()
    time.sleep(5)
    print('Fim da Coleta dos Dados')