from modulo_discovery import modulo_discovery
from modulo_extraction import modulo_extracao
from especificador_de_posicoes import especificador_posicoes
import time

print('Começando a Raspagem dos dados do Sofascore')
time.sleep(5)
modulo_discovery()
time.sleep(5)
modulo_extracao()
time.sleep(10)
especificador_posicoes()
time.sleep(10)
print('Fim')