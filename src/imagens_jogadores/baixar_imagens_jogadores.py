import pandas as pd
import os
import time

# IMPORTAÇÃO CORRETA DA ARTILHARIA PESADA
from curl_cffi import requests

# URL da sua planilha geral
URL_GERAL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT97utXArqjlobEogUGP875AkdKDdh0Ebgs5MQTclisxhe8c-oPiKJ0d-IdEdwlPSiy54zkZU0-LaI9/pub?gid=140211115&single=true&output=csv"

print("Lendo os dados da planilha...")
df = pd.read_csv(URL_GERAL)

# Remove possíveis linhas vazias ou sem ID
df = df.dropna(subset=['id'])
df = df.drop_duplicates(subset=['id'])

pasta_destino = r"C:\Users\marti\PycharmProjects\tcc-recomendacao-tatica\src\imagens_jogadores"
os.makedirs(pasta_destino, exist_ok=True)

print("Iniciando o download do elenco com curl_cffi...")

for index, row in df.iterrows():
    try:
        player_id = int(row['id'])
        nome = str(row['name']).strip()

        url_foto = f"https://api.sofascore.app/api/v1/player/{player_id}/image"

        # --- A MUDANÇA SALVADORA ---
        # Agora o arquivo será salvo com o ID (ex: 12345.png). Fim dos conflitos de nomes iguais!
        caminho_arquivo = os.path.join(pasta_destino, f"{player_id}.png")

        if not os.path.exists(caminho_arquivo):

            resposta = requests.get(url_foto, impersonate="chrome", timeout=15)

            if resposta.status_code == 200:
                with open(caminho_arquivo, 'wb') as f:
                    f.write(resposta.content)
                # Mantive o nome no print só para você saber quem está sendo baixado no log
                print(f"✅ Baixado: {nome} (ID: {player_id})")
            else:
                print(f"❌ Erro HTTP {resposta.status_code} no {nome}")

            time.sleep(1)

        else:
            print(f"⏭️ Já existe na pasta: {nome} (ID: {player_id})")

    except Exception as e:
        print(f"⚠️ Erro ao processar a linha {index}: {e}")

print("\nOperação Fantasma concluída! Imagens na base.")