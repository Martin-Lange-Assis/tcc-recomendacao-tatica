import os
import base64
import pandas as pd
from PIL import Image
import io

# Caminho base das imagens
caminho_pasta = r"caminho_das_imagens"
dados_imagens = []

# Definição do tamanho da miniatura
tamanho_maximo = (100, 100)

for ficheiro in os.listdir(caminho_pasta):
    if ficheiro.lower().endswith(('.png', '.jpg', '.jpeg')):
        caminho_completo = os.path.join(caminho_pasta, ficheiro)
        player_id = os.path.splitext(ficheiro)[0]

        try:
            # Abre e cria a miniatura da imagem
            img = Image.open(caminho_completo)
            img.thumbnail(tamanho_maximo)

            # Salva a imagem redimensionada na memória
            buffer = io.BytesIO()
            formato = img.format if img.format else 'PNG'
            img.save(buffer, format=formato)

            # Converte os dados para base64
            string_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            prefixo = f"data:image/{formato.lower()};base64,"

            dados_imagens.append({
                "ID_Jogador": player_id,
                "Imagem_Base64": prefixo + string_base64
            })
        except Exception as e:
            print(f"Erro ao processar {ficheiro}: {e}")

# Gera o arquivo CSV final
df = pd.DataFrame(dados_imagens)
df.to_csv("imagens_jogadores_base64.csv", index=False)
print("Ficheiro CSV gerado com sucesso!")