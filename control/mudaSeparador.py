import pandas as pd

# Defina o caminho do arquivo CSV
caminho_arquivo = 'dataBase/a.csv'

# Leia o arquivo CSV com ponto e vírgula como separador
df = pd.read_csv(caminho_arquivo, delimiter=';')

# Salve o arquivo com vírgula como separador
novo_caminho_arquivo = 'dataBase/naoPubBrasil.csv'

df.to_csv(novo_caminho_arquivo, index=False, sep=',', encoding='utf-8-sig')

print(f"Arquivo alterado com sucesso! Novo arquivo salvo em: {novo_caminho_arquivo}")
