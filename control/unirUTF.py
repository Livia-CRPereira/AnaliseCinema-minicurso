import pandas as pd
import os
import glob

# Defina o caminho da pasta onde estão os arquivos CSV
pasta = r'C:\Users\livia\Desktop\ufmg_codes\curso IF 14-12\dataBase\bilheteria-diaria-obras-por-distribuidoras-csv'

# Verifique se o diretório existe
if not os.path.exists(pasta):
    print(f"Erro: O caminho especificado não existe: {pasta}")
    exit()

print(f"Caminho encontrado: {pasta}")

# Use glob para pegar todos os arquivos CSV no diretório
pattern = os.path.join(pasta, 'bilheteria-diaria-obras-por-distribuidoras-*.csv')
print(f"Usando o padrão: {pattern}")
csv_files = glob.glob(pattern)

# Exibir os arquivos encontrados para debug
print(f"Arquivos CSV encontrados: {csv_files}")

# Verifica se encontrou arquivos
if not csv_files:
    print("Nenhum arquivo CSV foi encontrado na pasta especificada.")
    exit()

# Inicialize uma lista para armazenar os DataFrames agrupados
df_lista = []

# Nomes das colunas esperadas
colunas_esperadas = [
    'TITULO_ORIGINAL', 'TITULO_BRASIL', 'PUBLICO', 'RAZAO_SOCIAL_DISTRIBUIDORA', 'PAIS_OBRA'
]

# Loop por cada arquivo CSV encontrado
for file in csv_files:
    try:
        # Extraia o ano do nome do arquivo
        ano = int(file.split('-')[-2])

        # Leia o arquivo CSV
        print(f"Lendo o arquivo: {file}")
        df = pd.read_csv(file, delimiter=';', encoding='utf-8', lineterminator='\n', low_memory=False)

        # Verifique se as colunas esperadas estão presentes
        colunas_faltando = [col for col in colunas_esperadas if col not in df.columns]
        if colunas_faltando:
            print(f"Arquivo {file} não contém todas as colunas esperadas. Faltando: {colunas_faltando}. Pulando...")
            continue

        # Seleciona apenas as colunas necessárias
        df = df[colunas_esperadas]

        # Preenche células vazias com valores padrão
        df.fillna('', inplace=True)

        # Certifique-se de que a coluna 'PUBLICO' seja numérica e substitua valores inválidos por 0
        df['PUBLICO'] = pd.to_numeric(df['PUBLICO'], errors='coerce').fillna(0).astype(int)

        # Agrupa os dados pelos títulos e soma o público
        df_agrupado = df.groupby(
            ['TITULO_ORIGINAL', 'TITULO_BRASIL', 'RAZAO_SOCIAL_DISTRIBUIDORA', 'PAIS_OBRA'],
            as_index=False
        ).agg({'PUBLICO': 'sum'})

        # Adiciona a coluna do ano
        df_agrupado['ANO'] = ano

        # Adiciona o DataFrame agrupado à lista
        df_lista.append(df_agrupado)
        print(f"Arquivo {file} foi processado e adicionado à lista com sucesso.")

    except Exception as e:
        print(f"Ocorreu um erro ao processar o arquivo {file}: {e}")
        continue

# Combina todos os DataFrames em um único
if df_lista:
    df_final = pd.concat(df_lista, ignore_index=True)

    # Reagrupa para consolidar títulos que podem estar em múltiplos arquivos
    df_final = df_final.groupby(
        ['TITULO_ORIGINAL', 'TITULO_BRASIL', 'RAZAO_SOCIAL_DISTRIBUIDORA', 'PAIS_OBRA'],
        as_index=False
    ).agg({'PUBLICO': 'sum', 'ANO': lambda x: ', '.join(map(str, sorted(set(x))))})

    # Especifica o caminho de saída do arquivo CSV final
    caminho_saida = r'C:\Users\livia\Desktop\ufmg_codes\curso IF 14-12\dataBase\bilheteria_agrupada_utf.csv'

    # Salva o DataFrame final em um arquivo CSV com codificação UTF-8 SIG
    df_final.to_csv(caminho_saida, index=False, encoding='utf-8-sig', sep=';')
    print(f"Arquivo CSV combinado foi salvo com sucesso em: {caminho_saida}")
else:
    print("Nenhum dado foi processado com sucesso.")
