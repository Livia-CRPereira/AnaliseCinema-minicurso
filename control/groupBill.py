import babypandas as bpd
import pandas as pd

# Caminho do arquivo CSV original
input_file = r"C:\Users\livia\Desktop\ufmg_codes\curso IF 14-12\dataBase\bilheteria_diaria_completa.csv"

# Caminho do arquivo CSV de saída
output_file = r"C:\Users\livia\Desktop\ufmg_codes\curso IF 14-12\dataBase\bilheteria_agrupada.csv"

# Função para processar os dados
def processar_dados(input_file, output_file, chunk_size=100000):
    try:
        # Inicializa um DataFrame vazio para armazenar os dados agrupados
        df_agrupado_total = pd.DataFrame()

        # Lê o arquivo em partes (chunks)
        for chunk in pd.read_csv(input_file, chunksize=chunk_size):
            # Remove duplicação de nomes de colunas, se existir
            if (chunk.iloc[0] == chunk.columns).all():
                chunk = chunk.iloc[1:].reset_index(drop=True)
            
            # Renomeia as colunas para garantir que elas mantenham os nomes únicos
            chunk.columns = [col.strip() for col in chunk.columns]

            # Filtra as colunas de interesse
            colunas_necessarias = [
                "TITULO_ORIGINAL", "TITULO_BRASIL", "REGISTRO_GRUPO_EXIBIDOR",
                "RAZAO_SOCIAL_DISTRIBUIDORA", "PUBLICO"
            ]
            chunk = chunk[colunas_necessarias]

            # Converte a coluna PUBLICO para numérica (caso necessário)
            chunk["PUBLICO"] = chunk["PUBLICO"].replace("", 0).astype(float)

            # Agrupa os dados do chunk
            df_agrupado_chunk = (
                chunk.groupby(["TITULO_ORIGINAL", "TITULO_BRASIL", "REGISTRO_GRUPO_EXIBIDOR", "RAZAO_SOCIAL_DISTRIBUIDORA"])
                .sum()
                .reset_index()
            )
            
            # Concatena o resultado do chunk com o total
            df_agrupado_total = pd.concat([df_agrupado_total, df_agrupado_chunk], ignore_index=True)

            print(f"Chunk processado com sucesso, total de {df_agrupado_total.shape[0]} linhas.")

        # Salva o resultado em um novo arquivo CSV
        df_agrupado_total.to_csv(output_file, index=False)
        print(f"Arquivo processado e salvo como '{output_file}'.")
    
    except Exception as e:
        print(f"Erro ao processar os dados: {e}")

# Executa a função
processar_dados(input_file, output_file)

