import os
import pandas as pd

# Caminho da pasta onde os arquivos CSV estão localizados
folder_path = r"C:\Users\livia\Desktop\ufmg_codes\curso IF 14-12\dataBase\bilheteria-diaria-obras-por-distribuidoras-csv"

# Nome do arquivo de saída
output_file = "bilheteria_diaria_completa.csv"

def merge_csv_files(folder_path, output_file):
    try:
        # Lista todos os arquivos no diretório que correspondem ao formato esperado
        csv_files = [file for file in os.listdir(folder_path) if file.endswith(".csv") and "bilheteria-diaria-obras-por-distribuidoras" in file]
        
        if not csv_files:
            print("Nenhum arquivo CSV encontrado no formato especificado.")
            return
        
        # Lista para armazenar os DataFrames
        dataframes = []
        
        for csv_file in csv_files:
            file_path = os.path.join(folder_path, csv_file)
            try:
                # Lê o arquivo CSV, lidando com possíveis erros
                df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip', low_memory=False)
                dataframes.append(df)
                print(f"Arquivo '{csv_file}' carregado com sucesso.")
            except Exception as e:
                print(f"Erro ao processar o arquivo '{csv_file}': {e}")
        
        # Concatena todos os DataFrames, ignorando o índice para evitar duplicações
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        # Salva o DataFrame combinado em um único arquivo CSV
        combined_df.to_csv(os.path.join(folder_path, output_file), index=False, encoding='utf-8')
        print(f"Arquivo combinado salvo como '{output_file}' com sucesso.")
    
    except Exception as e:
        print(f"Ocorreu um erro geral: {e}")

# Executa a função para combinar os arquivos
merge_csv_files(folder_path, output_file)
