# Programa para remover as quebras de linha

import pandas as pd

arquivo_de_entrada = '/home/christian/Downloads/spotify_millsongdata.csv'
arquivo_de_saida = '/home/christian/Documentos/CSVs_processados/spotify_millsongdata_SEM_QUEBRAS_LINHA.csv'

try:
    df = pd.read_csv(arquivo_de_entrada)
    if 'text' in df.columns:
        df['text'] = df['text'].str.replace('\n', ' ', regex=False)
        df.to_csv(arquivo_de_saida, index=False)

        print("CSV processado e salvo")
        print(arquivo_de_saida)
    else:
        print("Coluna não encontrada")

except FileNotFoundError:
    print("Arquivo não encontrado")
except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")