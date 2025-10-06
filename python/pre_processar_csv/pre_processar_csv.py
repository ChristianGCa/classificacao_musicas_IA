# Programa para remover as quebras de linha

import pandas as pd
import re

arquivo_de_entrada = '/home/christian/Downloads/spotify_millsongdata.csv'
arquivo_de_saida = '/home/christian/Documentos/CSVs_processados/spotify_millsongdata_SEM_QUEBRAS_LINHA.csv'

def clean_text(text):
    text = re.sub(r'[-.,!?;:“”‘’]', ' ', text) # Substitui pontuação por espaço
    text = re.sub(r'\s+', ' ', text).strip()
    return text

try:
    df = pd.read_csv(arquivo_de_entrada)
    if 'text' in df.columns:
        df['text'] = df['text'].str.replace('\n', ' ', regex=False)
        df['text'] = df['text'].apply(clean_text)
        df.to_csv(arquivo_de_saida, index=False)

        print("CSV processado e salvo")
        print(arquivo_de_saida)
    else:
        print("Coluna não encontrada")

except FileNotFoundError:
    print("Arquivo não encontrado")
except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")