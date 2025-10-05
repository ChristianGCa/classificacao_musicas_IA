import ollama
import pandas as pd
import os
import sys

dataset_csv = "/home/christian/Documentos/CSVs_processados/spotify_millsongdata_SEM_QUEBRAS_LINHA.csv"
classified_csv = "/home/christian/Documentos/spotify_millsongdata_CLASSIFICADO.csv"

processed_lines = 0
BATCH_SIZE = 20

def classificar_letra(text: str) -> str:
    prompt = f"""
Classifique o sentimento da música abaixo em apenas UMA palavra: 'positivo', 'negativo' ou 'neutro'.
Não explique e não escreva mais nada além da categoria.

Música:
{text}
"""
    response = ollama.generate(model="gemma3:1b", prompt=prompt)
    return response["response"].strip().lower()

def save_and_exit(results):

    print("\nInterrompendo o programa...")

    if not results:
        print("Nenhum dado novo para salvar. Encerrando.")
        sys.exit(0)
    
    print(f"Escrevendo {len(results)} linhas no CSV")
    new_df = pd.DataFrame(results)

    try:
        new_df.to_csv(classified_csv, mode="a", header=False, index=False)
        print(f"Dados salvos em {classified_csv}")
    except Exception as e:
        print(f"Erro ao salvar dados: {e}")
        sys.exit(1)
    
    sys.exit(0)

if os.path.exists(classified_csv):
    print("Lendo arquivo de saída existente")
    classified_songs_df = pd.read_csv(classified_csv)
    processed_lines = len(classified_songs_df)
    print(f"Músicas processadas no arquivo: {processed_lines}")
else:
    print("Criando novo arquivo de saída")
    classified_songs_df = pd.DataFrame(columns=["artist", "song", "text", "feeling"])
    classified_songs_df.to_csv(classified_csv, index=False)
    processed_lines = 0

try:
    print("Lendo dataset")
    df = pd.read_csv(dataset_csv)

    results = []
    for i in range (processed_lines, len(df)):
        text = df["text"].iloc[i]
        feeling = classificar_letra(text)
        print(f"{i+1}/{len(df)}", " | ", df["song"].iloc[i], " - ", feeling)

        new_line = {
            "artist": df["artist"].iloc[i],
            "song": df["song"].iloc[i],
            "text": text,
            "feeling": feeling
        }

        results.append(new_line)

        if len(results) >= BATCH_SIZE:
            print(f"Salvamento automático de {BATCH_SIZE} linhas")
            pd.DataFrame(results).to_csv(classified_csv, mode="a", header=False, index=False)
            results.clear()
    
    save_and_exit(results)

except KeyboardInterrupt as e:
    save_and_exit(results)
