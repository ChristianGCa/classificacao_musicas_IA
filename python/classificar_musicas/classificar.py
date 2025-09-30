import ollama
import pandas as pd
import os

dataset_csv = "/home/christian/Downloads/spotify_millsongdata_teste_30.csv"
classified_csv = "/home/christian/Documentos/spotify_millsongdata_CLASSIFICADO.csv"


def classificar_letra(letra: str) -> str:
    prompt = f"""
Classifique o sentimento da música abaixo em apenas UMA palavra: 'positivo', 'negativo' ou 'neutro'.
Não explique e não escreva mais nada além da categoria.

Música:
{letra}
"""
    response = ollama.generate(model="mistral:latest", prompt=prompt)
    return response["response"].strip().lower()


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

print("Lendo dataset")
df = pd.read_csv(dataset_csv)

results = []
for i in range (processed_lines, len(df)):
    text = df["text"].iloc[i]
    feeling = classificar_letra(text)
    results.append(feeling)
    print(f"{i}/{len(df)}", " | ", df["song"].iloc[i-1], " - ", feeling)

    new_line = {
        "artist": df["artist"].iloc[i],
        "song": df["song"].iloc[i],
        "text": text,
        "feeling": feeling
    }

    pd.DataFrame([new_line]).to_csv(classified_csv, mode="a", header=False, index=False)

print(df[["artist", "song", "feeling"]])
