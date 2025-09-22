import ollama
import pandas as pd

def classificar_letra(letra: str) -> str:
    prompt = f"""
Classifique o sentimento da música abaixo em apenas UMA palavra: 'positivo', 'negativo' ou 'neutro'.
Não explique e não escreva mais nada além da categoria.

Música:
{letra}
"""
    response = ollama.generate(model="gemma3:1b", prompt=prompt)
    return response["response"].strip().lower()

print("Tentando ler")

df = pd.read_csv("/home/christian/Downloads/spotify_millsongdata_teste2.csv")

print("leu")

resultados = []
for i, letra in enumerate(df["text"], start=1):
    sentimento = classificar_letra(letra)
    resultados.append(sentimento)
    print(f"{i}/{len(df)}", " | ", df["song"].iloc[i-1], " - ", sentimento)

df["sentimento"] = resultados

print(df[["artist", "song", "sentimento"]])

