import ollama
import pandas as pd

def classificar_lote(letras: list[str]) -> list[str]:
    prompt = "Classifique o sentimento de cada música abaixo em apenas UMA palavra ('positivo', 'negativo' ou 'neutro').\n"
    prompt += "Responda com UMA linha por música, na mesma ordem.\n"
    prompt += "Não explique e não escreva mais nada além das categorias.\n\n"

    for i, letra in enumerate(letras, start=1):
        prompt += f"Música {i}:\n{letra}\n\n"

    response = ollama.generate(model="gemma3:1b", prompt=prompt)
    saida = response["response"].strip().lower().splitlines()
    return [s.strip() for s in saida if s.strip()]

print("Tentando ler")

df = pd.read_csv("/home/christian/Downloads/spotify_millsongdata_teste2.csv")

print("leu")

resultados = []

# Processa em lotes de 5
lote = 5
for i in range(0, len(df), lote):
    letras = df["text"].iloc[i:i+lote].tolist()
    classificacoes = classificar_lote(letras)
    resultados.extend(classificacoes)

    for j, sentimento in enumerate(classificacoes, start=i+1):
        print(f"{j}/{len(df)} | {df['song'].iloc[j-1]} - {sentimento}")

df["sentimento"] = resultados

print(df[["artist", "song", "sentimento"]])

df.to_csv("musicas_classificadas.csv", index=False)
