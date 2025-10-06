import pandas as pd
import matplotlib.pyplot as plt
import os

classified_csv = "/home/christian/Documentos/spotify_millsongdata_CLASSIFICADO.csv"

def generate_sentiment_chart():
    if not os.path.exists(classified_csv):
        print("Arquivo não encontrado")
        return

    df = pd.read_csv(classified_csv)

    sentiment_counts = df['feeling'].value_counts()

    plt.figure(figsize=(8, 6))
    sentiment_counts.plot(kind='bar', color=['green', 'red', 'blue'])
    plt.title('Contagem de Músicas por Sentimento')
    plt.xlabel('Sentimento')
    plt.ylabel('Número de Músicas')
    plt.xticks(rotation=0)
    plt.grid(axis='y', linestyle='--')
    plt.tight_layout()

    plt.show()

if __name__ == "__main__":
    generate_sentiment_chart()
