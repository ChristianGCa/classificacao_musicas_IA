# Classificar Músicas com IA e Contar Palavras/Músicas

Este repositório contém programas para análise de letras de música. Inclui um script Python para classificação com um modelo de IA local e programas C para processamento paralelo com MPI para contar ocorrências de palavras e músicas em datasets.

## Dataset

Este projeto utiliza o dataset [Spotify Million Song Dataset](https://www.kaggle.com/datasets/notshrirang/spotify-million-song-dataset) do Kaggle, que contém informações sobre artistas, músicas e suas letras.

## Pré-requisitos

### Para programas C:
- GCC (GNU Compiler Collection)
- MPI (Message Passing Interface)

### Para scripts Python:
- Python 3
- Dependências listadas em `requirements.txt`

## Como Executar

Certifique-se de estar na raiz do projeto (`classificar_musicas_IA/`).

### 1. Pré-processamento dos dados (Python)

Essa fase consiste em remover as quebras de linha da coluna "text" do CSV original e gerar o arquivo `spotify_millsongdata_SEM_QUEBRAS_LINHA.csv`.

```bash
source env/bin/activate
```

Instalar as dependências do Python:
```bash
pip install -r requirements.txt
```

Navegar para o diretório de pré-processamento:
```bash
cd python/pre_processar_csv
```

Executar o script de pré-processamento:
```bash
python pre_processar_csv.py
```

Voltar para a raiz do projeto:
```bash
cd ../..
```

### 2. Compilar os programas C

Navegue até o diretório `c` e use o `Makefile` para compilar todos os programas:

```bash
cd c
```

```bash
make
```

Isso irá gerar os seguintes executáveis:
- `mostrar_musicas`
- `1_1_contar_palavras_seq`
- `1_2_contar_palavras_mpi`
- `2_contar_musicas_mpi`
- `3_contar_palavras_e_musicas_mpi`

### 3. Executar o programa sequencial de contagem de palavras (C)

Este programa processa o CSV e gera um ranking de palavras, salvando-o em `ranking_palavras.csv`.

```bash
cd c
```

Executar o programa usando o Makefile:
```bash
make 1_1_run_seq_palavras
```

### 4. Executar os programas paralelos (MPI) de contagem

Certifique-se de que o MPI está configurado (instale `mpich` se ainda não o fez, como nos pré-requisitos).

**a) Contar apenas palavras (MPI): `1_2_contar_palavras_mpi`**

```bash
cd c
```

Executar com 2 processos usando o Makefile:
```bash
make 1_2_run_palavras_2
```

Executar com 8 processos usando o Makefile:
```bash
make 1_2_run_palavras_8
```

**b) Contar apenas músicas (MPI): `2_contar_musicas_mpi`**

```bash
cd c
```

Executar com 2 processos usando o Makefile:
```bash
make 2_run_musicas_2
```

Executar com 8 processos usando o Makefile:
```bash
make 2_run_musicas_8
```

**c) Contar palavras e músicas (MPI): `3_contar_palavras_e_musicas_mpi`**

```bash
cd c
```

Executar com 2 processos:
```bash
make 3_run_palavras_e_musicas_mpi_2
```

Executar com 8 processos:
```bash
make 3_run_palavras_e_musicas_mpi_8
```

### 5. Classificação com IA (Python)

Este script utiliza o `ollama` para classificar o sentimento das letras das músicas.

```bash
source env/bin/activate
```

Navegar para o diretório de classificação:
```bash
cd python/classificar_musicas
```

Executar o script de classificação:
```bash
python classificar.py
```

Voltar para a raiz do projeto:
```bash
cd ../..
```

### 6. Limpar executáveis e arquivos gerados (C)

Para remover todos os executáveis compilados e o arquivo `ranking_palavras.csv` gerado, use o seguinte comando no diretório `c`:

```bash
make clean
```
