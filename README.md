# Classificar Músicas com IA

Este repositório contém programas para análise de letras de música usando processamento paralelo com MPI.

## Dataset

Este projeto utiliza o dataset [Spotify Million Song Dataset](https://www.kaggle.com/datasets/notshrirang/spotify-million-song-dataset) do Kaggle, que contém informações sobre músicas e suas letras.

## Pré-requisitos

### Para programas C:
- GCC
- MPI (MPICH ou OpenMPI)

### Para scripts Python:
- Python 3
- Dependências listadas em `requirements.txt`

## Como Executar

### 1. Pré-processamento dos dados (Python)

```bash
source env/bin/activate
```

```bash
pip install -r requirements.txt
```

```bash
cd python/pre_processar_csv
python pre_processar_csv.py
```

### 2. Compilar os programas C

```bash
cd c
make compile
```

### 3. Executar versão sequencial

```bash
make run_seq
```

```bash
./contar_palavras
```

### 4. Executar versão paralela (MPI)

```bash
make run_mpi
```

```bash
make run_mpi_8
```

```bash
mpirun -np 2 ./mpi_contar_palavras
```

```bash
mpirun -np 8 ./mpi_contar_palavras
```

### 5. Classificação com IA (Python)

```bash
cd python/classificar_musicas
python classificar.py
```
