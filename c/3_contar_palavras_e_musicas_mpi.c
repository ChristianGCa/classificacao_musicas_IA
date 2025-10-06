// Compilar: mpicc -o mpi_contar_palavras mpi_contar_palavras.c
// Executar: mpirun -np 2 ./mpi_contar_palavras

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <mpi.h>

#define MAX_TEXT 10000000 // 10 MB
#define MAX_WORD_LEN 128
#define MAX_WORDS 100000
#define HASH_SIZE 100003

typedef struct Word {
    char *text;
    int count;
    struct Word *next;
} Word;


typedef struct {
    char palavra[MAX_WORD_LEN];
    int contagem;
} PalavraContagem;

typedef struct {
    char artista[MAX_WORD_LEN];
    int contagem;
} ArtistaContagem;


unsigned long hash(const char *str) {
    unsigned long h = 5381;
    int c;
    while ((c = *str++)) {
        h = ((h << 5) + h) + c;
    }
    return h % HASH_SIZE;
}

void normalize(char *word) {
    char *src = word, *dst = word;
    while (*src) {
        if (isalpha((unsigned char)*src) || *src == '\'') {
            *dst++ = tolower((unsigned char)*src);
        }
        src++;
    }
    *dst = '\0';
}

void add_word(Word **hash_table, const char *w) {
    if (strlen(w) == 0) return;

    unsigned long idx = hash(w);
    Word *node = hash_table[idx];
    while (node) {
        if (strcmp(node->text, w) == 0) {
            node->count++;
            return;
        }
        node = node->next;
    }
    Word *new_word = malloc(sizeof(Word));
    new_word->text = strdup(w);
    new_word->count = 1;
    new_word->next = hash_table[idx];
    hash_table[idx] = new_word;
}

void process_text(Word **hash_table, char *text) {
    char *token = strtok(text, " \t\r\n");
    while (token) {
        char buffer[MAX_WORD_LEN];
        strncpy(buffer, token, MAX_WORD_LEN - 1);
        buffer[MAX_WORD_LEN - 1] = '\0';
        normalize(buffer);
        add_word(hash_table, buffer);
        token = strtok(NULL, " \t\r\n");
    }
}

int hash_to_array(Word **hash_table, PalavraContagem *palavras) {
    int n = 0;
    for (int i = 0; i < HASH_SIZE; i++) {
        Word *node = hash_table[i];
        while (node) {
            strcpy(palavras[n].palavra, node->text);
            palavras[n].contagem = node->count;
            n++;
            node = node->next;
        }
    }
    return n;
}

int hash_to_artist_array(Word **hash_table, ArtistaContagem *artistas) {
    int n = 0;
    for (int i = 0; i < HASH_SIZE; i++) {
        Word *node = hash_table[i];
        while (node) {
            strcpy(artistas[n].artista, node->text);
            artistas[n].contagem = node->count;
            n++;
            node = node->next;
        }
    }
    return n;
}


int comparar_contagem(const void *a, const void *b) {
    PalavraContagem *pa = (PalavraContagem *)a;
    PalavraContagem *pb = (PalavraContagem *)b;
    return pb->contagem - pa->contagem; // Ordem decrescente
}

int comparar_contagem_artista(const void *a, const void *b) {
    ArtistaContagem *pa = (ArtistaContagem *)a;
    ArtistaContagem *pb = (ArtistaContagem *)b;
    return pb->contagem - pa->contagem; // Ordem decrescente
}


int main(int argc, char *argv[]) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    Word **hash_table = malloc(HASH_SIZE * sizeof(Word*));
    for (int i = 0; i < HASH_SIZE; i++) {
        hash_table[i] = NULL;
    }

    Word **artist_hash_table = malloc(HASH_SIZE * sizeof(Word*));
    for (int i = 0; i < HASH_SIZE; i++) {
        artist_hash_table[i] = NULL;
    }


    int total_palavras_processadas = 0;
    int total_linhas = 0;
    char **linhas = NULL;
    int num_linhas = 0;

    if (rank == 0) {
        FILE *f = fopen("/home/christian/Documentos/CSVs_processados/spotify_millsongdata_SEM_QUEBRAS_LINHA.csv", "r");
        if (!f) {
            perror("Erro ao abrir arquivo");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        char linha[8192];
        fgets(linha, sizeof(linha), f);
        total_linhas = 1; // Cabeçalho

        while (fgets(linha, sizeof(linha), f)) {
            linha[strcspn(linha, "\n")] = 0;
            char *linha_copy = strdup(linha);
            char *artist = strtok(linha_copy, ",");
            char *song   = strtok(NULL, ",");
            char *link   = strtok(NULL, ",");
            char *text   = strtok(NULL, ",");
            if (text) {
                total_linhas++;
            }
            free(linha_copy);
        }
        fclose(f);

        printf("Arquivo tem %d linhas de dados\n", total_linhas - 1);

        f = fopen("/home/christian/Documentos/CSVs_processados/spotify_millsongdata_SEM_QUEBRAS_LINHA.csv", "r");
        fgets(linha, sizeof(linha), f); // Descarta cabeçalho
        
        linhas = malloc((total_linhas - 1) * sizeof(char*));
        num_linhas = 0;
        
        while (fgets(linha, sizeof(linha), f)) {
            linha[strcspn(linha, "\n")] = 0;
            char *linha_copy = strdup(linha);
            char *artist = strtok(linha_copy, ",");
            char *song   = strtok(NULL, ",");
            char *link   = strtok(NULL, ",");
            char *text   = strtok(NULL, ",");
            if (text) {
                linhas[num_linhas] = malloc(8192);
                strcpy(linhas[num_linhas], linha);
                num_linhas++;
            }
            free(linha_copy);
        }
        fclose(f);
    }

    MPI_Bcast(&num_linhas, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int linhas_por_processo = num_linhas / size;
    int linhas_restantes = num_linhas % size;
    
    int minhas_linhas = linhas_por_processo + (rank < linhas_restantes ? 1 : 0);
    int inicio_linha = 0;

    for (int i = 0; i < rank; i++) {
        inicio_linha += linhas_por_processo + (i < linhas_restantes ? 1 : 0);
    }

    if (rank == 0) {
        for (int proc = 1; proc < size; proc++) {
            int proc_linhas = linhas_por_processo + (proc < linhas_restantes ? 1 : 0);
            int proc_inicio = 0;
            for (int i = 0; i < proc; i++) {
                proc_inicio += linhas_por_processo + (i < linhas_restantes ? 1 : 0);
            }
            
            for (int i = 0; i < proc_linhas; i++) {
                int linha_idx = proc_inicio + i;
                MPI_Send(linhas[linha_idx], 8192, MPI_CHAR, proc, i, MPI_COMM_WORLD);
            }
        }
    }

    for (int i = 0; i < minhas_linhas; i++) {
        char linha[8192];
        
        if (rank == 0) {
            int linha_idx = inicio_linha + i;
            strcpy(linha, linhas[linha_idx]);
        } else {
            MPI_Recv(linha, 8192, MPI_CHAR, 0, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }

        linha[strcspn(linha, "\n")] = 0;

        char *linha_copy = strdup(linha);
        char *artist = strtok(linha_copy, ",");
        char *song   = strtok(NULL, ",");
        char *link   = strtok(NULL, ",");
        char *text   = strtok(NULL, ",");

        if (artist) {
            add_word(artist_hash_table, artist);
        }
        
        if (text) {
            char *temp_text = strdup(text);
            char *token = strtok(temp_text, " \t\r\n");
            while (token) {
                total_palavras_processadas++;
                token = strtok(NULL, " \t\r\n");
            }
            free(temp_text);
            
            process_text(hash_table, text);
        }
        
        free(linha_copy);
    }

    PalavraContagem *palavras_locais = malloc(MAX_WORDS * sizeof(PalavraContagem));
    int num_palavras_local = hash_to_array(hash_table, palavras_locais);

    ArtistaContagem *artistas_locais = malloc(MAX_WORDS * sizeof(ArtistaContagem));
    int num_artistas_local = hash_to_artist_array(artist_hash_table, artistas_locais);


    printf("Processo %d: processou %d linhas, %d palavras, %d palavras únicas, %d artistas únicos\n", 
           rank, minhas_linhas, total_palavras_processadas, num_palavras_local, num_artistas_local);

    if (rank == 0) {
        PalavraContagem *palavras_finais = malloc(MAX_WORDS * sizeof(PalavraContagem));
        int num_finais = 0;
        int total_palavras_geral = total_palavras_processadas;
        
        for (int i = 0; i < num_palavras_local; i++) {
            strcpy(palavras_finais[num_finais].palavra, palavras_locais[i].palavra);
            palavras_finais[num_finais].contagem = palavras_locais[i].contagem;
            num_finais++;
        }
        
        ArtistaContagem *artistas_finais = malloc(MAX_WORDS * sizeof(ArtistaContagem));
        int num_artistas_finais = 0;

        for (int i = 0; i < num_artistas_local; i++) {
            strcpy(artistas_finais[num_artistas_finais].artista, artistas_locais[i].artista);
            artistas_finais[num_artistas_finais].contagem = artistas_locais[i].contagem;
            num_artistas_finais++;
        }

        for (int proc = 1; proc < size; proc++) {
            int palavras_recebidas;
            int palavras_proc;
            MPI_Recv(&palavras_recebidas, 1, MPI_INT, proc, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&palavras_proc, 1, MPI_INT, proc, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            total_palavras_geral += palavras_proc;
            
            PalavraContagem *palavras_proc_array = malloc(palavras_recebidas * sizeof(PalavraContagem));
            MPI_Recv(palavras_proc_array, palavras_recebidas * sizeof(PalavraContagem), MPI_CHAR, 
                     proc, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            for (int i = 0; i < palavras_recebidas; i++) {
                int encontrada = 0;
                for (int j = 0; j < num_finais; j++) {
                    if (strcmp(palavras_proc_array[i].palavra, palavras_finais[j].palavra) == 0) {
                        palavras_finais[j].contagem += palavras_proc_array[i].contagem;
                        encontrada = 1;
                        break;
                    }
                }
                if (!encontrada) {
                    strcpy(palavras_finais[num_finais].palavra, palavras_proc_array[i].palavra);
                    palavras_finais[num_finais].contagem = palavras_proc_array[i].contagem;
                    num_finais++;
                }
            }
            free(palavras_proc_array);

            int artistas_recebidos;
            MPI_Recv(&artistas_recebidos, 1, MPI_INT, proc, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            ArtistaContagem *artistas_proc_array = malloc(artistas_recebidos * sizeof(ArtistaContagem));
            MPI_Recv(artistas_proc_array, artistas_recebidos * sizeof(ArtistaContagem), MPI_CHAR,
                     proc, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            for (int i = 0; i < artistas_recebidos; i++) {
                int encontrada = 0;
                for (int j = 0; j < num_artistas_finais; j++) {
                    if (strcmp(artistas_proc_array[i].artista, artistas_finais[j].artista) == 0) {
                        artistas_finais[j].contagem += artistas_proc_array[i].contagem;
                        encontrada = 1;
                        break;
                    }
                }
                if (!encontrada) {
                    strcpy(artistas_finais[num_artistas_finais].artista, artistas_proc_array[i].artista);
                    artistas_finais[num_artistas_finais].contagem = artistas_proc_array[i].contagem;
                    num_artistas_finais++;
                }
            }
            free(artistas_proc_array);
        }

        qsort(palavras_finais, num_finais, sizeof(PalavraContagem), comparar_contagem);
        qsort(artistas_finais, num_artistas_finais, sizeof(ArtistaContagem), comparar_contagem_artista);

        printf("Palavras totais: %d\n", total_palavras_geral);
        printf("Palavras únicas: %d\n", num_finais);
        printf("Artistas únicos: %d\n", num_artistas_finais);
        
        printf("\nARTISTAS E NÚMERO DE MÚSICAS\n");
        for (int i = 0; i < 20 && i < num_artistas_finais; i++) {
            printf("%d. %s -> %d\n", i + 1, artistas_finais[i].artista, artistas_finais[i].contagem);
        }


        printf("\nAPARIÇÕES DE CADA PALAVRA\n");
        for (int i = 0; i < 20 && i < num_finais; i++) {
            printf("%d. %s -> %d\n", i + 1, palavras_finais[i].palavra, palavras_finais[i].contagem);
        }

        // Liberar memória
        for (int i = 0; i < num_linhas; i++) {
            free(linhas[i]);
        }
        free(linhas);
        free(palavras_finais);
        free(artistas_finais);
    } else {
        MPI_Send(&num_palavras_local, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(&total_palavras_processadas, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(palavras_locais, num_palavras_local * sizeof(PalavraContagem), MPI_CHAR, 
                 0, 2, MPI_COMM_WORLD);

        MPI_Send(&num_artistas_local, 1, MPI_INT, 0, 3, MPI_COMM_WORLD);
        MPI_Send(artistas_locais, num_artistas_local * sizeof(ArtistaContagem), MPI_CHAR,
                 0, 4, MPI_COMM_WORLD);

    }
    for (int i = 0; i < HASH_SIZE; i++) {
        Word *node = hash_table[i];
        while (node) {
            Word *next = node->next;
            free(node->text);
            free(node);
            node = next;
        }
        Word *artist_node = artist_hash_table[i];
        while(artist_node){
            Word *next = artist_node->next;
            free(artist_node->text);
            free(artist_node);
            artist_node = next;
        }
    }
    free(hash_table);
    free(palavras_locais);
    free(artist_hash_table);
    free(artistas_locais);
    
    MPI_Finalize();
    return 0;
}