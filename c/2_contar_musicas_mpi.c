// Compilar: mpicc -o mpi_ranking_artistas mpi_ranking_artistas.c
// Executar: mpirun -np 2 ./mpi_ranking_artistas

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define MAX_LINE_LENGTH 8192 // Tamanho máximo de uma linha
#define MAX_ARTIST_LEN 256   // Tamanho máximo do nome do artista
#define MAX_ARTISTS 100000   // Número máximo de artistas únicos
#define HASH_SIZE 100003     // Tabela hash

// Estrutura para a Tabela Hash (Lista Encadeada)
typedef struct ArtistNode {
    char *name;
    int count;
    struct ArtistNode *next;
} ArtistNode;

// Estrutura para o Array de Coleta
typedef struct {
    char artist[MAX_ARTIST_LEN];
    int count;
} ArtistCount;

// Função Hash
unsigned long hash(const char *str) {
    unsigned long h = 5381;
    int c;
    while ((c = *str++)) {
        h = ((h << 5) + h) + c;
    }
    return h % HASH_SIZE;
}

/**
 * @brief Adiciona ou incrementa a contagem de um artista na tabela hash local.
 */
void add_artist(ArtistNode **hash_table, const char *a) {
    if (strlen(a) == 0) return;

    unsigned long idx = hash(a);
    ArtistNode *node = hash_table[idx];
    while (node) {
        if (strcmp(node->name, a) == 0) {
            node->count++;
            return;
        }
        node = node->next;
    }
    // Novo artista
    ArtistNode *new_artist = malloc(sizeof(ArtistNode));
    new_artist->name = strdup(a);
    new_artist->count = 1;
    new_artist->next = hash_table[idx];
    hash_table[idx] = new_artist;
}

/**
 * @brief Extrai o nome do artista de uma linha do CSV e o adiciona à contagem.
 * * Assumimos que o delimitador é tabulação (\t) ou vírgula (,) e o artista é o primeiro campo.
 */
void process_line(ArtistNode **hash_table, char *line) {
    // Faz uma cópia da linha, pois strtok altera a string original
    char line_copy[MAX_LINE_LENGTH];
    strncpy(line_copy, line, MAX_LINE_LENGTH - 1);
    line_copy[MAX_LINE_LENGTH - 1] = '\0';
    
    // O seu exemplo de CSV usa tabulação (\t) como delimitador, mas o código base usa vírgula (,)
    // Vou usar a vírgula para manter a consistência com a lógica de tokenização do código base.
    char *artist = strtok(line_copy, ",");

    if (artist) {
        // Remove espaços em branco antes/depois se houver (opcional, para limpeza)
        // Simplificação: apenas adiciona
        add_artist(hash_table, artist);
    }
}

/**
 * @brief Converte a tabela hash local para um array para envio via MPI.
 */
int hash_to_array(ArtistNode **hash_table, ArtistCount *artists) {
    int n = 0;
    for (int i = 0; i < HASH_SIZE; i++) {
        ArtistNode *node = hash_table[i];
        while (node) {
            strncpy(artists[n].artist, node->name, MAX_ARTIST_LEN - 1);
            artists[n].artist[MAX_ARTIST_LEN - 1] = '\0';
            artists[n].count = node->count;
            n++;
            node = node->next;
        }
    }
    return n;
}

// Função de comparação para qsort (do maior para o menor)
int comparar_contagem(const void *a, const void *b) {
    ArtistCount *pa = (ArtistCount *)a;
    ArtistCount *pb = (ArtistCount *)b;
    return pb->count - pa->count; // Ordem decrescente
}

int main(int argc, char *argv[]) {
    int rank, size;
    double start_time, end_time;
    
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    start_time = MPI_Wtime();

    // Inicializa tabela hash
    ArtistNode **hash_table = malloc(HASH_SIZE * sizeof(ArtistNode*));
    for (int i = 0; i < HASH_SIZE; i++) {
        hash_table[i] = NULL;
    }

    int total_linhas_processadas = 0;
    int total_linhas = 0;
    char **linhas = NULL;
    int num_linhas = 0;
    char *filepath = NULL; // Usado para armazenar o caminho do arquivo

    if (rank == 0) {
        if (argc < 2) {
            fprintf(stderr, "Uso: mpirun -np <num_processos> %s <nome_arquivo.csv>\n", argv[0]);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        filepath = argv[1];

        // Processo 0 lê o arquivo e conta linhas
        FILE *f = fopen(filepath, "r");
        if (!f) {
            perror("Erro ao abrir arquivo");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        char linha[MAX_LINE_LENGTH];
        // Lê a primeira linha descartando o cabeçalho
        fgets(linha, sizeof(linha), f);

        while (fgets(linha, sizeof(linha), f)) {
            total_linhas++;
        }
        fclose(f);

        printf("Arquivo tem %d linhas de dados\n", total_linhas);

        f = fopen(filepath, "r");
        fgets(linha, sizeof(linha), f); // Descarta cabeçalho
        
        linhas = malloc(total_linhas * sizeof(char*));
        num_linhas = 0; // Vai contar apenas as linhas lidas
        
        while (fgets(linha, sizeof(linha), f)) {
            // Remove a quebra de linha
            linha[strcspn(linha, "\n")] = 0;
            
            // Aloca e copia a linha inteira para o array de linhas
            linhas[num_linhas] = strdup(linha);
            num_linhas++;
        }
        fclose(f);
    }
    
    // Distribui o nome do arquivo para os outros processos, caso precisem (neste código não é estritamente necessário)
    // Se fosse usado MPI-IO, o broadcast do filepath seria crucial.
    
    // Broadcast do número total de linhas
    MPI_Bcast(&num_linhas, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int linhas_por_processo = num_linhas / size;
    int linhas_restantes = num_linhas % size;
    
    int minhas_linhas = linhas_por_processo + (rank < linhas_restantes ? 1 : 0);
    int inicio_linha = 0;

    for (int i = 0; i < rank; i++) {
        inicio_linha += linhas_por_processo + (i < linhas_restantes ? 1 : 0);
    }

    // Processo 0 envia as linhas para os outros processos
    if (rank == 0) {
        for (int proc = 1; proc < size; proc++) {
            int proc_linhas = linhas_por_processo + (proc < linhas_restantes ? 1 : 0);
            int proc_inicio = 0;
            for (int i = 0; i < proc; i++) {
                proc_inicio += linhas_por_processo + (i < linhas_restantes ? 1 : 0);
            }
            
            for (int i = 0; i < proc_linhas; i++) {
                int linha_idx = proc_inicio + i;
                MPI_Send(linhas[linha_idx], MAX_LINE_LENGTH, MPI_CHAR, proc, i, MPI_COMM_WORLD);
            }
        }
    }

    // Cada processo (incluindo o 0) processa seu bloco de linhas
    for (int i = 0; i < minhas_linhas; i++) {
        char linha[MAX_LINE_LENGTH];
        
        if (rank == 0) {
            int linha_idx = inicio_linha + i;
            // O processo 0 copia do seu array de linhas lidas
            strcpy(linha, linhas[linha_idx]);
        } else {
            // Outros processos recebem a linha do processo 0
            MPI_Recv(linha, MAX_LINE_LENGTH, MPI_CHAR, 0, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }

        // Processamento da linha para contar o artista
        process_line(hash_table, linha);
        total_linhas_processadas++;
    }

    // Converte hash table local para array para o MPI
    ArtistCount *artists_locais = malloc(MAX_ARTISTS * sizeof(ArtistCount));
    int num_artists_local = hash_to_array(hash_table, artists_locais);

    printf("Processo %d: processou %d linhas, %d artistas únicos localmente\n", 
           rank, total_linhas_processadas, num_artists_local);

    // =============================================================
    // Coleta e Consolidação dos Resultados (Rank 0)
    // =============================================================
    if (rank == 0) {
        ArtistCount *artists_finais = NULL;
        int num_finais = 0;
        
        // Alocação inicial para o array final
        artists_finais = malloc(num_artists_local * sizeof(ArtistCount));

        // 1. Adiciona seus próprios artistas
        for (int i = 0; i < num_artists_local; i++) {
            artists_finais[i] = artists_locais[i];
        }
        num_finais = num_artists_local;
        
        // 2. Recebe artistas dos outros processos
        for (int proc = 1; proc < size; proc++) {
            int artists_recebidos;
            
            // Recebe o número de artistas do processo
            MPI_Recv(&artists_recebidos, 1, MPI_INT, proc, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            if (artists_recebidos == 0) continue;
            
            ArtistCount *artists_proc_array = malloc(artists_recebidos * sizeof(ArtistCount));
            
            // Recebe o array de artistas (usando MPI_CHAR, pois o struct é heterogêneo)
            MPI_Recv(artists_proc_array, artists_recebidos * sizeof(ArtistCount), MPI_CHAR, 
                    proc, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            // Consolida e adiciona artistas do processo
            for (int i = 0; i < artists_recebidos; i++) {
                int encontrada = 0;
                for (int j = 0; j < num_finais; j++) {
                    if (strcmp(artists_proc_array[i].artist, artists_finais[j].artist) == 0) {
                        artists_finais[j].count += artists_proc_array[i].count;
                        encontrada = 1;
                        break;
                    }
                }
                if (!encontrada) {
                    // Realoca o array final para adicionar um novo artista
                    artists_finais = (ArtistCount *)realloc(artists_finais, (num_finais + 1) * sizeof(ArtistCount));
                    if (artists_finais == NULL) {
                        fprintf(stderr, "Erro de realocação na consolidação.\n");
                        MPI_Abort(MPI_COMM_WORLD, 1);
                    }
                    artists_finais[num_finais] = artists_proc_array[i];
                    num_finais++;
                }
            }
            
            free(artists_proc_array);
        }

        // 3. Ordenar e Imprimir Ranking
        qsort(artists_finais, num_finais, sizeof(ArtistCount), comparar_contagem);

        end_time = MPI_Wtime();

        printf("\n---- RANKING FINAL DE ARTISTAS ----\n");
        printf("Total de músicas processadas: %d\n", num_linhas);
        printf("Artistas únicos (consolidados): %d\n", num_finais);
        printf("Tempo total de execução: %.4f segundos\n", end_time - start_time);
        
        printf("\n--- Top 100 Artistas ---\n");
        printf("  Pos. | Contagem | Artista\n");
        printf("-------|----------|--------------------------\n");

        for (int i = 0; i < 100 && i < num_finais; i++) {
            printf(" %4d | %8d | %s\n", i + 1, artists_finais[i].count, artists_finais[i].artist);
        }
        
        // Liberar memória
        for (int i = 0; i < num_linhas; i++) {
            free(linhas[i]);
        }
        free(linhas);
        free(artists_finais);
        
    } else {
        // Outros processos enviam seus resultados para o Rank 0
        // 1. Envia o número de artistas únicos
        MPI_Send(&num_artists_local, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        
        // 2. Envia o array de artistas (usa MPI_CHAR para enviar a estrutura como um bloco de bytes)
        MPI_Send(artists_locais, num_artists_local * sizeof(ArtistCount), MPI_CHAR, 
                0, 1, MPI_COMM_WORLD);
    }
    
    // Liberar memória das estruturas de dados locais em todos os processos
    for (int i = 0; i < HASH_SIZE; i++) {
        ArtistNode *node = hash_table[i];
        while (node) {
            ArtistNode *next = node->next;
            free(node->name);
            free(node);
            node = next;
        }
    }
    free(hash_table);
    free(artists_locais);
    
    MPI_Finalize();
    return 0;
}