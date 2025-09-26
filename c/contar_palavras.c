#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define MAX_LINE 8192
#define MAX_WORD 128
#define HASH_SIZE 100003  // número primo para tabela hash

// Estrutura para armazenar palavra e contagem
typedef struct Word {
    char *text;
    int count;
    struct Word *next;
} Word;

Word *hash_table[HASH_SIZE];

// Função hash simples
unsigned long hash(const char *str) {
    unsigned long h = 5381;
    int c;
    while ((c = *str++)) {
        h = ((h << 5) + h) + c;
    }
    return h % HASH_SIZE;
}

// Adiciona palavra no hash
void add_word(const char *w) {
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
    // nova palavra
    Word *new_word = malloc(sizeof(Word));
    new_word->text = strdup(w);
    new_word->count = 1;
    new_word->next = hash_table[idx];
    hash_table[idx] = new_word;
}

// Normaliza palavra (tudo minúsculo e remove pontuação nas bordas)
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

// Divide texto em palavras e adiciona ao contador
void process_text(char *text) {
    char *token = strtok(text, " \t\r\n");
    while (token) {
        char buffer[MAX_WORD];
        strncpy(buffer, token, MAX_WORD - 1);
        buffer[MAX_WORD - 1] = '\0';
        normalize(buffer);
        add_word(buffer);
        token = strtok(NULL, " \t\r\n");
    }
}

// Estrutura auxiliar para ordenar
typedef struct {
    char *word;
    int count;
} WordCount;

int compare(const void *a, const void *b) {
    return ((WordCount*)b)->count - ((WordCount*)a)->count;
}

int main() {
    FILE *file_pointer;
    char linha[MAX_LINE];
    int total_palavras = 0;
    int total_linhas = 0;

    file_pointer = fopen("/home/christian/Documentos/CSVs_processados/spotify_millsongdata_SEM_QUEBRAS_LINHA.csv", "r");
    if (file_pointer == NULL) {
        perror("Erro ao abrir arquivo");
        return 1;
    }

    // Lê a primeira linha e descarta (cabeçalho)
    fgets(linha, MAX_LINE, file_pointer);
    total_linhas = 1; // Cabeçalho

    while (fgets(linha, MAX_LINE, file_pointer)) {
        linha[strcspn(linha, "\n")] = 0;

        char *artist = strtok(linha, ",");
        char *song   = strtok(NULL, ",");
        char *link   = strtok(NULL, ",");
        char *text   = strtok(NULL, ",");

        if (text) {
            total_linhas++; // Só conta linhas que têm texto válido
            // Conta palavras antes de processar
            char *temp_text = strdup(text);
            char *token = strtok(temp_text, " \t\r\n");
            while (token) {
                total_palavras++;
                token = strtok(NULL, " \t\r\n");
            }
            free(temp_text);
            
            process_text(text);
        }
    }

    fclose(file_pointer);

    // Joga tudo em vetor para ordenar
    WordCount *arr = malloc(sizeof(WordCount) * 100000);
    int n = 0;

    for (int i = 0; i < HASH_SIZE; i++) {
        Word *node = hash_table[i];
        while (node) {
            arr[n].word = node->text;
            arr[n].count = node->count;
            n++;
            node = node->next;
        }
    }

    qsort(arr, n, sizeof(WordCount), compare);

    printf("\n=== ESTATÍSTICAS GERAIS ===\n");
    printf("Linhas de dados processadas: %d\n", total_linhas - 1);
    printf("Total de palavras processadas: %d\n", total_palavras);
    printf("Palavras únicas encontradas: %d\n", n);
    printf("\n=== Ranking das palavras ===\n");
    for (int i = 0; i < 50 && i < n; i++) { // mostra top 50
        printf("%d. %s -> %d\n", i + 1, arr[i].word, arr[i].count);
    }

    free(arr);
    return 0;
}
