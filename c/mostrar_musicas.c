#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE 2048

int main() {
    FILE *file_pointer;
    char linha[MAX_LINE];

    file_pointer = fopen("/home/christian/Documentos/CSVs_processados/spotify_millsongdata_SEM_QUEBRAS_LINHA.csv", "r");
    if (file_pointer == NULL) {
        perror("Erro ao abrir arquivo");
        return 1;
    }

    // Lê a primeira linha e descarta
    fgets(linha, MAX_LINE, file_pointer);

    while (fgets(linha, MAX_LINE, file_pointer)) {

        linha[strcspn(linha, "\n")] = 0;

        char *artist = strtok(linha, ",");
        char *song   = strtok(NULL, ",");
        char *link   = strtok(NULL, ",");
        char *text   = strtok(NULL, ",");

        if (artist && song && text) {
            printf("Artista: %s | Música: %s | Letra: %.1024s...\n",
                   artist, song, text);
        }
    }

    fclose(file_pointer);
    return 0;
}