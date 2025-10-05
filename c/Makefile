all: compile
	
compile:
	gcc -o mostrar_musicas mostrar_musicas.c
	gcc -o 1_1_contar_palavras_seq 1_1_contar_palavras_seq.c
	mpicc -o 1_2_contar_palavras_mpi 1_2_contar_palavras_mpi.c
	mpicc -o 2_contar_musicas_mpi 2_contar_musicas_mpi.c
	mpicc -o 3_contar_palavras_e_musicas_mpi 3_contar_palavras_e_musicas_mpi.c
	
1_run_seq_palavras:
	./1_1_contar_palavras_seq

2_run_palavras_2:
	mpirun -np 2 ./1_2_contar_palavras_mpi spotify_millsongdata_SEM_QUEBRAS_LINHA.csv

2_run_palavras_8:
	mpirun -np 8 ./1_2_contar_palavras_mpi spotify_millsongdata_SEM_QUEBRAS_LINHA.csv



3_run_musicas_2:
	mpirun -np 2 ./2_contar_musicas_mpi spotify_millsongdata_SEM_QUEBRAS_LINHA.csv

3_run_musicas_8:
	mpirun -np 8 ./2_contar_musicas_mpi spotify_millsongdata_SEM_QUEBRAS_LINHA.csv


4_run_palavras_e_musicas_mpi_2:
	mpirun -np 2 ./3_contar_palavras_e_musicas_mpi
	
4_run_palavras_e_musicas_mpi_8:
	mpirun -np 8 ./3_contar_palavras_e_musicas_mpi
	



limpa:
	clear



