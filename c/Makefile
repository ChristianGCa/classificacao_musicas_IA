all: compile
	
compile:
	gcc -o mostrar_musicas mostrar_musicas.c
	gcc -o contar_palavras contar_palavras.c
	mpicc -o contar_palavras_mpi contar_palavras_mpi.c
	mpicc -o contar_palavras_e_musicas_mpi contar_palavras_e_musicas_mpi.c
	mpicc -o contar_musicas_mpi contar_musicas_mpi.c
	
run_seq_palavras:
	./contar_palavras

run_palavras_2:
	mpirun -np 2 ./contar_palavras_mpi spotify_millsongdata_SEM_QUEBRAS_LINHA.csv



run_musicas_2:
	mpirun -np 2 ./contar_musicas_mpi spotify_millsongdata_SEM_QUEBRAS_LINHA.csv


run_palavras_e_musicas_mpi_2:
	mpirun -np 2 ./contar_palavras_e_musicas_mpi
	
run_palavras_e_musicas_mpi_8:
	mpirun -np 8 ./contar_palavras_e_musicas_mpi
	



limpa:
	clear



