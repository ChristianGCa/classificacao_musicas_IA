all: compile
	
compile:
	gcc -o mostrar_musicas mostrar_musicas.c
	gcc -o contar_palavras contar_palavras.c
	mpicc -o mpi_contar_palavras mpi_contar_palavras.c
	
run_mpi:
	mpirun -np 2 ./mpi_contar_palavras
	
run_mpi_8:
	mpirun -np 8 ./mpi_contar_palavras
	
run_seq:
	./contar_palavras
	
limpa:
	clear



