all: table2bin

table2bin: table2bin.c
	gcc -pedantic -std=c99 -o table2bin table2bin.c -I/usr/include/postgresql -L/usr/lib/postgresql -lpq  

