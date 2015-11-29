#include <stdlib.h>
#include <stdio.h>
#include <kos_client.h>
#include <semaphore.h>

#define KV_SIZE 20

typedef struct buffer {
	 char key[KV_SIZE];
	 char value[KV_SIZE]; 
	 char nomeFuncao[1];
	 int shardId;
	 int clientid;
	 int dim;
	 char* resposta;
	 KV_t* arrayKeys;
	 sem_t semaforo_resposta;
}buffer;



