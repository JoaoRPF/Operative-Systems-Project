#include <stdlib.h>
#include <stdio.h>
#include <buffer.h>
#include <shard.h>
#include <semaphore.h>
#include <string.h>
#include <pthread.h>

#define NUM_THREADS 25 //numero de tarefas, tamanho do vetor buffer

int hash(char*key);

extern shard_t ** vetorShards; //incializa o vetor de shards


void server_get(buffer*aux, int shardId, char* key) {
	buffer*aux2;
	aux2=aux;
	shard_t* d;
	char *resposta;
	resposta=(char*)malloc(KV_SIZE*sizeof(char));
	int index;
	d=vetorShards[shardId];
	index=hash(key);
	resposta=list_lookup(d->array[index], key,shardId);
	aux2->resposta=resposta;
}



void server_put(buffer*aux, int shardId, char* key, char* value) {
	buffer*aux2;
	aux2=aux;
	char *resposta;
	resposta=(char*)malloc(KV_SIZE*sizeof(char));
	shard_t * t;
	int index;
	t=vetorShards[shardId];
	index=hash(key);
	resposta=list_insert(t->array[index], key, value,shardId);
	aux2->resposta=resposta;
}

void server_remove(buffer*aux, int shardId, char* key) {
	buffer*aux2;
	aux2=aux;
	shard_t * e;
	int index;
	char *resposta;
	resposta=(char*)malloc(KV_SIZE*sizeof(char));
	e=vetorShards[shardId];
	index=hash(key);
	resposta=list_remove(e->array[index], key,shardId);
	aux2->resposta=resposta;
}
	
void server_getAllKeys(buffer*aux1, int shardId, int dim) 
{
	buffer*aux2;
	aux2=aux1;
	shard_t*k;
	k=vetorShards[shardId];
	int i, j;
	int nova_dim=0;
	for(i=0;i<HT_SIZE;i++)
		nova_dim+=totalKeys(k->array[i]);
	aux2->dim=nova_dim;
	KV_t* vetor, *aux;
	vetor = (KV_t*)(malloc(nova_dim*sizeof(KV_t))); 
	aux = (KV_t*)(malloc(nova_dim*sizeof(KV_t))); 
	for (j=0;j<HT_SIZE;j++) 
		aux=list_getAllKeys(k->array[j],vetor,nova_dim,shardId);
	aux2->arrayKeys=aux;
}

void insereEmFicheiro(int shardid, char *key, char *value){
	shard_t *aux;
	aux=vetorShards[shardid];
	int index=hash(key);
	char file[1000];
	sprintf(file,"f%d.txt",shardid);
	char *existe = list_lookup(aux->array[index], key,shardid);
	if(existe == NULL){
		FILE *pFile = fopen(file,"w");
			if (!pFile){
				exit (1);
			}
			fprintf(pFile,"%s %s\n", key, value);
			fclose(pFile);
	}
	else{
		FILE *pFile= fopen(file,"a");
			if (!pFile){
    	    exit (1);
    	}
      char keyInserir[KV_SIZE], valueInserir[KV_SIZE];
			while(fscanf(pFile,"%s %s", keyInserir, valueInserir) == 2){
				if(strcmp(keyInserir, key) != 0)
					fprintf(pFile,"%s %s\n", keyInserir, valueInserir);
			}
			fclose(pFile);
	}
}

void removeFicheiro(int shardid, char *key){
	shard_t *aux;
	aux=vetorShards[shardid];
	int index=hash(key);
	char *existe = list_lookup(aux->array[index], key,shardid);
	char* buraco = calloc(KV_SIZE,sizeof(char));
	if(existe != NULL){
		char file[1000];
		sprintf(file,"f%d",shardid);
		FILE* pFile= fopen(file,"w+");
		if (!pFile){
			exit (1);
			}
		char keyInserir[KV_SIZE], valueInserir[KV_SIZE];
		while(fscanf(pFile,"%s %s", keyInserir, valueInserir) == 2){
			if(strcmp(keyInserir, key) == 0){
				fprintf(pFile,"%s %s\n", keyInserir, buraco);
			}
		}
		fclose(pFile);
	}
}



