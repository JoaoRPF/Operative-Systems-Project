#include <kos_client.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <shard.h>
#include <bufferFirst.h>


#define NUM_THREADS 25 //numero de tarefas, tamanho do vetor buffer
#define TRUE 1

void* thr_servidora();
void server_get(buffer*aux, int shardId, char* key);
void server_put(buffer*aux, int shardId, char* key, char* value);
void server_remove(buffer*aux, int shardId, char* key);
void server_getAllKeys(buffer*aux1, int shardId, int dim);
void insereEmFicheiro(int shardid, char *key, char *value);
void removeFicheiro(int shardid, char *key);

bufferFirst *bf;
shard_t ** vetorShards; //incializa o vetor de shards
int total_shards; 
int tamanho_buf;
int podeServidor=0;
int podeClient=0;
sem_t semaforo_cliente;
sem_t semaforo_servidora;
pthread_mutex_t servidor;
pthread_mutex_t acesso;
pthread_mutex_t client;


int kos_init(int num_server_threads, int buf_size, int num_shards)
{
	int i,q;
	total_shards=num_shards;
	tamanho_buf=buf_size;
	bf=(bufferFirst*)(malloc(buf_size*sizeof(bufferFirst)));
	for(i=0; i<tamanho_buf;i++){
		bf[i].buf=NULL;
	}
	pthread_t servidora[num_server_threads];
	sem_init(&semaforo_cliente,0,buf_size);
	sem_init(&semaforo_servidora,0,0);
	pthread_mutex_init(&servidor,NULL);
	pthread_mutex_init(&client,NULL);
	pthread_mutex_init(&acesso,NULL);
	vetorShards=(shard_t**)(malloc(num_shards*sizeof(shard_t*)));
	for(i=0; i<num_shards; i++)
		vetorShards[i]=shard_new(i); 
	for(q=0; q<num_server_threads;q++)
	{
		if (pthread_create(&servidora[q], NULL, thr_servidora, NULL)) 
			return -1;
	}
	return 0;
}



char* kos_get(int clientid, int shardId, char* key) 
{
	if(clientid<0 || shardId>total_shards || shardId<0 || key==NULL)
		return NULL;
	char g[]="g";
	char *resposta;
	resposta=(char*)malloc(KV_SIZE*sizeof(char));
	buffer*aux; 		
	sem_wait(&semaforo_cliente);
	pthread_mutex_lock(&client);
	aux=(buffer*)malloc(sizeof(buffer));
	sem_init(&aux->semaforo_resposta,0,0);
	aux->shardId=shardId;
	strcpy(aux->key,key);
	strcpy(aux->nomeFuncao,g);
	bf[podeClient].buf=aux;
	podeClient=(podeClient+1)%tamanho_buf;
	pthread_mutex_unlock(&client);
	sem_post(&semaforo_servidora);
	sem_wait(&aux->semaforo_resposta);
	if(aux->resposta!=NULL)
		strcpy(resposta,aux->resposta);
	else
		resposta=aux->resposta;
	free(aux);
	aux=NULL;
	return resposta;
}


char* kos_put(int clientid, int shardId, char* key, char* value) 
{
		if(clientid<0 || shardId>total_shards || shardId<0 || key==NULL  || value==NULL)
		return NULL;
	char p[]="p";
	char *resposta;
	resposta=(char*)malloc(KV_SIZE*sizeof(char));
	buffer*aux; 		
	sem_wait(&semaforo_cliente);
	pthread_mutex_lock(&client);
	aux=(buffer*)malloc(sizeof(buffer));
	sem_init(&aux->semaforo_resposta,0,0);
	aux->shardId=shardId;
	strcpy(aux->key,key);
	strcpy(aux->value,value);
	strcpy(aux->nomeFuncao,p);
	bf[podeClient].buf=aux;
	podeClient=(podeClient+1)%tamanho_buf;
	pthread_mutex_unlock(&client);
	sem_post(&semaforo_servidora);
	sem_wait(&aux->semaforo_resposta);
	if(aux->resposta!=NULL)
		strcpy(resposta,aux->resposta);
	else
		resposta=aux->resposta;
	free(aux);
	aux=NULL;
	return resposta;
}

char* kos_remove(int clientid, int shardId, char* key) 
{
		if(clientid<0 || shardId>total_shards || shardId<0 || key==NULL)
		return NULL;
	char r[]="r";
	char *resposta;
	resposta=(char*)malloc(KV_SIZE*sizeof(char));
	buffer*aux; 		
	sem_wait(&semaforo_cliente);
	pthread_mutex_lock(&client);
	aux=(buffer*)malloc(sizeof(buffer));
	sem_init(&aux->semaforo_resposta,0,0);
	aux->shardId=shardId;
	strcpy(aux->key,key);
	strcpy(aux->nomeFuncao,r);
	bf[podeClient].buf=aux;
	podeClient=(podeClient+1)%tamanho_buf;
	pthread_mutex_unlock(&client);
	sem_post(&semaforo_servidora);
	sem_wait(&aux->semaforo_resposta);
	if(aux->resposta!=NULL)
		strcpy(resposta,aux->resposta);
	else
		resposta=aux->resposta;
	free(aux);
	aux=NULL;
	return resposta;
}


KV_t* kos_getAllKeys(int clientid, int shardId, int* dim) 
{
	if(clientid<0 || shardId>total_shards || shardId<0)
		return NULL;
	char k[]="k";
	KV_t* auxiliar;
	buffer* aux;
	sem_wait(&semaforo_cliente);
	pthread_mutex_lock(&client);
	aux=(buffer*)malloc(sizeof(buffer));
	sem_init(&aux->semaforo_resposta,0,0);
	aux->shardId=shardId;
	strcpy(aux->nomeFuncao,k);
	bf[podeClient].buf=aux;
	podeClient=(podeClient+1)%tamanho_buf;
	pthread_mutex_unlock(&client);
	sem_post(&semaforo_servidora);
	sem_wait(&aux->semaforo_resposta);
	*dim=aux->dim;
	auxiliar=aux->arrayKeys;
	free(aux);
	aux=NULL;
	return auxiliar;
}



void* thr_servidora()   
{
	while (TRUE)
	{
		sem_wait(&semaforo_servidora);
		buffer*aux;
		pthread_mutex_lock(&servidor);
		char *k,*v, *a;
		k=(char*)malloc(KV_SIZE*sizeof(char));
		v=(char*)malloc(KV_SIZE*sizeof(char));
		int ir;
		int d;
		aux=bf[podeServidor].buf;
		a=aux->nomeFuncao;
		ir=aux->shardId;
		k=aux->key;
		v=aux->value;
		d=aux->dim;
		podeServidor=(podeServidor+1)%tamanho_buf;
		pthread_mutex_unlock(&servidor);
		sem_post(&semaforo_cliente);
		pthread_mutex_lock(&acesso);
		if(strcmp(a,"g")==0)
		{
 			server_get(aux, ir, k);
 		}
 		if(strcmp(a,"p")==0)
 		{
 			server_put(aux, ir, k,v);
 			insereEmFicheiro(ir,k,v);
 			
 		}
		if(strcmp(a,"r")==0){
 			server_remove(aux, ir, k);
 			removeFicheiro(ir,k);
 		}
 		if(strcmp(a,"k")==0)
 			server_getAllKeys(aux, ir, d);
 		pthread_mutex_unlock(&acesso);
		sem_post(&aux->semaforo_resposta);
 	}
 	return NULL;
}

	
	
	
