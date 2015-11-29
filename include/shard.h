#include <stdlib.h>
#include <stdio.h>
#include <kos_client.h>
#include <pthread.h>
#include <semaphore.h>

#define HT_SIZE 10

typedef struct lst_iitem {
   char key[KV_SIZE];
	 char value[KV_SIZE];
   struct lst_iitem *next;
} lst_iitem_t;


typedef struct {
   lst_iitem_t * first;
} list_t;


typedef struct shard_t
{
	int ShardId;
	list_t* array[HT_SIZE];
	pthread_mutex_t trinco;
	int nLeitores;
	int em_escrita;
	int leitores_espera;
	int escritores_espera;
	sem_t leitores;
	sem_t escritores;
} shard_t;




list_t* lst_new();
char *list_insert(list_t *list, char *key, char *value, int shardId);
char *list_lookup(list_t* list, char* key, int shardId);
char *list_remove(list_t *list, char *key, int shardId);
KV_t* list_getAllKeys(list_t* list, KV_t* vetor, int nova_dim, int shardId);

int totalKeys(list_t* list);

shard_t* shard_new(int shardId);
