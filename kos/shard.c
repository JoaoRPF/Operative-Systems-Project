#include <stdlib.h>
#include <stdio.h>
#include <shard.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>

#define HT_SIZE 10

extern shard_t ** vetorShards;

shard_t* shard_new(int shardId)
{
	int i;
	shard_t* s;
	s=(shard_t*)malloc(sizeof(shard_t));
	s->ShardId=shardId;
	pthread_mutex_init(&s->trinco,NULL);
	s->nLeitores=0;
	s->em_escrita=0;
	s->escritores_espera=0;
	s->leitores_espera=0;
	sem_init(&s->leitores,0,0);
	sem_init(&s->escritores,0,0);
	for(i=0; i<HT_SIZE; i++)
		s->array[i]=lst_new();
  return s;	
}

list_t* lst_new()
{
   list_t *list;
   list = (list_t*) malloc(sizeof(list_t));
   list->first = NULL;
   return list;
}

void inicia_leitura(int shardId){
	pthread_mutex_lock(&vetorShards[shardId]->trinco);
	if(vetorShards[shardId]->em_escrita == 1 || vetorShards[shardId]->escritores_espera > 0){
		vetorShards[shardId]->leitores_espera++;
		pthread_mutex_unlock(&vetorShards[shardId]->trinco);
		sem_wait(&vetorShards[shardId]->leitores);
		pthread_mutex_lock(&vetorShards[shardId]->trinco);
		vetorShards[shardId]->leitores_espera--;
	}
	else
		vetorShards[shardId]->nLeitores++;
	pthread_mutex_unlock(&vetorShards[shardId]->trinco);
}

void acaba_leitura(int shardId){
	pthread_mutex_lock(&vetorShards[shardId]->trinco);
	vetorShards[shardId]->nLeitores--;
	if(vetorShards[shardId]->nLeitores == 0 && vetorShards[shardId]->escritores_espera > 0){
		sem_post(&vetorShards[shardId]->escritores);
		vetorShards[shardId]->em_escrita = 1;
		vetorShards[shardId]->escritores_espera--;	
	}
	pthread_mutex_unlock(&vetorShards[shardId]->trinco);
}

void inicia_escrita(int shardId){
	pthread_mutex_lock(&vetorShards[shardId]->trinco);
	if(vetorShards[shardId]->em_escrita == 1 || vetorShards[shardId]->nLeitores>0 || vetorShards[shardId]->leitores_espera>0){
		vetorShards[shardId]->escritores_espera++;
		pthread_mutex_unlock(&vetorShards[shardId]->trinco);
		sem_wait(&vetorShards[shardId]->escritores);
		pthread_mutex_lock(&vetorShards[shardId]->trinco);
		vetorShards[shardId]->escritores_espera--;
	}
	vetorShards[shardId]->em_escrita = 1;
	pthread_mutex_unlock(&vetorShards[shardId]->trinco);
}
	
void acaba_escrita(int shardId){
	pthread_mutex_lock(&vetorShards[shardId]->trinco);
	vetorShards[shardId]->em_escrita = 0;
	if(vetorShards[shardId]->leitores_espera > 0){
		sem_post(&vetorShards[shardId]->leitores);
		vetorShards[shardId]->nLeitores++;
		vetorShards[shardId]->leitores_espera--;
	}
	else if(vetorShards[shardId]->escritores_espera > 0){
		sem_post(&vetorShards[shardId]->escritores);
		vetorShards[shardId]->em_escrita = 1;
		vetorShards[shardId]->escritores_espera--;
	}
	pthread_mutex_unlock(&vetorShards[shardId]->trinco);
}


char*list_insert(list_t *list, char *key, char *value,int shardId)
{
	inicia_escrita(shardId);
	char *resposta;
	resposta=(char*)malloc(KV_SIZE*sizeof(char));
	list_t* c_cabeca;
	lst_iitem_t *seguinte, *nova, *t;
	for(t = list->first; t != NULL; t = t->next)
	{
		if(strcmp(t->key, key) == 0)
		{
			strcpy(resposta, t->value);
			strcpy(t->value, value);
			acaba_escrita(shardId);
			return resposta;
		}
	}

	nova = (lst_iitem_t*) malloc(sizeof(lst_iitem_t));
	strcpy(nova->key,key);
	strcpy(nova->value,value);

	c_cabeca = list;
	seguinte = list->first;

	nova->next = seguinte; 
	c_cabeca->first = nova;
	acaba_escrita(shardId);
	return NULL;
}

char* list_lookup(list_t* list, char* key, int shardId)
{
	inicia_leitura(shardId);
	char *resposta;
	resposta=(char*)malloc(KV_SIZE*sizeof(char));
	lst_iitem_t* t;
	for(t = list->first; t != NULL; t = t->next)
	{
		if(strcmp(t->key, key) == 0)
		{
			strcpy(resposta, t->value);
			acaba_leitura(shardId);
			return resposta;
		}
	}
	acaba_leitura(shardId);
	return NULL;
}


char *list_remove(list_t *list, char *key, int shardId) {
	inicia_escrita(shardId); 
	lst_iitem_t *s_cabeca, *seguinte;
	list_t *c_cabeca;
	char *a;
	a=(char*)malloc(KV_SIZE*sizeof(char));
  if (list->first != NULL) 
  {  	
  		if (strcmp(list->first->key,key) == 0) 
	 	{ 
	 		strcpy(a,list->first->value);
			c_cabeca = list;
	  		seguinte = list->first;
			c_cabeca->first = seguinte->next; 
		 	free(seguinte);
		 	acaba_escrita(shardId);
		 	return a;
	   }//retorna o valor que a key tinha
	
	
	 	else 
	 	{
	 		s_cabeca = list->first;
			seguinte = list->first->next;

			//procura a célula cujo valor é o pretendido
			while (seguinte!=NULL && strcmp(seguinte->key,key) != 0) 
			{
				s_cabeca = seguinte;
				seguinte = seguinte->next;
			}
		
			//limpa a memória da celula removida caso esta nao seja a ultima
			if (seguinte !=NULL) 
			{
				strcpy(a,seguinte->value);
			
				s_cabeca->next = seguinte->next;
			
				free(seguinte);
				acaba_escrita(shardId);
				return a;
			}
	 	}
	 }
	 acaba_escrita(shardId);
	return NULL;
}

int totalKeys(list_t* list){
	lst_iitem_t* t;
	int i=0;
	for(t=list->first; t!= NULL; t=t->next){
		i++;
	}
	return i;
}

KV_t* list_getAllKeys(list_t* list, KV_t* vetor, int nova_dim, int shardId)
{
	inicia_leitura(shardId);
	lst_iitem_t* t;
	int i=0;
	for(t = list->first; t != NULL; t = t->next)
	{
			strcpy(vetor[i].key,t->key);
			strcpy(vetor[i].value,t->value);
			i++;
	}
	acaba_leitura(shardId);
	return vetor;
}
