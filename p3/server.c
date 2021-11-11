#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/time.h>
#include <time.h>
#include "util.h"
#include <stdbool.h>
#include <unistd.h>
#include <signal.h>
#include <stdint.h>

#define MAX_THREADS 100
#define MAX_queue_len 100
#define MAX_CE 100
#define INVALID -1
#define BUFF_SIZE 1024

/*
  THE CODE STRUCTURE GIVEN BELOW IS JUST A SUGGESTION. FEEL FREE TO MODIFY AS NEEDED
*/
FILE *log_file;
pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond1 = PTHREAD_COND_INITIALIZER;
int full = 1; //0 is not full and 1 is full

int start=0,end=0;
/*
  THE CODE STRUCTURE GIVEN BELOW IS JUST A SUGGESTION. FEEL FREE TO MODIFY AS NEEDED
*/
// structs:
typedef struct request_queue {
   int fd;
   char *request;
} request_t;

request_t rq[MAX_queue_len];

typedef struct cache_entry {
    int len;
    char *request;
    char *content;
} cache_entry_t;

request_t *queue; //request queue to add and revive from 
int qlen; //length of queue
//file
char filename[BUFF_SIZE]; //file
int fd; //file descripter

static volatile sig_atomic_t doneflag = 0;
static void setdoneflag(int signo) {
  doneflag = 1;
}

/* ******************** Dynamic Pool Code  [Extra Credit A] **********************/
// Extra Credit: This function implements the policy to change the worker thread pool dynamically
// depending on the number of requests
void * dynamic_pool_size_update(void *arg) {
  while(1) {
    // Run at regular intervals
    // Increase / decrease dynamically based on your policy
  }
}
/**********************************************************************************/

/* ************************ Cache Code [Extra Credit B] **************************/

// Function to check whether the given request is present in cache
int getCacheIndex(char *request){
  /// return the index if the request is present in the cache
  return 0;
}

// Function to add the request and its file content into the cache
void addIntoCache(char *mybuf, char *memory , int memory_size){
  // It should add the request at an index according to the cache replacement policy
  // Make sure to allocate/free memory when adding or replacing cache entries
}

// clear the memory allocated to the cache
void deleteCache(){
  // De-allocate/free the cache memory
}

// Function to initialize the cache
void initCache(){
  // Allocating memory and initializing the cache array
}

/**********************************************************************************/

/* ************************************ Utilities ********************************/
// Function to get the content type from the request
char* getContentType(char * mybuf) {
  // Should return the content type based on the file type in the request
  // (See Section 5 in Project description for more details)
  char *files;
  //get the file ending from mybuf that match with our file endings 
  if ( ((files = strstr(mybuf, ".html")) != NULL) || ((files = strstr(mybuf, ".htm")) != NULL) ) {
    return  "text/html";
  } else if ( (files = strstr(mybuf, ".jpg")) != NULL ) {
    return  "image/jpeg";
  } else if ( (files = strstr(mybuf, ".gif")) != NULL)  {
    return  "image/jpeg";
  } else if((files = strstr(mybuf, ".txt")) != NULL){
    return "txt/plain";
  } else{
	return NULL;
	}
}

// Function to open and read the file from the disk into the memory
// Add necessary arguments as needed
//Code source:http://www.cplusplus.com/reference/cstdio/fread/
int readFromDisk(char *filename, char **buffer) {
    // Open and read the contents of file given the request
    long read;
    long lSize;
    FILE *pFile;
    //FILE *file;
    //file = fopen(pFile, "r");
    pFile = fopen (filename , "r");
    if (pFile==NULL) {
		fputs ("File error",stderr); 
		return -1;
		//exit (1);
	}
   
    fseek (pFile , 0 , SEEK_END);
    lSize = ftell (pFile);
    rewind (pFile);
    
    *buffer = (char*) malloc (sizeof(char)*lSize);    
    if (buffer == NULL) {fputs ("Memory error",stderr); exit (2);}

    read = fread(*buffer, 1, lSize, pFile);
    fclose(pFile);
    return read;
    
}


/**********************************************************************************/
  

// Function to receive the request from the client and add to the queue
void * dispatch(void *arg) {
	
  while(1){
      char filename[BUFF_SIZE];
      int fd = accept_connection();
      //printf("ID: %d\n", fd);
      int getreq=get_request(fd, filename);
      //printf("dispatch loop1\n");
      if(fd > INVALID && (getreq==0)){
		  request_t r;
		    r.fd=fd;
		    r.request=malloc(sizeof(filename));
		    sprintf(r.request,".%s",filename);
		    pthread_mutex_lock(&mtx);
		    while(end==qlen){//keep waiting for space if the queue is full,not sure about the condition ???
				pthread_cond_wait(&cond1,&mtx);
			}
        
		    rq[end]=r;
		    end++;
		    end%=MAX_queue_len;
        
			pthread_cond_signal(&cond1);
		    pthread_mutex_unlock(&mtx);
      }
      //if(getreq!=0){
		 // return_error(fd,"
  }
    return NULL;

}

/**********************************************************************************/

// Function to retrieve the request from the queue, process it and then return a result to the clie
int requested = 0;
void * worker(void *arg) {
  //sleep(10);
  char *content_type;
  //int tid = *(int *) arg;
  int tid=(intptr_t)arg;
  //printf("%d\n",tid);
  request_t thing;

	//int requested = 0;
  //grab from queue add sync latter, read from disk, return what ever read from df
  long size= 0;
	char *buf=NULL;
   while (1) {

  // Get the request from the queue
	pthread_mutex_lock(&mtx);
	while(start==end){
		pthread_cond_wait(&cond1,&mtx);//keep waiting until there is a request in the queue
	}
	thing=rq[start];//first thing in the request queue
	start++;
	start%=MAX_queue_len;
	//log_file=fopen("../webserver_log","w");
	pthread_cond_signal(&cond1);
	pthread_mutex_unlock(&mtx);
	requested++;//count of num requested
    // Get the data from the disk or the cache (extra credit B)
    //puts("Here");
    // Log the request into the file and terminal
    size=readFromDisk(thing.request,&buf);
    if(size==0){
		return_error(thing.fd,"no such file");
		fprintf(log_file, "[%d][%d][%d][%s][%s][MISS]\n",tid,requested,thing.fd,thing.request,"file not found");
		printf("[%d][%d][%d][%s][%s][MISS]\n",tid,requested,thing.fd,thing.request,"file not found");
		continue;
	}
	content_type=getContentType(thing.request);
	if(content_type==NULL){//if not valid content type
		return_error(thing.fd,"no such file type");
		free(buf);
		fprintf(log_file, "[%d][%d][%d][%s][%s][MISS]\n",tid,requested,thing.fd,thing.request,"file type not found");
		printf("[%d][%d][%d][%s][%s][MISS]\n",tid,requested,thing.fd,thing.request,"file type not found");
		continue;
	}else{
		return_result(thing.fd,content_type,buf,size);
	}

    //if(log_file != NULL){
      fprintf(log_file, "[%d][%d][%d][%s][%ld]\n",tid,requested,thing.fd,thing.request,size);
      printf("[%d][%d][%d][%s][%ld]\n",tid,requested,thing.fd,thing.request,size);
      free(buf);
    //} 
  }
  return NULL;
}

/**********************************************************************************/

int main(int argc, char **argv) {

  // Error check on number of arguments
  if(argc != 8){
    printf("usage: %s port path num_dispatcher num_workers dynamic_flag queue_length cache_size\n", argv[0]);
    return -1;
  }
  //step 9
  // Get the input args
  //0 is the server
  //input 1
  int port = atoi(argv[1]);
  //input 2 Change the current working directory to server root directory
  int path = chdir(argv[2]);
  //input 3
  int num_dispatcher = atoi(argv[3]);
  //input 4
  int num_workers = atoi(argv[4]);
  //input 5
  int dynamic_flag = atoi(argv[5]);
  //input 6
  qlen = atoi(argv[6]);
  //input 7
  int cache_entries = atoi(argv[7]);


  // Perform error checks on the input arguments
  //check for port
   if (port < 1025 || port > 65535){
      printf("Invalid port: %d \n", port);
      exit(INVALID);
   }
   
  // Check for valid path
   if(path == INVALID){
     printf("Path Invalid: %d \n", path);
      exit(INVALID);
   }

   //check for dispathcer
   if(num_dispatcher > MAX_THREADS){
     printf("Invalid num dispathcers: %d \n", num_dispatcher);
     exit(INVALID);
   }
   //check for workers
   if(num_workers > MAX_THREADS){
     printf("Invalid num workers: %d \n", num_workers);
     exit(INVALID);
   }

   //check for flag
   if (dynamic_flag < 0){
     printf("Invalid flag: %d \n", dynamic_flag);
     exit(INVALID);
   }
   else if (dynamic_flag > 1){
     printf("Invalid flag: %d \n", dynamic_flag);
     exit(INVALID);
   }

   //check for queue length
   if (qlen > MAX_queue_len) {
     printf("Invalid queue length: %d \n", qlen);
     exit(INVALID);
   }
   //check for cache entries
   if (cache_entries > MAX_CE) {
     printf("Invalid cache entries: %d \n", cache_entries);
     exit(INVALID);
   }


  log_file = fopen("../webserver_log", "w");  //write log info into file

  // Initialize cache (extra credit B)

  // Start the server
  init (port); // run once
  //dispatcher and worker ID arrays
  pthread_t dtid[num_dispatcher]; 
  pthread_t wtid[num_workers]; 
  //for loop here to populate into 0 -nworkers -1
  int workerId[100]; 
	// int i;
  //pthread_create(&dtid, NULL, dispatch, NULL);
  //pthread_detach(dtid);
     // printf("Main1\n");

  //pthread_create(&wtid, NULL, worker,  (void *) wtid);
      //printf("Main2\n");

  //pthread_detach(wtid);
	
  for(int i=0;i<num_dispatcher;i++){
		pthread_create(&dtid[i], NULL, dispatch, NULL);
		pthread_detach(dtid[i]);
	}
  
	for(int i=0;i<num_workers;i++){
    workerId[i] = i;
		pthread_create(&wtid[i], NULL, worker, (void *) (intptr_t) workerId[i]);
		pthread_detach(wtid[i]);
	} 
  
  //
  // Create dynamic pool manager thread (extra credit A)

  // Terminate server gracefully
  // Change SIGINT action for grace termination

  sigset_t intmask;
  sigemptyset(&intmask);
  
  // add SIGINT to sigset - sigaddset
  sigaddset(&intmask, SIGINT);

  struct sigaction act;
  act.sa_handler = setdoneflag; //set up signal handler
  act.sa_flags = 0;
  if ((sigemptyset(&act.sa_mask) == -1) ||
    (sigaction(SIGINT, &act, NULL) == -1)) {
     perror("Failed to set SIGINT handler");
     return 1;
  }

  while(!doneflag) {
    sleep(10);
  }
  // Print the number of pending requests in the request queue
  printf("Server terminating ...\nThe number of pending requests in the request queue: %d\n", cache_entries);
  printf("Main thread exiting.\n");
  //free(buffer);
  // close log file
  fclose(log_file);
  // Remove cache (extra credit B)

    return 0;
  } 
