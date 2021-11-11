#include "../include/utils.h"
#define PERM 0666
#define WORDSIZE 50	//guess of possible word size inside input file
#define PATH "."

char *getChunkData(int mapperID) {
	struct msgBuffer msg;
    int messageQueueID;
    key_t key;
    key = ftok(PATH, 4061);
    //open message queue
    messageQueueID = msgget(key, PERM | IPC_CREAT);
    if(messageQueueID < 0) {
        perror("msgget error\n");
        exit(1);
    }
    if(msgrcv(messageQueueID, &msg, sizeof(msg.msgText), mapperID, 0)<0){
		perror("msgrcv error\n");
		exit(1);
	}
	char *chunk = malloc(sizeof(char) * MSGSIZE);
	strcpy(chunk, msg.msgText);
    if (strcmp(msg.msgText, "END") == 0) {
        return NULL;
    }
	return chunk; 
}

// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers) {
	key_t key=ftok(PATH, 4061);
	struct msgBuffer msg;
	int msgid;
	if((msgid=msgget(key, IPC_CREAT|PERM))<0){
		perror("msgget error");
	}
	FILE* fp=fopen(inputFile,"r");
	char chunk[chunkSize+1];
	memset(chunk,'\0',chunkSize);
	char word[WORDSIZE];//word holder to hold each word
	long i=0;//mapperID
	int check=1;//serves as bool to check if the last chunk sent is full sized 1024B
	while(fscanf(fp,"%s",word)>0){
		//check if plus the added last word passes 1024B
		if((strlen(word)+strlen(chunk))>chunkSize){
			//retrieve back the file pointer position
			fseeko(fp,-strlen(word)-1,SEEK_CUR);
            i++;
            //checking if it is the last mapper to send
            if(i==nMappers){
				msg.msgType=i;
				i--;
			}else{
				msg.msgType = i % nMappers;
			}
            strcpy(msg.msgText,chunk);
            //sending chunk to mapper
            if(msgsnd(msgid,&msg,sizeof(msg.msgText),0)<0){
				perror("msgsnd error");
			}
			//refresh chunk
			memset(chunk,'\0',chunkSize);
			check=0;
		}else if((strlen(word)+strlen(chunk))==chunkSize){
			//if plus the added last word equal to exactly 1024B 
			strcat(chunk,word);
			i++;
			//checking if it is the last mapper to send
			if(i==nMappers){
				msg.msgType=i;
				i--;
			}else{
				msg.msgType = i % nMappers;
			}
			strcpy(msg.msgText,chunk);	
			if(msgsnd(msgid,&msg,sizeof(msg.msgText),0)<0){
				perror("msgsnd error");
			}
			memset(chunk,'\0',chunkSize);	
			check=1;		
		}else{
			//keep concatenating words into chunk if size still under 1024
			strcat(chunk,word);
			strcat(chunk," ");
			check=0;
		}
	}
	fclose(fp);//closing file
	//sending to the last mapper
	if(check==0){ 
		i++;
		msg.msgType = i;
		strcpy(msg.msgText,chunk);
		if(msgsnd(msgid,&msg,sizeof(msg.msgText),0)<0){
			perror("msgsnd error");
		}
	}
	//sending END
	for(i=1;i<=nMappers;i++){
		msg.msgType=i;
		strcpy(msg.msgText,"END");
		if(msgsnd(msgid,&msg,sizeof(msg.msgText),0)<0){
			perror("msgsnd error");
		}
	}
}/*
char *getChunkData(int mapperID) {
	struct msgBuffer msg;
    int messageQueueID;
    key_t key;
    key = ftok(PATH, 4061);
    //open message queue
    messageQueueID = msgget(key, PERM | IPC_CREAT);
    if(messageQueueID < 0) {
        perror("msgget error\n");
        exit(1);
    }
    msgrcv(messageQueueID, &msg, sizeof(msg.msgText), mapperID, 0);
	char *chunk = malloc(sizeof(char) * MSGSIZE);
	strcpy(chunk, msg.msgText);
    if (strcmp(msg.msgText, "END") == 0) {
        return NULL;
    }
	return chunk; 
}

// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers) {
	key_t key=ftok(PATH, 4061);
	struct msgBuffer msg;
	int msgid;
	if((msgid=msgget(key, IPC_CREAT|PERM))<0){
		perror("msgget error");
	}
	FILE* fp=fopen(inputFile,"r");
	char chunk[chunkSize+1];
	memset(chunk,'\0',chunkSize);
	char word[WORDSIZE];
	long i=0;//mapperID
	int check=1;//serves as bool to check if the last chunk sent is full sized 1024B
	//printf("start while\n");
	while(fscanf(fp,"%s",word)>0){
		if((strlen(word)+strlen(chunk))>chunkSize){
			fseeko(fp,-strlen(word)-1,SEEK_CUR);
            i++;
            //checking if it is the last mapper to send
            if(i==nMappers){
				msg.msgType=i;
				i--;
			}else{
				msg.msgType = i % nMappers;
			}
			//msg.msgType = i % nMappers;
            strcpy(msg.msgText,chunk);
            msgsnd(msgid,&msg,sizeof(msg.msgText),0);
			memset(chunk,'\0',chunkSize);
			check=0;
		}else if((strlen(word)+strlen(chunk))==chunkSize){
			strcat(chunk,word);
			i++;
			//checking if it is the last mapper to send
			if(i==nMappers){
				msg.msgType=i;
				i--;
			}else{
				msg.msgType = i % nMappers;
			}
			//msg.msgType = i % nMappers;
			strcpy(msg.msgText,chunk);	
			msgsnd(msgid,&msg,sizeof(msg.msgText),0);
			memset(chunk,'\0',chunkSize);	
			check=1;		
		}else{
			strcat(chunk,word);
			strcat(chunk," ");
			check=0;
		}
	}
	fclose(fp);
	//sending to the last mapper
	if(check==0){ 
		i++;
		msg.msgType = i;
		strcpy(msg.msgText,chunk);
		msgsnd(msgid,&msg,sizeof(msg.msgText),0);
	}
	//sending END
	for(i=1;i<=nMappers;i++){
		//printf("sendChunk6\n");
		msg.msgType=i;
		strcpy(msg.msgText,"END");
		msgsnd(msgid,&msg,sizeof(msg.msgText),0);
	}
}*/

// hash function to divide the list of word.txt files across reducers
//http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
	unsigned long hash = 0;
    int c;
    while ((c = *key++)!='\0')
        hash = c + (hash << 6) + (hash << 16) - hash;
    return (hash % reducers);
}

int getInterData(char *key, int reducerID) {
	//int receive;	
	struct msgBuffer msg;
	//int messageQueue;	
	key_t key1;
	key1 = ftok(PATH, 4061);
	//open message queue
	msgid = msgget(key1, PERM | IPC_CREAT);
	if(msgid < 0) {
		perror("msgget error\n");
		exit(1);
	}
	memset(msg.msgText, '\0', chunkSize);
	msgrcv(msgid, &msg, sizeof(msg.msgText), reducerID, 0);
	//strcpy(key, msg.msgText);
	strcpy(key, msg.msgText);
	if(strcmp(msg.msgText, "END") == 0){
		return 0;
	} else {
		return 1;
	}
}

void shuffle(int nMappers, int nReducers) {
	//get reducer
	struct msgBuffer msg;
	int reducerID;
    key_t key;	
	DIR* dir;
	char mapdir[chunkSize+1];	
	memset(mapdir, '\0', chunkSize);
	key = ftok(PATH, 4061);	
	//make sure dir exists
	if(dir==NULL){
        printf("The path passed is invalid");
        return;
    }
	struct dirent* entry;
	//open message queue
	msgid = msgget(key, 0666 | IPC_CREAT);
	if(msgid < 0) {
		perror("msgget\n");
		exit(1);
	}
	for(int i = 1; i <= nMappers; i++) {
		sprintf(mapdir, "output/MapOut/Map_%d/", i);
		dir = opendir(mapdir);
		//printf("dir: %s \n", mapdir);
		while ((entry = readdir(dir)) != NULL) {
			if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) {
				continue;
			}
			if (entry->d_type == DT_REG) {
                reducerID = hashFunction(entry->d_name, nReducers);
                msg.msgType = reducerID+1;
				memset(msg.msgText, '\0', chunkSize); //???
                //strcpy(msg.msgText, strcat(mapdir, entry->d_name));
				strcpy(msg.msgText, mapdir);
				strcat(msg.msgText, entry->d_name);
                msgsnd(msgid, &msg, sizeof(msg.msgText), 0);
        	}
        } 
	}
	int i;
	for(i=1;i<=nReducers;i++){
		msg.msgType=i;
		strcpy(msg.msgText,"END");
		msgsnd(msgid,&msg,sizeof(msg.msgText),0);
	}
}

// check if the character is valid for a word
int validChar(char c){
	return (tolower(c) >= 'a' && tolower(c) <='z') ||
					(c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
	char *buffer = (char *)malloc(sizeof(char) * chunkSize);
	memset(buffer, '\0', chunkSize);
	int j = 0;
	while((*i) < strlen(chunk)) {
		// read a single word at a time from chunk
		if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
			buffer[j] = '\0';
			if(strlen(buffer) > 0){
				(*i)++;
				return buffer;
			}
			j = 0;
			(*i)++;
			continue;
		}
		buffer[j] = chunk[(*i)];
		j++;
		(*i)++;
	}
	if(strlen(buffer) > 0)
		return buffer;
	return NULL;
}

void createOutputDir(){
	mkdir("output", ACCESSPERMS);
	mkdir("output/MapOut", ACCESSPERMS);
	mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	mkdir(dirName, ACCESSPERMS);
	return dirName;
}

void removeOutputDir(){
	pid_t pid = fork();
	if(pid == 0){
		char *argv[] = {"rm", "-rf", "output", NULL};
		if (execvp(*argv, argv) < 0) {
			printf("ERROR: exec failed\n");
			exit(1);
		}
		exit(0);
	} else{
		wait(NULL);
	}
}

void bookeepingCode(){
	removeOutputDir();
	sleep(1);
	createOutputDir();
}
