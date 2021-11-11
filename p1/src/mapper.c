#include "mapper.h"
//Global space word dictionary, starts as NULL
intermediateDS* wordDictionary;

// combined value list corresponding to a word <1,1,1,1....>
valueList *createNewValueListNode(char *value){
	valueList *newNode = (valueList *)malloc (sizeof(valueList));
	strcpy(newNode -> value, value);
	newNode -> next = NULL;
	return newNode;
}

// insert new count to value list
valueList *insertNewValueToList(valueList *root, char *count){
	valueList *tempNode = root;
	if(root == NULL)
		return createNewValueListNode(count);
	while(tempNode -> next != NULL)
		tempNode = tempNode -> next;
	tempNode -> next = createNewValueListNode(count);
	return root;
}

// free value list
void freeValueList(valueList *root) {
	if(root == NULL) return;

	valueList *tempNode = NULL;
	while (root != NULL){
		tempNode = root;
		root = root -> next;
		free(tempNode);
	}
}

// create <word, value list>
intermediateDS *createNewInterDSNode(char *word, char *count){
	intermediateDS *newNode = (intermediateDS *)malloc (sizeof(intermediateDS));
	strcpy(newNode -> key, word);
	newNode -> value = NULL;
	newNode -> value = insertNewValueToList(newNode -> value, count);
	newNode -> next = NULL;
	return newNode;
}

// insert or update a <word, value> to intermediate DS
intermediateDS *insertPairToInterDS(intermediateDS *root, char *word, char *count){
	intermediateDS *tempNode = root;
	if(root == NULL)
		return createNewInterDSNode(word, count);
	while(tempNode -> next != NULL) {
		if(strcmp(tempNode -> key, word) == 0){
			tempNode -> value = insertNewValueToList(tempNode -> value, count);
			return root;
		}
		tempNode = tempNode -> next;
		
	}
	if(strcmp(tempNode -> key, word) == 0){
		tempNode -> value = insertNewValueToList(tempNode -> value, count);
	} else {
		tempNode -> next = createNewInterDSNode(word, count);
	}
	return root;
}

// free the DS after usage. Call this once you are done with the writing of DS into file
void freeInterDS(intermediateDS *root) {
	if(root == NULL) return;

	intermediateDS *tempNode = NULL;
	while (root != NULL){
		tempNode = root;
		root = root -> next;
		freeValueList(tempNode -> value);
		free(tempNode);
	}
}

// emit the <key, value> into intermediate DS 
void emit(char *key, char *value) {
	//Didn't need
}

// map function
void map(char *chunkData){
	int i = 0; //Variable to put word index
	char* buffer; //Buffer to hold next word

	while ((buffer = getWord(chunkData, &i)) != NULL) { //While there are words
		if (wordDictionary == NULL) { //If wordDictionary isn't create
			wordDictionary = createNewInterDSNode(buffer, "1");
		} else {
			insertPairToInterDS(wordDictionary, buffer, "1");
		}
	}
	// you can use getWord to retrieve words from the 
	// chunkData one by one. Example usage in utils.h
}

// write intermediate data to separate word.txt files
// Each file will have only one line : word 1 1 1 1 1 ...
void writeIntermediateDS() {
	if (wordDictionary != NULL) { //If we were given at least 1 word, because there's a chance we didn't get any words

		//Index into wordDictionary
		intermediateDS* index = wordDictionary;

		//Allocating space for path name to word.txt file
		char path[strlen(mapOutDir) + MAXKEYSZ];

		//Loop through all words in wordDictionary
		while (index != NULL) {
			//Calculate file path based on word
			sprintf(path, "%s/%s.txt", mapOutDir, index->key);

			FILE* file = fopen(path, "w");

			//Print the word first
			fprintf(file, "%s", index->key);
			
			//Create an index variable to loop through the valueList inside the wordDictionary for the current word
			valueList* indexVal = index->value;

			//Loop through all the values for the current word
			while (indexVal != NULL) {
				//Printing each value, space separated
				fprintf(file, " %s", indexVal->value);
				indexVal = indexVal->next;
			}

			index = index->next;
			fclose(file);
		}

		freeInterDS(wordDictionary);
	}
}

int main(int argc, char *argv[]) {

	if (argc < 2) {
		printf("Less number of arguments.\n");
		printf("./mapper mapperID\n");
		exit(0);
	}
	// ###### DO NOT REMOVE ######
	mapperID = strtol(argv[1], NULL, 10);

	// ###### DO NOT REMOVE ######
	// create folder specifically for this mapper in output/MapOut
	// mapOutDir has the path to the folder where the outputs of 
	// this mapper should be stored
	mapOutDir = createMapDir(mapperID);

	// ###### DO NOT REMOVE ######
	while(1) {
		// create an array of chunkSize=1024B and intialize all 
		// elements with '\0'
		char chunkData[chunkSize + 1]; // +1 for '\0'
		memset(chunkData, '\0', chunkSize + 1);

		char *retChunk = getChunkData(mapperID);
		if(retChunk == NULL) {
			break;
		}

		strcpy(chunkData, retChunk);
		free(retChunk);

		map(chunkData);
	}

	// ###### DO NOT REMOVE ######
	writeIntermediateDS();

	return 0;
}