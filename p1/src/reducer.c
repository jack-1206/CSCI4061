#include "reducer.h"
//Global space word dictionary, starts as NULL
finalKeyValueDS* wordDictionary;

// create a key value node
finalKeyValueDS *createFinalKeyValueNode(char *word, int count){
	finalKeyValueDS *newNode = (finalKeyValueDS *)malloc (sizeof(finalKeyValueDS));
	strcpy(newNode -> key, word);
	newNode -> value = count;
	newNode -> next = NULL;
	return newNode;
}

// insert or update an key value
finalKeyValueDS *insertNewKeyValue(finalKeyValueDS *root, char *word, int count){
	finalKeyValueDS *tempNode = root;
	if(root == NULL)
		return createFinalKeyValueNode(word, count);
	while(tempNode -> next != NULL){
		if(strcmp(tempNode -> key, word) == 0){
			tempNode -> value += count;
			return root;
		}
		tempNode = tempNode -> next;
	}
	if(strcmp(tempNode -> key, word) == 0){
		tempNode -> value += count;
	} else{
		tempNode -> next = createFinalKeyValueNode(word, count);
	}
	return root;
}

// free the DS after usage. Call this once you are done with the writing of DS into file
void freeFinalDS(finalKeyValueDS *root) {
	if(root == NULL) return;

	finalKeyValueDS *tempNode = NULL;
	while (root != NULL){
		tempNode = root;
		root = root -> next;
		free(tempNode);
	}
}

// reduce function
void reduce(char *key) {
	FILE* file = fopen(key, "r");

	if (file == NULL) {
		perror("Couldn't open file");
		return;
	}

	char word[MAXKEYSZ + 1];
 	if (fscanf(file, "%s", word) <= 0) {
		perror("Word not found in file");
		return;
	}

	//Stores the next number in the file, should be "1" most of the time
	int num;
	
	//While there is still another "1", or integer, in the file
	while (fscanf(file, "%d", &num) > 0) {
		if (wordDictionary == NULL) { //If wordDictionary wasn't created yet
			wordDictionary = createFinalKeyValueNode(word, num);
		} else {
			insertNewKeyValue(wordDictionary, word, num);
		}
	}
	fclose(file);
}

// write the contents of the final intermediate structure
// to output/ReduceOut/Reduce_reducerID.txt
void writeFinalDS(int reducerID){
	if (wordDictionary != NULL) { //If we were given at least 1 word, because there's a chance we didn't get any words
		//Index into wordDictionary
		finalKeyValueDS* index = wordDictionary;

		//The directory where we are writing to
		char* dir = "./output/ReduceOut/";

		//Allocating space for path name to Reduce_reducerId.txt
		char path[strlen(dir) + MAXKEYSZ];

		//Putting the path for Reduce_reducerId.txt into path
		sprintf(path, "%sReducer_%d.txt", dir, reducerID);

		FILE* file = fopen(path, "w");

		//Loop through words in wordDictionary
		while (index != NULL) {
			//Output word and number to file
			fprintf(file, "%s %d\n", index->key, index->value);

			index = index->next;
		}

		freeFinalDS(wordDictionary);

		fclose(file);
	} else {
		perror("wordDictionary not created\n");
	}
}

int main(int argc, char *argv[]) {

	if(argc < 2){
		printf("Less number of arguments.\n");
		printf("./reducer reducerID");
	}

	// ###### DO NOT REMOVE ######
	// initialize 
	int reducerID = strtol(argv[1], NULL, 10);

	// ###### DO NOT REMOVE ######
	// master will continuously send the word.txt files
	// alloted to the reducer
	char key[MAXKEYSZ];
	while(getInterData(key, reducerID))
		reduce(key);

	// You may write this logic. You can somehow store the
	// <key, value> count and write to Reduce_reducerID.txt file
	// So you may delete this function and add your logic
	writeFinalDS(reducerID);

	return 0;
}