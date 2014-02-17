#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <string.h>

#define NEWFILES

int main(int argc, char **argv) {
	int length, splitPoint, line;

	FILE *infile, *outFileA, *outFileB;
	char fileA[40], fileB[40];
	unsigned char key[256];
	char *buffer, *token, *nextLine;
	long numbytes;

	if (argc != 2 || strlen(argv[1]) > 39) {
		fprintf(stderr, "Usage: %s filename\n", argv[0]);
		fprintf(stderr, "    Generates filenamea and filenameb, each containing half of filename\n");
		fprintf(stderr, "    Note that filename must be 39 characters or less\n");
		exit(0);
	}

#ifdef NEWFILES
	strcpy(fileA, argv[1]);
	strcat(fileA, "a");
	strcpy(fileB, argv[1]);
	strcat(fileB, "b");

	if (outFileA = fopen(fileA, "r")) {
		fprintf(stderr, "Error: %s already exists", fileA);
		fclose(outFileA);
		exit(0);
	}
	if (outFileB = fopen(fileB, "r")) {
		fprintf(stderr, "Error: %s already exists", fileB);
		fclose(outFileB);
		exit(0);
	}
#endif

	fprintf(stdout, "started splitting for %s\n", argv[1]);

	/* open an existing file for reading */
	infile = fopen(argv[1], "r");

	/* quit if the file does not exist */
	if (infile == NULL)
		fprintf(stderr, "Error: File does not exist"), exit(0);

	/* Get the number of bytes */
	fseek(infile, 0L, SEEK_END);
	numbytes = ftell(infile);

	/* reset the file position indicator to
	the beginning of the file */
	fseek(infile, 0L, SEEK_SET);

	/* grab sufficient memory for the
	buffer to hold the text */
	buffer = (char*)calloc(numbytes, sizeof(char));

	/* memory error */
	if (buffer == NULL)
		fprintf(stderr, "Error: Memory issue"), exit(0);

	/* copy all the text into the buffer */
	fread(buffer, sizeof(char), numbytes, infile);
	fclose(infile);

	/* confirm we have read the file by
	outputing it to the console */
	fprintf(stdout, "The file called %s has been loaded\n", argv[1]);

	/*Count number of lines in the file*/
	length = 0;
	nextLine = strchr(buffer, '\n');
	while (nextLine != NULL) {
		length++;
		nextLine = strchr(nextLine + 1, '\n');
	}

	splitPoint = (int)(length / 2);
	fprintf(stdout, "File contains %d lines; Half is %d\n", length, splitPoint);

#ifdef NEWFILES
	/*Create the outfiles*/
	outFileA = fopen(fileA, "ab");
	outFileB = fopen(fileB, "ab");

	if (outFileA == NULL || outFileB == NULL) {
		fprintf(stdout, "Error opening files!\n");
		exit(0);
	}

	/*Split the text by endline characters*/
	line = 0;
	token = strtok(buffer, "\n");
	while (token) {
		line++;
		strcpy(key, token);
		strcat(key, "\n");
		if (line <= splitPoint)
			fputs(key, outFileA);
		else
			fputs(key, outFileB);

		token = strtok(NULL, "\n");
	}

	fclose(outFileA);
	fclose(outFileB);
#endif

	/* free the memory we used for the buffer */
	free(buffer);

	fprintf(stdout, "finished splitting file %s with %d lines\n", argv[1], length);

	return 0;
}