#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <string.h>

int main(int argc, char **argv) {
	int i;
	FILE *infile, *outFile;
	unsigned char key[256];
	char *buffer, *token;
	long numbytes;

	if (argc != 4) {
		fprintf(stderr, "Usage: %s filenamea filenameb outfile\n", argv[0]);
		fprintf(stderr, "    Generates outfile containing filenamea concatinated with filenameb\n");
		exit(0);
	}

	if (outFile = fopen(argv[3], "r")) {
		fprintf(stderr, "Error: %s already exists", argv[3]);
		fclose(outFile);
		exit(0);
	}

	fprintf(stdout, "started merging\n");

	for (i = 1; i < 3; i++) {

		/* open an existing file for reading */
		infile = fopen(argv[i], "r");

		/* quit if the file does not exist */
		if (infile == NULL)
			fprintf(stderr, "Error: File %d does not exist", i), exit(0);

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
		fprintf(stdout, "The file called %s has been loaded\n", argv[i]);

		/*Create the outfiles*/
		outFile = fopen(argv[3], "ab");

		if (outFile == NULL) {
			fprintf(stdout, "Error opening files!\n");
			exit(0);
		}

		/*Split the text by endline characters*/
		token = strtok(buffer, "\n");
		while (token) {
			strcpy(key, token);
			strcat(key, "\n");
			fputs(key, outFile);

			token = strtok(NULL, "\n");
		}

		fclose(outFile);

		/* free the memory we used for the buffer */
		free(buffer);
	}

	fprintf(stdout, "finished merging files %s and %s to %s\n", argv[1], argv[2], argv[3]);

	return 0;
}