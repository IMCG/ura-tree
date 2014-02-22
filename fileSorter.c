#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <string.h>

int main(int argc, char **argv) {
	int line, len, i, j, n;
	FILE *infile, *outFile;
	unsigned char ch[1];
	char *buffer;
	long numbytes, numchars;

	if (argc != 3) {
		fprintf(stderr, "Usage: %s filename sortedfile\n", argv[0]);
		fprintf(stderr, "    Generates sortedfile containing keys from filename sorted alphabetically\n");
		exit(0);
	}

	if (outFile = fopen(argv[2], "r")) {
		fprintf(stderr, "Error: %s already exists", argv[2]);
		fclose(outFile);
		exit(0);
	}

	fprintf(stdout, "started sorting\n");

	/* open an existing file for reading */
	infile = fopen(argv[1], "r");

	/* quit if the file does not exist */
	if (infile == NULL)
		fprintf(stderr, "Error: File %s does not exist", argv[1]), exit(0);

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

	// Find out how many lines in file
	numchars = 0;
	line = 0;
	while (numchars < numbytes) {
		line++;
		len = 0;

		ch[0] = 'a';
		while (ch[0] != '\n') {
			memcpy(ch, buffer + numchars + len, 1);
			len++;
		}

		//memcpy(key, buffer + numchars, len - 1);
		numchars += len;
	}

	{
		unsigned char keylist[line][256], tmp[256];

		// Assign buffer values to keylist

		/* free the memory we used for the buffer */
		free(buffer);

		// Do a selection sort on keylist

		/*Create the outfile*/
		outFile = fopen(argv[2], "ab");

		if (outFile == NULL) {
			fprintf(stdout, "Error opening files!\n");
			exit(0);
		}

		// Write all values to output file

		fclose(outFile);
	}
	
	fprintf(stdout, "finished sorting file %s into %s\n", argv[1], argv[2]);

	return 0;
}