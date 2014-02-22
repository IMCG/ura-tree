#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <string.h>

char * seek_next(char * start, long ceil) {

}

int main(int argc, char **argv) {
	int line, len, n, minlen, nextlen;
	FILE *infile, *outFile;
	unsigned char ch[1], minkey[256], nextkey[256];
	char *buffer, *minpos, *nextpos;
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

	// open an existing file for reading
	infile = fopen(argv[1], "r");

	// quit if the file does not exist
	if (infile == NULL)
		fprintf(stderr, "Error: File %s does not exist", argv[1]), exit(0);

	// Get the number of bytes 
	fseek(infile, 0L, SEEK_END);
	numbytes = ftell(infile);

	// reset the file position indicator to the beginning of the file
	fseek(infile, 0L, SEEK_SET);

	// grab sufficient memory for the buffer to hold the text
	buffer = (char*)calloc(numbytes, sizeof(char));

	// memory error 
	if (buffer == NULL)
		fprintf(stderr, "Error: Memory issue"), exit(0);

	// copy all the text into the buffer 
	fread(buffer, sizeof(char), numbytes, infile);
	fclose(infile);

	// confirm we have read the file
	fprintf(stdout, "The file called %s has been loaded\n", argv[1]);

	
	// Find out how many lines in file
	numchars = 0;
	line = 0;
	while (numchars < numbytes) {
		line++;
		len = 0;

		ch[0] = '\0';
		while (ch[0] != '\n') {
			memcpy(ch, buffer + numchars + len, 1);
			len++;
		}

		numchars += len;
	}

	fprintf(stdout, "Finished counting lines: %d\n", line);

	// Create the outfile
	outFile = fopen(argv[2], "ab");

	if (outFile == NULL) {
		fprintf(stdout, "Error opening files!\n");
		exit(0);
	}

	// Go over buffer line times and find the minkey each time
	// Slow but other option is to create a string array, which is a LOT of memory
	for (n = 0; n < line; n++) {
		// Find first key
		len = 0;
		ch[0] = '\0';
		while (ch[0] == '\0') {
			memcpy(ch, buffer + len, 1);
			len++;
		}
		minpos = (char*)(buffer + len - 1);
		numchars = (long)(len - 1);

		len = 0;
		ch[0] = '\0';
		while (ch[0] != '\n') {
			memcpy(ch, minpos + len, 1);
			len++;
		}
		minlen = len-1;
		memcpy(minkey, minpos, minlen);
		minkey[minlen] = '\0';
		numchars += len;

		// We now have the first key
		// Compare with every other key
		
		while (numchars < numbytes) {
			// Find next key
			len = 0;
			ch[0] = '\0';
			while (ch[0] == '\0') {
				memcpy(ch, buffer + numchars + len, 1);
				len++;
			}
			nextpos = (char*)(buffer + numchars + len - 1);
			numchars += (len - 1);

			len = 0;
			ch[0] = '\0';
			while (ch[0] != '\n') {
				memcpy(ch, nextpos + len, 1);
				len++;
			}
			nextlen = len - 1;
			memcpy(nextkey, nextpos, nextlen);
			nextkey[nextlen] = '\0';
			numchars += len;

			// Compare with minimum and set new minimum, if necessary
			if (strcmp(minkey, nextkey) > 0) {
				strcpy(minkey, nextkey);
				minlen = nextlen;
				minpos = nextpos;
			}
		}

		// Minimum should currently hold minimum key
		// Write to file and delete from buffer
		fputs(minkey, outFile);
		fputc('\n', outFile);
		memset(minpos, '\0', minlen + 1);
	}

	fclose(outFile);

	free(buffer);

	/*

	// Assign buffer values to keylist
	numchars = 0;
	line = 0;
	while (numchars < numbytes) {
		len = 0;

		ch[0] = '\0';
		while (ch[0] != '\n') {
			memcpy(ch, buffer + numchars + len, 1);
			len++;
		}

		memcpy(keylist + (line*256), buffer + numchars, len - 1);
		memset(keylist + (line*256) + len, '\0', 1);
		numchars += len;
		line++;
	}

	fprintf(stdout, "Finished setting up string array\n");

	free(buffer);
	
	// Do a selection sort on keylist
	memset(tmp, '\0', 256);
	for (i = 0; i < line-1 ; i++) {
		min = i;

		for (j = i+1; j < line; j++) {
			if (strcmp(keylist[i], keylist[j]) < 0) {
				min = j;
			}
		}

		if (min != i) {
			strcpy(tmp, keylist[i]);
			strcpy(keylist[i], keylist[min]);
			strcpy(keylist[min], tmp);
		}
	}

	fprintf(stdout, "Sorted string array. Writing to file\n");

	// Create the outfile
	outFile = fopen(argv[2], "ab");

	if (outFile == NULL) {
		fprintf(stdout, "Error opening files!\n");
		exit(0);
	}

	// Write all values to output file
	for (i = 0; i < line; i++) {
		strcat(keylist[i], "\n");
		fputs(keylist[i], outFile);
	}

	
	*/
	
	fprintf(stdout, "finished sorting file %s into %s\n", argv[1], argv[2]);

	return 0;
}