#include <stdio.h>
#include <string.h>

int main()
{
	char *test = "Asdfasdfasdf\0";
	unsigned char fencekey[256];
	unsigned int after, nxt = 100000;

	memcpy(fencekey, test, strlen(test));

	printf("Check copy operation: %s\n", fencekey);
	printf("Length of string: %d\n", strlen(test));
	printf("Size of unsigned char: %d, char: %d, fencekey: %d\n", sizeof(unsigned char), sizeof(char), sizeof(fencekey));
	printf("Check nxt: %d\n", nxt);
	printf("Check fencekey int: %d, size: %d\n", *fencekey, sizeof(*fencekey));

	after = nxt - *fencekey;
	printf("Check after: %d\n", after);
	printf("Difference: %d\n", (nxt - after));

	return 0;
}