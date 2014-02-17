#include <stdio.h>
#include <string.h>

int main()
{
	char *test = "This is a test";
	char *asdf = test + 2;

	printf("%s\n", asdf);

	return 0;
}