#include <stdio.h>
#include <stdlib.h>

#define SWAPS 10000

main(int argc, char *argv[])
{

	int queries;
	int sequences;

	int q,s;
	int i;
	int x,y,z;

	int *seq;

	if (argc != 3) {
		fprintf(stderr,"usage: %s q s\n", argv[0]);
		fprintf(stderr,"\tq - number of queries in a sequence\n");
		fprintf(stderr,"\ts - number of sequences to generate\n");
		fprintf(stderr,"\te.g. for version 2.14.x\n\t\t %s 22 41\n",argv[0]);
		exit(1);
	}
	queries=atoi(argv[1]);
	sequences=atoi(argv[2]);

	seq = malloc(sizeof(int)*queries);

	printf("#define MAX_PERMUTE %d\n",sequences);
	printf("long permutation[%d][%d]={\n",sequences, queries);

	for (s=0; s<sequences; s++) {
		for (q=0; q<queries; q++)
			seq[q]=q+1;

		/*
		 * just cycle arond the sequence swapping with a random other entry
		 */
		for (i=0; i<SWAPS; i++) {
			x= i % queries;
			do 
				y=random() % queries;
			while (x == y);
			z = seq[x];
			seq[x] = seq[y];
			seq[y] = z;
		}

		printf("\t{");
		for (i=0; i<queries-1; i++)
			printf("%2d,",seq[i]);
		printf("%2d}%c\n",seq[queries-1],s==sequences-1?' ':',');
	}
	printf("}\n");
}

