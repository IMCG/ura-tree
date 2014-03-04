#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "utils.h"

static float ticksPerNano;

// Returns time in "ticks".  Use getTicksPerNano() to convert to nanoseconds.
unsigned long long inline preciseTime()
{
	unsigned long lo, hi;
	asm volatile (
		"rdtsc;"
		:"=a" (lo), "=d" (hi)
		);
	return ((unsigned long long) lo) | (((unsigned long long) hi) << 32);
}

void calibratePreciseTime() {
  struct timespec ts;
  unsigned long long start, diff;
  unsigned long long mindiff = 0;
  int i, ret;

  ts.tv_sec = 0;
  ts.tv_nsec = 1e8l;

  for (i = 0; i < 10; i++) {
    start = preciseTime();
    ret = nanosleep(&ts, NULL);
    diff = preciseTime() - start;    
    if (ret)
      i--;
    else if (mindiff == 0 || diff < mindiff)
      mindiff = diff;
    //printf("Diff: %lld\n", diff);
  }

  ticksPerNano = mindiff / (float)(ts.tv_sec * 1e9l + ts.tv_nsec);
}

float getTicksPerNano() {
  if (ticksPerNano == 0) {
    fprintf(stderr, "ERROR: call calibratePreciseTime() at the beginning of your program!\n");
    exit(-1);
  } else {
    return ticksPerNano;
  }
}
