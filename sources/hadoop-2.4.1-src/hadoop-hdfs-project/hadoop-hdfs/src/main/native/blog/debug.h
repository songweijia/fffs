#ifndef __DEBUG_H__
#define __DEBUG_H__

#include <stdio.h>
#include <sys/time.h>
#include <pthread.h>

#ifndef DEBUG_PRINT

  #ifdef DEBUG
  #define DEBUG_PRINT(arg,fmt...) {fprintf(stderr,"<%ld>",pthread_self()); fprintf(stderr,arg,##fmt ); fflush(stderr);}
  #else
  #define DEBUG_PRINT(arg,fmt...)
  #endif

#endif

#ifndef DEBUG_TIMESTAMP

  #ifdef DEBUG
  #define DEBUG_TIMESTAMP(t) gettimeofday(&t,NULL)
  #else
  #define DEBUG_TIMESTAMP(t)
  #endif

  #define TIMESPAN(t1,t2) ((t2.tv_sec-t1.tv_sec)*1000000+(t2.tv_usec-t1.tv_usec))

#endif

#endif//__DEBUG_H__
