#include <stdio.h>
#include <inttypes.h>
#include "bitmap.h"

int main(int argc, char **argv){
  BlogBitmap bb;
  int i=0;
  fprintf(stdout,"blog_bitmap_init(&bb,128)=%d\n",blog_bitmap_init(&bb,128));
  // set a bit
  for(i=0;i<128;i++)
  {
    blog_bitmap_togglebit(&bb,i,1);
    fprintf(stdout,"set bit:%d\t%"PRIx64",%"PRIx64"\n",i,blog_bitmap_get64bit(&bb,0),blog_bitmap_get64bit(&bb,64));
  }
  // test a bit
  for(i=0;i<128;i++)
    fprintf(stdout,"%d-%d\n",i,blog_bitmap_testbit(&bb,i));
  // clear a bit
  for(i=0;i<128;i++)
  {
    blog_bitmap_togglebit(&bb,i,0);
    fprintf(stdout,"clear bit:%d\t%"PRIx64",%"PRIx64"\n",i,blog_bitmap_get64bit(&bb,0),blog_bitmap_get64bit(&bb,64));
  }
  // test a bit
  for(i=0;i<128;i++)
    fprintf(stdout,"%d-%d\n",i,blog_bitmap_testbit(&bb,i));
  // set bits
  for(i=0;i<128;i+=4)
  {
    blog_bitmap_togglebits(&bb,i,4,1);
    fprintf(stdout,"set bit:%d[4]\t%"PRIx64",%"PRIx64"\n",i,blog_bitmap_get64bit(&bb,0),blog_bitmap_get64bit(&bb,64));
  }
  // clear bits
  for(i=0;i<128;i+=4)
  {
    blog_bitmap_togglebits(&bb,i,4,0);
    fprintf(stdout,"clear bit:%d[4]\t%"PRIx64",%"PRIx64"\n",i,blog_bitmap_get64bit(&bb,0),blog_bitmap_get64bit(&bb,64));
  }

  fprintf(stdout,"blog_bitmap_destroy(&bb)=%d\n",blog_bitmap_destroy(&bb));
}
