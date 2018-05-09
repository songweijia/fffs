#ifndef _BLOG_BITMAP_H
#define _BLOG_BITMAP_H

#include <stdint.h>
#include <pthread.h>
#include <stdlib.h>

typedef struct _blog_bitmap{
  uint64_t * bitmap;
  uint32_t nbits;
  pthread_spinlock_t lck;
} BlogBitmap;

/**
 * initialize a bitmap
 * bmap - pointer to a BlogBitmap structure
 * nbits - number of bits in this bitmap
 * RETURN VALUE
 * 0 - success
 * other - errors
 */
static inline int blog_bitmap_init(BlogBitmap *bmap, uint32_t nbits) {
  bmap->nbits = nbits;
  uint32_t i = ((nbits+63)>>6);
  bmap->bitmap = (uint64_t*)malloc(sizeof(uint64_t)*i);
  while(i--){
    bmap->bitmap[i] = 0UL;
  }
  return pthread_spin_init(&(bmap->lck),PTHREAD_PROCESS_PRIVATE);
}
/**
 * destroy a bitmap
 * x - pointer to a BlogBitmap structure
 */
static inline int blog_bitmap_destroy(BlogBitmap *bmap) {
  free(bmap->bitmap);
  return pthread_spin_destroy(&(bmap->lck));
}
/**
 * test a bit
 */
static inline int blog_bitmap_testbit(BlogBitmap *bmap,uint32_t pos){
  uint64_t x;
  pthread_spin_lock(&(bmap->lck));
  x = bmap->bitmap[pos>>6]&(1UL<<(pos%64));
  pthread_spin_unlock(&(bmap->lck));
  return (x!=0UL);
}
/**
 * test 64bits
 */
static inline uint64_t blog_bitmap_get64bit(BlogBitmap *bmap,uint32_t pos){
  uint64_t x;
  pthread_spin_lock(&(bmap->lck));
  x = bmap->bitmap[pos>>6];
  pthread_spin_unlock(&(bmap->lck));
  return x;
}
/**
 * set/clear a bit
 * val - 0/1
 */
static inline void blog_bitmap_togglebit(BlogBitmap *bmap,uint32_t pos,int val){
  pthread_spin_lock(&(bmap->lck));
  if(val)
    bmap->bitmap[pos>>6]|=(1UL<<(pos%64));
  else
    bmap->bitmap[pos>>6]&=~(1UL<<(pos%64));
  pthread_spin_unlock(&(bmap->lck));
}
/**
 * set/clear bits
 * val - 0/1
 */
static inline void blog_bitmap_togglebits(BlogBitmap *bmap,uint32_t pos,uint32_t nbits,int val){
  pthread_spin_lock(&(bmap->lck));
  uint32_t spos=(pos>>6),epos=((pos+nbits-1)>>6),i;
  for(i=spos;i<=epos;i++){
    uint64_t x = ~0UL; // all bits are set to 1
    if(i == spos)
      x &= (~0UL)<<(pos%64);
    if(i == epos)
      x &= ((~0UL)>>(64-(pos+nbits)%64));
    if(val)
      bmap->bitmap[i]|=x;
    else
      bmap->bitmap[i]&=~x;
  }
  pthread_spin_unlock(&(bmap->lck));
}

#endif//_BLOG_BITMAP_H
