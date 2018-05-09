#include <stdint.h>
#include <sys/queue.h>
#include <semaphore.h>
#include "map.h"
#include "bitmap.h"
#include "debug.h"
#include "InfiniBandRDMA.h"

#define BLOCK_MAP_SIZE 1024
#define LOG_MAP_SIZE 64
#define SNAPSHOT_MAP_SIZE 64
#define MAX_INMEM_BLOG_ENTRIES 16384
#define MAX_FLUSH_BATCH 128

typedef char* page_t;
typedef struct log log_t;
typedef struct block block_t;
typedef struct filesystem filesystem_t;
typedef struct snapshot snapshot_t;
typedef struct pers_queue_entry pers_event_t;

MAP_DECLARE(block, block_t);
MAP_DECLARE(log, uint64_t);
MAP_DECLARE(snapshot, snapshot_t);

enum OPERATION {
  BOL = 0,
  CREATE_BLOCK = 1,
  DELETE_BLOCK = 2,
  WRITE = 3,
  SET_GENSTAMP = 4
};

enum STATUS {
  ACTIVE,
  NON_ACTIVE
};

/**
 * Log Entry structure.
 * op           :   operation
 * block_length :   length of the block at point of log time
 * start_page   :   number of the first page in the block
 * nr_pages     :   number of pages
 * r            :   real-time component of HLC.
 * l            :   logical component of HLC.
 * u            :   user defined timestamp.
 * first_pn     :   page number of the first page we write.
 *              :   reused as generation time stamp for SET_GENSTAMP log
 */
struct log {
  uint32_t op : 4;
  uint32_t block_length : 28;
  uint32_t start_page;
  uint32_t nr_pages;
  uint32_t reserved; // padding
  uint64_t r;
  uint64_t l;
  uint64_t u;
  uint64_t first_pn;
};

/**
 * Block Entry structure.
 * id           :   block id
 * log_head     :   head of the log(start with 0)
 * log_tail     :   tail of the log(number of the log entries)
 * log_cap      :   log capacity
 * status       :   status of the block
 * length       :   length of the block in bytes
 * cap          :   storage allocated for this block in memory.
 * last_entry   :   last Log Entry for this block.
 * pages        :   index array that stores pointers of different pages.
 * log          :   log entry array
 * log_map_hlc  :   map from hlc to log entry
 * log_map_ut   :   map from ut to log entry
 * snapshot_map :   map from log entry to snapshot
 * log_pers     :   the next log entry to flush
 * blog_rwlock  :   the read/write lock protecting the blog
 * log_sem      :   semaphore for log slots in the ring buffer.
 * blog_wfd     :   blog file descriptor for writer
 */
struct block {
  uint64_t id;
  uint64_t log_head;
  uint64_t log_tail;
  uint64_t log_cap;
#define BLOG_IS_FULL(b) (((b)->log_tail - (b)->log_head - (b)->log_cap) == 0)
  uint32_t status : 4;
  uint32_t length : 28;
  uint32_t pages_cap;
  uint64_t *pages;
  volatile log_t *log;
#define BLOG_NEXT_ENTRY(b) ((log_t*)(b)->log+((b)->log_tail%MAX_INMEM_BLOG_ENTRIES))
#define BLOG_LAST_ENTRY(b) ((log_t*)(b)->log+(((b)->log_tail+MAX_INMEM_BLOG_ENTRIES-1)%MAX_INMEM_BLOG_ENTRIES))
#define BLOG_ENTRY(b,i) ((log_t*)(b)->log+(i)%MAX_INMEM_BLOG_ENTRIES)
  BLOG_MAP_TYPE(log) *log_map_hlc; // by hlc
  BLOG_MAP_TYPE(log) *log_map_ut; // by user timestamp
  BLOG_MAP_TYPE(snapshot) *snapshot_map;
  //The following members are for data persistent routine
#define BLOG_NEXT_PERS_ENTRY(b) ((log_t*)(b)->log+((b)->log_pers%MAX_INMEM_BLOG_ENTRIES))
  uint64_t log_pers;
#define BLOG_RDLOCK(b) pthread_rwlock_rdlock(&(b)->blog_rwlock)
#define BLOG_WRLOCK(b) pthread_rwlock_wrlock(&(b)->blog_rwlock)
#define BLOG_UNLOCK(b) pthread_rwlock_unlock(&(b)->blog_rwlock)
  pthread_rwlock_t blog_rwlock; // protect blog of a block.
  sem_t log_sem;
  int blog_wfd;
  int blog_rfd;
};

struct snapshot {
  uint64_t ref_count;
  uint32_t status : 4;
  uint32_t length : 28;
  uint64_t *pages;
};

TAILQ_HEAD(_pers_queue, pers_queue_entry);
struct pers_queue_entry {
  block_t *block;   // block pointer; NULL for End-OF-Queue
  uint64_t log_length; // The latest log updated to this length
  TAILQ_ENTRY(pers_queue_entry) lnk; // link pointers
}; 
#define IS_EOQ(e) (e->block == NULL)

/**
 * Filesystem structure.
 * block_size   :   block size in bytes.
 * page_size    :   page size in bytes.
 * block_map    :   hash map that contains all the blocks in the current state
 *                  (key: block id).
 * page_base_ring_buffer    :   base address for the ring buffer
 * page_base_mapped_file    :   base address for the memory mapped file
 * ring_buffer_size         :   size of the ring buffer
 * nr_page_head :   head of the page pool ring buffer
 * nr_page_tail :   tail of the page pool ring buffer
 * nr_page_pers :   next page to be flushed into memory
 * pers_bitmap  :   bitmap indicating corresponding page is flushed or not
 * head_rwlock  :   read/write lock on the head of the page ring buffer
 * tail_mutex   :   mutex lock on the tail of the page ring buffer
 * freepages_sem:   the number of free pages
 * clock_spinlock:  clock spin lock guarantee that the log entries are persistent in the order of its hlc clock.
 * page_wfd     :   page fd for write
 * page_rfd     :   page fd for read
 * page_shm_fd  :   ramdisk(tmpfs) page file descriptor
 * nr_pages_pers:   number of pages in persistent state.
 * pers_thrd    :   thread responsible for pushing the blog to the disk for
 *                  persistance.
 * pers_queue   :   persistent message queue.
 * pers_queue_sem : semaphore for the persistent message queue.
 */
struct filesystem {
  size_t block_size;
  size_t page_size;
#define PAGE_PER_BLOCK(fs) ((fs)->block_size/(fs)->page_size)
  BLOG_MAP_TYPE(block) *block_map;
#define PAGE_FRAME_NR(fs) ((fs)->ring_buffer_size/(fs)->page_size)
#define PAGE_NR_TO_PTR_RB(fs,nr) ((char *)(fs)->page_base_ring_buffer+((fs)->page_size*((nr)%PAGE_FRAME_NR(fs))))
#define PAGE_NR_TO_PTR_MF(fs,nr) ((char *)(fs)->page_base_mapped_file+(fs)->page_size*(nr))
#define INVALID_PAGE_NO  (~0x0UL)
  void *page_base_ring_buffer;
  void *page_base_mapped_file;
  uint64_t ring_buffer_size;
  uint64_t nr_page_head;
  uint64_t nr_page_tail;
#define PAGE_FRAME_FREE_NR(fs) ( \
  PAGE_FRAME_NR(fs) + (fs)->nr_page_head-(fs)->nr_page_tail \
)
  uint64_t nr_page_pers;
  BlogBitmap pers_bitmap; // indicating corresponding page is persistent or not.
  pthread_rwlock_t   head_rwlock;
  pthread_spinlock_t tail_spinlock;
  sem_t              freepages_sem;
  pthread_spinlock_t clock_spinlock;
  int page_wfd;
  int page_rfd;
  //The following members are for data persistent routine
  uint32_t page_shm_fd;
  pthread_t pers_thrd;
  char pers_path[256];
  #define PERS_ENQ(fs,e) do{ \
    if (e->block != NULL) { \
        DEBUG_PRINT("Enqueue: Block %" PRIu64 " Operation %" PRIu32 "\n", e->block->id, BLOG_ENTRY(e->block,e->log_length-1)->op); \
    } else { \
        DEBUG_PRINT("Enqueue: Null Event\n"); \
    } \
    pthread_spin_lock(&(fs)->queue_spinlock); \
    TAILQ_INSERT_TAIL(&(fs)->pers_queue,e,lnk); \
    pthread_spin_unlock(&(fs)->queue_spinlock); \
    if(sem_post(&fs->pers_queue_sem) !=0){ \
      fprintf(stderr,"Error: failed post semaphore, Error: %s\n", strerror(errno)); \
      exit(1); \
    } \
  }while(0)
  #define PERS_DEQ(fs,pe) do{ \
    if(sem_wait(&fs->pers_queue_sem) != 0){ \
      fprintf(stderr,"Error: failed waiting on semaphore, Error: %s\n", strerror(errno)); \
      exit(1); \
    } \
    pthread_spin_lock(&(fs)->queue_spinlock); \
    if(!TAILQ_EMPTY(&(fs)->pers_queue)){ \
      *(pe) = TAILQ_FIRST(&(fs)->pers_queue); \
      TAILQ_REMOVE(&(fs)->pers_queue,*(pe),lnk); \
    } else *pe = NULL; \
    pthread_spin_unlock(&(fs)->queue_spinlock); \
  }while(0)
  #define PERS_QLEN(fs,ql) do{ \
    if(sem_getvalue(&fs->pers_queue_sem,&ql)!=0){ \
      fprintf(stderr, "Error: failed to get semaphore value, Error: %s\n", strerror(errno)); \
      exit(1); \
    } \
  }while(0)
  struct _pers_queue pers_queue;
  pthread_spinlock_t queue_spinlock;
  sem_t pers_queue_sem;

  RDMACtxt *rdmaCtxt; // RDMA Context
};

