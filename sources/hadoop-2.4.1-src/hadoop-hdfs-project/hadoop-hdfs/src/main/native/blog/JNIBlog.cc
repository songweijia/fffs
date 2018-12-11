/*
 * JNIBlog.cc
 *
 * Created on: Dec 15, 2016
 * Author: weijia, theo
 */
#include <cinttypes>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <string>
#include <dirent.h>
#include <sys/statvfs.h>

#include "InfiniBandRDMA.h"
#include "JNIBlog.h"
#include "types.h"

#define MAKE_VERSION(x,y) ((((x)&0xffffl)<<16)|((y)&0xffff))
#define FFFS_VERSION MAKE_VERSION(0,1)

// Define files for storing persistent data.
#define BLOGFILE_SUFFIX "blog"
/*
 * Note that by default, a file in ext4 file system use 32 bit for block numbers and the block size is 4K. This limit
 * the file size to 16TB. So we limit the file size to 8TB here. This can be extended by using 64bit feature with ext4
 * at formatting the file system. To check if 64bit is supported, use "tun2efs -l /dev/xxx" to see if "64bit" is in the
 * feature list.
 */
#define MAX_PERS_PAGE_SIZE (1UL<<43)
#define SHM_PAGE_FN "fffs.pg"
#define PERS_PAGE_FN "fffs.pg"
// Definitions to be substituted by configuration:
#define MAX(x,y) (((x)>(y))?(x):(y))
#define MIN(x,y) (((x)<(y))?(x):(y))

// Type definitions for dictionaries.
MAP_DEFINE(block, block_t, BLOCK_MAP_SIZE);
MAP_DEFINE(log, uint64_t, LOG_MAP_SIZE);
MAP_DEFINE(snapshot, snapshot_t, SNAPSHOT_MAP_SIZE);

//local typedefs
typedef struct transport_parameters
{
#define TP_LOCAL_BUFFER (0)
#define TP_RDMA (1)
    int mode;
    union
    {
        struct tp_local_buffer_param
        {
            jbyteArray buf;
            jint bufOfst;
            jlong user_timestamp; // only for write.
        } lbuf;
        struct tp_rdma_param
        {
            jbyteArray client_ip;
            jint remote_pid;
            jlong vaddr;
            jobject record_parser; // only for write.
        } rdma;
    } param;
} transport_parameters_t;

// some internal tools
#define LOG2(x) calc_log2(x)
static inline int calc_log2(uint64_t val) {

  int i = 0;
  while (((val >> i) & 0x1) == 0 && (i < 64))
    i++;
  return i;
}

// Printer Functions.
char *log_to_string(filesystem_t *fs, log_t *log, size_t page_size) {
  char *res = (char *) malloc(1024 * 1024 * sizeof(char));
  char buf[1025 + 18];
  uint32_t length, i;

  switch (log->op) {
    case BOL:
      strcpy(res, "Operation: Beginning of Log\n");
      break;
    case CREATE_BLOCK:
      strcpy(res, "Operation: Create Block\n");
      sprintf(buf, "HLC Value: (%" PRIu64 ",%" PRIu64 ")\n", log->r, log->l);
      strcat(res, buf);
      break;
    case DELETE_BLOCK:
      strcpy(res, "Operation: Delete Block\n");
      sprintf(buf, "HLC Value: (%" PRIu64 ",%" PRIu64 ")\n", log->r, log->l);
      strcat(res, buf);
      break;
    case WRITE:
      strcpy(res, "Operation: Write\n");
      sprintf(buf, "Block Length: %" PRIu32 "\n", (uint32_t) log->block_length);
      strcat(res, buf);
      sprintf(buf, "Starting Page: %" PRIu32 "\n", log->start_page);
      strcat(res, buf);
      sprintf(buf, "Number of Pages: %" PRIu32 "\n", log->nr_pages);
      strcat(res, buf);
      for (i = 0; i < log->nr_pages - 1; i++) {
        sprintf(buf, "Page %" PRIu32 ":\n", log->start_page + i);
        strcat(res, buf);
        snprintf(buf, 1025, "[%p]%s", (char*) PAGE_NR_TO_PTR_RB(fs, log->first_pn + i),
            (char*) PAGE_NR_TO_PTR_RB(fs, log->first_pn + i));
        buf[1024] = '\0';
        strcat(res, buf);
        strcat(res, "\n");
      }
      if (log->block_length > log->nr_pages * page_size)
        length = page_size;
      else
        length = log->block_length - (log->nr_pages - 1) * page_size;
      sprintf(buf, "Page %" PRIu32 ":\n", log->start_page + log->nr_pages - 1);
      strcat(res, buf);
      length = MAX(length, 1024);
      snprintf(buf, length + 1, "[%p]%s", (char*) PAGE_NR_TO_PTR_RB(fs, log->first_pn + log->nr_pages - 1),
          (char*) PAGE_NR_TO_PTR_RB(fs, log->first_pn + log->nr_pages - 1));
      buf[1024] = '\0';
      strcat(res, buf);
      strcat(res, "\n");
      sprintf(buf, "HLC Value: (%" PRIu64 ",%" PRIu64 ")\n", log->r, log->l);
      strcat(res, buf);
      break;
    default:
      strcat(res, "Operation: Unknown Operation\n");
  }
  return res;
}

char *snapshot_to_string(filesystem_t *fs, snapshot_t *snapshot, size_t page_size) {
  char *res = (char *) malloc((1024 * page_size) * sizeof(char));
  char buf[1025];
  uint32_t i;

  sprintf(buf, "Reference Count: %" PRIu64 "\n", snapshot->ref_count);
  strcpy(res, buf);
  switch (snapshot->status) {
    case ACTIVE:
      strcat(res, "Status: Active");
      break;
    case NON_ACTIVE:
      strcat(res, "Status: Non Active");
      break;
    default:
      fprintf(stderr, "ERROR: Status should be either ACTIVE or NON_ACTIVE\n");
      exit(0);
  }
  sprintf(buf, "Length: %" PRIu32 "\n", (uint32_t) snapshot->length);
  strcat(res, buf);
  if (snapshot->length != 0) {
    for (i = 0; i <= (snapshot->length - 1) / page_size; i++) {
      sprintf(buf, "Page %" PRIu32 ":\n", i);
      strcat(res, buf);
      snprintf(buf, page_size + 2, "%s\n", (const char*) PAGE_NR_TO_PTR_RB(fs, snapshot->pages[i]));
      strcat(res, buf);
    }
  }
  return res;
}

char *block_to_string(filesystem_t *fs, block_t *block, size_t page_size) {
  char *res = (char *) malloc(1024 * 1024 * sizeof(char));
  char buf[1025];
  snapshot_t *snapshot;
  uint64_t *snapshot_ids = nullptr;
  uint64_t *log_ptr;
  uint32_t i;
  uint64_t j, log_index, map_length;
  log_t *log = (log_t*) block->log;

  // Print id.
  sprintf(buf, "ID: %" PRIu64 "\n", block->id);
  strcpy(res, buf);

  // Print blog.
  sprintf(buf, "Log Length: %" PRIu64 "\n", block->log_tail);
  strcat(res, buf);
  sprintf(buf, "Log Capacity: %" PRIu64 "\n", block->log_cap);
  strcat(res, buf);
  for (j = 0; j < block->log_tail; j++) {
    sprintf(buf, "Log %" PRIu64 ":\n", j);
    strcat(res, buf);
    strcat(res, log_to_string(fs, log + j, page_size));
    strcat(res, "\n");
  }

  // Print current state.
  sprintf(buf, "Status: %s\n", block->status == ACTIVE ? "Active" : "Non-Active");
  strcat(res, buf);
  sprintf(buf, "Length: %" PRIu32 "\n", (uint32_t) block->length);
  strcat(res, buf);
  sprintf(buf, "Pages Capacity: %" PRIu32 "\n", block->pages_cap);
  strcat(res, buf);
  if (block->length != 0) {
    for (i = 0; i <= (block->length - 1) / page_size; i++) {
      sprintf(buf, "Page %" PRIu32 ":\n", i);
      strcat(res, buf);
      snprintf(buf, page_size + 1, "%s\n", (char *) PAGE_NR_TO_PTR_RB(fs, block->pages[i]));
      strcat(res, buf);
      strcat(res, "\n");
    }
  }
  strcat(res, "\n");

  // Print Snapshots.
  strcat(res, "Snapshots:\n");
  for (i = 0; i < LOG_MAP_SIZE; i++)
    MAP_LOCK(log, block->log_map_hlc, i, 'r');
  map_length = MAP_LENGTH(log, block->log_map_hlc);
  sprintf(buf, "Number of Snapshots: %" PRIu64 "\n", map_length);
  strcat(res, buf);
  if (map_length > 0)
    snapshot_ids = MAP_GET_IDS(log, block->log_map_hlc, map_length);
  for (i = 0; i < map_length; i++) {
    sprintf(buf, "RTC: %" PRIu64 "\n", snapshot_ids[i]);
    strcat(res, buf);
    if (MAP_READ(log, block->log_map_hlc, snapshot_ids[i], &log_ptr) != 0) {
      fprintf(stderr, "ERROR: Cannot read log index whose time is contained in log map.\n");
      exit(0);
    }
    log_index = *log_ptr;
    sprintf(buf, "Last Log Index: %" PRIu64 "\n", log_index);
    strcat(res, buf);
    MAP_LOCK(snapshot, block->snapshot_map, log_index, 'r');
    if (MAP_READ(snapshot, block->snapshot_map, log_index, &snapshot) != 0) {
      fprintf(stderr, "ERROR: Cannot read snapshot whose last log index is contained in snapshot map.\n");
      exit(0);
    }
    strcat(res, snapshot_to_string(fs, snapshot, page_size));
    strcat(res, "\n");
    MAP_UNLOCK(snapshot, block->snapshot_map, *log_ptr);
  }
  for (i = 0; i < LOG_MAP_SIZE; i++)
    MAP_UNLOCK(log, block->log_map_hlc, i);
  if (map_length > 0)
    free(snapshot_ids);
  strcat(res, "--------------------------------------------------\n");
  return res;
}

char *filesystem_to_string(filesystem_t *filesystem) {
  char *res = (char *) malloc((1024 * filesystem->page_size) * sizeof(char));
  char buf[1025];
  block_t *block;
  uint64_t *block_ids = nullptr;
  uint64_t length, i;

  // Print Filesystem.
  strcpy(res, "Filesystem\n");
  strcat(res, "----------\n");
  sprintf(buf, "Block Size: %" PRIu64 "\n", (uint64_t) filesystem->block_size);
  strcat(res, buf);
  sprintf(buf, "Page Size: %" PRIu64 "\n", (uint64_t) filesystem->page_size);
  strcat(res, buf);

  // Print Blocks.
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    MAP_LOCK(block, filesystem->block_map, i, 'r');
  length = MAP_LENGTH(block, filesystem->block_map);
  sprintf(buf, "Length: %" PRIu64 "\n", length);
  strcat(res, buf);
  if (length > 0)
    block_ids = MAP_GET_IDS(block, filesystem->block_map, length);
  for (i = 0; i < length; i++) {
    if (MAP_READ(block, filesystem->block_map, block_ids[i], &block) != 0) {
      fprintf(stderr, "ERROR: Cannot read block whose id is contained in block map.\n");
      exit(0);
    }
    strcat(res, block_to_string(filesystem, block, filesystem->page_size));
  }
  for (i = 0; i < BLOCK_MAP_SIZE; i++)
    MAP_UNLOCK(block, filesystem->block_map, i);
  if (length > 0)
    free(block_ids);
  return res;
}

// Helper functions for clock updates - See JAVA counterpart for more details.
void update_log_clock(JNIEnv *env, jobject hlc, log_t *log) {
  jclass hlcClass = env->GetObjectClass(hlc);
  jfieldID rfield = env->GetFieldID(hlcClass, "r", "J");
  jfieldID cfield = env->GetFieldID(hlcClass, "c", "J");

  log->r = env->GetLongField(hlc, rfield);
  log->l = env->GetLongField(hlc, cfield);
}

void tick_hybrid_logical_clock(JNIEnv *env, jobject hlc, jobject mhlc) {
  jclass hlcClass = env->GetObjectClass(hlc);
  jmethodID mid = env->GetMethodID(hlcClass, "tickOnRecv", "(Ledu/cornell/cs/sa/HybridLogicalClock;)V");

  env->CallObjectMethod(hlc, mid, mhlc);
}

jobject get_hybrid_logical_clock(JNIEnv *env, jobject thisObj) {
  jclass thisCls = env->GetObjectClass(thisObj);
  jfieldID fid = env->GetFieldID(thisCls, "hlc", "Ledu/cornell/cs/sa/HybridLogicalClock;");

  return env->GetObjectField(thisObj, fid);
}

// Helper function for obtaining the local filesystem object - See JAVA counterpart for more details.
filesystem_t *get_filesystem(JNIEnv *env, jobject thisObj) {
  jclass thisCls = env->GetObjectClass(thisObj);
  jfieldID fid = env->GetFieldID(thisCls, "jniData", "J");

  return (filesystem_t *) env->GetLongField(thisObj, fid);
}

/**
 * Read local RTC value.
 * return clock value.
 */
uint64_t read_local_rtc() {
  struct timeval tv;
  uint64_t rtc;

  gettimeofday(&tv, NULL);
  rtc = tv.tv_sec * 1000 + tv.tv_usec / 1000;
  return rtc;
}

int compare(uint64_t r1, uint64_t c1, uint64_t r2, uint64_t c2) {
  if (r1 > r2)
    return 1;
  if (r2 > r1)
    return -1;
  if (c1 > c2)
    return 1;
  if (c2 > c1)
    return -1;
  return 0;
}

// NOTE: acquire read lock on blog before call this function
// Find the log entry, from memory cache or disk file...
// - block: block structure
// - idx: log index
// - rfd: read-only file descriptor of blog file
// - le: log_entry buffer, it is used if data retrieved from disk
// RETURN VALUE:
// NULL for error, or pointer to the log entry.
static inline log_t* get_log_entry(block_t *block, uint64_t idx, int rfd, log_t *le) {
  log_t *rle;
  if (idx >= block->log_tail)
    rle = NULL;
  else if (idx < block->log_head) {

    // read from file
    if (lseek(rfd, (off_t) idx * sizeof(log_t), SEEK_SET) < 0) {
      fprintf(stderr, "Cannot lseek to the %" PRIu64 "-th log. block:%" PRIu64 ",errcode:%d,reason:%s", idx, block->id,
          errno, strerror(errno));
      rle = NULL;
    } else if (read(rfd, (void*) le, sizeof(log_t)) < 0) {
      fprintf(stderr, "Cannot read the %" PRIu64 "-th log. block:%" PRIu64 ",errcode:%d,reason:%s", idx, block->id,
          errno, strerror(errno));
      rle = NULL;
    } else
      rle = le;

  } else {

    // read from memory
    rle = BLOG_ENTRY(block, idx);

  }

  return rle;
}

// NOTE: acquire read lock on blog before call this function.
// Find last log entry that has timestamp less or equal than (r,l).
int find_last_entry(block_t *block, uint64_t r, uint64_t l, uint64_t *last_entry) {
  uint64_t length, log_index, cur_diff;
  int rfd = dup(block->blog_rfd);
  log_t eb1, eb2;
  log_t *le1, *le2;

  length = block->log_tail;

  if (compare(r, l, BLOG_LAST_ENTRY(block)->r, BLOG_LAST_ENTRY(block)->l) > 0) {
    if (read_local_rtc() < r) {
      close(rfd);
      return -1;
    }
    log_index = length - 1;
  } else if (compare(r, l, BLOG_LAST_ENTRY(block)->r, BLOG_LAST_ENTRY(block)->l) == 0) {
    log_index = length - 1;
  } else {
    log_index = length / 2 > 0 ? length / 2 - 1 : 0;
    cur_diff = length / 4 > 1 ? length / 4 : 1;

    le1 = get_log_entry(block, log_index, rfd, &eb1);
    le2 = get_log_entry(block, log_index + 1, rfd, &eb2);
    while ((compare(r, l, le1->r, le1->l) == -1) || (compare(r, l, le2->r, le2->l) >= 0)) {
      if (compare(r, l, le1->r, le1->l) == -1)
        log_index -= cur_diff;
      else
        log_index += cur_diff;
      cur_diff = cur_diff > 1 ? cur_diff / 2 : 1;
      le1 = get_log_entry(block, log_index, rfd, &eb1);
      le2 = get_log_entry(block, log_index + 1, rfd, &eb2);
    }
  }
  *last_entry = log_index;

  close(rfd);
  return 0;
}

// NOTE: acquire read lock on blog before call this function
// Find last log entry that has user timestamp less or equal than ut.
void find_last_entry_by_ut(block_t *block, uint64_t ut, uint64_t *last_entry) {
  uint64_t length, log_index, cur_diff;
  int rfd = dup(block->blog_rfd);
  log_t eb1, eb2;
  log_t *le1, *le2;

  length = block->log_tail;
  if (BLOG_LAST_ENTRY(block)->u <= ut) {
    log_index = length - 1;
  } else {
    log_index = length / 2 > 0 ? length / 2 - 1 : 0;
    cur_diff = length / 4 > 1 ? length / 4 : 1;
    le1 = get_log_entry(block, log_index, rfd, &eb1);
    le2 = get_log_entry(block, log_index + 1, rfd, &eb2);
    while (le1->u > ut || le2->u <= ut) {
      if (le1->u > ut)
        log_index -= cur_diff;
      else
        log_index += cur_diff;
      cur_diff = cur_diff > 1 ? cur_diff / 2 : 1;
      le1 = get_log_entry(block, log_index, rfd, &eb1);
      le2 = get_log_entry(block, log_index + 1, rfd, &eb2);
    }
  }
  *last_entry = log_index;

  close(rfd);
}

/**
 * Check if the log capacity needs to be increased.
 * Do NOT acquire any lock on blog before calling me.
 * block  - Block object.
 * return  0, if successful,
 *        -1, otherwise.
 */
int check_and_increase_log_cap(block_t *block) {
  if (BLOG_IS_FULL(block)) {
    if (block->log_cap < MAX_INMEM_BLOG_ENTRIES) { // grow
      BLOG_WRLOCK(block);
      block->log = (log_t *) realloc((void *) block->log, 2 * block->log_cap * sizeof(log_t));
      block->log_cap *= 2;
      BLOG_UNLOCK(block);
      if (block->log == NULL)
        return -1;
    } else { // evict the oldest log_entry
      sem_wait(&block->log_sem);
      BLOG_WRLOCK(block);
      block->log_head++;
      BLOG_UNLOCK(block);
    }
  }
  return 0;
}

/**
 * Fills the snapshot from the log.
 * NOTE: acquire read lock on the blog befaore call this
 * @param block - block structure
 * @param log_entry - Last log entry for snapshot.
 * @param page_size - Page size for the filesystem.
 * @return Snapshot object.
 */
snapshot_t *fill_snapshot(block_t *block, uint64_t log_entry, uint32_t page_size) {
  snapshot_t *snapshot = (snapshot_t *) malloc(sizeof(snapshot_t));
  uint32_t nr_pages, i, cur_page;
  int rfd = dup(block->blog_rfd);
  log_t eb;
  log_t *le;

  snapshot->ref_count = 0;

  //skip the first SET_GENSTAMP
  le = get_log_entry(block, log_entry, rfd, &eb);
  while (le->op == SET_GENSTAMP) {
    log_entry--;
    le = get_log_entry(block, log_entry, rfd, &eb);
  }

  switch (le->op) {
    case CREATE_BLOCK:
      snapshot->status = ACTIVE;
      snapshot->length = 0;
      snapshot->pages = NULL;
      break;
    case DELETE_BLOCK:
    case BOL:
      snapshot->status = NON_ACTIVE;
      snapshot->length = 0;
      snapshot->pages = NULL;
      break;
    case WRITE:
      snapshot->status = ACTIVE;
      snapshot->length = le->block_length;
      nr_pages = (snapshot->length - 1 + page_size) / page_size;
      snapshot->pages = (uint64_t *) malloc(nr_pages * sizeof(uint64_t));
      for (i = 0; i < nr_pages; i++)
        snapshot->pages[i] = INVALID_PAGE_NO;
      while (nr_pages > 0) {
        for (i = 0; i < le->nr_pages; i++) {
          cur_page = i + le->start_page;
          if (snapshot->pages[cur_page] == INVALID_PAGE_NO) {
            snapshot->pages[cur_page] = le->first_pn + i;
            nr_pages--;
          }
        }
        log_entry--;
        le = get_log_entry(block, log_entry, rfd, &eb);
      }
      break;
    default:
      fprintf(stderr, "ERROR: Unknown operation %" PRIu32 " detected\n", le->op);
      exit(0);
  }

  close(rfd);
  return snapshot;
}

/**
 * Find a snapshot object with specific time. If not found, instatiate one.
 * NOTE: acquire read lock on blog before call this function.
 * block        - Block object.
 * snapshot_time- Time to take the snapshot.
 *                Must be lower or equal than a timestamp that might appear in the future.
 * snapshot_ptr - Snapshot pointer used for returning the correct snapshot instance.
 * by_ut        - if it is for user timestamp or not.
 * return  1, if created snapshot,
 *         0, if found snapshot,
 *         error code, otherwise. 
 */
int find_or_create_snapshot(block_t *block, uint64_t snapshot_time, size_t page_size, snapshot_t **snapshot_ptr,
    int by_ut) {
  snapshot_t *snapshot;
  uint64_t *log_ptr;
  uint64_t log_index;

  // If snapshot already exists return the existing snapshot.
  MAP_LOCK(log, by_ut ? block->log_map_ut : block->log_map_hlc, snapshot_time, 'w');
  if (MAP_READ(log, by_ut?block->log_map_ut:block->log_map_hlc, snapshot_time, &log_ptr) == 0) {
    log_index = *log_ptr;
    MAP_LOCK(snapshot, block->snapshot_map, log_index, 'r');
    if (MAP_READ(snapshot, block->snapshot_map, log_index, snapshot_ptr) == -1) {
      fprintf(stderr, "ERROR: Could not find snapshot in snapshot map although ID exists in log map.\n");
      exit(0);
    }
    MAP_UNLOCK(snapshot, block->snapshot_map, log_index);
    MAP_UNLOCK(log, by_ut ? block->log_map_ut : block->log_map_hlc, snapshot_time);
    return 0;
  }

  // for hlc: If snapshot can include future references do not create it.
  log_ptr = (uint64_t *) malloc(sizeof(uint64_t));
  if (by_ut) {
    find_last_entry_by_ut(block, snapshot_time, log_ptr);
  } else {
    if (find_last_entry(block, snapshot_time - 1, ULLONG_MAX, log_ptr) == -1) {
      free(log_ptr);
      fprintf(stderr, "WARNING: Snapshot was not created because it might have future entries.\n");
      return -1;
    }
  }
  log_index = *log_ptr;
  // Create snapshot.
  if (by_ut && log_index == block->log_tail &&
  BLOG_LAST_ENTRY(block)->u < snapshot_time) {
    // we do not create a log_map entry at this point because
    // future data may come later.
  } else if (MAP_CREATE_AND_WRITE(log, by_ut?block->log_map_ut:block->log_map_hlc, snapshot_time, log_ptr) == -1) {
    fprintf(stderr, "ERROR: Could not create snapshot although it does not exist in the log map.\n");
    exit(0);
  }
  MAP_LOCK(snapshot, block->snapshot_map, log_index, 'w');
  if (MAP_READ(snapshot, block->snapshot_map, log_index, snapshot_ptr) == 0) {
    snapshot = *snapshot_ptr;
    snapshot->ref_count++;
  } else {
    snapshot = fill_snapshot(block, log_index, page_size);
    snapshot->ref_count++;
    if (MAP_CREATE_AND_WRITE(snapshot, block->snapshot_map, log_index, snapshot) == -1) {
      fprintf(stderr, "ERROR: Could not create snapshot although it does not exist in the snapshot map.\n");
      exit(0);
    }
    (*snapshot_ptr) = snapshot;
  }
  MAP_UNLOCK(snapshot, block->snapshot_map, log_index);
  MAP_UNLOCK(log, by_ut ? block->log_map_ut : block->log_map_hlc, snapshot_time);
  return 1;
}

static void update_pers_bitmap(filesystem_t *fs, uint64_t first_pn, uint64_t nr_pages, int bSet) {
  if (first_pn / PAGE_FRAME_NR(fs) == (first_pn + nr_pages - 1) / PAGE_FRAME_NR(fs)) {
    blog_bitmap_togglebits(&fs->pers_bitmap, first_pn % PAGE_FRAME_NR(fs), nr_pages, bSet);
  } else {
    blog_bitmap_togglebits(&fs->pers_bitmap, first_pn % PAGE_FRAME_NR(fs),
        (PAGE_FRAME_NR(fs) - first_pn % PAGE_FRAME_NR(fs)), bSet);
    blog_bitmap_togglebits(&fs->pers_bitmap, 0, (first_pn + nr_pages) % PAGE_FRAME_NR(fs), bSet);
  }
}

#ifdef FLUSH_BATCHING
/*
 * flushBlogInBatch: flush the log entries specified by a batch of evt:
 * evt->block:          the block;
 * evt->log_length:     the log length;
 */
int flushBlogInBatch(filesystem_t *fs, struct _pers_queue * pbatch) {
  DEBUG_PRINT("flushBlogInBatch - begins.\n");

  int log_entry_batch_cap = MAX_FLUSH_BATCH, nr_log_entry = 0;
  int blog_wfd_batch_cap = MAX_FLUSH_BATCH, nr_blog_wfd = 0;
  int i;
  log_t * next_entry;
  log_t * log_entry_batch = (log_t *)malloc(sizeof(log_t)*log_entry_batch_cap);
  if(log_entry_batch == NULL) {
    fprintf(stderr, "flushBlogInBatch, cannot allocate memory for batched log entries. Error:%s\n\n",strerror(errno));
    return -1;
  }
  int * blog_wfd_batch = (int *)malloc(sizeof(int)*2*blog_wfd_batch_cap);
  if(blog_wfd_batch == NULL) {
    fprintf(stderr, "flushBlogInBatch, cannot allocate memory for batched blog file descriptors. Error:%s\n\n",strerror(errno));
    free(log_entry_batch);
    return -1;
  }
#define BLOG_BATCH_WFD(x) blog_wfd_batch[2*(x)]
#define BLOG_BATCH_CNT(x) blog_wfd_batch[2*(x)+1]
#define FLUSH_BATCH_FREE do { \
  if(log_entry_batch)free(log_entry_batch); \
  if(blog_wfd_batch)free(blog_wfd_batch); \
} while(0)

  // 1 - get all log_entries
  DEBUG_PRINT("flushBlogInBatch(1): get all log entries.\n");
  while(!TAILQ_EMPTY(pbatch)) {
    pers_event_t *evt = TAILQ_FIRST(pbatch);
    TAILQ_REMOVE(pbatch,evt,lnk);

    // prepare the blog_wfd_batch entry
    if(nr_blog_wfd == 0 || BLOG_BATCH_WFD(nr_blog_wfd-1) != evt->block->blog_wfd) {
      if( nr_blog_wfd == blog_wfd_batch_cap ) {    //make sure we have enough space for blog_wfd entries
        blog_wfd_batch_cap = (blog_wfd_batch_cap << 1);
        blog_wfd_batch = (int *)realloc(log_entry_batch,sizeof(int)*2*blog_wfd_batch_cap);
        if(blog_wfd_batch == NULL) {
          fprintf(stderr, "flushBlogInBatch, cannot reallocate memory for batched blog file descriptor. Error:%s\n", strerror(errno));
          FLUSH_BATCH_FREE;
          return -1;
        }
      }
      BLOG_BATCH_WFD(nr_blog_wfd) = evt->block->blog_wfd;
      BLOG_BATCH_CNT(nr_blog_wfd) = 0;
      nr_blog_wfd ++;
    }

    // flush pages
    for(;evt->block->log_pers < evt->log_length; evt->block->log_pers++) {
      if( nr_log_entry == log_entry_batch_cap ) {    //make sure we have enough space for log entries
        log_entry_batch_cap = (log_entry_batch_cap << 1);
        log_entry_batch = (log_t *)realloc(log_entry_batch,sizeof(log_t)*log_entry_batch_cap);
        if( log_entry_batch == NULL ) {
          fprintf(stderr, "flushBlogInBatch, cannot reallocate memory for batched log entries/blog file descriptor. Error:%s\n\n",strerror(errno));
          FLUSH_BATCH_FREE;
          return -1;
        }
      }

      // fill log entry in buffer.
      BLOG_RDLOCK(evt->block);
      log_entry_batch[nr_log_entry++] = *BLOG_NEXT_PERS_ENTRY(evt->block);
      BLOG_UNLOCK(evt->block);

      // increment the entry counter
      BLOG_BATCH_CNT(nr_blog_wfd-1)++;
    }
  }

  // 2 - flush pages for all writes.
  DEBUG_PRINT("flushBlogInBatch(2): flush pages. nr_log_entry=%d\n",nr_log_entry);
  for(i=0;i<nr_log_entry;i++) {
    void *pages;
    ssize_t nWrite;
    uint64_t first_pn,nr_pages;
    off_t page_file_ofst;

    if(log_entry_batch[i].op == WRITE) {
      first_pn = log_entry_batch[i].first_pn;
      nr_pages = log_entry_batch[i].nr_pages;
    } else
    continue;

#ifdef NO_PERSISTENCE
    pages = pages;
#else
    pages = PAGE_NR_TO_PTR_RB(fs,first_pn);
    // lseek to page location
    page_file_ofst = (off_t)(first_pn*fs->page_size);
    if(lseek(fs->page_wfd, page_file_ofst, SEEK_SET) == -1) {
      fprintf(stderr,"blogFlushInBatch, cannot lseek to location %ld in pagefile, Error:%s\n",page_file_ofst,strerror(errno));
      FLUSH_BATCH_FREE;
      return -1;
    }
    //write pages.
    if((first_pn/PAGE_FRAME_NR(fs)) != ((first_pn+nr_pages-1)/PAGE_FRAME_NR(fs))) {
      //   |-------------------------|
      //      ^last                ^first

      nWrite = write(fs->page_wfd, pages, (PAGE_FRAME_NR(fs)-(first_pn%PAGE_FRAME_NR(fs)))*fs->page_size);
      if(nWrite>0)
      nWrite = write(fs->page_wfd, fs->page_base_ring_buffer, ((first_pn+nr_pages)%PAGE_FRAME_NR(fs))*fs->page_size);
    } else {
      //   |-------------------------|
      //      ^first ^last
      nWrite = write(fs->page_wfd, pages, nr_pages*fs->page_size);
    }
    if(nWrite<0) {
      fprintf(stderr, "blogFlushInBatch, cannot write to persistent page file, Error:%s\n",strerror(errno));
      FLUSH_BATCH_FREE;
      return -1;
    }
    // touch the bitmap
    update_pers_bitmap(fs,first_pn,nr_pages,1);
#endif//NO_PERSISTENCE
  }
  if(fsync(fs->page_wfd)>0) {
    fprintf(stderr, "blogFlushInBatch, cannot write to persistent page file, Error:%s\n",strerror(errno));
    FLUSH_BATCH_FREE;
    return -1;
  }

  // 3 - flush log_entries
#ifdef NO_PERSISTENCE
#else
  DEBUG_PRINT("flushBlogInBatch(3): flush log entries.\n");
  next_entry = log_entry_batch;
  for(i=0;i<nr_blog_wfd;i++) {
    if(write(BLOG_BATCH_WFD(i),next_entry,sizeof(log_t)*BLOG_BATCH_CNT(i)) != (uint32_t) sizeof(log_t)*BLOG_BATCH_CNT(i)) {
      fprintf(stderr, "blogFlushInBatch, cannot write to blog file, Error:%s\n",strerror(errno));
      FLUSH_BATCH_FREE;
      return -2;
    }
    fsync(BLOG_BATCH_WFD(i));
    next_entry += BLOG_BATCH_CNT(i);
  }
#endif//NO_PERISTENCE
  FLUSH_BATCH_FREE; // free the resources.

  // 4 - update fs->nr_page_pers
  DEBUG_PRINT("flushBlogInBatch(4): update fs->nr_page_pers\n");
  pthread_spin_lock(&fs->tail_spinlock);
  while(fs->nr_page_pers<fs->nr_page_tail && blog_bitmap_testbit(&fs->pers_bitmap, fs->nr_page_pers%PAGE_FRAME_NR(fs))) {
    fs->nr_page_pers++;
  }
  pthread_spin_unlock(&fs->tail_spinlock);

  DEBUG_PRINT("flushBlogInBatch - ends.\n");
  return 0;
}
#else //FLUSH_BATCHING
/*
 * flushBlog: flush the log entries specified by evt:
 * evt->block:          the block;
 * evt->log_length:     the log length;
 */
static int flushBlog(filesystem_t *fs, pers_event_t *evt) {
  block_t *block = evt->block;
  DEBUG_PRINT("flushBlog:block=%" PRIu64 ",block->log_pers=%" PRIu64 ",evt->log_length=%" PRIu64 ".\n", block->id,
      block->log_pers, evt->log_length);

  // no new log to be flushed
  if (block->log_pers == evt->log_length)
    return 0;

  // flush to file
  for (; block->log_pers < evt->log_length; block->log_pers++) {

    log_t log_entry_val;
    void *pages;
    ssize_t nWrite;
    uint64_t first_pn, nr_pages;
    off_t page_file_ofst;

    // get log entry (because blog->log may move to other address due to realloc())
    BLOG_RDLOCK(block);
    log_entry_val = *BLOG_NEXT_PERS_ENTRY(block);
    BLOG_UNLOCK(block);

    // get pages
    if (log_entry_val.op == WRITE) {
      first_pn = log_entry_val.first_pn;
      nr_pages = log_entry_val.nr_pages;
    } else
      nr_pages = 0UL;

    // flush pages NOTE: page_fd was not opened with O_SYNC and O_DIRECT any more.
    if (nr_pages > 0UL) {
#ifdef NO_PERSISTENCE
      pages = pages;
#else
      pages = PAGE_NR_TO_PTR_RB(fs, first_pn);
      // lseek to page location
      page_file_ofst = (off_t) (first_pn * fs->page_size);
      if (lseek(fs->page_wfd, page_file_ofst, SEEK_SET) == -1) {
        fprintf(stderr, "Flush, cannot lseek to location %ld in pagefile, Error:%s\n", page_file_ofst, strerror(errno));
        return -1;
      }
      //write pages.
      if ((first_pn / PAGE_FRAME_NR(fs)) != ((first_pn + nr_pages - 1) / PAGE_FRAME_NR(fs))) {
        //   |-------------------------|
        //      ^last                ^first

        nWrite = write(fs->page_wfd, pages, (PAGE_FRAME_NR(fs) - (first_pn % PAGE_FRAME_NR(fs))) * fs->page_size);
        if (nWrite > 0)
          nWrite = write(fs->page_wfd, fs->page_base_ring_buffer,
              ((first_pn + nr_pages) % PAGE_FRAME_NR(fs)) * fs->page_size);
      } else {
        //   |-------------------------|
        //      ^first ^last
        nWrite = write(fs->page_wfd, pages, nr_pages * fs->page_size);
      }
      if (nWrite < 0 || fsync(fs->page_wfd) < 0) {
        fprintf(stderr, "Flush, cannot write to persistent page file, Error:%s\n", strerror(errno));
        return -1;
      }
      // touch the bitmap
      update_pers_bitmap(fs, first_pn, nr_pages, 1);
#endif
    }

    // write log
#ifdef NO_PERSISTENCE
#else
    if (write(block->blog_wfd, &log_entry_val, sizeof(log_t)) != sizeof(log_t)) {
      fprintf(stderr, "Flush, cannot write to blog file %ld."BLOGFILE_SUFFIX", Error:%s\n", block->id, strerror(errno));
      return -2;
    }
#endif

    // post log semaphore
    sem_post(&block->log_sem);
    DEBUG_PRINT("flushBlog:done.\n");
  }

#ifdef NO_PERSISTENCE
#else
  // flush to disk
  if (fsync(block->blog_wfd) != 0) {
    fprintf(stderr, "Flush, cannot fsync blog file, block id:%ld, Error:%s\n", block->id, strerror(errno));
    return -3;
  }
#endif

  // update fs->nr_page_pers
  pthread_spin_lock(&fs->tail_spinlock);
  while (fs->nr_page_pers < fs->nr_page_tail
      && blog_bitmap_testbit(&fs->pers_bitmap, fs->nr_page_pers % PAGE_FRAME_NR(fs))) {
    fs->nr_page_pers++;
  }
  pthread_spin_unlock(&fs->tail_spinlock);

  return 0;
}
#endif //FLUSH_BATCHING

/*
 * blog_pers_routine()
 * PARAM param: the blog writer context
 * RETURN: pointer to the param
 */
static void *blog_pers_routine(void * param) {
  filesystem_t *fs = (filesystem_t*) param;
  pers_event_t *evt;
  int evicTrigger = 0;
  uint32_t i;
  uint64_t nr_page_evic = 0UL;
#ifdef FLUSH_BATCHING
  int bStop = 0;
#endif

  while (1) {
#ifdef FLUSH_BATCHING
    int nr_evt = 0, qlen;
    struct _pers_queue pers_batch;
    TAILQ_INIT(&pers_batch);
    //STEP 1: Get the Queue
    do {
      PERS_DEQ(fs,&evt);
      TAILQ_INSERT_TAIL(&pers_batch,evt,lnk);
      nr_evt ++;
      PERS_QLEN(fs,qlen);
    }while(qlen>0 && nr_evt < MAX_FLUSH_BATCH);
    //STEP 2: flush blog in batch
    if(flushBlogInBatch(fs, &pers_batch) != 0) {
      fprintf(stderr, "Failed to flush blog in batch.\n");
      exit(1);
    }
    //STEP 3: free events
    while(!TAILQ_EMPTY(&pers_batch)) {
      evt = TAILQ_FIRST(&pers_batch);
      TAILQ_REMOVE(&pers_batch,evt,lnk);
      // End of Queue
      if(IS_EOQ(evt))
      bStop = 1;
      free(evt);
    }
    if(bStop)
    break;
#else //FLUSH_BATCHING
    // get event
    PERS_DEQ(fs, &evt);

    // End of Queue
    if (IS_EOQ(evt)) {
      free(evt);
      break;
    }

    // flush blog and touch the bitmaps...
    if (flushBlog(fs, evt) != 0) {
      fprintf(stderr, "Failed to flush blog: id=%ld,log_length=%ld\n", evt->block->id, evt->log_length);
      exit(1);
    }

    // free event
    free(evt);
#endif // FLUSH_BATCHING

    // eviction
    pthread_spin_lock(&fs->tail_spinlock);
    evicTrigger = (PAGE_FRAME_FREE_NR(fs) < PAGE_PER_BLOCK(fs));
    DEBUG_PRINT("Flush: Eviction triggered? %d.\n", evicTrigger);
    if (evicTrigger) {
      // We only evict space enough for two blocks or all the possible space for eviction, whichever is smaller. Let's design a smarter eviction policy here.
      nr_page_evic = MIN(fs->nr_page_pers - fs->nr_page_head, 2*PAGE_PER_BLOCK(fs));
    }
    pthread_spin_unlock(&fs->tail_spinlock);

    if (evicTrigger) {

      //clear bitmap
      for (i = 0; i < nr_page_evic; i++) {
        blog_bitmap_togglebit(&fs->pers_bitmap, (uint32_t) ((fs->nr_page_head + i) % PAGE_FRAME_NR(fs)), 0);
        sem_post(&fs->freepages_sem);
      }

      //advance the head
      pthread_rwlock_wrlock(&fs->head_rwlock);
      fs->nr_page_head += nr_page_evic;
      pthread_rwlock_unlock(&fs->head_rwlock);
    }
  }
  return param;
}

/*
 * createShmFile:
 * 1) create a file of "size" in tmpfs
 * 2) return the file descriptor
 * 3) return a negative integer on error.
 */
static int createShmFile(const char *fn, uint64_t size) {
  const char * tmpfs_dirs[] = { "/run/shm", "/dev/shm", NULL };
  const char ** p_tmpfs_dir = tmpfs_dirs;

  while (access(*p_tmpfs_dir, R_OK | W_OK) < 0 && *p_tmpfs_dir != NULL) {
    p_tmpfs_dir++;
  }

  if (p_tmpfs_dir == NULL) {
    fprintf(stderr, "Cannot find a tmpfs system.\n");
    return -1;
  }

  char fullname[64];
  sprintf(fullname, "%s/%s", *p_tmpfs_dir, fn);
  // check if we have enough space
  struct statvfs stat;

  if (statvfs(*p_tmpfs_dir, &stat) == -1) {
    fprintf(stderr, "Cannot open tmpfs VFS:%s,error=%s\n", *p_tmpfs_dir, strerror(errno));
    return -2;
  }
  if (size > stat.f_bsize * stat.f_bavail) {
    fprintf(stderr, "Cannot create file %s because we have only 0x%lx bytes free, but asked 0x%lx bytes.\n", fullname,
        stat.f_bsize * stat.f_bavail, size);
    return -3;
  }
  // create file
  int fd = open(fullname, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP | S_IROTH);
  if (fd < 0) {
    fprintf(stderr, "Cannot open/create file %s. Error: %s\n", fullname, strerror(errno));
    return -4;
  }

  // truncate it to given size
  if (ftruncate(fd, size) < 0) {
    fprintf(stderr, "Cannot truncate file %s to size 0x%lx, Error: %s\n", fullname, size, strerror(errno));
    close(fd);
    return -5;
  }
  return fd;
}

/*
 * mapPages:
 * filesystem.page_base_ring_buffer
 * filesystem.page_shm_fd
 * filesystem.nr_page_head = 0;
 * filesystem.nr_page_pers = 0;
 * filesystem.nr_page_tail = 0;
 * 1) check if tmpfs ramdisk space is enough
 * 2) create a huge file in tmpfs
 * 3) map it to memory
 * 4) update filesystem
 * return a negative integer on error
 */
static int mapPages(filesystem_t *fs) {

  // 1 - create a file in tmpfs
  if ((fs->page_shm_fd = createShmFile(SHM_PAGE_FN, fs->ring_buffer_size)) < 0) {
    return -1;
  }

  // 2 - map it in memory and setup file system parameters:
  if ((fs->page_base_ring_buffer = mmap(NULL, fs->ring_buffer_size, PROT_READ | PROT_WRITE, MAP_SHARED, fs->page_shm_fd,
      0)) < 0) {
    fprintf(stderr, "Fail to mmap page file %s. Error: %s\n", SHM_PAGE_FN, strerror(errno));
    return -2;
  }

  // 3 - nr_page_xxx = 0
  fs->nr_page_head = 0;
  fs->nr_page_pers = 0;
  fs->nr_page_tail = 0;

  return 0;
}

/*
 * loadPages:
 * filesystem.page_fd
 * filesystem.nr_pages
 * filesystem.nr_pages_pers
 * 1) find and open page file
 * 2) load page file contents into memory
 * return a negative integer on error
 */
/* TODO: do not load pages anymore
 static int loadPages(filesystem_t *fs, const char *pp){
 char fullname[256];
 sprintf(fullname,"%s/%s",pp,PAGEFILE);
 // STEP 1 Is file exists?
 fs->page_fd = open(fullname,O_RDWR|O_CREAT,S_IWUSR|S_IRUSR|S_IRGRP|S_IWGRP|S_IROTH);
 if(fs->page_fd < 0){
 fprintf(stderr, "Fail to open persistent page file %s. Error: %s\n",fullname, strerror(errno));
 return -1;
 }
 off_t fsize = lseek(fs->page_fd, 0, SEEK_END);
 // STEP 2 load
 if(fsize < fs->page_size || fsize < sizeof(PageHeader)){
 //create a new PageHeader
 PageHeader ph;
 ph.version = FFFS_VERSION;
 ph.block_size = fs->block_size;
 ph.page_size = fs->page_size;
 ph.nr_pages = 0;
 if(write(fs->page_fd,(const void *)&ph,sizeof ph) != sizeof ph){
 fprintf(stderr,"Fail to write to on-disk page file %s. Error: %s\n",fullname, strerror(errno));
 close(fs->page_fd);
 fs->page_fd = -1;
 return -2;
 }
 if(sizeof ph < fs->page_size){
 const int pdsz = fs->page_size - sizeof ph;
 void *padding = malloc(pdsz);
 memset(padding,0,pdsz);
 if(write(fs->page_fd,(const void *)padding, pdsz)!=pdsz){
 fprintf(stderr,"Fail to write to on-disk page file %s. Error: %s\n",fullname, strerror(errno));
 close(fs->page_fd);
 fs->page_fd = -1;
 return -3;
 }
 free(padding);
 }
 fs->nr_pages = 0;
 fs->nr_pages_pers = 0;
 }else{
 //load valid pages.
 lseek(fs->page_fd, 0, SEEK_SET);
 PageHeader ph;
 if(read(fs->page_fd,(void *)&ph,sizeof ph)!=sizeof ph){// if we can not read page header
 fprintf(stderr,"Fail to read from on-disk page file %s. Error: %s\n",fullname, strerror(errno));
 close(fs->page_fd);
 fs->page_fd = -1;
 return -4;
 }
 if(fsize < (MAX(fs->page_size,sizeof ph) + ph.nr_pages*fs->page_size)){// corrupted page file
 fprintf(stderr,"Fail to load pages, header shows %ld pages. But file size is %ld\n", ph.nr_pages, fsize);
 close(fs->page_fd);
 fs->page_fd = -1;
 return -5;
 }else{// valid page file
 if(fsize > (MAX(fs->page_size,sizeof ph) + ph.nr_pages*fs->page_size)){
 // truncate file to correct size, this may be from aborted writes.
 if(ftruncate(fs->page_fd, (MAX(fs->page_size,sizeof ph) + ph.nr_pages*fs->page_size))!=0){
 fprintf(stderr,"Fail to truncate page file %s, Error: %s\n", fullname, strerror(errno));
 close(fs->page_fd);
 fs->page_fd = -1;
 return -6;
 }
 }
 lseek(fs->page_fd, MAX(fs->page_size,sizeof ph), SEEK_SET); // jump to the first page.
 uint64_t i;
 for(i=0;i<ph.nr_pages;i++){// read them into memory.
 if(read(fs->page_fd, (void *)(fs->page_base + i*fs->page_size), fs->page_size)!=fs->page_size){
 fprintf(stderr,"Fail to read from on-disk page file %s. Error: %s\n",fullname, strerror(errno));
 close(fs->page_fd);
 fs->page_fd = -1;
 return -7;
 }
 }
 fs->nr_pages = ph.nr_pages;
 fs->nr_pages_pers = ph.nr_pages;
 }
 }
 return 0;
 }
 */

/*
 * replayBlog() will replay all log entries to reconstruct block info
 * It assume the following entries to be intialized:
 * 1) block->id
 * 2) block->log_head and log_tail are set.
 * 3) block->log_cap is set.
 * 4) fs->pers_path is set.
 * The following will be set:
 * 1) block->status
 * 2) block->length
 * 3) block->pages_cap
 * 4) block->pages
 * 5) Java metadata is initialzied by JNIBlog.replayLogOnMetadata().
 * RETURN VALUE:
 * the maximum page number ever seen.
 * REMARK:
 * Here we just go back to the last "CREAT" log entry and replay all
 * the log entries after that. In some application, there maybe too
 * many random write for a small file. Therefore it is unecessary to
 * go back to the CREATE log one. Fix this if necessary in future.
 */
static uint64_t replayBlog(JNIEnv *env, jobject thisObj, filesystem_t *fs, block_t *block, int rfd) {
  uint64_t i;
  uint32_t j;
  jclass thisCls = env->GetObjectClass(thisObj);
  jmethodID rl_mid = env->GetMethodID(thisCls, "replayLogOnMetadata", "(JIJ)V");
  uint64_t nr_blog_entries, guess_next_free_nr_page = 0UL;
  off_t blog_file_size;
  log_t log_entry;

  // STEP 1 - find the end of the blog
  blog_file_size = lseek(rfd, 0, SEEK_END);
  if (blog_file_size < 0) {
    fprintf(stderr, "WARN: Cannot seek to the end of blog file(block id=%" PRIu64 "): errcode:%d, reason:%s\n",
        block->id, errno, strerror(errno));
    return 0UL;
  }
  nr_blog_entries = blog_file_size / sizeof(log_t);
  if (nr_blog_entries == 0) {
    fprintf(stderr, "ERROR: Empty log for block:%" PRIu64, block->id);
    exit(-1);
  }

  // STEP 2 - go back to the lastest "CREATE"
  for (i = nr_blog_entries - 1; i >= 0; i--) {
    if (lseek(rfd, i * sizeof(log_t), SEEK_SET) < 0) {
      fprintf(stderr, "WARN: Cannot seek to %" PRIu64 " (block id=%" PRIu64 "). errcode:%d, reason:%s\n",
          i * sizeof(log_t), block->id, errno, strerror(errno));
      return 0UL;
    }
    if (read(rfd, &log_entry, sizeof(log_t)) < 0) {
      fprintf(stderr, "WARN: Cannot load log entry %" PRIu64 "(block id=%" PRIu64 "). errcode:%d, reason:%s\n", i,
          block->id, errno, strerror(errno));
      return 0UL;
    }
    DEBUG_PRINT("goback:[%" PRIu64 "]block_id=%" PRIu64 ",op=%d\n", i, block->id, log_entry.op);
    if (log_entry.op == CREATE_BLOCK)
      break;
  }

  // STEP 3 - replay block from "CREATE"
  block->status = ACTIVE;
  block->length = 0;
  if (MAP_CREATE_AND_WRITE(block, fs->block_map, block->id, block) != 0) {
    fprintf(stderr, "replayBlog:Faile to create block:%ld.\n", block->id);
    exit(1);
  }
  env->CallObjectMethod(thisObj, rl_mid, block->id, log_entry.op, log_entry.first_pn);
  for (i++; i < nr_blog_entries; i++) {
    if (read(rfd, &log_entry, sizeof(log_t)) < 0) {
      fprintf(stderr, "WARN: Cannot load log entry %" PRIu64 "(block id=%" PRIu64 "). errcode:%d, reason:%s\n", i,
          block->id, errno, strerror(errno));
      return 0UL;
    }

    DEBUG_PRINT("[%" PRIu64 "]replay:block_id=%" PRIu64 ",op=%d\n, first_pn=%" PRIu64 ", nr_pages=%" PRIu32 "", i,
        block->id, log_entry.op, log_entry.first_pn, log_entry.nr_pages);

    switch (log_entry.op) {
      case BOL:
      case SET_GENSTAMP:
      case CREATE_BLOCK:
        break;

      case WRITE:
        block->length = log_entry.block_length;
        while (block->pages_cap < (block->length + fs->page_size - 1) / fs->page_size)
          block->pages_cap = MAX(block->pages_cap * 2, block->pages_cap + 2);
        block->pages = (uint64_t*) realloc(block->pages, sizeof(uint64_t) * block->pages_cap);

        //file block->pages
        for (j = 0; j < log_entry.nr_pages; j++)
          block->pages[j] = (log_entry.first_pn + j);

        //UPDATE guess_next_free_nr_page
        guess_next_free_nr_page = MAX(guess_next_free_nr_page, log_entry.first_pn + log_entry.nr_pages);
        break;

      case DELETE_BLOCK:
        block->status = NON_ACTIVE;
        block->log_head = 0;
        block->log_tail = 0;
        if (block->log_cap > 0)
          free(block->pages);
        block->log_cap = 0;
        block->pages = NULL;
        break;

      default:
        fprintf(stderr, "replayBlog: unknown log type:%d, %ld-th log entry of block-%ld\n", log_entry.op, i, block->id);
        exit(1);
    }

    DEBUG_PRINT("[%" PRIu64 "]replay:java\n", i);
    // replay log on java metadata.
    env->CallObjectMethod(thisObj, rl_mid, block->id, log_entry.op, log_entry.first_pn);
    DEBUG_PRINT("[%" PRIu64 "]replay:next,nr_blog_entries=%" PRIu64 "\n", i, nr_blog_entries);
  }
  DEBUG_PRINT("reply return.\n");
  return guess_next_free_nr_page;
}

/*
 * loadBlogs()
 *   we assume that the following members of "filesystem structure" have been initialized already:
 *   - block_size
 *   - page_size
 *   - page_base_ring_buffer
 *   - page_base_mapped_file
 *   on success, the following members should have been initialized:
 *   - block_map
 *   - nr_page_head
 *   - nr_page_pers
 *   - nr_page_tail
 * PARAM 
 *   fs: file system structure
 *   pp: path to the persistent data.
 * RETURN VALUES: 
 *    0 for succeed. and writer_thrd will be initialized.
 *   -1 for "log file is corrupted", 
 *   -2 for "page file is correupted".
 *   -3 for "block operation".
 *   -4 for "unknown errors"
 */
static int loadBlogs(JNIEnv *env, jobject thisObj, filesystem_t *fs, const char *pp) {
  // STEP 1 find all <id>.blog files
  DIR *d;
  struct dirent *dir;
  uint64_t block_id, next_nr_page = 0ul;
  d = opendir(pp);
  if (d == NULL) {
    fprintf(stderr, "Cannot open directory:%s, Error:%s\n", pp, strerror(errno));
    return -1;
  };
  // STEP 2 for each .blog file
  while ((dir = readdir(d)) != NULL) {
    DEBUG_PRINT("Loading %s...", dir->d_name);
    /// 2.1 if it is not a .blog file, go to the next
    char blog_filename[256], page_filename[256];
    if (strlen(dir->d_name) < strlen(BLOGFILE_SUFFIX) + 1 || dir->d_type != DT_REG
        || strcmp(dir->d_name + strlen(dir->d_name) - strlen(BLOGFILE_SUFFIX) - 1, "." BLOGFILE_SUFFIX)) {
      DEBUG_PRINT("skipped.\n");
      continue;
    }
    sscanf(dir->d_name, "%ld." BLOGFILE_SUFFIX, &block_id);
    DEBUG_PRINT("Block id = %ld\n", block_id);

    /// 2.2 open file
    sprintf(blog_filename, "%s/%ld." BLOGFILE_SUFFIX, pp, block_id);
    int blog_rfd = open(blog_filename, O_RDONLY);
    int blog_wfd = open(blog_filename, O_WRONLY);
    if (blog_rfd < 0 || blog_wfd < 0) {
      fprintf(stderr, "Cannot open file: %s or %s, error:%s\n", blog_filename, page_filename, strerror(errno));
      return -2;
    }

    /// 2.3 check file length, truncate it when required.
    off_t fsize = lseek(blog_rfd, 0, SEEK_END);
    off_t corrupted_bytes = fsize % sizeof(log_t);
    off_t exp_fsize = fsize - corrupted_bytes;
    if (corrupted_bytes > 0) {        //this is due to aborted log flush.
      if (ftruncate(blog_wfd, exp_fsize) < 0) {
        fprintf(stderr,
            "WARNING: Cannot load blog: %ld." BLOGFILE_SUFFIX", because it cannot be truncated to expected size:%ld, Error%s\n",
            block_id, exp_fsize, strerror(errno));
        close(blog_rfd);
        close(blog_wfd);
        continue;
      }
    }
    lseek(blog_rfd, 0, SEEK_SET);
    lseek(blog_wfd, 0, SEEK_END);
    if (exp_fsize == 0) { // no log entry exists
      fprintf(stderr, "WARNiNG: empty log entry: for block%" PRIu64 ", skip it...\n", block_id);
      close(blog_rfd);
      close(blog_wfd);
      continue;
    }

    /// 2.4 create a block structure;
    block_t *block = (block_t*) malloc(sizeof(block_t));
    block->id = block_id;

    block->log_pers = exp_fsize / sizeof(log_t);
    block->log_tail = block->log_pers;
    block->log_head = (block->log_tail > MAX_INMEM_BLOG_ENTRIES) ? (block->log_tail - MAX_INMEM_BLOG_ENTRIES) : 0;

    if (block->log_pers >= MAX_INMEM_BLOG_ENTRIES)
      block->log_cap = MAX_INMEM_BLOG_ENTRIES;
    else {
      block->log_cap = 16;
      while (block->log_cap < block->log_pers)
        block->log_cap = block->log_cap << 1;
    }
    block->log = (log_t*) malloc(sizeof(log_t) * block->log_cap);
    block->pages = NULL;
    block->pages_cap = 0;
    block->log_map_hlc = MAP_INITIALIZE(log);
    block->log_map_ut = MAP_INITIALIZE(log);
    block->snapshot_map = MAP_INITIALIZE(snapshot);
    if (pthread_rwlock_init(&block->blog_rwlock, NULL) != 0) {
      fprintf(stderr, "ERROR: cannot initialize rw lock for block:%ld\n", block->id);
      exit(-1);
    }
    if (sem_init(&block->log_sem, 0, block->log_tail - block->log_head) != 0) {
      fprintf(stderr, "ERROR: cannot initialize semaphore for block:%" PRIu64 "\n", block->id);
      exit(-1);
    }
    block->blog_wfd = blog_wfd;
    block->blog_rfd = blog_rfd;

    /// 2.5 load log entries
    uint64_t i;
    lseek(blog_rfd, block->log_head * sizeof(log_t), SEEK_SET);
    for (i = block->log_head; i < block->log_tail; i++) {
      if (read(blog_rfd, (void*) (block->log + (i % MAX_INMEM_BLOG_ENTRIES)), sizeof(log_t)) != sizeof(log_t)) {
        fprintf(stderr, "WARNING: Cannot read log entry from %s, error:%s\n", blog_filename, strerror(errno));
        close(blog_rfd);
        close(blog_wfd);
        free((void*) block->log);
        free(block);
        continue;
      }
    }

    /// 2.6 replay log entries: reconstruct the block status
    uint64_t guess_next_nr_page = replayBlog(env, thisObj, fs, block, blog_rfd);
    next_nr_page = MAX(guess_next_nr_page, next_nr_page);
    DEBUG_PRINT("done.\n");
  }
  closedir(d);

  // 3 - update nr_page_head/pers/tail
  fs->nr_page_head = next_nr_page;
  fs->nr_page_pers = next_nr_page;
  fs->nr_page_tail = next_nr_page;

  return 0;
}

/**
 * Open/Create page persistent
 * after openPageFile, the following members of fs is initialized:
 * - fs->page_wfd
 */
static void openPageFile(filesystem_t *fs, const char *persPath) {
  // 1 - prepare the path
  char buf[256];
  sprintf(buf, "%s/%s", persPath, PERS_PAGE_FN);
  // 2 - open the file
  //fs->page_wfd = open(buf,O_WRONLY|O_CREAT|O_SYNC|O_DIRECT,S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
  fs->page_wfd = open(buf, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
  fs->page_rfd = open(buf, O_RDONLY);
  if (fs->page_wfd < 0 || fs->page_rfd < 0) {
    fprintf(stderr, "ERROR: Cannot open persistent page file:%s, error:%d, reason:%s\n", buf, errno, strerror(errno));
    exit(-1);
  }
  // 3 - truncate the page file
  if (ftruncate(fs->page_wfd, MAX_PERS_PAGE_SIZE) < 0) {
    fprintf(stderr, "ERROR: Cannot truncate page file:%s, error:%d, reason:%s\n", buf, errno, strerror(errno));
    close(fs->page_wfd);
    exit(-1);
  }
  // 4 - map it
  fs->page_base_mapped_file = mmap(NULL, MAX_PERS_PAGE_SIZE, PROT_READ, MAP_SHARED, fs->page_rfd, 0UL);
  if (fs->page_base_mapped_file == MAP_FAILED) {
    fprintf(stderr, "ERROR: Cannot map page file:%s, error:%d, reason:%s\n", buf, errno, strerror(errno));
    close(fs->page_wfd);
    close(fs->page_rfd);
    exit(-1);
  }
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
int initializeInternal(JNIEnv *env, jobject thisObj, uint64_t poolSize, uint32_t blockSize, uint32_t pageSize,
    const char *persPath, int useRDMA, const char *devname, const uint16_t rdmaPort) {

  DEBUG_PRINT("initializeInternal is called\n");

  jclass thisCls = env->GetObjectClass(thisObj);
  jfieldID long_id = env->GetFieldID(thisCls, "jniData", "J");
  jfieldID hlc_id = env->GetFieldID(thisCls, "hlc", "Ledu/cornell/cs/sa/HybridLogicalClock;");
  jclass hlc_class = env->FindClass("edu/cornell/cs/sa/HybridLogicalClock");
  jmethodID cid = env->GetMethodID(hlc_class, "<init>", "()V");
  jobject hlc_object = env->NewObject(hlc_class, cid);
  filesystem_t *fs = (filesystem_t *) malloc(sizeof(filesystem_t));
  if (fs == NULL) {
    perror("Error");
    exit(1);
  }

  // STEP 1: Initialize file system members
  fs->block_size = blockSize;
  fs->ring_buffer_size = poolSize;
  fs->page_size = pageSize;
  if (blog_bitmap_init(&(fs->pers_bitmap), (uint32_t) (poolSize / pageSize)) != 0) {
    fprintf(stderr, "ERROR: Cannot initialize fs->pers_bitmap. errcode=%d, reason=%s\n", errno, strerror(errno));
    exit(-1);
  }
  fs->block_map = MAP_INITIALIZE(block);
  if (fs->block_map == NULL) {
    fprintf(stderr, "ERROR: Allocation of block_map failed.\n");
    exit(-1);
  }
  strcpy(fs->pers_path, persPath);

  // STEP 2: create ramdisk file and map it into memory.
  if (mapPages(fs) != 0) {
    perror("mapPages Error");
    exit(1);
  }

  // STEP 2.5: for RDMA
  if (useRDMA) {
    fs->rdmaCtxt = (RDMACtxt*) malloc(sizeof(RDMACtxt));
    if (initializeContext(fs->rdmaCtxt, fs->page_base_ring_buffer, LOG2(poolSize), LOG2(pageSize), devname, rdmaPort,
        0/*this is for the server side*/)) {
      fprintf(stderr, "Initialize: fail to initialize RDMA context.\n");
      exit(1);
    }
  } else {
    fs->rdmaCtxt = NULL;
  }

  // STEP 2.8: initialize page fd
  openPageFile(fs, persPath);

  // STEP 3: load blogs.
  if (loadBlogs(env, thisObj, fs, persPath) != 0) {
    fprintf(stderr, "Fail to load blogs.\n");
    exit(1);
  }

  // STEP 4: initialize locks & semaphores
  if (pthread_rwlock_init(&fs->head_rwlock, NULL) != 0) {
    fprintf(stderr, "Fail to initialize head read/write lock, errcode:%d, reason:%s\n", errno, strerror(errno));
    exit(1);
  }

  if (pthread_spin_init(&fs->tail_spinlock, PTHREAD_PROCESS_PRIVATE) != 0) {
    fprintf(stderr, "Fail to initialize tail spin lock, errcode:%d, reason:%s\n", errno, strerror(errno));
    exit(1);
  }

  if (sem_init(&fs->freepages_sem, 0, (uint32_t) (fs->ring_buffer_size / fs->page_size)) != 0) {
    fprintf(stderr, "Fail to initialize freepage semaphore, errcode:%d, reason:%s\n", errno, strerror(errno));
    exit(1);
  }

  if (pthread_spin_init(&fs->clock_spinlock, PTHREAD_PROCESS_PRIVATE) != 0) {
    fprintf(stderr, "Fail to initialize clock spin lock, error: %s\n", strerror(errno));
    exit(1);
  }

  env->SetObjectField(thisObj, hlc_id, hlc_object);
  env->SetLongField(thisObj, long_id, (uint64_t) fs);

  // STEP 5: initialize queues, semaphore and flags.
  if (sem_init(&fs->pers_queue_sem, 0, 0) < 0) {
    fprintf(stderr, "Cannot initialize semaphore, Error:%s\n", strerror(errno));
    exit(-1);
  }
  TAILQ_INIT(&fs->pers_queue); // queue
  if (pthread_spin_init(&fs->queue_spinlock, PTHREAD_PROCESS_PRIVATE) != 0) {
    fprintf(stderr, "Fail to initialize queue spin lock, error: %s\n", strerror(errno));
    exit(1);
  }

  // STEP 6: start the persistent thread.
  if (pthread_create(&fs->pers_thrd, NULL, blog_pers_routine, (void*) fs)) {
    fprintf(stderr, "CANNOT create blogPersistent thread, exit\n");
    exit(-1);
  }

  DEBUG_PRINT("initializeInternal() is done\n");
  return 0;
}
#pragma GCC diagnostic pop

/*
 * Class:     edu_cornell_cs_blog_JNIBlog_initialize
 * Method:    initialize
 * Signature: (JIILjava/lang/String;)I
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_initialize(JNIEnv *env, jobject thisObj, jlong poolSize,
    jint blockSize, jint pageSize, jstring persPath) {
  const char * pp = env->GetStringUTFChars(persPath, NULL); // get the presistent path
  jint ret;

  //call internal initializer:
  ret = initializeInternal(env, thisObj, poolSize, blockSize, pageSize, pp, 0, NULL, 0);

  env->ReleaseStringUTFChars(persPath, pp);

  return ret;
}
#pragma GCC diagnostic pop

/*
 * Class:     edu_cornell_cs_blog_JNIBlog_initializeRDMA
 * Method:    initialize
 * Signature: (II)I
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_initializeRDMA(JNIEnv *env, jobject thisObj, jlong poolSize,
    jint blockSize, jint pageSize, jstring persPath, jstring dev, jint port) {
  const char * pp = env->GetStringUTFChars(persPath, NULL); // get the presistent path
  const char * devname = env->GetStringUTFChars(dev, NULL); // get the device name
  jint ret;

  //call internal initializer:
  ret = initializeInternal(env, thisObj, poolSize, blockSize, pageSize, pp, 1, devname, (const uint16_t) port);

  env->ReleaseStringUTFChars(persPath, pp);
  env->ReleaseStringUTFChars(dev, devname);

  return ret;
}
#pragma GCC diagnostic pop

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    createBlock
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;JJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_createBlock(JNIEnv *env, jobject thisObj, jobject mhlc,
    jlong blockId, jlong genStamp) {
  DEBUG_PRINT("begin createBlock:genStamp=%ld\n", genStamp);
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  uint64_t block_id = (uint64_t) blockId;
  block_t *block;
  log_t *log_entry;
  char fullname[256];

  // If the block already exists return an error.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) == 0) {
    fprintf(stderr, "WARNING: Block with ID %" PRIu64 " already exists.\n", block_id);
    MAP_UNLOCK(block, filesystem->block_map, block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);

  // Create the block structure.
  block = (block_t *) malloc(sizeof(block_t));
  block->id = block_id;
  block->log_head = 0;
  block->log_tail = 0;
  if (sem_init(&block->log_sem, 0, block->log_tail - block->log_head) != 0) {
    fprintf(stderr, "ERROR: cannot initialize semaphore for block:%" PRIu64 "\n", block->id);
    exit(-1);
  }
  block->log_cap = 2;
  block->status = ACTIVE;
  block->length = 0;
  block->pages_cap = 0;
  block->pages = NULL;
  block->log = (log_t*) malloc(2 * sizeof(log_t));
  block->log_map_hlc = MAP_INITIALIZE(log);
  block->log_map_ut = MAP_INITIALIZE(log);
  block->snapshot_map = MAP_INITIALIZE(snapshot);

  // Open Files.
  sprintf(fullname, "%s/%ld." BLOGFILE_SUFFIX, filesystem->pers_path, block_id);
  block->blog_wfd = open(fullname, O_WRONLY | O_CREAT | O_SYNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
  block->blog_rfd = open(fullname, O_RDONLY);

  if ((block->blog_rfd < 0) || (block->blog_wfd < 0)) {
    fprintf(stderr, "ERROR: cannot open blog file for write, block id=%ld, Error: %s\n", block_id, strerror(errno));
    exit(-2);
  }
  block->log_pers = 0;

  // Initialize lock.
  if (pthread_rwlock_init(&block->blog_rwlock, NULL) != 0) {
    fprintf(stderr, "ERROR: cannot initialize rw lock for block:%ld\n", block->id);
    exit(-1);
  }

  // Notify the persistent thread.
  pers_event_t *evt = (pers_event_t*) malloc(sizeof(pers_event_t));

  // add the first two entries.
  log_entry = (log_t*) block->log;
  log_entry->op = BOL;
  log_entry->block_length = 0;
  log_entry->start_page = 0;
  log_entry->nr_pages = 0;
  log_entry->r = 0;
  log_entry->l = 0;
  log_entry->u = 0;
  log_entry->first_pn = 0;
  log_entry++;
  log_entry->op = CREATE_BLOCK;
  log_entry->block_length = 0;
  log_entry->start_page = 0;
  log_entry->nr_pages = 0;
  log_entry->u = 0;
  log_entry->first_pn = (uint64_t) genStamp;
  pthread_spin_lock(&(filesystem->clock_spinlock));
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env, mhlc, log_entry);
  block->log_head = 0;
  block->log_tail = 2;
  evt->block = block;
  evt->log_length = block->log_tail;
  PERS_ENQ(filesystem, evt);
  pthread_spin_unlock(&(filesystem->clock_spinlock));

  // Put block to block map.
  MAP_LOCK(block, filesystem->block_map, block_id, 'w');
  if (MAP_CREATE_AND_WRITE(block, filesystem->block_map, block_id, block) != 0) {
    fprintf(stderr, "ERROR: Block with ID %" PRIu64 " was not found in the block map while it should be there.\n",
            block_id);
    exit(0);
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);
  DEBUG_PRINT("end createBlock\n");
  return 0;
}

/*
 * NOTE: acquire write lock on blog before calling me.
 * estimate the user's timestamp where it is not avaialbe.
 * For deleteBlock(), we need this.
 * we assume that u = K*r; where K is a linear coefficient.
 * so u2 = u1/r1*r2
 */
long estimate_user_timestamp(block_t *block, long r) {
  log_t *le = BLOG_LAST_ENTRY(block);

  if (block->log_tail == 0 || le->r == 0)
    return r;
  else
    return (long) ((double) le->u / (double) le->r * r);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    deleteBlock
 * Signature: (Ledu/cornell/cs/sa/VectorClock;J)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_deleteBlock(JNIEnv *env, jobject thisObj, jobject mhlc,
    jlong blockId) {
  DEBUG_PRINT("begin deleteBlock.\n");
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  uint64_t block_id = (uint64_t) blockId;
  block_t *block;
  log_t *log_entry;

  // If the block does not exist return an error.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) == -1 || block->status != ACTIVE) {
    fprintf(stderr, "WARNING: It is not possible to delete block with ID %" PRIu64 " because it does not exist.\n",
        block_id);
    MAP_UNLOCK(block, filesystem->block_map, block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);

  // Notify the persistent thread.
  pers_event_t *evt = (pers_event_t*) malloc(sizeof(pers_event_t));

  // Create the corresponding log entry.
  check_and_increase_log_cap(block);
  if (BLOG_WRLOCK(block) != 0) {
    fprintf(stderr, "ERROR: Cannot acquire write lock on block:%ld, Error:%s\n", block_id, strerror(errno));
    return -2;
  }
  log_entry = BLOG_NEXT_ENTRY(block);
  log_entry->op = DELETE_BLOCK;
  log_entry->block_length = 0;
  log_entry->start_page = 0;
  log_entry->nr_pages = 0;
  log_entry->u = estimate_user_timestamp(block, log_entry->r);
  log_entry->first_pn = 0;
  pthread_spin_lock(&(filesystem->clock_spinlock));
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env, mhlc, log_entry);
  block->log_tail += 1;
  evt->block = block;
  evt->log_length = block->log_tail;
  PERS_ENQ(filesystem, evt);
  pthread_spin_unlock(&(filesystem->clock_spinlock));
  BLOG_UNLOCK(block);

  // Release the current state of the block.
  block->status = NON_ACTIVE;
  block->length = 0;
  if (block->pages_cap > 0)
    free(block->pages);
  block->pages_cap = 0;
  block->pages = NULL;
  return 0;
}

// readBlockInternal: read the latest state of a block
int readBlockInternal(JNIEnv *env, jobject thisObj, jlong blockId, jint blkOfst, jint length,
    const transport_parameters_t *tp) {
  DEBUG_PRINT("begin readBlockInternal:blockId=%" PRIu64 ",blkOfst=%d,length=%d\n", blockId, blkOfst, length);
#ifdef DEBUG
  struct timeval tv1,tv2;
#endif
  DEBUG_TIMESTAMP(tv1);
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  snapshot_t *snapshot;
  block_t *block;
  uint64_t block_id = (uint64_t) blockId;
  uint32_t block_offset = (uint32_t) blkOfst;
  uint32_t read_length = (uint32_t) length;
  uint64_t log_index;
  char *page_data;

  // Find the block.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) != 0 || block->status != ACTIVE) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " has never been active\n", block_id);
    MAP_UNLOCK(block, filesystem->block_map, block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);

  DEBUG_PRINT("[readBlockInternal]A-Find the block...done\n");

  // Find/create a snapshot.
  if (BLOG_RDLOCK(block) != 0) {
    fprintf(stderr, "ERROR: Cannot acquire read lock on block:%ld, Error:%s\n", block_id, strerror(errno));
    return -1;
  }
  log_index = block->log_tail - 1;
  MAP_LOCK(snapshot, block->snapshot_map, log_index, 'w');
  if (MAP_READ(snapshot, block->snapshot_map, log_index, &snapshot) != 0) {
    snapshot = fill_snapshot(block, log_index, filesystem->page_size);
    if (MAP_CREATE_AND_WRITE(snapshot, block->snapshot_map, log_index, snapshot) != 0) {
      fprintf(stderr, "ERROR: Cannot create or read snapshot for Block %" PRIu64 " and index %" PRIu64 "\n", block_id,
          log_index);
      exit(0);
    }
  }
  MAP_UNLOCK(snapshot, block->snapshot_map, log_index);
  BLOG_UNLOCK(block);

  DEBUG_PRINT("[readBlockInternal]A-Find/create the snapshot...done\n");

  // In case the block does not exist return an error.
  if (snapshot->status == NON_ACTIVE) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " is not active at snapshot with log index %" PRIu64 ".\n",
        block_id, log_index);
    return -1;
  }

  // In case the data you ask is not written return an error.
  if (block_offset >= snapshot->length) {
    fprintf(stderr, "WARNING: Block %" PRIu64 " is not written at %" PRIu32 " byte.\n", block_id, block_offset);
    return -2;
  }

  // See if the data is partially written.
  if (block_offset + read_length > snapshot->length)
    read_length = snapshot->length - block_offset;

  if (tp->mode == TP_LOCAL_BUFFER) { // read to local buffer;
    uint32_t cur_length, page_id, page_offset;
    page_id = block_offset / filesystem->page_size;
    jbyteArray buf = tp->param.lbuf.buf;
    uint32_t buffer_offset = (uint32_t) tp->param.lbuf.bufOfst;
    // Fill the buffer:
    pthread_rwlock_rdlock(&filesystem->head_rwlock); // use read lock on system pages...

    // If all the data is transfered
    page_offset = block_offset % filesystem->page_size;
    page_data =
        (snapshot->pages[page_id] >= filesystem->nr_page_head) ?
            PAGE_NR_TO_PTR_RB(filesystem, snapshot->pages[page_id]) :
            PAGE_NR_TO_PTR_MF(filesystem, snapshot->pages[page_id]);
    page_data += page_offset;
    cur_length = filesystem->page_size - page_offset;
    DEBUG_PRINT("page_offset=%d,page_base_ring_buffer=%p,page_base_mapped_file=%p\n", page_offset,
        filesystem->page_base_ring_buffer, filesystem->page_base_mapped_file);
    if (cur_length >= read_length) {
      env->SetByteArrayRegion(buf, buffer_offset, read_length, (jbyte*) page_data);
      DEBUG_PRINT("[readBlockInternal]:[%p:%d]\n", page_data, read_length);
    } else {
      env->SetByteArrayRegion(buf, buffer_offset, cur_length, (jbyte*) page_data);
      DEBUG_PRINT("[readBlockInternal]:[%p:%d]\n", page_data, cur_length);
      page_id++;
      while (1) {
        page_data =
            (snapshot->pages[page_id] >= filesystem->nr_page_head) ?
                PAGE_NR_TO_PTR_RB(filesystem, snapshot->pages[page_id]) :
                PAGE_NR_TO_PTR_MF(filesystem, snapshot->pages[page_id]);
        if (cur_length + filesystem->page_size >= read_length) {
          env->SetByteArrayRegion(buf, buffer_offset + cur_length, read_length - cur_length, (jbyte*) page_data);
          DEBUG_PRINT("[readBlockInternal]:[%p:%d]\n", page_data, read_length - cur_length);
          break;
        }
        env->SetByteArrayRegion(buf, buffer_offset + cur_length, filesystem->page_size, (jbyte*) page_data);
        DEBUG_PRINT("[readBlockInternal]:[%p:%ld]\n", page_data, filesystem->page_size);
        cur_length += filesystem->page_size;
        page_id++;
      }
    }

    pthread_rwlock_unlock(&filesystem->head_rwlock); // release read lock on page pool...
  } else { // read to remote memory(RDMA)
#ifdef ODP_ENABLED
  uint32_t start_page_id = blkOfst / filesystem->page_size;
  uint32_t end_page_id = (blkOfst+length-1) / filesystem->page_size;
  uint32_t npage = 0;
  void **paddrlist = (void**)malloc(sizeof(void*)*(end_page_id - start_page_id + 1));
  pthread_rwlock_rdlock(&filesystem->head_rwlock); // use read lock on system pages ...

  while(start_page_id<=end_page_id) {
    // TODO: check if data is in the memory or not.
    paddrlist[npage] = PAGE_NR_TO_PTR(filesystem,snapshot->pages[start_page_id]);
    start_page_id ++;
    npage ++;
  }

  // get ip str
  int ipSize = (int)env->GetArrayLength(env,tp->param.rdma.client_ip);
  jbyte ipStr[16];
  env->GetByteArrayRegion(tp->param.rdma.client_ip, 0, ipSize, ipStr);
  ipStr[ipSize] = 0;
  uint32_t ipkey = inet_addr((const char*)ipStr);

  // get remote address
  uint64_t vaddr = tp->param.rdma.vaddr;// vaddr is start of a block.
  const uint64_t address = vaddr + block_offset - (block_offset % filesystem->page_size);

  // rdma write...
  int rc = rdmaWrite(filesystem->rdmaCtxt, (const uint32_t)ipkey, tp->param.rdma.remote_pid, (const uint64_t)address, (const void **)paddrlist,npage,0);

  pthread_rwlock_unlock(&filesystem->head_rwlock);// release read lock on page pool...

  free(paddrlist);
  if(rc !=0 ) {
    fprintf(stderr, "readBlockRDMA: rdmaWrite failed with error code=%d.\n", rc);
    return -2;
  }
#else
    fprintf(stderr, "readBlockRDMA: cannot do RDMA Read without ODP_ENABLED, please re-build FFFS with ODP_ENABLED.\n");
    return -2;
#endif
  }
  DEBUG_TIMESTAMP(tv2);

  DEBUG_PRINT("end readBlockInternal. %dbytes %ldus %.3fMB/s\n", length, TIMESPAN(tv1,tv2),
      (double)length/TIMESPAN(tv1,tv2));
  return read_length;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlock
 * Signature: (JJJIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlock__JIII_3B(JNIEnv *env, jobject thisObj, jlong blockId,
    jint blkOfst, jint bufOfst, jint length, jbyteArray buf) {
  // setup transport parameter.
  transport_parameters_t tp;
  tp.mode = TP_LOCAL_BUFFER;
  tp.param.lbuf.buf = buf;
  tp.param.lbuf.bufOfst = bufOfst;
  return (jint) readBlockInternal(env, thisObj, blockId, blkOfst, length, &tp);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlockRDMA
 * Signature: (JII[BIJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlockRDMA__JII_3BIJ(JNIEnv *env, jobject thisObj,
    jlong blockId, jint blkOfst, jint length, jbyteArray clientIp, jint rpid, jlong vaddr) {

  // setup transport parameter.
  transport_parameters_t tp;
  tp.mode = TP_RDMA;
  tp.param.rdma.client_ip = clientIp;
  tp.param.rdma.remote_pid = rpid;
  tp.param.rdma.vaddr = vaddr;

  return (jint) readBlockInternal(env, thisObj, blockId, blkOfst, length, &tp);
}

// readBlockInternalByTime(): read block state at given timestamp...
int readBlockInternalByTime(JNIEnv *env, jobject thisObj, jlong blockId, jlong t, jint blkOfst, jint length,
    transport_parameters_t *tp, jboolean byUserTimestamp) {
  DEBUG_PRINT("begin readBlockInternalByTime.\n");
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  snapshot_t *snapshot;
  block_t *block;
  uint64_t snapshot_time = (uint64_t) t;
  uint64_t block_id = (uint64_t) blockId;
  uint32_t block_offset = (uint32_t) blkOfst;
  uint32_t read_length = (uint32_t) length;
  uint32_t cur_length, page_id, page_offset;
  char *page_data;

  // Find the block.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) != 0) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " has never been active\n", block_id);
    return -2;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);

  // Create snapshot.
  if (BLOG_RDLOCK(block) != 0) {
    fprintf(stderr, "ERROR: Cannot acquire read lock on block:%ld, Error:%s\n", block_id, strerror(errno));
    return -1;
  }
  if (find_or_create_snapshot(block, snapshot_time, filesystem->page_size, &snapshot, byUserTimestamp == JNI_TRUE)
      < 0) {
    fprintf(stderr, "WARNING: Snapshot for time %" PRIu64 " cannot be created.\n", snapshot_time);
    BLOG_UNLOCK(block);
    return -1;
  }
  BLOG_UNLOCK(block);

  // In case the block does not exist return an error.
  if (snapshot->status == NON_ACTIVE) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " is not active at snapshot with rtc %" PRIu64 ".\n", block_id,
        snapshot_time);
    return -2;
  }

  // In case the data you ask is not written return an error.
  if (block_offset >= snapshot->length) {
    fprintf(stderr, "WARNING: Block %" PRIu64 " is not written at %" PRIu32 " byte.\n", block_id, block_offset);
    return -3;
  }

  // See if the data is partially written.
  if (block_offset + read_length > snapshot->length)
    read_length = snapshot->length - block_offset;

  // Fill the buffer.
  if (tp->mode == TP_LOCAL_BUFFER) { //read to local buffer
    jbyteArray buf = tp->param.lbuf.buf;
    uint32_t buffer_offset = (uint32_t) tp->param.lbuf.bufOfst;
    page_id = block_offset / filesystem->page_size;

    pthread_rwlock_rdlock(&filesystem->head_rwlock); // read lock on ringbuffer head

    page_offset = block_offset % filesystem->page_size;
    page_data =
        (snapshot->pages[page_id] >= filesystem->nr_page_head) ?
            PAGE_NR_TO_PTR_RB(filesystem, snapshot->pages[page_id]) :
            PAGE_NR_TO_PTR_MF(filesystem, snapshot->pages[page_id]);
    page_data += page_offset;
    cur_length = filesystem->page_size - page_offset;
    if (cur_length >= read_length) {
      env->SetByteArrayRegion(buf, buffer_offset, read_length, (jbyte*) page_data);
    } else {
      env->SetByteArrayRegion(buf, buffer_offset, cur_length, (jbyte*) page_data);
      page_id++;
      while (1) {
        page_data =
            (snapshot->pages[page_id] >= filesystem->nr_page_head) ?
                PAGE_NR_TO_PTR_RB(filesystem, snapshot->pages[page_id]) :
                PAGE_NR_TO_PTR_MF(filesystem, snapshot->pages[page_id]);
        if (cur_length + filesystem->page_size >= read_length) {
          env->SetByteArrayRegion(buf, buffer_offset + cur_length, read_length - cur_length, (jbyte*) page_data);
          break;
        }
        env->SetByteArrayRegion(buf, buffer_offset + cur_length, filesystem->page_size, (jbyte*) page_data);
        cur_length += filesystem->page_size;
        page_id++;
      }
    }

    pthread_rwlock_unlock(&filesystem->head_rwlock); // release lock
  } else { // read to remote buffer(RDMA)
#ifdef ODP_ENABLED
  // TODO: revise design part with Implicit/explicit ODP
  uint32_t start_page_id = blkOfst / filesystem->page_size;
  uint32_t end_page_id = (blkOfst+length-1) / filesystem->page_size;
  uint32_t npage = 0;
  void **paddrlist = (void**)malloc(sizeof(void*)*(end_page_id - start_page_id + 1));
  while(start_page_id<=end_page_id) {
    paddrlist[npage] = PAGE_NR_TO_PTR(filesystem,snapshot->pages[start_page_id]);
    start_page_id ++;
    npage ++;
  }

  // get ip str
  int ipSize = (int)env->GetArrayLength(env,tp->param.rdma.client_ip);
  jbyte ipStr[16];
  env->GetByteArrayRegion(tp->param.rdma.client_ip, 0, ipSize, ipStr);
  ipStr[ipSize] = 0;
  uint32_t ipkey = inet_addr((const char*)ipStr);

  // get remote address
  uint64_t vaddr = tp->param.rdma.vaddr;// vaddr is the start of the block.
  const uint64_t address = vaddr + block_offset - (block_offset % filesystem->page_size);

  // rdma write...
  int rc = rdmaWrite(filesystem->rdmaCtxt, (const uint32_t)ipkey, tp->param.rdma.remote_pid, (const uint64_t)address, (const void **)paddrlist,npage,0);
  free(paddrlist);
  if(rc !=0 ) {
    fprintf(stderr, "readBlockRDMA: rdmaWrite failed with error code=%d.\n", rc);
    return -2;
  }
#else
    fprintf(stderr, "readBlockRDMA: cannot do RDMA Read without ODP_ENABLED, please re-build FFFS with ODP_ENABLED.\n");
    return -2;
#endif
  }
  DEBUG_PRINT("end readBlockInternalByTime.\n");
  return read_length;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlock
 * Signature: (JJIII[BZ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlock__JJIII_3BZ(JNIEnv *env, jobject thisObj,
    jlong blockId, jlong t, jint blkOfst, jint bufOfst, jint length, jbyteArray buf, jboolean byUserTimestamp) {
  // setup the transport parameter
  transport_parameters_t tp;
  tp.mode = TP_LOCAL_BUFFER;
  tp.param.lbuf.buf = buf;
  tp.param.lbuf.bufOfst = bufOfst;

  return (jint) readBlockInternalByTime(env, thisObj, blockId, t, blkOfst, length, &tp, byUserTimestamp);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readBlockRDMA
 * Signature: (JJII[BIJZ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_readBlockRDMA__JJII_3BIJZ(JNIEnv *env, jobject thisObj,
    jlong blockId, jlong t, jint blkOfst, jint length, jbyteArray clientIp, jint rpid, jlong vaddr,
    jboolean byUserTimestamp) {
  // setup the transport parameter
  transport_parameters_t tp;
  tp.mode = TP_RDMA;
  tp.param.rdma.client_ip = clientIp;
  tp.param.rdma.remote_pid = rpid;
  tp.param.rdma.vaddr = vaddr;

  return (jint) readBlockInternalByTime(env, thisObj, blockId, t, blkOfst, length, &tp, byUserTimestamp);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (JJJ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getNumberOfBytes__J(JNIEnv *env, jobject thisObj,
    jlong blockId) {
  DEBUG_PRINT("begin getNumberOfBytes__J.\n");
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  block_t *block;
  uint64_t log_index;
  uint64_t block_id = (uint64_t) blockId;
  jint ret;

  // Find the block.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) != 0 || block->status != ACTIVE) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " has never been active\n", block_id);
    MAP_UNLOCK(block, filesystem->block_map, block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);

  // Find the last entry.
  if (BLOG_RDLOCK(block) != 0) {
    fprintf(stderr, "ERROR: Cannot acquire read lock on block:%ld, Error:%s\n", block_id, strerror(errno));
    return -1;
  }
  log_index = ((block->log_tail - 1) % block->log_cap);
  ret = (jint) block->log[log_index].block_length;
  BLOG_UNLOCK(block);

  DEBUG_PRINT("end of getNumberOfBytes__J.\n");
  return ret;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (JJZ)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getNumberOfBytes__JJZ(JNIEnv *env, jobject thisObj,
    jlong blockId, jlong t, jboolean by_ut) {
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  snapshot_t *snapshot;
  block_t *block;
  // uint64_t *log_ptr;
  // uint64_t log_index;
  uint64_t block_id = (uint64_t) blockId;
  uint64_t snapshot_time = (uint64_t) t;

  // Find the block.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) != 0) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " has never been active\n", block_id);
    MAP_UNLOCK(block, filesystem->block_map, block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);

  // Find/create snapshot for the timestamp.
  if (BLOG_RDLOCK(block) != 0) {
    fprintf(stderr, "ERROR: Cannot acquire read lock on block:%ld, Error:%s\n", block_id, strerror(errno));
    return -1;
  }
  if (find_or_create_snapshot(block, snapshot_time, filesystem->page_size, &snapshot, by_ut == JNI_TRUE) < 0) {
    fprintf(stderr, "WARNING: Snapshot for time %" PRIu64 " cannot be created.\n", snapshot_time);
    BLOG_UNLOCK(block);
    return -2;
  }
  BLOG_UNLOCK(block);

  // In case the block does not exist return an error.
  if (snapshot->status == NON_ACTIVE) {
    fprintf(stderr, "WARNING: Block with id %" PRIu64 " is not active at snapshot with rtc %" PRIu64 ".\n", block_id,
        snapshot_time);
    return -1;
  }

  return (jint) snapshot->length;
}

/*
 * return the number of first page. use PAGE_NR_TO_PTR_RB(fs,nr) to convert
 * page number to pointer
 */
static inline uint64_t blog_allocate_pages(filesystem_t *fs, int npage) {
  int i;

  uint64_t fpn;

  // STEP 1 - get semaphore
  for (i = 0; i < npage; i++)
    sem_wait(&fs->freepages_sem);

  // STEP 2 - lock tail
  pthread_spin_lock(&fs->tail_spinlock);

  // STEP 3 - update tail
  fpn = fs->nr_page_tail;
  fs->nr_page_tail += npage;

  // STEP 4 - unlock tail
  pthread_spin_unlock(&fs->tail_spinlock);

  DEBUG_PRINT("blog_allocate_pages:allocate:%" PRIu64 ":L%d\n", fpn, npage);

  return fpn;
}

// writeBlockInternal: write block
int writeBlockInternal(JNIEnv *env, jobject thisObj, jobject mhlc, jlong blockId, jint blkOfst, jint length,
                       const transport_parameters_t * tp) {
  DEBUG_PRINT("begin writeBlock:blockId=%ld,blkOfst=%d,length=%d\n", blockId, blkOfst, length);
  filesystem_t *filesystem = get_filesystem(env, thisObj);
  uint64_t block_id = (uint64_t) blockId;
  uint64_t first_pn, last_pn;
  uint32_t block_offset = (uint32_t) blkOfst;
  uint32_t write_length = (uint32_t) length;
  size_t page_size = filesystem->page_size;
  log_t *log_entry;
  block_t *block;
  uint64_t userTimestamp;
  uint32_t first_page, last_page, end_of_write, write_page_length, first_page_offset, last_page_length, pages_cap;
  char *data, *temp_data;
  uint32_t i;

  // If the block does not exist return an error.
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) == -1 || block->status != ACTIVE) {
    fprintf(stderr, "WARNING: It is not possible to write to block with ID %" PRIu64 " because it does not exist.\n",
        block_id);
    MAP_UNLOCK(block, filesystem->block_map, block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);

  // In case you write after the end of the block return an error.
  if (block_offset > block->length) {
    fprintf(stderr, "WARNING: Block %" PRIu64 " cannot be written at %" PRIu32 " byte.\n", block_id, block_offset);
    return -2;
  }

  // Create the new pages.
  first_page = block_offset / page_size;
  last_page = (block_offset + write_length - 1) / page_size;
  first_page_offset = block_offset % page_size;
  first_pn = blog_allocate_pages(filesystem, last_page - first_page + 1);
  last_pn = first_pn + last_page - first_page;
  DEBUG_PRINT(
      "[writeBlockInternal-1]first_page=%d,last_page=%d,first_page_offset=%d,first_pn=%" PRIu64 ",last_pn=%" PRIu64 "\n",
      first_page, last_page, first_page_offset, first_pn, last_pn);

  if (tp->mode == TP_LOCAL_BUFFER) {  // write to local buffer
    jbyteArray buf = tp->param.lbuf.buf;
    uint32_t buffer_offset = (uint32_t) tp->param.lbuf.bufOfst;
    temp_data = PAGE_NR_TO_PTR_RB(filesystem, first_pn) + first_page_offset;
    DEBUG_PRINT("[writeBlockInternal-1.1]temp_data=%p\n", temp_data);

    // Write all the data from the packet.
    if (first_page == last_page) {
      env->GetByteArrayRegion(buf, (jint) buffer_offset, (jint) write_length, (jbyte *) temp_data);
      temp_data += write_length;
      last_page_length = first_page_offset + write_length;
    } else {
      write_page_length = page_size - first_page_offset;
      env->GetByteArrayRegion(buf, (jint) buffer_offset, (jint) write_page_length, (jbyte *) temp_data);
      buffer_offset += write_page_length;
      for (i = 1; i < last_page - first_page; i++) {
        temp_data = PAGE_NR_TO_PTR_RB(filesystem, first_pn + i);
        write_page_length = page_size;
        env->GetByteArrayRegion(buf, (jint) buffer_offset, (jint) write_page_length, (jbyte *) temp_data);
        buffer_offset += write_page_length;
      }
      temp_data = PAGE_NR_TO_PTR_RB(filesystem, last_pn);
      write_page_length = (block_offset + write_length - 1) % page_size + 1;
      env->GetByteArrayRegion(buf, (jint) buffer_offset, (jint) write_page_length, (jbyte *) temp_data);
      temp_data += write_page_length;
      last_page_length = write_page_length;
    }

    // get user timestamp
    userTimestamp = tp->param.lbuf.user_timestamp;
  } else { // read from remote memory
    // set up position markers
    last_page_length = (block_offset + length) % page_size; // could be zero!!!
    if (last_page_length == 0)
      last_page_length = page_size; // fix it.
    uint32_t new_pages_length = last_page - first_page + 1; // number of new pages.

    // setup page array
    void **paddrlist = (void**) malloc(new_pages_length * sizeof(void*)); //page list for rdma transfer
    for (i = 0; i < new_pages_length; i++)
      paddrlist[i] = (void*) PAGE_NR_TO_PTR_RB(filesystem, first_pn + i);

    // do write by rdma read...
    // - get ipkey
    int ipSize = (int) env->GetArrayLength(tp->param.rdma.client_ip);
    jbyte ipStr[16];
    env->GetByteArrayRegion(tp->param.rdma.client_ip, 0, ipSize, ipStr);
    ipStr[ipSize] = 0;
    uint32_t ipkey = inet_addr((const char*) ipStr);

    // - get remote address, remote address is start of the block
    const uint64_t address = tp->param.rdma.vaddr + first_page * page_size;
    // - rdma read by big chunk 
    // int rc = rdmaRead(filesystem->rdmaCtxt, (const uint32_t)ipkey, tp->param.rdma.remote_pid, (const uint64_t)address, (const void**)&paddr, 1, new_pages_length*page_size);
    // or by pages.
    int rc = rdmaRead(filesystem->rdmaCtxt, (const uint32_t )ipkey, tp->param.rdma.remote_pid, (const uint64_t )address,
        (const void** )paddrlist, new_pages_length, page_size);
    free(paddrlist);
    if (rc != 0) {
      fprintf(stderr, "writeBlockInternal: rdmaRead failed with error code = %d.\n", rc);
      return -3;
    }
    // update temp_data to the last character in position.
    temp_data = PAGE_NR_TO_PTR_RB(filesystem,last_pn) + ((block_offset + write_length - 1) % page_size + 1);

    // get user timestamp
    {
      //// create direct byte buffer
      data = PAGE_NR_TO_PTR_RB(filesystem, first_pn);
      jobject bbObj = env->NewDirectByteBuffer(data, new_pages_length * page_size);
      if (bbObj == NULL) {
        fprintf(stderr,
            "writeBlockInternal: cannot create java DirectByteBuffer using NewDirectByteBuffer(env=%p,data=%p,capacity=%ld)\n",
            env, data, (last_page - first_page) * page_size);
        return -4;
      }
      //// set position and limit
      jclass bbClz = env->GetObjectClass(bbObj);
      if (bbClz == NULL) {
        fprintf(stderr, "writeBlockInternal: cannot get DirectByteBuffer class!\n");
        return -5;
      }
      jmethodID position_id = env->GetMethodID(bbClz, "position", "(I)Ljava/nio/Buffer;");
      jmethodID limit_id = env->GetMethodID(bbClz, "limit", "(I)Ljava/nio/Buffer;");
      env->CallObjectMethod(bbObj, position_id, first_page_offset);
      env->CallObjectMethod(bbObj, limit_id, first_page_offset + length);

      //// call record paerser
      jobject rpObj = tp->param.rdma.record_parser;
      if (rpObj == NULL) {
        fprintf(stderr, "writeBlockInternal: record_parser is NULL!\n");
        return -6;
      }
      jclass rpClz = env->GetObjectClass(rpObj);
      if (rpClz == NULL) {
        fprintf(stderr, "writeBlockInternal: cannot get record_parser class!\n");
        return -7;
      }
      jmethodID parse_record_id = env->GetMethodID(rpClz, "ParseRecord", "(Ljava/nio/ByteBuffer;)I");
      jmethodID get_user_timestamp_id = env->GetMethodID(rpClz, "getUserTimestamp", "()J");
      env->CallIntMethod(rpObj, parse_record_id, bbObj);
      if (env->ExceptionCheck() == JNI_TRUE) {
        fprintf(stderr, "writeBlockInternal: record_parser cannot parse data!\n");
        return -8;
      }
      userTimestamp = (uint64_t) env->CallLongMethod(rpObj, get_user_timestamp_id);
    } // end get user timestamp
  }

  //fill the first written page, if required.
  pthread_rwlock_rdlock(&filesystem->head_rwlock);
  if (first_page_offset > 0) {
    void * page_ptr =
        filesystem->nr_page_head <= block->pages[first_page] ?
            PAGE_NR_TO_PTR_RB(filesystem, block->pages[first_page]) :
            PAGE_NR_TO_PTR_MF(filesystem, block->pages[first_page]);
    DEBUG_PRINT(
        "[writeBlockInternal-1.5]first_page_in_block=%d,first_pn_in_page_pool=%" PRIu64 ",first_page_offset=%d\n",
        first_page, first_pn, first_page_offset);
    memcpy(PAGE_NR_TO_PTR_RB(filesystem, first_pn), page_ptr, first_page_offset);
  }

  //fill the last written page, if required.
  end_of_write = last_page * page_size + last_page_length;
  if (end_of_write < block->length) {
    write_page_length =
        (last_page + 1) * page_size <= block->length ?
            page_size - last_page_length : block->length % page_size - last_page_length;
    char * page_ptr =
        filesystem->nr_page_head <= block->pages[last_page] ?
            PAGE_NR_TO_PTR_RB(filesystem, block->pages[last_page]) :
            PAGE_NR_TO_PTR_MF(filesystem, block->pages[last_page]);
    DEBUG_PRINT(
        "[writeBlockInternal-1.6]first_page_in_block=%d,first_pn_in_page_pool=%" PRIu64 ",first_page_offset=%d\n",
        first_page, first_pn, first_page_offset);
    memcpy(temp_data, page_ptr + last_page_length, write_page_length);
  } else { // update the block length
    block->length = end_of_write;
  }
  pthread_rwlock_unlock(&filesystem->head_rwlock);

  // Fill the event for persistent thread.
  pers_event_t *event = (pers_event_t *) malloc(sizeof(pers_event_t));
  event->block = block;
  event->log_length = block->log_tail + 1;

  // Create the corresponding log entry. 
  check_and_increase_log_cap(block);
  if (BLOG_WRLOCK(block) != 0) {
    fprintf(stderr, "ERROR: Cannot acquire write lock on block:%ld, Error:%s\n", block_id, strerror(errno));
    return -9;
  }
  log_entry = BLOG_NEXT_ENTRY(block);
  log_entry->op = WRITE;
  log_entry->block_length = block->length;
  log_entry->start_page = first_page;
  log_entry->nr_pages = last_page - first_page + 1;
  log_entry->u = userTimestamp;
  log_entry->first_pn = first_pn;
  pthread_spin_lock(&(filesystem->clock_spinlock));
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env, mhlc, log_entry);
  block->log_tail++;
  PERS_ENQ(filesystem, event);
  pthread_spin_unlock(&(filesystem->clock_spinlock));
  BLOG_UNLOCK(block);

  // Fill block with the appropriate information.
  if (block->pages_cap == 0)
    pages_cap = 1;
  else
    pages_cap = block->pages_cap;
  while ((block->length - 1) / filesystem->page_size >= pages_cap)
    pages_cap *= 2;
  if (pages_cap > block->pages_cap) {
    block->pages_cap = pages_cap;
    block->pages = (uint64_t*) realloc(block->pages, block->pages_cap * sizeof(uint64_t));
  }
  for (i = 0; i < log_entry->nr_pages; i++)
    block->pages[log_entry->start_page + i] = first_pn + i;
  DEBUG_PRINT("end writeBlock.\n");
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    writeBlock
 * Signature: (Ledu/cornell/cs/sa/VectorClock;JIII[B)I
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_writeBlock(JNIEnv *env, jobject thisObj, jobject mhlc,
    jlong userTimestamp, jlong blockId, jint blkOfst, jint bufOfst, jint length, jbyteArray buf) {

  //setup transport parameters
  transport_parameters_t tp;
  tp.mode = TP_LOCAL_BUFFER;
  tp.param.lbuf.buf = buf;
  tp.param.lbuf.bufOfst = bufOfst;
  tp.param.lbuf.user_timestamp = userTimestamp;

  return (jint) writeBlockInternal(env, thisObj, mhlc, blockId, blkOfst, length, &tp);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    getNumberOfBytes
 * Signature: (JJZ)I
 * Method:    writeBlock
 * Signature: (Ledu/cornell/cs/sa/HybridLogicalClock;Ledu/cornell/cs/blog/IRecordParser;JII[BIJ)I 
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_writeBlockRDMA(JNIEnv *env, jobject thisObj, jobject mhlc,
    jobject rp, jlong blockId, jint blkOfst, jint length, jbyteArray clientIp, jint rpid, jlong bufaddr) {

  //setup transport parameters
  transport_parameters_t tp;
  tp.mode = TP_RDMA;
  tp.param.rdma.client_ip = clientIp;
  tp.param.rdma.remote_pid = rpid;
  tp.param.rdma.vaddr = bufaddr;
  tp.param.rdma.record_parser = rp; // only for write.

  return (jint) writeBlockInternal(env, thisObj, mhlc, blockId, blkOfst, length, &tp);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    readLocalRTC
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_edu_cornell_cs_blog_JNIBlog_readLocalRTC(JNIEnv *env, jclass thisCls) {
  return read_local_rtc();
}

JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_getPid(JNIEnv *env, jclass thisCls) {
  return getpid();
}

JNIEXPORT void Java_edu_cornell_cs_blog_JNIBlog_destroy(JNIEnv *env, jobject thisObj) {
  DEBUG_PRINT("beginning destroy.\n");
  filesystem_t *fs = get_filesystem(env, thisObj);
  void * ret;
  uint64_t nr_blog, block_id;
  uint64_t *ids;
  block_t *block;
  char fullname[256];

  // kill blog writer
  pers_event_t *evt = (pers_event_t*) malloc(sizeof(pers_event_t));
  evt->block = NULL;
  PERS_ENQ(fs, evt);
  DEBUG_PRINT("Waiting for persistent thread to join\n");
  if (pthread_join(fs->pers_thrd, &ret) != 0)
    fprintf(stderr, "waiting for blogWriter thread error...we may lose some data.\n");
  DEBUG_PRINT("Joined\n");

  // fs destroy the spin locks and sempahores
  pthread_rwlock_destroy(&fs->head_rwlock);
  pthread_spin_destroy(&fs->tail_spinlock);
  pthread_spin_destroy(&fs->clock_spinlock);
  sem_destroy(&fs->freepages_sem);
  sem_destroy(&fs->pers_queue_sem);
  pthread_spin_destroy(&fs->queue_spinlock);
  DEBUG_PRINT("Destroyed Spinlocks\n");

  // fs destroy the blog locks
  nr_blog = MAP_LENGTH(block, fs->block_map);
  ids = MAP_GET_IDS(block, fs->block_map, nr_blog);
  while (nr_blog--) {
    block_id = ids[nr_blog];
    DEBUG_PRINT("Block ID: %" PRIu64 " Remaining: %" PRIu64 "\n", block_id, nr_blog);
    MAP_LOCK(block, fs->block_map, block_id, 'w');
    DEBUG_PRINT("Block ID: %" PRIu64 " Remaining: %" PRIu64 "\n", block_id, nr_blog);
    if (MAP_READ(block,fs->block_map, block_id, &block) == -1) {
      fprintf(stderr, "Warning: cannot read block from map, id=%ld.\n", block_id);
      continue;
    }
    pthread_rwlock_destroy(&block->blog_rwlock);
    close(block->blog_wfd);
    close(block->blog_rfd);
    MAP_UNLOCK(block, fs->block_map, block_id);
  }
  DEBUG_PRINT("Destroy Locks\n");

  // destroy RDMAContext
  if (fs->rdmaCtxt != NULL)
    destroyContext(fs->rdmaCtxt);
  DEBUG_PRINT("Destroy RDMA Context\n");

  // close files
  if (fs->page_shm_fd != INT32_MAX) {
    munmap(fs->page_base_ring_buffer, fs->ring_buffer_size);
    close(fs->page_shm_fd);
    fs->page_shm_fd = INT32_MAX;
  }
  if (fs->page_wfd != -1) {
    munmap(fs->page_base_mapped_file, MAX_PERS_PAGE_SIZE);
    close(fs->page_wfd);
    close(fs->page_rfd);
  }
  DEBUG_PRINT("Close files\n");

  // remove shm file.
  sprintf(fullname, "/run/shm/" SHM_PAGE_FN);
  if (access(fullname, R_OK | W_OK) != 0) {
    sprintf(fullname, "/dev/shm" SHM_PAGE_FN);
  }
  if (remove(fullname) != 0) {
    fprintf(stderr, "WARNING: Fail to remove page cache file %s, error:%s.\n", fullname, strerror(errno));
  }

  DEBUG_PRINT("end destroy.\n");
}

/**
 * set generation stamp a block. // this is for Datanode.
 */
JNIEXPORT jint JNICALL Java_edu_cornell_cs_blog_JNIBlog_setGenStamp(JNIEnv *env, jobject thisObj, jobject mhlc,
    jlong blockId, jlong genStamp) {
  DEBUG_PRINT("begin setGenStamp.\n");
  filesystem_t *filesystem;
  uint64_t block_id = (uint64_t) blockId;
  block_t *block;
  log_t *log_entry;

  filesystem = get_filesystem(env, thisObj);
  //STEP 1: check if blockId is included
  MAP_LOCK(block, filesystem->block_map, block_id, 'r');
  if (MAP_READ(block, filesystem->block_map, block_id, &block) == -1) {
    fprintf(stderr,
        "WARNING: It is not possible to set generation stamp for block with ID %" PRIu64 " because it does not exist.\n",
        block_id);
    MAP_UNLOCK(block, filesystem->block_map, block_id);
    return -1;
  }
  MAP_UNLOCK(block, filesystem->block_map, block_id);

  // Notify the persistent thread.
  pers_event_t *evt = (pers_event_t*) malloc(sizeof(pers_event_t));

  //STEP 2: append log
  check_and_increase_log_cap(block);
  if (BLOG_WRLOCK(block) != 0) {
    fprintf(stderr, "ERROR: Cannot acquire write lock on block:%ld, Error:%s\n", block_id, strerror(errno));
    return -2;
  }
  log_entry = BLOG_NEXT_ENTRY(block);
  log_entry->op = SET_GENSTAMP;
  log_entry->block_length = block->length;
  log_entry->start_page = 0;
  log_entry->nr_pages = 0;
  log_entry->u = BLOG_LAST_ENTRY(block)->u; // we reuse the last userTimestamp
  log_entry->first_pn = (uint64_t) genStamp;
  pthread_spin_lock(&(filesystem->clock_spinlock));
  tick_hybrid_logical_clock(env, get_hybrid_logical_clock(env, thisObj), mhlc);
  update_log_clock(env, mhlc, log_entry);
  block->log_tail += 1;
  evt->block = block;
  evt->log_length = block->log_tail;
  PERS_ENQ(filesystem, evt);
  pthread_spin_unlock(&(filesystem->clock_spinlock));
  BLOG_UNLOCK(block);

  DEBUG_PRINT("end setGenStamp.\n");
  return 0;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpInitialize
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpInitialize(JNIEnv *env, jclass thisCls, jint psz,
    jint align, jstring dev, jint port) {
  RDMACtxt *ctxt = (RDMACtxt*) malloc(sizeof(RDMACtxt));
  const char * devname = env->GetStringUTFChars(dev, NULL); // get the RDMA device name.
  if (initializeContext(ctxt, NULL, (const uint32_t) psz, (const uint32_t) align, devname, (const uint16_t) port, 1)) { // this is for client
    free(ctxt);
    fprintf(stderr, "Cannot initialize rdma context.\n");
    if (devname)
      env->ReleaseStringUTFChars(dev, devname);
    return (jlong) 0;
  }
  if (devname)
    env->ReleaseStringUTFChars(dev, devname);
  return (jlong) ctxt;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpDestroy
(JNIEnv *env, jclass thisCls, jlong hRDMABufferPool) {
  destroyContext((RDMACtxt*)hRDMABufferPool);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpAllocateBlockBuffer
 * Signature: (J)Ledu/cornell/cs/blog/JNIBlog$RBPBuffer;
 */
JNIEXPORT jobject JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpAllocateBlockBuffer(JNIEnv *env, jclass thisCls,
    jlong hRDMABufferPool) {
  // STEP 1: create an object
  jclass bufCls = env->FindClass("edu/cornell/cs/blog/JNIBlog$RBPBuffer");
  if (bufCls == NULL) {
    fprintf(stderr, "Cannot find the buffers.");
    return NULL;
  }
  jmethodID bufConstructorId = env->GetMethodID(bufCls, "<init>", "()V");
  if (bufConstructorId == NULL) {
    fprintf(stderr, "Cannot find buffer constructor method.\n");
    return NULL;
  }
  jobject bufObj = env->NewObject(bufCls, bufConstructorId);
  if (bufObj == NULL) {
    fprintf(stderr, "Cannot create buffer object.");
    return NULL;
  }
  jfieldID addressId = env->GetFieldID(bufCls, "address", "J");
  jfieldID bufferId = env->GetFieldID(bufCls, "buffer", "Ljava/nio/ByteBuffer;");
  if (addressId == NULL || bufferId == NULL) {
    fprintf(stderr, "Cannot get some field of buffer class");
    return NULL;
  }
  // STEP 2: allocate buffer
  RDMACtxt *ctxt = (RDMACtxt*) hRDMABufferPool;
  void *buf;
  if (allocateBuffer(ctxt, &buf)) {
    fprintf(stderr, "Cannot allocate buffer.\n");
    return NULL;
  }
  jobject bbObj = env->NewDirectByteBuffer(buf, (jlong) 1l << ctxt->align);
  //STEP 3: fill buffer object
  env->SetLongField(bufObj, addressId, (jlong) buf);
  env->SetObjectField(bufObj, bufferId, bbObj);

  return bufObj;
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpReleaseBuffer
 * Signature: (JLedu/cornell/cs/blog/JNIBlog$RBPBuffer;)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpReleaseBuffer
(JNIEnv *env, jclass thisCls, jlong hRDMABufferPool, jobject rbpBuffer) {
  RDMACtxt *ctxt = (RDMACtxt*)hRDMABufferPool;
  void* bufAddr;
  // STEP 1: get rbpbuffer class
  jclass bufCls = env->FindClass("edu/cornell/cs/blog/JNIBlog$RBPBuffer");
  if(bufCls==NULL) {
    fprintf(stderr,"Cannot find the buffers.");
    return;
  }
  jfieldID addressId = env->GetFieldID(bufCls, "address", "J");
  // STEP 2: get fields
  bufAddr = (void*)env->GetLongField(rbpBuffer, addressId);
  // STEP 3: release buffer
  if(releaseBuffer(ctxt,bufAddr))
  fprintf(stderr,"Cannot release buffer@%p\n",bufAddr);
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpConnect
 * Signature: (JII)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpConnect
(JNIEnv *env, jclass thisCls, jlong hRDMABufferPool, jbyteArray hostIp) {
  RDMACtxt *ctxt = (RDMACtxt*)hRDMABufferPool;
  int ipSize = (int)env->GetArrayLength(hostIp);
  jbyte ipStr[16];
  env->GetByteArrayRegion(hostIp, 0, ipSize, ipStr);
  ipStr[ipSize] = 0;
  uint32_t ipkey = inet_addr((const char*)ipStr);
  int rc = rdmaConnect(ctxt, (const uint32_t)ipkey);
  if (rc == 0 || rc == -1) {
    // do nothing, -1 means duplicated connection.
  } else {
    fprintf(stderr,"Setting up RDMA connection to %s failed with error %d.\n", (char*)ipStr, rc);
  }
}

/*
 * Class:     edu_cornell_cs_blog_JNIBlog
 * Method:    rbpRDMAWrite
 * Signature: (IJJ[J)V
 */
JNIEXPORT void JNICALL Java_edu_cornell_cs_blog_JNIBlog_rbpRDMAWrite
(JNIEnv *env, jobject thisObj, jbyteArray clientIp, jlong address, jlongArray pageList) {
  // get filesystem
  filesystem_t *fs = get_filesystem(env,thisObj);
  // get ipkey
  int ipSize = (int)env->GetArrayLength(clientIp);
  jbyte ipStr[16];
  env->GetByteArrayRegion(clientIp, 0, ipSize, ipStr);
  ipStr[ipSize] = 0;
  uint32_t ipkey = inet_addr((const char*)ipStr);
  // get pagelist
  int npage = env->GetArrayLength(pageList);
  long *plist = (long*)malloc(sizeof(long)*npage);
  void **paddrlist = (void**)malloc(sizeof(void*)*npage);
  env->GetLongArrayRegion(pageList, 0, npage, plist);
  int i;
  for(i=0; i<npage; i++)
  paddrlist[i] = (void*)plist[i];
  // rdma write...
  int rc = rdmaWrite(fs->rdmaCtxt, (const uint32_t)ipkey, fs->rdmaCtxt->port, (const uint64_t)address, (const void **)paddrlist,npage,0);
  if(rc !=0 )
  fprintf(stderr, "rdmaWrite failed with error code=%d.\n", rc);
}
