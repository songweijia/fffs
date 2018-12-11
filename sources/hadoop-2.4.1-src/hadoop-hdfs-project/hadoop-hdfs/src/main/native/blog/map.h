#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <inttypes.h>

#define MAP_DECLARE(NAME,TYPE)                                                                  \
                                                                                                \
typedef struct NAME##_entry_t {                                                                 \
  uint64_t id;                                                                                  \
  TYPE *value;                                                                                  \
  struct NAME##_entry_t *next;                                                                  \
} NAME##_entry_t;                                                                               \
                                                                                                \
typedef struct NAME##_map_t {                                                                   \
  pthread_rwlock_t *lock;                                                                       \
  NAME##_entry_t *entry;                                                                        \
} NAME##_map_t;                                                                                 \
                                                                                                \
/**
 * Initialize and allocate all the appropriate structures.                                       
 * Return 0 for success and -1 otherwise.
 */                                                                                             \
BLOG_MAP_TYPE(NAME) *NAME##_map_initialize();                                                   \
                                                                                                \
/**
 * Release all memory occupied by the map.
 */                                                                                             \
void NAME##_map_destroy(NAME##_map_t *map);                                                     \
                                                                                                \
/**
 * Create a map entry.
 * Return 0 for success.
 * Return -1 if entry already exists.
 */                                                                                             \
int NAME##_map_create(NAME##_map_t *map, uint64_t id);                                          \
                                                                                                \
/*
 * Create a map entry and initialize it.
 * Return 0 for success.
 * Return -1 if entry already exists.
 */                                                                                             \
int NAME##_map_create_and_write(BLOG_MAP_TYPE(NAME) *map, uint64_t id, TYPE* value);            \
                                                                                                \
/**
 * Delete a map entry.
 * Return 0 for success.
 * Return -1 if entry does not exist.
 */                                                                                             \
int NAME##_map_delete(NAME##_map_t *map, uint64_t id);                                          \
                                                                                                \
/**
 * Assign a value to an entry.
 * Return 0 for success.
 * Return -1 if entry does not exist.
 */                                                                                             \
int NAME##_map_write(NAME##_map_t *map, uint64_t id, TYPE *value);                              \
                                                                                                \
/**
 * Read the value from an entry.
 * Return 0 for success.
 * Return -1 if entry does not exist.
 */                                                                                             \
int NAME##_map_read(NAME##_map_t *map, uint64_t id, TYPE **value);                              \
                                                                                                \
/**
 * Get map length.
 */                                                                                             \
uint64_t NAME##_map_length(BLOG_MAP_TYPE(NAME) *map);                                           \
                                                                                                \
/**
 * Get all the keys in the dictionary.
 */                                                                                             \
uint64_t *NAME##_map_get_ids(NAME##_map_t *map, uint64_t length);                               \
                                                                                                \
/**
 * Lock the structure in the appropriate position.
 */                                                                                             \
int NAME##_map_lock(NAME##_map_t *map, uint64_t id, char read_write);                           \
                                                                                                \
/**
 * Unlock the structure in the appropriate position.
 */                                                                                             \
int NAME##_map_unlock(NAME##_map_t *map, uint64_t id);
                                                                                                
#define ENTRY_TYPE(NAME)                                                                        \
NAME##_entry_t

#define BLOG_MAP_TYPE(NAME)                                                                         \
NAME##_map_t

#define MAP_DEFINE(NAME,TYPE,SIZE)                                                                  \
                                                                                                    \
BLOG_MAP_TYPE(NAME) *NAME##_map_initialize() {                                                      \
  BLOG_MAP_TYPE(NAME) *map = (BLOG_MAP_TYPE(NAME) *) malloc(SIZE*sizeof(BLOG_MAP_TYPE(NAME)));      \
  uint64_t i;                                                                                       \
                                                                                                    \
  for (i = 0; i < SIZE; i++) {                                                                      \
    map[i].lock = (pthread_rwlock_t *) malloc(sizeof(pthread_rwlock_t));                            \
    map[i].entry = NULL;                                                                            \
    if (pthread_rwlock_init(map[i].lock, NULL) != 0) {                                              \
      fprintf(stderr, "Map Initialize: Lock %" PRIu64 " is not initialized correctly\n", i);        \
      return NULL;                                                                                  \
    }                                                                                               \
  }                                                                                                 \
  return map;                                                                                       \
}                                                                                                   \
                                                                                                    \
void NAME##_map_destroy(BLOG_MAP_TYPE(NAME) *map) {                                                 \
  uint64_t i;                                                                                       \
                                                                                                    \
  for (i = 0; i < SIZE; i++) {                                                                      \
    pthread_rwlock_destroy(map[i].lock);                                                            \
    free(map[i].lock);                                                                              \
    free(map[i].entry);                                                                             \
  }                                                                                                 \
  free(map);                                                                                        \
}                                                                                                   \
                                                                                                    \
int NAME##_map_create(BLOG_MAP_TYPE(NAME) *map, uint64_t id) {                                      \
  return NAME##_map_create_and_write(map, id, NULL);                                                \
}                                                                                                   \
                                                                                                    \
int NAME##_map_create_and_write(BLOG_MAP_TYPE(NAME) *map, uint64_t id, TYPE* value) {               \
  uint64_t hash = id % SIZE;                                                                        \
  ENTRY_TYPE(NAME) *entry = map[hash].entry;                                                        \
  ENTRY_TYPE(NAME) *last_entry = NULL;                                                              \
                                                                                                    \
  while ((entry != NULL) && (entry->id != id)) {                                                    \
    last_entry = entry;                                                                             \
    entry = entry->next;                                                                            \
  }                                                                                                 \
  if (entry != NULL) {                                                                              \
    fprintf(stderr, "Map Create: Entry ID %" PRIu64 " already exists in map.\n", id);               \
    return -1;                                                                                      \
  }                                                                                                 \
  entry = (NAME##_entry_t *) malloc(sizeof(NAME##_entry_t));                                        \
  entry->id = id;                                                                                   \
  entry->value = value;                                                                             \
  entry->next = NULL;                                                                               \
  if (last_entry == NULL)                                                                           \
    map[hash].entry = entry;                                                                        \
  else                                                                                              \
    last_entry->next = entry;                                                                       \
  return 0;                                                                                         \
}                                                                                                   \
                                                                                                    \
int NAME##_map_delete(BLOG_MAP_TYPE(NAME) *map, uint64_t id) {                                      \
  uint64_t hash = id % SIZE;                                                                        \
  ENTRY_TYPE(NAME) *entry = map[hash].entry;                                                        \
  ENTRY_TYPE(NAME) *last_entry = NULL;                                                              \
                                                                                                    \
  while ((entry != NULL) && (entry->id != id)) {                                                    \
    last_entry = entry;                                                                             \
    entry = entry->next;                                                                            \
  }                                                                                                 \
  if (entry == NULL) {                                                                              \
    fprintf(stderr, "Map Delete: Entry ID %" PRIu64 " does not exist in map.\n", id);               \
    return -1;                                                                                      \
  }                                                                                                 \
  if (last_entry == NULL)                                                                           \
    map[hash].entry = entry->next;                                                                  \
  else                                                                                              \
    last_entry->next = entry->next;                                                                 \
  free(entry);                                                                                      \
  return 0;                                                                                         \
}                                                                                                   \
                                                                                                    \
int NAME##_map_write(BLOG_MAP_TYPE(NAME) *map, uint64_t id, TYPE *value) {                          \
  uint64_t hash = id % SIZE;                                                                        \
  ENTRY_TYPE(NAME) *entry = map[hash].entry;                                                        \
                                                                                                    \
  while ((entry != NULL) && (entry->id != id))                                                      \
    entry = entry->next;                                                                            \
  if (entry == NULL) {                                                                              \
    fprintf(stderr, "Map Write: Entry ID %ld does not exist in map.\n", id);                        \
    return -1;                                                                                      \
  }                                                                                                 \
  entry->value = value;                                                                             \
  return 0;                                                                                         \
}                                                                                                   \
                                                                                                    \
int NAME##_map_read(BLOG_MAP_TYPE(NAME) *map, uint64_t id, TYPE **value) {                          \
  uint64_t hash = id % SIZE;                                                                        \
  ENTRY_TYPE(NAME) *entry = map[hash].entry;                                                        \
                                                                                                    \
  while ((entry != NULL) && (entry->id != id))                                                      \
    entry = entry->next;                                                                            \
  if (entry == NULL) {                                                                              \
    DEBUG_PRINT("Map Read: Entry ID %ld does not exist in map.\n", id);                             \
    return -1;                                                                                      \
  }                                                                                                 \
  *value = entry->value;                                                                            \
  return 0;                                                                                         \
}                                                                                                   \
                                                                                                    \
uint64_t NAME##_map_length(BLOG_MAP_TYPE(NAME) *map) {                                              \
  ENTRY_TYPE(NAME) *entry;                                                                          \
  uint64_t length, i;                                                                               \
                                                                                                    \
  length = 0;                                                                                       \
  for (i = 0; i < SIZE; i++) {                                                                      \
    entry = map[i].entry;                                                                           \
    while (entry != NULL) {                                                                         \
      entry = entry->next;                                                                          \
      length++;                                                                                     \
    }                                                                                               \
  }                                                                                                 \
  return length;                                                                                    \
}                                                                                                   \
                                                                                                    \
uint64_t *NAME##_map_get_ids(BLOG_MAP_TYPE(NAME) *map, uint64_t length) {                           \
  ENTRY_TYPE(NAME) *entry;                                                                          \
  uint64_t *ids = (uint64_t *) malloc(length*sizeof(uint64_t));                                     \
  uint64_t index = 0;                                                                               \
  uint64_t i;                                                                                       \
                                                                                                    \
  for (i = 0; i < SIZE; i++) {                                                                      \
     entry = map[i].entry;                                                                          \
     while (entry != NULL) {                                                                        \
       ids[index++] = entry->id;                                                                    \
       entry = entry->next;                                                                         \
     }                                                                                              \
  }                                                                                                 \
  return ids;                                                                                       \
}                                                                                                   \
                                                                                                    \
int NAME##_map_lock(NAME##_map_t *map, uint64_t id, char read_write) {                              \
  uint64_t hash = id % SIZE;                                                                        \
                                                                                                    \
  if (read_write == 'w')                                                                            \
    return pthread_rwlock_wrlock(map[hash].lock);                                                   \
  if (read_write == 'r')                                                                            \
    return pthread_rwlock_rdlock(map[hash].lock);                                                   \
  fprintf(stderr, "Map Lock: Wrong read/write character.\n");                                       \
  return -1;                                                                                        \
}                                                                                                   \
                                                                                                    \
int NAME##_map_unlock(NAME##_map_t *map, uint64_t id) {                                             \
  uint64_t hash = id % SIZE;                                                                        \
                                                                                                    \
  return pthread_rwlock_unlock(map[hash].lock);                                                     \
}

#define MAP_INITIALIZE(NAME)                                                                    \
NAME##_map_initialize()

#define MAP_DESTROY(NAME,map)                                                                   \
NAME##_map_destroy(map)

#define MAP_CREATE(NAME,map,id)                                                                 \
NAME##_map_create(map,id)

#define MAP_CREATE_AND_WRITE(NAME,map,id,value)                                                 \
NAME##_map_create_and_write(map,id,value)

#define MAP_DELETE(NAME,map,id)                                                                 \
NAME##_map_delete(map,id)

#define MAP_WRITE(NAME,map,id,value)                                                            \
NAME##_map_write(map,id,value)

#define MAP_READ(NAME,map,id,value)                                                             \
NAME##_map_read(map,id,value)

#define MAP_LENGTH(NAME,map)                                                                    \
NAME##_map_length(map)

#define MAP_GET_IDS(NAME,map,length)                                                            \
NAME##_map_get_ids(map,length)

#define MAP_LOCK(NAME,map,id,c)                                                                 \
NAME##_map_lock(map,id,c)

#define MAP_UNLOCK(NAME,map,id)                                                                 \
NAME##_map_unlock(map,id)
