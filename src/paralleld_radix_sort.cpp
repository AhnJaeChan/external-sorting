//
// Created by 안재찬 on 27/09/2019.
//

#include "paralleld_radix_sort.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <omp.h>


void parallel_radix_sort(tuple_key_t *data, size_t sz, size_t level, size_t num_threads) {
  if (sz <= 64) {
    std::sort(data, data + sz);
    return;
  }

  if (num_threads <= 0) {
    num_threads = 1;
  }

  size_t buckets[NUM_BUCKETS];
  memset(buckets, 0, sizeof(size_t) * NUM_BUCKETS);

  // Build histogram
  #pragma omp parallel shared(data, sz, level, buckets, num_threads) default(none)
  {
    #pragma omp for
    for (size_t i = 0; i < num_threads; i++) {
      size_t head_offset = i * sz / num_threads;
      size_t chunk_size = i == num_threads - 1 ? sz / num_threads + sz % num_threads : sz / num_threads;
      for (size_t offset = head_offset; offset < head_offset + chunk_size; offset++) {
        size_t b = bucket(data[offset], level);
        #pragma omp atomic
        buckets[b]++;
      }
    }
  }

  size_t sum = 0;
  section_t g[NUM_BUCKETS];
  section_t *p[NUM_BUCKETS];

  for (size_t i = 0; i < NUM_BUCKETS; i++) {
    if ((p[i] = (section_t *) malloc(sizeof(section_t) * num_threads)) == NULL) {
      printf("memory allocation failed\n");
      return;
    }
  }

  // Set bucket [head, tail]
  // Partition for repair
  for (size_t i = 0; i < NUM_BUCKETS; i++) {
    g[i].head = sum;
    sum += buckets[i];
    g[i].tail = sum;
  }

//  while (sum > 0) {
  // Partition For Permutation
  for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
    size_t total = g[bucket_id].tail - g[bucket_id].head;

    if (total == 0) {
      for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
        p[bucket_id][thread_id].head = p[bucket_id][thread_id].tail = g[bucket_id].tail;
      }
      continue;
    }

    size_t needed_threads = num_threads;
    size_t chunk_size = total / num_threads;

    if (total < num_threads) {
      needed_threads = 1;
      chunk_size = 1;
    }

    for (size_t thread_id = 0; thread_id < needed_threads; thread_id++) {
      p[bucket_id][thread_id].head = g[bucket_id].head + chunk_size * thread_id;
      p[bucket_id][thread_id].tail = p[bucket_id][thread_id].head + chunk_size;
    }
    p[bucket_id][needed_threads - 1].tail = g[bucket_id].tail;
//      if (needed_threads != num_threads) {
//        memset(&p[bucket_id][needed_threads], 0,
//               sizeof(section_t) * (num_threads - needed_threads));
//      }
    for (size_t thread_id = needed_threads; thread_id < num_threads; thread_id++) {
      p[bucket_id][thread_id].head = p[bucket_id][thread_id].tail = g[bucket_id].tail;
    }
  }

  // Check divide permutation
//  for (size_t i = 0; i < NUM_BUCKETS; i++) {
//    printf("Bucket[%zu]: %zu ~ %zu\n", i, g[i].head, g[i].tail);
//    for (size_t j = 0; j < num_threads; j++) {
//      printf("\tthread[%zu]: %zu ~ %zu\n", j, p[i][j].head, p[i][j].tail);
//    }
//  }

  #pragma omp parallel shared(data, level, num_threads, p) default(none)
  {
    #pragma omp for
    for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
      permute(data, level, p, thread_id);
    }
  }

  printf("\n\n@@@ AFTER PERMUTATION @@@\n\n");

  // Check after permutation
  for (size_t i = 0; i < NUM_BUCKETS; i++) {
    printf("Bucket[%zu]: %zu ~ %zu\n", i, g[i].head, g[i].tail);
    size_t head = g[i].head;
    for (size_t j = 0; j < num_threads; j++) {
      printf("\tthread[%zu]: %zu ~ %zu\n", j, p[i][j].head, p[i][j].tail);
      for (size_t k = head; k < p[i][j].head; k++) {
        if (bucket(data[k], level) != i) {
          printf("\t\t[%zu] Wrong element\n", k);
        }
      }
      head = p[i][j].tail;
    }
  }

  // Synchronization

  #pragma omp parallel shared(data, level, num_threads, g, p) default(none)
  {
    #pragma omp for
    for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
      repair(data, level, g, p, bucket_id, num_threads);
    }
  }

  sum = 0;
//  for (size_t i = 0; i < NUM_BUCKETS; i++) {
//    printf("bucket[%zu] %zu ~ %zu\n", i, g[i].head, g[i].tail);
//    for (size_t j = sum; j < g[i].head; j++) {
//      if (bucket(data[j], level) != i) {
//        printf("\t[%zu] Doesn't belong here\n", j);
//      }
//    }
//    sum = g[i].tail;
////    for (size_t j = 0; j < num_threads; j++) {
////      if (bucket(data[g[i].head - 1], level) != i) {
////      }
////      printf("\tthread[%zu] %zu ~ %zu\n", j, p[i][j].head, p[i][j].tail);
////    }
//  }

  // Synchronization

  sum = 0;
  for (size_t i = 0; i < NUM_BUCKETS; i++) {
    sum += g[i].tail - g[i].head;
  }
//  }

//  sum = 0;
//  size_t cnt = 0;
//  for (size_t i = 0; i < NUM_BUCKETS; i++) {
//    printf("bucket[%zu]\n", i);
//    for (size_t j = 0; j < buckets[i]; j++) {
//      if (bucket(data[sum + j], level) != i) {
//        printf("\titem [%zu], %zu\n", j, bucket(data[sum + j], level));
//        cnt++;
//      }
//    }
//    sum += buckets[i];
//  }
//  printf("[Level %zu] %zu items in wrong bucket\n", level, cnt);

  for (size_t i = 0; i < NUM_BUCKETS; i++) {
    free(p[i]);
  }

//  if (level < KEY_SIZE) {
//    size_t offset = 0;
//    for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
//      parallel_radix_sort(data + offset, buckets[bucket_id], level + 1, NUM_BUCKETS / num_threads);
//    }
//  }
}

// 8-bit radix sorting
size_t bucket(const tuple_key_t &data, const size_t &level) {
  return (size_t) (data.key[level] & 0xFF);
}

void permute(tuple_key_t *data, const size_t &level, section_t *p[NUM_BUCKETS], const size_t &thread_id) {
  for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
    size_t head = p[bucket_id][thread_id].head;
    while (head < p[bucket_id][thread_id].tail) {
      tuple_key_t &v = data[head];
      size_t k = bucket(v, level);
      while (k != bucket_id && p[k][thread_id].head < p[k][thread_id].tail) {
        std::swap(v, data[p[k][thread_id].head++]);
        k = bucket(v, level);
      }
//      head++;
      if (k == bucket_id) {
//        p[bucket_id][thread_id].head++;
        data[head++] = data[p[bucket_id][thread_id].head];
        data[p[bucket_id][thread_id].head++] = v;
      } else {
        data[head++] = v;
      }
    }
  }
}

void repair(tuple_key_t *data, const size_t &level, section_t *g, section_t *p[NUM_BUCKETS], const size_t &bucket_id,
            const size_t &num_threads) {
  size_t tail = g[bucket_id].tail;
  for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
    size_t head = p[bucket_id][thread_id].head;
    while (head < p[bucket_id][thread_id].tail && head < tail) {
      tuple_key_t &v = data[head++];
      if (bucket(v, level) != bucket_id) {
        while (head < tail) {
          tuple_key_t &w = data[--tail];
          if (bucket(w, level) == bucket_id) {
            std::swap(v, w);
            break;
          }
        }
      }
    }
  }
  g[bucket_id].head = tail;
}