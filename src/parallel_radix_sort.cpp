//
// Created by 안재찬 on 27/09/2019.
//

#include "parallel_radix_sort.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <omp.h>


void parallel_radix_sort(tuple_key_t *data, size_t sz, size_t level, size_t num_processors) {
  if (sz <= 64) {
    std::sort(data, data + sz);
    return;
  }

  if (num_processors == 0) {
    num_processors = 1;
  }

  size_t buckets[NUM_BUCKETS];
  memset(buckets, 0, sizeof(size_t) * NUM_BUCKETS);

  // Build histogram
  #pragma omp parallel shared(data, sz, level, buckets, num_processors) default(none)
  {
    #pragma omp for
    for (size_t i = 0; i < num_processors; i++) {
      size_t head_offset = i * sz / num_processors;
      size_t chunk_size = i == num_processors - 1 ? sz / num_processors + sz % num_processors : sz / num_processors;
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
    if ((p[i] = (section_t *) malloc(sizeof(section_t) * num_processors)) == NULL) {
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

  size_t last_none_empty = 0;
  while (g[last_none_empty].tail - g[last_none_empty].head > 0) {
    // Partition For Permutation
    for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
      size_t total = g[bucket_id].tail - g[bucket_id].head;

      if (total == 0) {
        for (size_t thread_id = 0; thread_id < num_processors; thread_id++) {
          p[bucket_id][thread_id].head = p[bucket_id][thread_id].tail = g[bucket_id].tail;
        }
        continue;
      }

      size_t needed_threads = num_processors;
      size_t chunk_size = total / num_processors;

      if (total < num_processors) {
        needed_threads = 1;
        chunk_size = 1;
      }

      for (size_t thread_id = 0; thread_id < needed_threads; thread_id++) {
        p[bucket_id][thread_id].head = g[bucket_id].head + chunk_size * thread_id;
        p[bucket_id][thread_id].tail = p[bucket_id][thread_id].head + chunk_size;
      }
      p[bucket_id][needed_threads - 1].tail = g[bucket_id].tail;

      for (size_t thread_id = needed_threads; thread_id < num_processors; thread_id++) {
        p[bucket_id][thread_id].head = p[bucket_id][thread_id].tail = g[bucket_id].tail;
      }
    }

    #pragma omp parallel shared(data, level, num_processors, p) default(none)
    {
      #pragma omp for
      for (size_t thread_id = 0; thread_id < num_processors; thread_id++) {
        permute(data, level, p, thread_id);
      }
    }

    // Synchronization

    #pragma omp parallel shared(data, level, num_processors, g, p) default(none)
    {
      #pragma omp for
      for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
        repair(data, level, g, p, bucket_id, num_processors);
      }
    }

    // Synchronization

    for (size_t i = last_none_empty; i < NUM_BUCKETS; i++) {
      if (g[i].tail - g[i].head != 0) {
        last_none_empty = i;
        break;
      }
    }
  }

  for (size_t i = 0; i < NUM_BUCKETS; i++) {
    free(p[i]);
  }

  if (level < KEY_SIZE) {
    size_t offset = 0;
    for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
      parallel_radix_sort(data + offset, buckets[bucket_id], level + 1, num_processors);
      offset += buckets[bucket_id];
    }
  }
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
      head++;
      if (k == bucket_id) {
        p[bucket_id][thread_id].head++;
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