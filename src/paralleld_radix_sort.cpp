//
// Created by 안재찬 on 27/09/2019.
//

#include "paralleld_radix_sort.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <omp.h>


void parallel_radix_sort(tuple_key_t *data, const size_t &sz, const size_t &level, size_t num_threads) {
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

  while (sum > 0) {
    // Partition For Permutation
    for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
      size_t total = g[bucket_id].tail - g[bucket_id].head;

      if (total == 0) continue;

      size_t needed_threads = num_threads;
      size_t chunk_size = (total - 1) / num_threads + 1;

      if (total < num_threads) {
        needed_threads = total % num_threads;
        chunk_size = 1;
      }

      for (size_t thread_id = 0; thread_id < needed_threads; thread_id++) {
        p[bucket_id][thread_id].head = g[bucket_id].head + chunk_size * thread_id;
        p[bucket_id][thread_id].tail = p[bucket_id][thread_id].head + chunk_size;
      }

      if (num_threads - needed_threads > 0) {
        memset(&p[bucket_id][needed_threads], g[bucket_id].tail, sizeof(section_t) * (num_threads - needed_threads));
      }
      p[bucket_id][needed_threads - 1].tail = g[bucket_id].tail;
    }

    #pragma omp parallel shared(data, level, num_threads, p) default(none)
    {
      #pragma omp for
      for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
        permute(data, level, (section_t **) p, thread_id);
      }
    }

    // Synchronization

    #pragma omp parallel shared(data, level, num_threads, g, p) default(none)
    {
      #pragma omp for
      for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
        repair(data, level, g, (section_t **) p, bucket_id, num_threads);
      }
    }

    // Synchronization

    sum = 0;
    for (size_t i = 0; i < NUM_BUCKETS; i++) {
      sum += g[i].tail - g[i].head;
    }
  }

  sum = 0;
  size_t cnt = 0;
  for (size_t i = 0; i < NUM_BUCKETS; i++) {
    for (size_t j = 0; j < buckets[i]; j++) {
      if (bucket(data[sum + j], level) != i) {
        cnt++;
      }
    }
    sum += buckets[i];
  }
  printf("%zu items in wrong bucket\n", cnt);
  printf("%zu items in original bucket\n", sum);

  for (size_t i = 0; i < NUM_BUCKETS; i++) {
    free(p[i]);
  }
}

// 8-bit radix sorting
size_t bucket(const tuple_key_t &data, const size_t &level) {
  return (size_t) (data.key[level] & 0xFF);
}

void permute(tuple_key_t *data, const size_t &level, section_t **p, const size_t &thread_id) {
  for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
    size_t head = p[bucket_id][thread_id].head;
    while (head < p[bucket_id][thread_id].tail) {
      tuple_key_t &v = data[head];
      size_t k = bucket(v, level);
      while (k != bucket_id && p[k][thread_id].head < p[k][thread_id].tail) {
        std::swap(v, data[p[k][thread_id].head++]);
        k = bucket(v, level);
      }
      if (k == bucket_id) {
        head++;
        p[bucket_id][thread_id].head++;
      } else {
        head++;
      }
    }
  }
}

void repair(tuple_key_t *data, const size_t &level, section_t *g, section_t **p, const size_t &bucket_id,
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