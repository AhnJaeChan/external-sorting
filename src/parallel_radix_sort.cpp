//
// Created by 안재찬 on 27/09/2019.
//

#include "parallel_radix_sort.h"

#include <cstdio>
#include <utility>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <omp.h>

namespace radix_sort {

  template void parallel_radix_sort<tuple_key_t>(tuple_key_t *, size_t, size_t);
  template void parallel_radix_sort<tuple_t>(tuple_t *, size_t, size_t);

  template<class T>
  void parallel_radix_sort(T *data, size_t sz, size_t level) {
    if (sz < 64) {
      std::sort(data, data + sz);
      return;
    }

    size_t buckets[NUM_BUCKETS];
    memset(buckets, 0, sizeof(size_t) * NUM_BUCKETS);

    // Build histogram
    #pragma omp parallel for shared(data, sz, level, buckets) default(none)
    for (size_t i = 0; i < sz; i++) {
      size_t b = bucket(&data[i], level);
      #pragma omp atomic
      buckets[b]++;
    }

    size_t sum = 0;
    section_t g[NUM_BUCKETS];

    // Set bucket [head, tail]
    for (size_t i = 0; i < NUM_BUCKETS; i++) {
      g[i].head = sum;
      sum += buckets[i];
      g[i].tail = sum;
    }

    for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
      size_t head = g[bucket_id].head;
      while (head < g[bucket_id].tail) {
        size_t b = bucket(&data[head], level);
        while (b != bucket_id) {
          std::swap(data[head], data[g[b].head++]);
          b = bucket(&data[head], level);
        }
        head++;
      }
    }

    if (level < KEY_SIZE) {
      size_t offsets[NUM_BUCKETS];
      offsets[0] = 0;
      for (size_t i = 1; i < NUM_BUCKETS; i++) {
        offsets[i] = g[i - 1].tail;
      }

      #pragma omp parallel for shared(data, offsets, buckets, level) default(none)
      for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
        parallel_radix_sort(data + offsets[bucket_id], buckets[bucket_id], level + 1);
      }
    }
  }

  // 8-bit used for radix
  size_t bucket(void *data, const size_t &level) {
    return (size_t) (*(static_cast<char *>(data) + level) & 0xFF);
  }

  template<class T>
  void permute(T *data, const size_t &level, section_t *p, const size_t &num_threads, const size_t &thread_id) {
    for (size_t bucket_id = 0; bucket_id < NUM_BUCKETS; bucket_id++) {
      size_t head = ((p + bucket_id * num_threads) + thread_id)->head;
      while (head < ((p + bucket_id * num_threads) + thread_id)->tail) {
        T &v = data[head];
        size_t k = bucket(&v, level);
        while (k != bucket_id &&
               ((p + k * num_threads) + thread_id)->head < ((p + k * num_threads) + thread_id)->tail) {
          std::swap(v, data[((p + k * num_threads) + thread_id)->head++]);
          k = bucket(&v, level);
        }
        head++;
        if (k == bucket_id) {
          ((p + bucket_id * num_threads) + thread_id)->head++;
        }
      }
    }
  }

  template<class T>
  void repair(T *data, const size_t &level, section_t *g, section_t *p, const size_t &num_threads,
              const size_t &bucket_id) {
    size_t tail = g[bucket_id].tail;
    for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
      size_t head = ((p + bucket_id * num_threads) + thread_id)->head;
      while (head < ((p + bucket_id * num_threads) + thread_id)->tail && head < tail) {
        T &v = data[head++];
        if (bucket(&v, level) != bucket_id) {
          while (head < tail) {
            T &w = data[--tail];
            if (bucket(&w, level) == bucket_id) {
              std::swap(v, w);
              break;
            }
          }
        }
      }
    }
    g[bucket_id].head = tail;
  }

}
