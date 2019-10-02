//
// Created by 안재찬 on 30/09/2019.
//

#include "parallel_counting_sort.h"

#include <cstdio>
#include <utility>
#include <algorithm>
#include <omp.h>

namespace counting_sort {

  size_t bucket(tuple_t &data, const tuple_key_t *thresholds, const size_t &num_thresholds) {
    for (size_t i = 0; i < num_thresholds; i++) {
      if ((tuple_key_t &) data < thresholds[i]) {
        return i;
      }
    }
    return num_thresholds;
  }

  void parallel_counting_sort_v2(tuple_t *data, size_t sz, tuple_key_t *thresholds,
                                 size_t *buckets, size_t num_buckets, size_t num_processors) {
    size_t num_thresholds = num_buckets - 1;
    memset(buckets, 0, sizeof(size_t) * num_buckets);

    // Build histogram
    #pragma omp parallel shared(sz, data, thresholds, num_thresholds, buckets, num_processors) default(none)
    {
      #pragma omp for
      for (size_t i = 0; i < num_processors; i++) {
        size_t head_offset = i * sz / num_processors;
        size_t chunk_size = i == num_processors - 1 ? sz / num_processors + sz % num_processors : sz / num_processors;
        for (size_t offset = head_offset; offset < head_offset + chunk_size; offset++) {
          size_t b = bucket(data[offset], thresholds, num_thresholds);
          #pragma omp atomic
          buckets[b]++;
        }
      }
    }

    size_t sum = 0;
    section_t g[num_buckets];
    section_t p[num_buckets][num_processors];

    // Set bucket [head, tail]
    // Partition for repair
    for (size_t i = 0; i < num_buckets; i++) {
      g[i].head = sum;
      sum += buckets[i];
      g[i].tail = sum;
    }

    bool is_empty = false;
    while (!is_empty) {
      // Partition For Permutation
      for (size_t bucket_id = 0; bucket_id < num_buckets; bucket_id++) {
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

      // Permutation stage
      #pragma omp parallel shared(data, thresholds, num_thresholds, num_processors, p, num_buckets) default(none)
      {
        #pragma omp for
        for (size_t thread_id = 0; thread_id < num_processors; thread_id++) {
          permute(data, thresholds, num_thresholds, (section_t *) p, num_processors, thread_id, num_buckets);
        }
      }

      // Repair stage
      is_empty = true;
      #pragma omp parallel shared(data, thresholds, num_thresholds, num_processors, g, p, is_empty, num_buckets) default(none)
      {
        #pragma omp for
        for (size_t bucket_id = 0; bucket_id < num_buckets; bucket_id++) {
          repair(data, thresholds, num_thresholds, g, (section_t *) p, num_processors, bucket_id);
          if (g[bucket_id].tail - g[bucket_id].head > 0) {
            is_empty = false;
          }
        }
      }
    }
  }

  void permute(tuple_t *data, const tuple_key_t *thresholds, const size_t &num_thresholds, section_t *p,
               const size_t &num_threads, const size_t &thread_id, const size_t &num_buckets) {
    for (size_t bucket_id = 0; bucket_id < num_buckets; bucket_id++) {
      size_t head = ((p + bucket_id * num_threads) + thread_id)->head;
      while (head < ((p + bucket_id * num_threads) + thread_id)->tail) {
        tuple_t &v = data[head];
        size_t k = bucket(v, thresholds, num_thresholds);
        while (k != bucket_id &&
               ((p + k * num_threads) + thread_id)->head < ((p + k * num_threads) + thread_id)->tail) {
          std::swap(v, data[((p + k * num_threads) + thread_id)->head++]);
          k = bucket(v, thresholds, num_thresholds);
        }
        head++;
        if (k == bucket_id) {
          ((p + bucket_id * num_threads) + thread_id)->head++;
        }
      }
    }
  }

  void repair(tuple_t *data, const tuple_key_t *thresholds, const size_t &num_thresholds, section_t *g, section_t *p,
              const size_t &num_threads, const size_t &bucket_id) {
    size_t tail = g[bucket_id].tail;
    for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
      size_t head = ((p + bucket_id * num_threads) + thread_id)->head;
      while (head < ((p + bucket_id * num_threads) + thread_id)->tail && head < tail) {
        tuple_t &v = data[head++];
        if (bucket(v, thresholds, num_thresholds) != bucket_id) {
          while (head < tail) {
            tuple_t &w = data[--tail];
            if (bucket(w, thresholds, num_thresholds) == bucket_id) {
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
