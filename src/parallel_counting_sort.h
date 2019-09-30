//
// Created by 안재찬 on 30/09/2019.
//

#ifndef PROJECT1_PARALLEL_COUNTING_SORT_H
#define PROJECT1_PARALLEL_COUNTING_SORT_H

#include <cstddef>
#include "global.h"

namespace counting_sort {
  size_t bucket(tuple_t &data, const tuple_key_t *thresholds, const size_t &num_thresholds);
  void parallel_counting_sort_v2(tuple_t *data, size_t sz, tuple_key_t *thresholds,
                                 size_t *buckets, size_t num_buckets, size_t num_processors);
  void permute(tuple_t *data, const tuple_key_t *thresholds, const size_t &num_thresholds, section_t *p,
               const size_t &num_threads, const size_t &thread_id, const size_t &num_buckets);
  void repair(tuple_t *data, const tuple_key_t *thresholds, const size_t &num_thresholds, section_t *g, section_t *p,
              const size_t &num_threads, const size_t &bucket_id);
}


#endif //PROJECT1_PARALLEL_COUNTING_SORT_H
