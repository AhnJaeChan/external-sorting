//
// Created by 안재찬 on 27/09/2019.
//

#ifndef MULTICORE_EXTERNAL_SORT_PARALLEL_RADIX_SORT_H
#define MULTICORE_EXTERNAL_SORT_PARALLEL_RADIX_SORT_H

#include <cstddef>
#include "global.h"

typedef struct tuple_key tuple_key_t;

namespace radix_sort {
  template<typename T>
  void parallel_radix_sort(T *data, size_t sz, size_t level);

  size_t bucket(void *data, const size_t &level);
  template<class T>
  void permute(T *data, const size_t &level, section_t *p, const size_t &num_threads, const size_t &thread_id);
  template<class T>
  void repair(T *data, const size_t &level, section_t *g, section_t *p, const size_t &num_threads,
              const size_t &bucket_id);
}

#endif //MULTICORE_EXTERNAL_SORT_PARALLEL_RADIX_SORT_H
