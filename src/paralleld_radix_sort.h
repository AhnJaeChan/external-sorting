//
// Created by 안재찬 on 27/09/2019.
//

#ifndef MULTICORE_EXTERNAL_SORT_PARALLELD_RADIX_SORT_H
#define MULTICORE_EXTERNAL_SORT_PARALLELD_RADIX_SORT_H

#include <cstddef>
#include "global.h"

typedef struct tuple_key tuple_key_t;

typedef struct section {
  size_t head;
  size_t tail;
} section_t;

void parallel_radix_sort(tuple_key_t *data, size_t sz, size_t level, size_t num_threads);
size_t bucket(const tuple_key_t &data, const size_t &level);
void permute(tuple_key_t *data, const size_t &level, section_t **p, const size_t &thread_id);
void repair(tuple_key_t *data, const size_t &level, section_t *g, section_t **p, const size_t &bucket_id,
            const size_t &num_threads);

#endif //MULTICORE_EXTERNAL_SORT_PARALLELD_RADIX_SORT_H
