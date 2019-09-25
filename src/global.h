//
// Created by 안재찬 on 25/09/2019.
//

#ifndef MULTICORE_EXTERNAL_SORT_GLOBAL_H
#define MULTICORE_EXTERNAL_SORT_GLOBAL_H

#include <cstring>

#define TUPLE_SIZE (100)
#define KEY_SIZE (10)
#define PHASE1_BUFFER_SIZE (300000000)  // 1000000000
#define PHASE2_BUFFER_SIZE (500000000)  // 500Mb
#define PHASE3_BUFFER_SIZE (1000000000) // 1Gb
#define NUM_PARTITIONS (4) // Partitioning input data to N equal sized data

#define TMP_DIRECTORY "./data/"

typedef struct tuple {
  char data[100];

  bool operator<(const tuple &a) const {
    return memcmp(data, &a, KEY_SIZE) < 0;
  }

  bool operator>(const tuple &a) const {
    return memcmp(data, &a, KEY_SIZE) > 0;
  }
} tuple_t;

typedef struct tuple_key {
  char key[10];

  bool operator<(const struct tuple_key &op) const {
    return memcmp(key, &op, KEY_SIZE) < 0;
  }

  bool operator>(const struct tuple_key &op) const {
    return memcmp(key, &op, KEY_SIZE) > 0;
  }
} tuple_key_t;

#endif //MULTICORE_EXTERNAL_SORT_GLOBAL_H
