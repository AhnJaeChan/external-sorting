//
// Created by 안재찬 on 25/09/2019.
//

#ifndef MULTICORE_EXTERNAL_SORT_GLOBAL_H
#define MULTICORE_EXTERNAL_SORT_GLOBAL_H

#include <cstring>

#define MAX_BUFFER (1800000000)
#define NUM_THREADS (40)

#define TUPLE_SIZE (100)
#define KEY_SIZE (10)
#define READ_BUFFER_SIZE (500000000)  // 1GB

#define NUM_BUCKETS (256)

#define TMP_DIRECTORY ("./data/")

typedef struct tuple {
  char data[100];

  bool operator<(const struct tuple &op) const {
    return memcmp(data, &op, KEY_SIZE) < 0;
  }

  bool operator>(const struct tuple &op) const {
    return memcmp(data, &op, KEY_SIZE) > 0;
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

  bool operator==(const struct tuple_key &op) const {
    return memcmp(key, &op, KEY_SIZE) == 0;
  }

  bool operator!=(const struct tuple_key &op) const {
    return memcmp(key, &op, KEY_SIZE) != 0;
  }
} tuple_key_t;

typedef struct param {
  int input_fd;
  int output_fd;
  size_t total_file_size;
  size_t num_partitions;
  tuple_key_t *thresholds;
  size_t buffer_size;
} param_t;

#endif //MULTICORE_EXTERNAL_SORT_GLOBAL_H
