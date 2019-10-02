#include <iostream>
#include <algorithm>
#include <cstdlib>
#include <cstring>

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <chrono>
#include <omp.h>

#include "global.h"
#include "parallel_radix_sort.h"
#include "parallel_counting_sort.h"

using namespace std;

int prepare_environment();

void phase_small_file(param_t &param);
void phase1(param_t &param);
void phase2(param_t &param);
void phase3(param_t &param);

void check_output(char *filename, char *buffer);

chrono::time_point<chrono::system_clock> t1, t2;
long long int duration;

int main(int argc, char *argv[]) {
  if (argc < 3) {
    printf("Program usage: ./run input_file_name output_file_name\n");
    return 0;
  }

  param_t param;
  param.buffer = NULL;
  param.thresholds = NULL;
  char *buffer;
  if ((buffer = (char *) malloc(BUFFER_SIZE)) == NULL) {
    printf("Buffer allocation failed (input read buffer)\n");
    return 0;
  }
  param.buffer = buffer;

  /// [Phase 1] START
  if ((param.input_fd = open(argv[1], O_RDONLY | O_NONBLOCK)) == -1) {
    printf("[Error] failed to open input file %s\n", argv[1]);
    return 0;
  }
  param.total_file_size = lseek(param.input_fd, 0, SEEK_END);

  if ((param.output_fd = open(argv[2], O_RDWR | O_CREAT | O_TRUNC | O_SYNC, 0777)) == -1) {
    printf("[Error] failed to open input file %s\n", argv[2]);
    return 0;
  }

  if (param.total_file_size <= BUFFER_SIZE) {
    t1 = chrono::high_resolution_clock::now();
    phase_small_file(param);
    t2 = chrono::high_resolution_clock::now();
    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase small file] took: " << duration << " (milliseconds)" << endl;
  } else {
    t1 = chrono::high_resolution_clock::now();
    phase1(param);
    t2 = chrono::high_resolution_clock::now();

    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase1] took: " << duration << " (milliseconds)" << endl;
    /// [Phase 1] END

    /// [Phase 2] START
    t1 = chrono::high_resolution_clock::now();
//    phase2(param);
    t2 = chrono::high_resolution_clock::now();

    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase2] took: " << duration << " (milliseconds)" << endl;
    /// [Phase 2] END

    /// [Phase 3] START
    t1 = chrono::high_resolution_clock::now();
//    phase3(param);
    t2 = chrono::high_resolution_clock::now();

    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase3] took: " << duration << " (milliseconds)" << endl;
    /// [Phase 3] END
  }
  check_output(argv[2], param.buffer);

  t1 = chrono::high_resolution_clock::now();

  close(param.input_fd);
  close(param.output_fd);

  if (param.thresholds != NULL) {
    free(param.thresholds);
  }
  if (param.buffer != NULL) {
    free(param.buffer);
  }
  t2 = chrono::high_resolution_clock::now();

  duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
  cout << "[Clean up] took: " << duration << " (milliseconds)" << endl;

  return 0;
}

void phase_small_file(param_t &param) {
  for (size_t offset = 0; offset < param.total_file_size;) {
    size_t ret = pread(param.input_fd, param.buffer + offset, param.total_file_size - offset, offset);
    offset += ret;
  }

  t1 = chrono::high_resolution_clock::now();
  radix_sort::parallel_radix_sort((tuple_t *) param.buffer, param.total_file_size / TUPLE_SIZE, 0);
//  sort((tuple_t *) param.buffer, (tuple_t*) param.buffer + param.total_file_size / TUPLE_SIZE);
  t2 = chrono::high_resolution_clock::now();

  duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
  cout << "[Phase1] sorting: " << duration << " (milliseconds)" << endl;

  for (size_t offset = 0; offset < param.total_file_size;) {
    size_t ret = pwrite(param.output_fd, param.buffer + offset, param.total_file_size - offset, offset);
    offset += ret;
  }
}

void phase1(param_t &param) {
  size_t file_size = param.total_file_size; // Input file size
  size_t num_tuples = file_size / TUPLE_SIZE; // Number of tuples
  size_t num_partitions = (file_size - 1) / BUFFER_SIZE + 1; // Total cycles run to process the input file

  t1 = chrono::high_resolution_clock::now();

  tuple_key_t *keys;
  if ((keys = (tuple_key_t *) malloc(sizeof(tuple_key_t) * num_tuples)) == NULL) {
    printf("Buffer allocation failed (key buffer)\n");
    return;
  }

  // For each cycle, we must only extract the keys
  size_t key_offset = 0;
  size_t head_offset = 0;
  for (size_t i = 0; i < num_partitions; i++) {
    size_t read_amount = i != num_partitions - 1 ? BUFFER_SIZE :
                         (file_size - 1) % BUFFER_SIZE + 1; // The last part will have remainders

    for (size_t offset = 0; offset < read_amount;) {
      size_t ret = pread(param.input_fd, param.buffer + offset, head_offset + read_amount - offset,
                         head_offset + offset);
      offset += ret;
    }
    head_offset += read_amount;

    tuple_t *data = (tuple_t *) param.buffer; // Need to use buffer as tuple type below, just a typecasting pointer
    // Need to extract only the keys
    for (size_t j = 0; j < read_amount / TUPLE_SIZE; j++) {
      memcpy(&keys[key_offset + j], &data[j], KEY_SIZE);
    }
    key_offset += (read_amount / TUPLE_SIZE);
  }

  t2 = chrono::high_resolution_clock::now();
  duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
  cout << "[Phase1] reading: " << duration << " (milliseconds)" << endl;

  // Sort the key list, ascending order
  t1 = chrono::high_resolution_clock::now();
  radix_sort::parallel_radix_sort<tuple_key_t>(keys, num_tuples, 0);
  t2 = chrono::high_resolution_clock::now();
  duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
  cout << "[Phase1] sorting: " << duration << " (milliseconds)" << endl;

  size_t cnt = 0;
  for (size_t i = 1; i < num_tuples; i++) {
    if (keys[i - 1] > keys[i]) {
      cnt++;
    }
  }
  printf("\tSorting done with %zu unordered items\n", cnt);

  // Need $(num_partitions - 1) keys to separate the whole input into $num_partitions parts.
  // They will be used as following (ex. num_partitions = 4)
  // if (key < threshold[0]): put to file 0,
  // else if (key < threshold[1]): put to file 1,
  // else if (key < threshold[2]): put to file 2,
  // else: put to file 3
  tuple_key_t *thresholds = (tuple_key_t *) malloc(sizeof(tuple_key_t) * (num_partitions - 1));
  const size_t chunk_size = num_tuples / num_partitions; // Tuples per partition

  for (size_t i = 1; i < num_partitions; i++) {
    memcpy(&thresholds[i - 1], &keys[chunk_size * i], KEY_SIZE);
  }

  param.total_file_size = file_size;
  param.thresholds = thresholds;
  param.num_partitions = num_partitions;

  free(keys);
}

void phase2(param_t &param) {
  size_t head_offsets[param.num_partitions];
  for (size_t i = 0; i < param.num_partitions; i++) {
    head_offsets[i] = BUFFER_SIZE * i;
  }

  for (size_t partition_id = 0; partition_id < param.num_partitions; partition_id++) {
    size_t head_offset = BUFFER_SIZE * partition_id;
    size_t read_amount = partition_id != param.num_partitions - 1 ? BUFFER_SIZE :
                         (param.total_file_size - 1) % BUFFER_SIZE + 1; // The last part will have remainders

    t1 = chrono::high_resolution_clock::now();

    for (size_t offset = 0; offset < read_amount;) {
      size_t ret = pread(param.input_fd, param.buffer + offset, read_amount - offset,
                         head_offset + offset);
      offset += ret;
    }

    t2 = chrono::high_resolution_clock::now();
    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase2] reading (" << partition_id << "): " << duration << " (milliseconds)" << endl;

    t1 = chrono::high_resolution_clock::now();
    size_t buckets[param.num_partitions];
    counting_sort::parallel_counting_sort_v2((tuple_t *) param.buffer, read_amount / TUPLE_SIZE, param.thresholds,
                                             buckets, param.num_partitions, 1);
    t2 = chrono::high_resolution_clock::now();
    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase2] counting sort (" << partition_id << "): " << duration << " (milliseconds)" << endl;

    t1 = chrono::high_resolution_clock::now();
    size_t accumulated = 0;
    for (size_t j = 0; j < param.num_partitions; j++) {
      size_t sz = TUPLE_SIZE * buckets[j];
      size_t head = head_offsets[j];
      head_offsets[j] += sz;
      for (size_t offset = 0; offset < sz;) {
        size_t ret = pwrite(param.output_fd, param.buffer + offset + accumulated,
                            sz - offset, head + offset);
        offset += ret;
      }
      accumulated += sz;
    }
    t2 = chrono::high_resolution_clock::now();
    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase2] writing (" << partition_id << "): " << duration << " (milliseconds)" << endl;
  }
}

void phase3(param_t &param) {
  // Just a type casting
  tuple_t *data = (tuple_t *) param.buffer;

  for (size_t partition_id = 0; partition_id < param.num_partitions; partition_id++) {
    size_t head_offset = BUFFER_SIZE * partition_id;
    size_t read_amount = partition_id != param.num_partitions - 1 ? BUFFER_SIZE :
                         (param.total_file_size - 1) % BUFFER_SIZE + 1; // The last part will have remainders

    t1 = chrono::high_resolution_clock::now();
    for (size_t offset = 0; offset < read_amount;) {
      size_t ret = pread(param.output_fd, param.buffer + offset, read_amount - offset,
                         head_offset + offset);
      offset += ret;
    }

    t2 = chrono::high_resolution_clock::now();
    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase3] reading (" << partition_id << "): " << duration << " (milliseconds)" << endl;

    t1 = chrono::high_resolution_clock::now();
    radix_sort::parallel_radix_sort(data, read_amount / TUPLE_SIZE, 0);
    t2 = chrono::high_resolution_clock::now();
    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase3] radix sort (" << partition_id << "): " << duration << " (milliseconds)" << endl;

    t1 = chrono::high_resolution_clock::now();
    for (size_t offset = 0; offset < read_amount;) {
      size_t ret = pwrite(param.output_fd, param.buffer + offset, read_amount - offset,
                          head_offset + offset);
      offset += ret;
    }

    t2 = chrono::high_resolution_clock::now();
    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase3] writing (" << partition_id << "): " << duration << " (milliseconds)" << endl;
  }
}

void check_output(char *filename, char *buffer) {
  int fd;
  if ((fd = open(filename, O_RDONLY | O_NONBLOCK)) == -1) {
    printf("Can't open output file\n");
    return;
  }

  size_t file_size = lseek(fd, 0, SEEK_END);                  // Input file size
  size_t num_tuples = file_size / TUPLE_SIZE;                       // Number of tuples
  size_t num_partitions = (file_size - 1) / BUFFER_SIZE + 1;   // Total cycles run to process the input file

  tuple_key_t *keys;
  if ((keys = (tuple_key_t *) malloc(sizeof(tuple_key_t) * num_tuples)) == NULL) {
    printf("Buffer allocation failed (key buffer)\n");
    return;
  }

  // For each cycle, we must only extract the keys
  size_t key_offset = 0;
  size_t head_offset = 0;
  for (size_t i = 0; i < num_partitions; i++) {
    size_t read_amount = i != num_partitions - 1 ? BUFFER_SIZE :
                         (file_size - 1) % BUFFER_SIZE + 1; // The last part will have remainders

    for (size_t offset = 0; offset < read_amount;) {
      size_t ret = pread(fd, buffer + offset, head_offset + read_amount - offset,
                         head_offset + offset);
      offset += ret;
    }
    head_offset += read_amount;

    tuple_t *data = (tuple_t *) buffer; // Need to use buffer as tuple type below, just a typecasting pointer
    // Need to extract only the keys
    for (size_t j = 0; j < read_amount / TUPLE_SIZE; j++) {
      memcpy(&keys[key_offset + j], &data[j], KEY_SIZE);
    }
    key_offset += (read_amount / TUPLE_SIZE);
  }

  size_t cnt = 0;
  for (size_t i = 1; i < num_tuples; i++) {
    if (keys[i - 1] > keys[i]) {
//      printf("[Validation] Tuple %zu is in the wrong place\n", i);
      cnt++;
    }
  }
  printf("[Validation] Total of %zu tuples in the wrong place\n", cnt);
  close(fd);
  free(keys);
}