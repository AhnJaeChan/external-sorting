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

using namespace std;

int prepare_environment();

void phase1(param_t &param);
void phase2(param_t &param);
size_t bucket(const tuple_key &key, const tuple_key *thresholds, const size_t &num_thresholds);
void radix_sort(tuple_t *data, size_t sz, tuple_key_t *thresholds, size_t *buckets, size_t num_buckets);
void phase3(param_t &param);

int main(int argc, char *argv[]) {
  if (argc < 3) {
    printf("Program usage: ./run input_file_name output_file_name\n");
    return 0;
  }

  if (prepare_environment() == -1) {
    printf("[Error] directory cannot be made\n");
  }

  param_t param;
  chrono::time_point<chrono::system_clock> t1, t2;
  long long int duration;

  /// [Phase 1] START
  if ((param.input_fd = open(argv[1], O_RDONLY)) == -1) {
    printf("[Error] failed to open input file %s\n", argv[1]);
    return 0;
  }

  t1 = chrono::high_resolution_clock::now();
  phase1(param);
  t2 = chrono::high_resolution_clock::now();

  duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
  cout << "[Phase1] took: " << duration << "(milliseconds)" << endl;
  /// [Phase 1] END

  /// [Phase 2] START
  t1 = chrono::high_resolution_clock::now();
  phase2(param);
  t2 = chrono::high_resolution_clock::now();

  duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
  cout << "[Phase2] took: " << duration << "(milliseconds)" << endl;
  /// [Phase 2] END

  /// [Phase 3] START
  if ((param.output_fd = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, 0777)) == -1) {
    printf("[Error] failed to open input file %s\n", argv[2]);
    return 0;
  }
  t1 = chrono::high_resolution_clock::now();
  phase3(param);
  t2 = chrono::high_resolution_clock::now();

  duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
  cout << "[Phase3] took: " << duration << "(milliseconds)" << endl;
  /// [Phase 3] END

  close(param.input_fd);
  close(param.output_fd);

  free(param.thresholds);

  return 0;
}

int prepare_environment() {
  if (mkdir(TMP_DIRECTORY, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
    if (errno == EEXIST) {
      // alredy exists
    } else {
      printf("mkdir %s failed\n", TMP_DIRECTORY);
      return -1;
    }
  }
  omp_set_num_threads(NUM_THREADS);

  return 0;
}

void phase1(param_t &param) {
  size_t file_size = lseek(param.input_fd, 0, SEEK_END);                  // Input file size
  size_t num_tuples = file_size / TUPLE_SIZE;                       // Number of tuples
  size_t num_cycles = (file_size - 1) / READ_BUFFER_SIZE + 1;   // Total cycles run to process the input file
//  size_t num_partitions = (file_size - 1) / (MAX_BUFFER / NUM_THREADS) + 1; // Total partitions
  size_t num_partitions;
  if (file_size > MAX_BUFFER) {
    num_partitions = (file_size - 1) / (MAX_BUFFER / NUM_THREADS) + 1;
  } else {
    num_partitions = NUM_THREADS;
  }

  // Buffer for reading N bytes from file at once (N = READ_BUFFER_SIZE)
  char *input_buffer;
  if ((input_buffer = (char *) malloc(sizeof(char) * READ_BUFFER_SIZE)) == NULL) {
    printf("Buffer allocation failed (input read buffer)\n");
    return;
  }

  tuple_key_t *keys;
  if ((keys = (tuple_key_t *) malloc(sizeof(tuple_key_t) * num_tuples)) == NULL) {
    printf("Buffer allocation failed (key buffer)\n");
    return;
  }

  // For each cycle, we must only extract the keys
  size_t key_offset = 0;
  size_t head_offset = 0;
  for (size_t i = 0; i < num_cycles; i++) {
    size_t read_amount = i != num_cycles - 1 ? READ_BUFFER_SIZE :
                         (file_size - 1) % READ_BUFFER_SIZE + 1; // The last part will have remainders

    for (size_t offset = 0; offset < read_amount;) {
      size_t ret = pread(param.input_fd, input_buffer + offset, head_offset + read_amount - offset,
                         head_offset + offset);
      offset += ret;
    }
    head_offset += read_amount;

    tuple_t *data = (tuple_t *) input_buffer; // Need to use buffer as tuple type below, just a typecasting pointer
    // Need to extract only the keys
    for (size_t j = 0; j < read_amount / TUPLE_SIZE; j++) {
      memcpy(&keys[key_offset + j], &data[j], KEY_SIZE);
    }
    key_offset += (read_amount / TUPLE_SIZE);
  }

  // Sort the key list, ascending order
  sort(keys, keys + num_tuples);

  // Need $(num_partitions - 1) keys to separate the whole input into $num_partitions parts.
  // They will be used as following (ex. num_partitions = 4)
  // if (key < threshold[0]): put to file 0,
  // else if (key < threshold[1]): put to file 1,
  // else if (key < threshold[2]): put to file 2,
  // else: put to file 3
  tuple_key_t *thresholds = (tuple_key_t *) malloc(sizeof(tuple_key_t) * (num_partitions - 1));
  size_t num_partition_tuples = num_tuples / num_partitions; // Tuples per partition

  for (size_t i = 1; i < num_partitions; i++) {
    memcpy(&thresholds[i - 1], &keys[num_partition_tuples * i], KEY_SIZE);
  }

  param.total_file_size = file_size;
  param.num_partitions = num_partitions;
  param.thresholds = thresholds;

  free(keys);
  free(input_buffer);
}

void phase2(param_t &param) {
  size_t buffer_size = sizeof(char) * (MAX_BUFFER / NUM_THREADS);
  size_t num_cycles =
      (param.total_file_size - 1) / buffer_size + 1; // Total cycles run to process the input file

  int output_fds[param.num_partitions];
  size_t head_offsets[param.num_partitions];
  memset(head_offsets, 0, sizeof(size_t) * param.num_partitions);
  for (size_t i = 0; i < param.num_partitions; i++) {
    string filename(TMP_DIRECTORY);
    filename += to_string(i) + ".data";
    if ((output_fds[i] = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777)) == -1) {
      printf("tmp file open failure on phase 2\n");
      return;
    }
  }

  #pragma omp parallel num_threads(NUM_THREADS) shared(param, num_cycles, buffer_size, output_fds, head_offsets) default(none)
  {
    char *buffer;
    if ((buffer = (char *) malloc(buffer_size)) == NULL) {
      printf("Buffer allocation failed (input read buffer)\n");
      // TODO: exit program
    }

    #pragma omp for
    for (size_t i = 0; i < num_cycles; i++) {
      size_t head_offset = buffer_size * i;
      size_t read_amount = i != num_cycles - 1 ? buffer_size :
                           (param.total_file_size - 1) % buffer_size + 1; // The last part will have remainders

      for (size_t offset = 0; offset < read_amount;) {
        size_t ret = pread(param.input_fd, buffer + offset, read_amount - offset,
                           head_offset + offset);
        offset += ret;
      }

      size_t buckets[param.num_partitions];
      tuple_t *data = (tuple_t *) buffer; // Need to use buffer as tuple type below, just a typecasting pointer
      radix_sort(data, read_amount / TUPLE_SIZE, param.thresholds, buckets, param.num_partitions);

      size_t accumulated = 0;
      for (size_t j = 0; j < param.num_partitions; j++) {
        size_t head;
        size_t sz = TUPLE_SIZE * buckets[j];
        #pragma omp atomic capture
        {
          head = head_offsets[j];
          head_offsets[j] += sz;
        }
        for (size_t offset = 0; offset < sz;) {
          size_t ret = pwrite(output_fds[j], buffer + offset + accumulated,
                              sz - offset, head + offset);
          offset += ret;
        }
        accumulated += sz;
      }
    }

    free(buffer);
  }

  size_t sum = 0;
  for (size_t i = 0; i < param.num_partitions; i++) {
    sum += head_offsets[i];
    close(output_fds[i]);
  }
}

size_t bucket(const tuple_key_t &key, const tuple_key_t *thresholds, const size_t &num_thresholds) {
  return upper_bound(thresholds, thresholds + num_thresholds, key) - thresholds;
}

void radix_sort(tuple_t *data, size_t sz, tuple_key_t *thresholds, size_t *buckets, size_t num_buckets) {
  size_t num_thresholds = num_buckets - 1;
  memset(buckets, 0, sizeof(size_t) * num_buckets);
  for (size_t i = 0; i < sz; i++) {
    buckets[bucket(*(tuple_key_t *) &data[i], thresholds, num_thresholds)]++;
  }

  size_t sum = 0;
  size_t heads[num_buckets];
  size_t tails[num_buckets];
  for (size_t i = 0; i < num_buckets; i++) {
    heads[i] = sum;
    sum += buckets[i];
    tails[i] = sum;
  }

  for (size_t i = 0; i < num_buckets; i++) {
    while (heads[i] < tails[i]) {
      tuple_t &tuple = data[heads[i]];
      while (bucket(*(tuple_key_t *) &tuple, thresholds, num_thresholds) != i) {
        swap(tuple, data[heads[bucket(*(tuple_key_t *) &tuple, thresholds, num_thresholds)]++]);
      }
      heads[i]++;
    }
  }

}

void phase3(param_t &param) {
  size_t buffer_size = sizeof(char) * (MAX_BUFFER / NUM_THREADS);

  int input_fds[param.num_partitions];
  size_t file_sizes[param.num_partitions];
  size_t head_offsets[param.num_partitions];
  size_t sum = 0;
  for (size_t i = 0; i < param.num_partitions; i++) {
    head_offsets[i] = sum;
    string filename(TMP_DIRECTORY);
    filename += to_string(i) + ".data";
    if ((input_fds[i] = open(filename.c_str(), O_RDONLY)) == -1) {
      printf("tmp file open failure on phase 2\n");
      return;
    }
    file_sizes[i] = lseek(input_fds[i], 0, SEEK_END);
    sum += file_sizes[i];
  }

  #pragma omp parallel num_threads(NUM_THREADS) shared(param, buffer_size, input_fds, file_sizes, head_offsets) default(none)
  {
    char *buffer;
    if ((buffer = (char *) malloc(buffer_size)) == NULL) {
      printf("Buffer allocation failed (input read buffer)\n");
      // TODO: exit program
    }

    #pragma omp for
    for (size_t i = 0; i < param.num_partitions; i++) {
      for (size_t offset = 0; offset < file_sizes[i];) {
        size_t ret = pread(input_fds[i], buffer + offset, file_sizes[i] - offset, offset);
        offset += ret;
      }

      tuple_t *data = (tuple_t *) buffer;
      sort(data, data + (file_sizes[i] / TUPLE_SIZE));

      for (size_t offset = 0; offset < file_sizes[i];) {
        size_t ret = pwrite(param.output_fd, buffer + offset, file_sizes[i] - offset, head_offsets[i] + offset);
        offset += ret;
      }
    }

    free(buffer);
  }

  for (size_t i = 0; i < param.num_partitions; i++) {
    close(input_fds[i]);
  }
}
