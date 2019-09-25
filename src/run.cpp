#include <iostream>
#include <algorithm>
#include <cstdlib>
#include <cstring>

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "global.h"

using namespace std;

int prepare_environment();

tuple_key *phase1_v2(int input_fd);
void phase2(int input_fd, tuple_key *thresholds);
void radix_sort(tuple_t *data, size_t sz, tuple_key *thresholds, size_t *buckets, size_t num_buckets);
void phase3(int output_fd);

int main(int argc, char *argv[]) {
  ios_base::sync_with_stdio(false);

  if (argc < 3) {
    printf("Program usage: ./run input_file_name output_file_name");
    return 0;
  }

  prepare_environment();

  /// [Phase 1] START
  int input_fd;
  if ((input_fd = open(argv[1], O_RDONLY)) == -1) {
    printf("[Error] failed to open input file %s\n", argv[1]);
    return 0;
  }

  tuple_key *thresholds = phase1_v2(input_fd);
  /// [Phase 1] END

  /// [Phase 2] START
  phase2(input_fd, thresholds);
  /// [Phase 2] END

  /// [Phase 2] START
  int output_fd;
  if ((output_fd = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, 0777)) == -1) {
    printf("[Error] failed to open input file %s\n", argv[2]);
    return 0;
  }
  /// [Phase 2] END

  /// [Phase 3] START
  phase3(output_fd);
  /// [Phase 3] END

  close(input_fd);
  close(output_fd);

  free(thresholds);

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
  return 0;
}

tuple_key *phase1_v2(int input_fd) {
  size_t file_size = lseek(input_fd, 0, SEEK_END);                  // Input file size
  size_t num_tuples = file_size / TUPLE_SIZE;                       // Number of tuples
  size_t num_cycles = (file_size - 1) / PHASE1_BUFFER_SIZE + 1;   // Total cycles run to process the input file
  printf("File size: %zu, Tuples existing: %zu, Num partitions: %zu\n", file_size, num_tuples, num_cycles);

  // Buffer for reading N bytes from file at once (N = READ_BUFFER_SIZE)
  char *input_buffer;
  if ((input_buffer = (char *) malloc(sizeof(char) * PHASE1_BUFFER_SIZE)) == NULL) {
    printf("Buffer allocation failed (input read buffer)\n");
    return NULL;
  }

  tuple_key *keys;
  if ((keys = (tuple_key *) malloc(sizeof(tuple_key) * num_tuples)) == NULL) {
    printf("Buffer allocation failed (key buffer)\n");
    return NULL;
  }

  // For each cycle, we must only extract the keys
  size_t key_offset = 0;
  for (size_t i = 0; i < num_cycles; i++) {
    size_t head_offset = PHASE1_BUFFER_SIZE * i;
    size_t read_amount = i != num_cycles - 1 ? PHASE1_BUFFER_SIZE :
                         (file_size - 1) % PHASE1_BUFFER_SIZE + 1; // The last part will have remainders

    for (size_t offset = 0; offset < read_amount;) {
      size_t ret = pread(input_fd, input_buffer + offset, PHASE1_BUFFER_SIZE + head_offset - offset,
                         head_offset + offset);
      offset += ret;
    }

    printf("Read file offset %zu ~ %zu\n", head_offset, head_offset + read_amount);

    tuple_t *data = (tuple_t *) input_buffer; // Need to use buffer as tuple type below, just a typecasting pointer
    // Need to extract only the keys
    for (size_t j = 0; j < read_amount / TUPLE_SIZE; j++) {
      memcpy(keys + key_offset + j, data + j, sizeof(tuple_key));
    }
    key_offset += read_amount / TUPLE_SIZE;
  }
  free(input_buffer);
  printf("Key array generated: %zu\n", key_offset);

  // Sort the key list, ascending order
  sort(keys, keys + num_tuples);

  // Need $(num_partitions - 1) keys to separate the whole input into $num_partitions parts.
  // They will be used as following (ex. num_partitions = 4)
  // if (key < threshold[0]): put to file 0,
  // else if (key < threshold[1]): put to file 1,
  // else if (key < threshold[2]): put to file 2,
  // else: put to file 3
  tuple_key *thresholds = (tuple_key *) malloc(sizeof(tuple_key) * (NUM_PARTITIONS - 1));
  for (size_t i = 1; i < NUM_PARTITIONS; i++) {
    printf("Threshold %zu = %zu\n", i, i * num_tuples / NUM_PARTITIONS);
    memcpy(thresholds + i - 1, keys + i * num_tuples / NUM_PARTITIONS, sizeof(tuple_key));
  }
  free(keys);

  return thresholds;
}

void phase2(int input_fd, tuple_key *thresholds) {
  size_t file_size = lseek(input_fd, 0, SEEK_END);              // Input file size
  size_t num_cycles = (file_size - 1) / PHASE2_BUFFER_SIZE + 1; // Total cycles run to process the input file

  // Buffer for reading N bytes from file at once (N = READ_BUFFER_SIZE)
  char *input_buffer;
  if ((input_buffer = (char *) malloc(sizeof(char) * PHASE2_BUFFER_SIZE)) == NULL) {
    printf("Buffer allocation failed (input read buffer)\n");
    return;
  }

  int output_fds[NUM_PARTITIONS];
  size_t head_offsets[NUM_PARTITIONS];
  for (size_t i = 0; i < NUM_PARTITIONS; i++) {
    head_offsets[i] = 0;
    string filename(TMP_DIRECTORY);
    filename += to_string(i) + ".data";
    if ((output_fds[i] = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777)) == -1) {
      printf("tmp file open failure on phase 2\n");
      return;
    }
  }

  for (size_t i = 0; i < num_cycles; i++) {
    size_t head_offset = PHASE2_BUFFER_SIZE * i;
    size_t read_amount = i != num_cycles - 1 ? PHASE2_BUFFER_SIZE :
                         (file_size - 1) % PHASE2_BUFFER_SIZE + 1; // The last part will have remainders

    for (size_t offset = 0; offset < read_amount;) {
      size_t ret = pread(input_fd, input_buffer + offset, PHASE2_BUFFER_SIZE + head_offset - offset,
                         head_offset + offset);
      offset += ret;
    }

    printf("Read file offset %zu ~ %zu\n", head_offset, head_offset + read_amount);

    size_t buckets[NUM_PARTITIONS];
    tuple_t *data = (tuple_t *) input_buffer; // Need to use buffer as tuple type below, just a typecasting pointer
    radix_sort(data, read_amount / TUPLE_SIZE, thresholds, buckets, NUM_PARTITIONS);

    for (size_t j = 0; j < NUM_PARTITIONS; j++) {
      size_t sz = sizeof(tuple_t) * buckets[j];
      for (size_t offset = 0; offset < sz;) {
        size_t ret = pwrite(output_fds[j], input_buffer + offset, sz - offset, head_offsets[j] + offset);
        offset += ret;
      }
      head_offsets[j] += sz;
    }
  }

  size_t sum = 0;
  for (int i = 0; i < NUM_PARTITIONS; i++) {
    sum += head_offsets[i];
    close(output_fds[i]);
  }
  printf("[Phase 2] %zu tmp files written, total of %zu bytes\n", NUM_PARTITIONS, sum);

  free(input_buffer);
}

void phase3(int output_fd) {
  char *buffer;
  if ((buffer = (char *) malloc(sizeof(char) * PHASE3_BUFFER_SIZE)) == NULL) {
    printf("Buffer allocation failed (input read buffer)\n");
    return;
  }

  size_t head_offset = 0; // Offset on the last output file
  for (size_t i = 0; i < NUM_PARTITIONS; i++) {
    int input_fd;
    string filename(TMP_DIRECTORY);
    filename += to_string(i) + ".data";
    if ((input_fd = open(filename.c_str(), O_RDONLY)) == -1) {
      printf("tmp file open failure on phase 3\n");
      return;
    }

    size_t file_size = lseek(input_fd, 0, SEEK_END);
    for (size_t offset = 0; offset < file_size;) {
      size_t ret = pread(input_fd, buffer + offset, file_size - offset, offset);
      offset += ret;
    }

    tuple_t *data = (tuple_t *) buffer;
    sort(data, data + file_size / TUPLE_SIZE);

    for (size_t offset = 0; offset < file_size;) {
      size_t ret = pwrite(output_fd, buffer + offset, file_size - offset, head_offset + offset);
      offset += ret;
    }
    head_offset += file_size;
  }

  printf("[Phase 2] final output file written (%zu bytes)\n", head_offset);

  free(buffer);
}

size_t bucket(const tuple_key &key, const tuple_key *thresholds, size_t num_buckets) {
  size_t bucket = 0;
  while (bucket < num_buckets - 1) {
    if (key < thresholds[bucket]) {
      return bucket;
    }
    bucket++;
  }
  return num_buckets - 1;
}

void radix_sort(tuple_t *data, size_t sz, tuple_key *thresholds, size_t *buckets, size_t num_buckets) {
  memset(buckets, 0, sizeof(size_t) * num_buckets);

  for (size_t i = 0; i < sz; i++) {
    buckets[bucket(*(tuple_key *) &data[i], thresholds, num_buckets)]++;
  }
  size_t sum = 0;
  size_t heads[num_buckets];
  size_t tails[num_buckets];
  for (size_t i = 0; i < num_buckets; i++) {
    heads[i] = sum;
    sum += buckets[i];
    tails[i] = sum;
    printf("Bucket[%zu] %zu ~ %zu, contains %zu tuples.\n", i, heads[i], tails[i], buckets[i]);
  }
  printf("Total of %zu tuples processed.\n", sum);

  for (size_t i = 0; i < num_buckets; i++) {
    while (heads[i] < tails[i]) {
      tuple_t tuple = data[heads[i]];
      while (bucket(*(tuple_key *) &tuple, thresholds, num_buckets) != i) {
        swap(tuple, data[heads[bucket(*(tuple_key *) &tuple, thresholds, num_buckets)]++]);
      }
      data[heads[i]++] = tuple;
    }
  }

  sum = 0;
  for (size_t i = 0; i < num_buckets; i++) {
    for (size_t j = 0; j < buckets[i] - 1; j++) {
      if (bucket(*(tuple_key *) &data[sum + j], thresholds, num_buckets) != i) {
        printf("Bucket[%zu] contains wrong data which should be in bucket %zu\n", i,
               bucket(*(tuple_key *) &data[sum + j], thresholds, num_buckets));
      }
    }
    sum += buckets[i];
  }
  printf("Radix sort SUCCESS!\n");
}

