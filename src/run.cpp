#include <iostream>
#include <cstdlib>
#include <cstring>

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <chrono>

#include "global.h"
#include "parallel_radix_sort.h"

using namespace std;

int prepare_environment() {
  if (mkdir(TMP_DIRECTORY, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
    if (errno == EEXIST) {
      // already exists
    } else {
      // something else
      return -1;
    }
  }
  return 0;
}

void phase_small_file(param_t &param);
void phase1(param_t &param);
void phase2(param_t &param);

void check_output(char *filename, char *buffer);

int main(int argc, char *argv[]) {
  if (argc < 3) {
    printf("Program usage: ./run input_file_name output_file_name\n");
    return 0;
  }

  if (prepare_environment() == -1) {
    printf("%s directory couldn't be made\n", TMP_DIRECTORY);
    return 0;
  }

  param_t param;
  param.buffer = NULL;
  param.thresholds = NULL;
  char *buffer;
  if ((buffer = (char *) malloc(READ_BUFFER_SIZE)) == NULL) {
    printf("Buffer allocation failed (input read buffer)\n");
    return 0;
  }
  param.buffer = buffer;

  /// [Phase 1] START
  if ((param.input_fd = open(argv[1], O_RDONLY)) == -1) {
    printf("[Error] failed to open input file %s\n", argv[1]);
    return 0;
  }
  param.file_size = lseek(param.input_fd, 0, SEEK_END);

  if ((param.output_fd = open(argv[2], O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, 0777)) == -1) {
    printf("[Error] failed to open input file %s\n", argv[2]);
    return 0;
  }

  chrono::time_point<chrono::system_clock> t1, t2;
  long long int duration;

  if (param.file_size <= READ_BUFFER_SIZE) {
    t1 = chrono::high_resolution_clock::now();
    phase_small_file(param);
    t2 = chrono::high_resolution_clock::now();
    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase small file] took: " << duration << " (milliseconds)" << endl;
  } else {
    /// [Phase 1] START
    t1 = chrono::high_resolution_clock::now();
    phase1(param);
    t2 = chrono::high_resolution_clock::now();

    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase1] took: " << duration << " (milliseconds)" << endl;
    /// [Phase 1] END

    /// [Phase 2] START
    t1 = chrono::high_resolution_clock::now();
    phase2(param);
    t2 = chrono::high_resolution_clock::now();

    duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
    cout << "[Phase2] took: " << duration << " (milliseconds)" << endl;
    /// [Phase 2] END
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
  if (param.sorted_keys != NULL) {
    free(param.sorted_keys);
  }

  t2 = chrono::high_resolution_clock::now();
  duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
  cout << "[Clean up] took: " << duration << " (milliseconds)" << endl;

  return 0;
}

void phase_small_file(param_t &param) {
  chrono::time_point<chrono::system_clock> t1, t2;
  long long int duration;

  for (size_t offset = 0; offset < param.file_size;) {
    size_t ret = pread(param.input_fd, param.buffer + offset, param.file_size - offset, offset);
    offset += ret;
  }

  t1 = chrono::high_resolution_clock::now();
  radix_sort::parallel_radix_sort((tuple_t *) param.buffer, param.file_size / TUPLE_SIZE, 0);
  t2 = chrono::high_resolution_clock::now();

  duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
  cout << "[Phase1] sorting: " << duration << " (milliseconds)" << endl;

  for (size_t offset = 0; offset < param.file_size;) {
    size_t ret = pwrite(param.output_fd, param.buffer + offset, param.file_size - offset, offset);
    offset += ret;
  }
}

void phase1(param_t &param) {
  chrono::time_point<chrono::system_clock> t1, t2;
  long long int duration;

  size_t file_size = param.file_size; // Input file size
  size_t num_tuples = file_size / TUPLE_SIZE; // Number of tuples
  size_t num_partitions = (file_size - 1) / READ_BUFFER_SIZE + 1; // Total cycles run to process the input file

  tuple_key_t *keys;
  if ((keys = (tuple_key_t *) malloc(sizeof(tuple_key_t) * num_tuples)) == NULL) {
    printf("Buffer allocation failed (key buffer)\n");
    return;
  }

  // For each cycle, we must only extract the keys
  size_t key_offset = 0;
  size_t head_offset = 0;
  for (size_t i = 0; i < num_partitions; i++) {
    // Read to buffer
    size_t read_amount = i != num_partitions - 1 ? READ_BUFFER_SIZE :
                         (file_size - 1) % READ_BUFFER_SIZE + 1; // The last part will have remainders

    for (size_t offset = 0; offset < read_amount;) {
      size_t ret = pread(param.input_fd, param.buffer + offset, head_offset + read_amount - offset,
                         head_offset + offset);
      offset += ret;
    }
    head_offset += read_amount;

    // Key copy
    for (size_t j = 0; j < read_amount / TUPLE_SIZE; j++) {
      memcpy(&keys[key_offset + j], ((tuple_t *) param.buffer) + j, KEY_SIZE);
    }
    key_offset += (read_amount / TUPLE_SIZE);

    radix_sort::parallel_radix_sort((tuple_t *) param.buffer, read_amount / TUPLE_SIZE, 0);

    int output_fd;
    string filename(TMP_DIRECTORY);
    filename += to_string(i) + TMP_FILE_SUFFIX;
    if ((output_fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, 0777)) == -1) {
      printf("[Error] failed to open input file %s\n", filename.c_str());
      return;
    }

    for (size_t offset = 0; offset < read_amount;) {
      size_t ret = pwrite(output_fd, param.buffer + offset, read_amount - offset, offset);
      offset += ret;
    }

    close(output_fd);
  }

  // Sort the key list, ascending order
  t1 = chrono::high_resolution_clock::now();
  radix_sort::parallel_radix_sort<tuple_key_t>(keys, num_tuples, 0);
  t2 = chrono::high_resolution_clock::now();
  duration = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
  cout << "[Phase1] sorting: " << duration << " (milliseconds)" << endl;

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

  param.file_size = file_size;
  param.num_tuples = num_tuples;
  param.thresholds = thresholds;
  param.num_partitions = num_partitions;
  param.sorted_keys = keys;
}

void phase2(param_t &param) {
  int tmp_fds[param.num_partitions];
  char *input_buffers[param.num_partitions];
  size_t buffer_size = READ_BUFFER_SIZE / param.num_partitions;
  for (size_t i = 0; i < param.num_partitions; i++) {
    string filename(TMP_DIRECTORY);
    filename += to_string(i) + TMP_FILE_SUFFIX;
    if ((tmp_fds[i] = open(filename.c_str(), O_RDONLY)) == -1) {
      printf("[Error] failed to open input file %s\n", filename.c_str());
      return;
    }
    input_buffers[i] = i != param.num_partitions - 1 ? param.buffer + (i + 1) * buffer_size
                                                     : param.buffer;
    // Since last partition's first chunk is already set in param.buffer from before, leave it there.
    if (i != param.num_partitions) {
      for (size_t offset = 0; offset < buffer_size;) {
        size_t ret = pread(tmp_fds[i], input_buffers[i], buffer_size - offset, offset);
        offset += ret;
      }
    }
  }
  char *output_buffer;
  if ((output_buffer = (char *) malloc(sizeof(char) * WRITE_BUFFER_SIZE)) == NULL) {
    printf("Buffer allocation failed (output write buffer)\n");
    return;
  }

  size_t num_partitions = param.num_partitions;
  section_t output_buffer_section = {0, WRITE_BUFFER_SIZE};
  section_t input_buffer_sections[param.num_partitions];
  section_t input_buffer_global_sections[param.num_partitions];

  for (size_t partition_id = 0; partition_id < param.num_partitions; partition_id++) {
    input_buffer_global_sections[partition_id].head = buffer_size; // Already read the first chunks
    input_buffer_global_sections[partition_id].tail = READ_BUFFER_SIZE;
    input_buffer_sections[partition_id].head = 0;
    input_buffer_sections[partition_id].tail = buffer_size;
  }

  size_t output_offset = 0;
  // Before stepping into this step, we must fill in the input buffers with the first ata sets
  for (size_t tuple_id = 0; tuple_id < param.num_tuples; tuple_id++) {
    tuple_key_t &key = param.sorted_keys[tuple_id];

    size_t idx;
    for (idx = 0; idx < param.num_partitions; idx++) {
      if (input_buffer_sections[idx].head == input_buffer_sections[idx].tail) {
        continue;
      }

      tuple_key_t &comp = *(tuple_key_t *) (input_buffers[idx] + input_buffer_sections[idx].head);
      if (key == comp) {
        memcpy(output_buffer + output_buffer_section.head, &key, TUPLE_SIZE);
        input_buffer_sections[idx].head += TUPLE_SIZE;
        output_buffer_section.head += TUPLE_SIZE;
        break;
      }
    }

    // If partition buffer is all read, read another chunk
    if (input_buffer_sections[idx].head == input_buffer_sections[idx].tail) {
      // Read another chunk only if available
      if (input_buffer_global_sections[idx].head != input_buffer_global_sections[idx].tail) {
        for (size_t offset = 0; offset < buffer_size;) {
          size_t ret = pread(tmp_fds[idx], input_buffers[idx], buffer_size - offset,
                             input_buffer_global_sections[idx].head + offset);
          offset += ret;
        }
        input_buffer_global_sections[idx].head += buffer_size;
        input_buffer_sections[idx].head = 0;
      }
    }

    // If output buffer is full, flush
    if (output_buffer_section.head == output_buffer_section.tail) {
      for (size_t offset = 0; offset < WRITE_BUFFER_SIZE;) {
        size_t ret = pwrite(param.output_fd, output_buffer, WRITE_BUFFER_SIZE - offset, output_offset + offset);
        offset += ret;
      }
      output_buffer_section.head = 0;
      output_offset += WRITE_BUFFER_SIZE;
    }
  }

  free(output_buffer);
}

void check_output(char *filename, char *buffer) {
  int fd;
  if ((fd = open(filename, O_RDONLY | O_NONBLOCK)) == -1) {
    printf("Can't open output file\n");
    return;
  }

  size_t file_size = lseek(fd, 0, SEEK_END);                  // Input file size
  size_t num_tuples = file_size / TUPLE_SIZE;                       // Number of tuples
  size_t num_partitions = (file_size - 1) / READ_BUFFER_SIZE + 1;   // Total cycles run to process the input file

  tuple_key_t *keys;
  if ((keys = (tuple_key_t *) malloc(sizeof(tuple_key_t) * num_tuples)) == NULL) {
    printf("Buffer allocation failed (key buffer)\n");
    return;
  }

  // For each cycle, we must only extract the keys
  size_t key_offset = 0;
  size_t head_offset = 0;
  for (size_t i = 0; i < num_partitions; i++) {
    size_t read_amount = i != num_partitions - 1 ? READ_BUFFER_SIZE :
                         (file_size - 1) % READ_BUFFER_SIZE + 1; // The last part will have remainders

    for (size_t offset = 0; offset < read_amount;) {
      size_t ret = pread(fd, buffer + offset, head_offset + read_amount - offset,
                         head_offset + offset);
      offset += ret;
    }
    head_offset += read_amount;

    tuple_t *data = (tuple_t *) buffer;
    // Need to extract only the keys
    for (size_t j = 0; j < read_amount / TUPLE_SIZE; j++) {
      memcpy(&keys[key_offset + j], &data[j], KEY_SIZE);
    }
    key_offset += (read_amount / TUPLE_SIZE);
  }

  size_t cnt = 0;
  for (size_t i = 1; i < num_tuples; i++) {
    if (keys[i - 1] > keys[i]) {
      cnt++;
    }
  }
  printf("[Validation] Total of %zu tuples in the wrong place\n", cnt);
  close(fd);
  free(keys);
}