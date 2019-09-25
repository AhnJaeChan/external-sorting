//
// Created by 안재찬 on 2018. 10. 1..
//

#include "Node.h"

#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>

Node::Node(const std::string &filename) {
  // Input handling
  this->filename = filename;
  if ((fd = open(filename.c_str(), O_RDONLY))) {
    printf("[Error] failed to open input file %s\n", filename.c_str());
    return;
  }
  size_t file_size = lseek(fd, 0, SEEK_END);

  arr = (tuple_t *) malloc(file_size);
  char *buf = (char *) arr;
  for (size_t offset = 0; offset < file_size;) {
    size_t ret = pread(fd, buf + offset, file_size - offset, offset);
    offset += ret;
  }

  close(fd);

  this->sz = file_size / TUPLE_SIZE;
  this->idx = 0;
}

Node::~Node() {
  free(this->arr);
}

tuple_t *Node::front() {
  return &this->arr[this->idx];
}

tuple_t *Node::deque() {
  return &this->arr[this->idx++];
}

bool Node::is_valid() {
  return this->idx < this->sz;
}

bool Node::operator<(const Node &operand) const {
  return this->arr[this->idx] < operand.arr[operand.idx];
}

bool Node::operator>(const Node &operand) const {
  return this->arr[this->idx] > operand.arr[operand.idx];
}