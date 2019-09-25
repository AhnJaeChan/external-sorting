//
// Created by 안재찬 on 2018. 10. 1..
//

#ifndef PROJECT1_2_NODE_H
#define PROJECT1_2_NODE_H


#include <cstddef>
#include <string>

#include "global.h"

class Node {
public:
  std::string filename;
  int fd;
  size_t sz;
  size_t idx;
  tuple_t *arr;

  explicit Node(const std::string &filename);

  ~Node();

  tuple_t *front();
  tuple_t *deque();
  bool is_valid();

  bool operator<(const Node &operand) const;
  bool operator>(const Node &operand) const;
};


#endif //PROJECT1_2_NODE_H
