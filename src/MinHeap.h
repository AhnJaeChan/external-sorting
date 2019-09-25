//
// Created by 안재찬 on 25/09/2019.
//

#ifndef MULTICORE_EXTERNAL_SORT_MINHEAP_H
#define MULTICORE_EXTERNAL_SORT_MINHEAP_H


#include <cstddef>
#include <iostream>

class Node;

typedef struct tuple tuple_t;

class MinHeap {
public:
  Node **heap;
  size_t sz;

  MinHeap(Node **heap, size_t size);

  ~MinHeap();

  void min_heapify();

  void partial_min_heapify(size_t idx);

  tuple_t *extract_min();
};


#endif //MULTICORE_EXTERNAL_SORT_MINHEAP_H
