//
// Created by 안재찬 on 25/09/2019.
//

#include "MinHeap.h"

#include "Node.h"

using namespace std;

MinHeap::MinHeap(Node **heap, size_t sz) {
  this->heap = heap;
  this->sz = sz;
}

MinHeap::~MinHeap() {
}

void MinHeap::min_heapify() {
  for (size_t i = this->sz / 2; i >= 1; i--) {
    partial_min_heapify(i);
  }
}

void MinHeap::partial_min_heapify(size_t idx) {
  size_t left = idx * 2;
  size_t right = left + 1;
  size_t min_idx = idx;

  if (left <= this->sz && *this->heap[left] < *this->heap[idx]) {
    min_idx = left;
  }

  if (right <= this->sz && *this->heap[right] < *this->heap[min_idx]) {
    min_idx = right;
  }

  if (min_idx != idx) {
    swap(this->heap[idx], this->heap[min_idx]);
    partial_min_heapify(min_idx);
  }
}

tuple_t *MinHeap::extract_min() {
  tuple_t *tuple = this->heap[1]->deque();

  if (!this->heap[1]->is_valid()) {
    swap(this->heap[1], this->heap[this->sz--]);
  }

  partial_min_heapify(1);

  return tuple;
}