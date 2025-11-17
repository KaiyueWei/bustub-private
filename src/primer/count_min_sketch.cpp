//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  if (width == 0 || depth == 0) {
    throw std::invalid_argument("Width and depth must be > 0");
  }
  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  const size_t total = static_cast<size_t>(width_) * depth_;
  table_ = std::make_unique<std::atomic<uint32_t>[]>(total);
  for (size_t i = 0; i < total; i++) {
    table_[i].store(0, std::memory_order_relaxed);
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept
    : width_(other.width_),
      depth_(other.depth_),
      hash_functions_(std::move(other.hash_functions_)),
      table_(std::move(other.table_)) {
  /** @TODO(student) Implement this function! */
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  if (this != &other) {
    width_ = other.width_;
    depth_ = other.depth_;
    table_ = std::move(other.table_);
    hash_functions_ = std::move(other.hash_functions_);
  }
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < depth_; i++) {
    uint32_t col = hash_functions_[i](item) % width_;
    size_t index = static_cast<size_t>(i) * width_ + col;
    table_[index].fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  const size_t total = static_cast<size_t>(width_) * depth_;
  for (size_t i = 0; i < total; i++) {
    uint32_t val = other.table_[i].load(std::memory_order_relaxed);
    table_[i].fetch_add(val, std::memory_order_relaxed);
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t result = UINT32_MAX;
  for (size_t i = 0; i < depth_; i++) {
    uint32_t col = hash_functions_[i](item) % width_;
    size_t index = static_cast<size_t>(i) * width_ + col;
    uint32_t value = table_[index].load(std::memory_order_relaxed);
    result = std::min(result, value);
  }
  return (result == UINT32_MAX ? 0 : result);
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  const size_t total = static_cast<size_t>(width_) * depth_;
  for (size_t i = 0; i < total; i++) {
    table_[i].store(0, std::memory_order_relaxed);
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  std::vector<std::pair<KeyType, uint32_t>> results;
  results.reserve(candidates.size());

  for (size_t i = 0; i < candidates.size(); i++) {
    results.emplace_back(candidates[i], Count(candidates[i]));
  }
  auto cmp = [](const auto &lhs, const auto &rhs) -> bool {
    if (lhs.second != rhs.second) {
      return lhs.second > rhs.second;
    }
    return lhs.first > rhs.first;
  };

  std::sort(results.begin(), results.end(), cmp);

  if (results.size() > k) {
    results.erase(results.begin() + k, results.end());
  }

  return results;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
