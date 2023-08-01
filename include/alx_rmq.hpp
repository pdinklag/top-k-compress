/*******************************************************************************
 * alx/rmq/rmq.hpp
 *
 * Copyright (C) 2022 Alexander Herlez <alexander.herlez@tu-dortmund.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <cassert>
#include <cstdint>

#include <omp.h>
#include <vector>

namespace alx::rmq {

template <typename t_key_type, typename index_type = uint32_t>
class rmq_nlgn {
 public:
  using key_type = t_key_type;
  rmq_nlgn() {
  }

  rmq_nlgn(key_type const* data, size_t size) : m_data(data) {
    assert(size != 0);
    const uint8_t m_num_levels = std::bit_width(size) - 1;
    m_power_rmq.resize(m_num_levels);

    // Build first level
    if (!m_power_rmq.empty()) {
      m_power_rmq[0].resize(size - 1);
    }

#pragma omp parallel for
    for (size_t i = 0; i < size - 1; ++i) {
      m_power_rmq[0][i] = m_data[i] <= data[i + 1] ? i : (i + 1);
    }

    // Build the rest
    for (size_t l = 1; l < m_num_levels; ++l) {
      m_power_rmq[l].resize(size - ((uint64_t{2} << l) - 1));
      uint32_t const span = (uint64_t{1} << l);
#pragma omp parallel for
      for (size_t i = 0; i < m_power_rmq[l].size(); ++i) {
        const uint32_t l_interval_min = m_power_rmq[l - 1][i];
        const uint32_t r_interval_min = m_power_rmq[l - 1][i + span];
        m_power_rmq[l][i] = m_data[l_interval_min] <= m_data[r_interval_min]
                                ? l_interval_min
                                : r_interval_min;
      }
    }
  }

  template <typename C>
  rmq_nlgn(C const& container) : rmq_nlgn(container.data(), container.size()) {
  }

  // Return the index of the smallest element in m_data[left]..m_data[right] for
  // left = std::min(i, j) and right = std::max(i, j).
  size_t rmq(size_t const i, size_t const j) const {
    if (i == j) [[unlikely]] {
      return i;
    }
    return rmq_uneq(i, j);
  }

  // Return the index of the smallest element in m_data[left]..m_data[right] for
  // left = std::min(i, j) and right = std::max(i, j). Here i and j differ.
  size_t rmq_uneq(size_t const i, size_t const j) const {
    assert(i != j);
    size_t const left = std::min(i, j);
    size_t const right = std::max(i, j);
    return rmq_lr(left, right);
  }

  // Return the index of the smallest element in m_data[left]..m_data[right].
  // Here left must be smaller than right.
  size_t rmq_lr(size_t const left, size_t const right) const {
    assert(left < right);
    const uint32_t interval_size = right - left + 1;

    const uint32_t interval_log = std::bit_width(interval_size) - 1;
    const uint32_t max_power_span = (1ULL << interval_log);
    const uint32_t l_interval_min = m_power_rmq[interval_log - 1][left];
    const uint32_t r_interval_min =
        m_power_rmq[interval_log - 1][right + 1 - max_power_span];

    return m_data[l_interval_min] <= m_data[r_interval_min] ? l_interval_min
                                                            : r_interval_min;
  }

  // Return the index of the smallest element in m_data[left+1]..m_data[right]
  // for left = std::min(i, j) and right = std::max(i, j). Useful for the LCP
  // array.
  size_t rmq_shifted(size_t const i, size_t const j) const {
    assert(i != j);
    size_t const left = std::min(i, j) + 1;
    size_t const right = std::max(i, j);

    // We can not use rmq_lr because interval_size could be one.
    const uint32_t interval_size = right - left + 1;
    if (interval_size <= 2) {
      return m_data[left] <= m_data[right] ? left : right;
    }

    const uint32_t interval_log = std::bit_width(interval_size) - 1;
    const uint32_t max_power_span = (1ULL << interval_log);
    const uint32_t l_interval_min = m_power_rmq[interval_log - 1][left];
    const uint32_t r_interval_min =
        m_power_rmq[interval_log - 1][right + 1 - max_power_span];

    return m_data[l_interval_min] <= m_data[r_interval_min] ? l_interval_min
                                                            : r_interval_min;
  }

  size_t memory_size() const {
    size_t mem = 0;
    mem += m_power_rmq.capacity() * sizeof(std::vector<index_type>);
    for(auto const& v : m_power_rmq) {
      mem += v.capacity() * sizeof(index_type);
    }
    return mem;
  }

 private:
  key_type const* m_data = nullptr;
  std::vector<std::vector<index_type>> m_power_rmq;

};  // class rmq_nlgn

template <typename t_key_type, typename index_type = uint32_t,
          uint64_t t_block_size = 64>
class rmq_n {
 public:
  using key_type = t_key_type;
  rmq_n() {
  }

  rmq_n(key_type const* data, size_t size) : m_data(data), m_size(size) {
    const uint64_t num_sampled_elements = (m_size - 1) / t_block_size + 1;
    m_sampled_indexes.resize(num_sampled_elements);
    m_sampled_minimas.resize(num_sampled_elements);

// Get the minimal elements from the blocks.
#pragma omp parallel for
    for (size_t block = 0; block < num_sampled_elements; ++block) {
      index_type min_index = block * t_block_size;
      index_type end = std::min(((1 + block) * t_block_size), m_size);
      for (size_t i = min_index; i < end; ++i) {
        min_index = data[min_index] <= data[i] ? min_index : i;
      }
      m_sampled_indexes[block] = min_index;
      m_sampled_minimas[block] = m_data[min_index];
    }

    // Build an RMQ data structure for these block minimas.
    m_sampled_rmq = rmq_nlgn<key_type>(m_sampled_minimas);
  }

  template <typename C>
  rmq_n(C const& container) : rmq_n(container.data(), container.size()) {
  }

  // Return the index of the smallest element in m_data[left]..m_data[right]
  // for left = std::min(i, j) and right = std::max(i, j).
  size_t rmq(size_t const i, size_t const j) const {
    size_t const left = std::min(i, j);
    size_t const right = std::max(i, j);
    return rmq_lr(left, right);
  }

  // Return the index of the smallest element in m_data[left]..m_data[right].
  // Here left must be no more than right.
  size_t rmq_lr(size_t const left, size_t const right) const {
    assert(left <= right);
    if (right - left <= 3 * t_block_size) {
      size_t min = left;
      for (size_t i{left + 1}; i <= right; ++i) {
        min = m_data[min] <= m_data[i] ? min : i;
      }
      return min;
    }
    // Min in left block
    size_t const check_left_until = (1 + left / t_block_size) * t_block_size;
    assert(check_left_until < m_size);  // Because we scanned 3*t_block_size
    size_t min_beg = left;
    for (size_t i{left + 1}; i < check_left_until; ++i) {
      min_beg = m_data[min_beg] <= m_data[i] ? min_beg : i;
    }

    // Min in right block
    size_t const check_right_from = (right / t_block_size) * t_block_size;
    size_t min_end = check_right_from;
    for (size_t i{check_right_from + 1}; i <= right; ++i) {
      min_end = m_data[min_end] <= m_data[i] ? min_end : i;
    }

    // Now look for min in middle part.
    size_t const l_block = (left / t_block_size) + 1;
    size_t const r_block = (right / t_block_size) - 1;
    assert(l_block < r_block);  // Because we scanned 3*t_block_size before

    size_t const min_mid =
        m_sampled_indexes[m_sampled_rmq.rmq_lr(l_block, r_block)];
    
    size_t min = min_beg;
    min = m_data[min] <= m_data[min_mid] ? min : min_mid;
    min = m_data[min] <= m_data[min_end] ? min : min_end;
    return min; 
  }

  // Return the index of the smallest element in m_data[left+1]..m_data[right]
  // for left = std::min(i, j) and right = std::max(i, j). Useful for the
  // LCP array.
  size_t rmq_shifted(size_t const i, size_t const j) const {
    assert(i != j);
    size_t const left = std::min(i, j) + 1;
    size_t const right = std::max(i, j);
    return rmq_lr(left, right);
  }

  size_t memory_size() const {
    size_t mem = 0;
    mem += m_sampled_indexes.capacity() * sizeof(index_type);
    mem += m_sampled_minimas.capacity() * sizeof(key_type);
    mem += m_sampled_rmq.memory_size();
    return mem;
  }

 private:
  key_type const* m_data = nullptr;
  size_t m_size;

  std::vector<index_type> m_sampled_indexes;
  std::vector<key_type> m_sampled_minimas;
  rmq_nlgn<key_type, index_type> m_sampled_rmq;
};

}
