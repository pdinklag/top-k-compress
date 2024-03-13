#include <display.hpp>

#include <block_coding.hpp>
#include <pm/result.hpp>

#include <k_attractor.hpp>
#include <tlx/container/ring_buffer.hpp>

#include <valgrind.hpp>

namespace topk_attract {

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'M') << 24 |
    ((uint64_t)'T') << 16 |
    ((uint64_t)'C') << 8 |
    ((uint64_t)'H');

using Index = uint32_t;

template<iopp::InputIterator<char> In, iopp::BitSink Out>
void compress(In begin, In const& end, Out out, size_t const k, size_t const max_freq, size_t const block_size, pm::Result& result) {
    // tlx::RingBuffer<char> window(k);
    auto window = std::make_unique<char[]>(2 * k);
    size_t window_offs = 0;
    size_t window_size = 0;

    // init
    size_t num_phrases = 0;
    size_t longest = 0;
    size_t total_len = 0;

    using Topk = KAttractor<Index>;
    Topk topk(k, max_freq);
    Topk::MatchResult r(k);

    // read initial window
    while(begin != end && window_size < 2 * k) {
        window[window_size++] = *begin++;
    }

    // process window
    while(window_offs < window_size) {
        // match window against k-attractor
        topk.match(window.get() + window_offs, window_size - window_offs, r);
        // std::cout << "matched " << r.len << " characters at attractor pos " << r.pos << " (mismatch: " << display(r.mismatch) << ")" << std::endl;
        
        ++num_phrases;

        size_t const len = r.len + 1;
        total_len += len;
        longest = std::max(longest, len);

        // update k-attractor
        topk.update(r);

        // advance
        window_offs += len;

        if(window_offs >= k) {
            // slide window
            window_offs -= k;
            for(size_t i = 0; i < k; i++) {
                window[i] = window[i + k];
            }
            window_size -= k;
        }

        while(begin != end && window_size < 2 * k) {
            window[window_size++] = *begin++;
        }
    }

    // stats
    topk.print_debug_info();
    result.add("phrases_total", num_phrases);
    result.add("phrases_longest", longest);
    result.add("phrases_avg_len", std::round(100.0 * ((double)total_len / (double)num_phrases)) / 100.0);
}

}
