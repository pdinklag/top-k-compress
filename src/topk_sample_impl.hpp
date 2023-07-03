#include <pm/result.hpp>

#include <display.hpp>
#include <phrase_block_reader.hpp>
#include <phrase_block_writer.hpp>
#include <topk_header.hpp>
#include <topk_strings.hpp>
#include <rolling_karp_rabin.hpp>

#include <tlx/container/ring_buffer.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'_') << 24 |
    ((uint64_t)'S') << 16 |
    ((uint64_t)'M') << 8 |
    ((uint64_t)'P');

constexpr bool DEBUG = false;

constexpr size_t rolling_fp_base = (1ULL << 16) - 39;

constexpr size_t CHAR_BITS = 8;
constexpr size_t REF_BITS = 32;

template<iopp::BitSink Out>
void write_signal(Out out) {
    static std::string signal("$!REF!$");
    for(size_t i = 0; i < signal.length(); i++) {
        out.write(signal[i], CHAR_BITS);
    }
}

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_sample(In begin, In const& end, Out out, size_t const sample_exp, size_t const len_exp_min, size_t const len_exp_max, size_t const k, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size, pm::Result& result) {
    assert(len_exp_max >= len_exp_min);
    assert(len_exp_max <= 31);

    auto const sample_mask = (1ULL << sample_exp) - 1;

    size_t num_refs = 0;
    size_t num_literals = 0;
    size_t longest = 0;
    size_t total_len = 0;

    // initialize encoding
    TopkHeader header(k, len_exp_min, len_exp_max, sketch_rows, sketch_columns);
    header.encode(out, MAGIC);

    // init buffers
    size_t const max_len = 1ULL << len_exp_max;
    tlx::RingBuffer<char> buffer(max_len);

    auto const num_lens = len_exp_max - len_exp_min + 1;
    auto get_len = [&](size_t const i){ return 1ULL << (i + len_exp_min); };

    RollingKarpRabin hash[num_lens];
    uint64_t fp[num_lens];
    for(size_t i = 0; i < num_lens; i++) {
        hash[i] = RollingKarpRabin(get_len(i), rolling_fp_base);
        fp[i] = 0;
    }
    
    TopKStrings topk(k, sketch_rows, sketch_columns);

    // process
    size_t pos = 0; // the current position in the input
    size_t next = 0; // the next position that must be encoded

    while(begin != end) {
        // read next character
        auto const c = *begin++;

        // determine the character to drop from the buffer, if any
        auto const buffer_full = (buffer.size() == buffer.max_size());
        auto const drop = buffer_full ? buffer[0] : char(0);
        
        // update buffer
        if(buffer_full) buffer.pop_front();
        buffer.push_back(c);

        for(size_t i_ = num_lens; i_ > 0; i_--) {
            auto const i = i_ - 1;
            size_t const len = get_len(i);
            if(pos >= len) {
                // we have read enough characters to do meaningful things

                // lookup
                TopKStrings::Index slot;
                if(pos >= next && topk.find(fp[i], len, slot)) {
                    // we found it
                    if constexpr(DEBUG) std::cout << "pos=" << pos << ": found [" << (pos - len) << " .. " << pos - 1 << "] = 0x" << std::hex << fp[i] << " / " << std::dec << len << " at slot #" << slot << std::endl;
                    next += len;
                    ++num_refs;

                    write_signal(out);
                    out.write(slot, REF_BITS);

                    longest = std::max(longest, len);
                    total_len += len;

                    // increase its frequency
                    topk.insert(fp[i], len);
                }

                if((pos & sample_mask) == 0) {
                    // this is a sampling position, sample the current state
                    // which represents the string s[pos-len .. pos-1]
                    // if constexpr(DEBUG) std::cout << "pos=" << pos << ": sample [" << (pos - len) << " .. " << pos - 1 << "] = 0x" << std::hex << fp[i] << " / " << std::dec << len << std::endl;
                    topk.insert(fp[i], len);
                }
            }

            // update the rolling hash
            if(len == max_len) {
                fp[i] = hash[i].roll(fp[i], drop, c);
            } else {
                assert(len < max_len);
                if(buffer.size() > len) {
                    fp[i] = hash[i].roll(fp[i], buffer[buffer.size() - len - 1], c);
                } else {
                    fp[i] = hash[i].roll(fp[i], 0, c);
                }
            }
        }

        // potentially encode literal
        if(pos >= next) {
            out.write(c, CHAR_BITS);
            ++num_literals;
            ++next;
        }

        // advance
        ++pos;
    }

    // stats
    auto const num_phrases = num_refs + num_literals;
    result.add("phrases_total", num_phrases);
    result.add("phrases_ref", num_refs);
    result.add("phrases_literal", num_literals);
    result.add("phrases_longest", longest);
    result.add("phrases_avg_len", std::round(100.0 * ((double)total_len / (double)num_phrases)) / 100.0);
    result.add("phrases_avg_ref_len", std::round(100.0 * ((double)total_len / (double)num_refs)) / 100.0);
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void topk_decompress_sample(In in, Out out) {
    std::abort();
}
