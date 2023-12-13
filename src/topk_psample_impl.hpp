#include <omp.h>

#include <cassert>
#include <bit>
#include <queue>
#include <vector>

#include <display.hpp>
#include <rolling_karp_rabin.hpp>
#include <write_bytes.hpp>

#include <pm.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'P') << 24 |
    ((uint64_t)'S') << 16 |
    ((uint64_t)'M') << 8 |
    ((uint64_t)'P');

constexpr bool DEBUG = false;
constexpr bool PROTOCOL = false;

constexpr size_t rolling_fp_base = (1ULL << 16) - 39;

using Index = uint32_t;
constexpr size_t REF_BYTES = std::numeric_limits<Index>::digits >> 3;

constexpr char SIGNAL = '$';

struct Ref {
    size_t pos;
    size_t src;
} __attribute__((packed));

size_t get_len(size_t const l, size_t const len_exp_min) {
    return 1ULL << (l + len_exp_min);
}

bool should_sample(uint64_t const fp, uint64_t const s) {
    assert(std::has_single_bit(s));
    return (fp & (s-1)) == 0;
}

template<typename TopK, iopp::InputIterator<char> In, std::output_iterator<char> Out>
void topk_compress_psample(In begin, In const& end, Out out, size_t const window, size_t const sample_rsh, size_t const len_exp_min, size_t const len_exp_max, size_t const min_dist, size_t const k, size_t const sketch_rows, size_t const sketch_columns, pm::Result& result) {
    assert(len_exp_max >= len_exp_min);
    assert(len_exp_max <= 31);

    // stats
    size_t num_refs = 0;
    size_t num_literals = 0;
    size_t longest = 0;
    size_t total_len = 0;
    size_t furthest = 0;
    size_t total_dist = 0;
    size_t t_process = 0;
    size_t t_encode = 0;

    // initialize encoding
    write_uint(out, MAGIC, 8);
    write_uint(out, len_exp_min, 1);

    // init buffers
    auto const num_lens = len_exp_max - len_exp_min + 1;
    ssize_t const m = 1ULL << len_exp_max;
    assert(m <= window);

    auto block = std::make_unique<uint8_t[]>(window);
    auto memory = std::make_unique<uint8_t[]>(m); // stores the max_len final bytes from the previous block
    for(size_t i = 0; i < m; i++) {
        block[window - m + i] = 0; // seed with zero
    }

    auto topk = std::make_unique<std::unique_ptr<TopK>[]>(num_lens);
    auto src = std::make_unique<std::unique_ptr<size_t[]>[]>(num_lens);
    auto hash = std::make_unique<RollingKarpRabin[]>(num_lens);
    auto fp = std::make_unique<uint64_t[]>(num_lens);
    auto refs = std::make_unique<std::vector<Ref>[]>(num_lens);
    auto next = std::make_unique<size_t[]>(num_lens); // the next position at which we can encode a reference
    auto num_sampled = std::make_unique<size_t[]>(num_lens); // the next position at which we can encode a reference

    {
        size_t num = k >> 1;
        size_t cols = sketch_columns >> 1;

        for(size_t l = 0; l < num_lens; l++) {
            topk[l] = std::make_unique<TopK>(num, sketch_rows, cols);
            src[l] = std::make_unique<size_t[]>(num);
            hash[l] = RollingKarpRabin(get_len(l, len_exp_min), rolling_fp_base);
            fp[l] = 0;
            next[l] = 0;
            num_sampled[l] = 0;

            num >>= 1;
            cols >>= 1;
        }
    }

    // go
    size_t blocknum = 0;
    while(begin != end) {
        // memorize last m bytes from previous block
        for(size_t i = 0; i < m; i++) {
            memory[i] = block[window - m + i];
        }

        // read next block
        size_t blocksize = 0;
        while(begin != end && blocksize < window) {
            block[blocksize++] = uint8_t(*begin++);
        }

        // process block for each length in parallel
        {
            pm::Stopwatch t;
            t.start();

            if constexpr(DEBUG) {
                std::cout << "processing block " << blocknum << " (" << blocksize << " bytes) ..." << std::endl;
            }

            #pragma omp parallel for
            for(size_t l = 0; l < num_lens; l++) {
                auto const len = get_len(l, len_exp_min);
                for(size_t j = 0; j < blocksize; j++) {
                    // we want to fingerprint the string [i, j] of length len
                    // for this, we need to drop the character at position i - 1 = j - len
                    // the position may be negative -- then we take it from the previous block memory
                    ssize_t const i = ssize_t(j) - len + 1;
                    if(i - 1 < 0) {
                        assert(m + i - 1 >= 0);
                        assert(m + i - 1 < m);
                    }
                    auto const pop = (i - 1 >= 0) ? block[i - 1] : memory[m + i - 1];
                    ssize_t const global_pos = ssize_t(blocknum * window) + i;

                    // update fingerprint
                    fp[l] = hash[l].roll(fp[l], pop, block[j]);

                    // build debug string
                    std::string s;
                    if constexpr(DEBUG) {
                        if(omp_get_num_threads() == 1) {
                            for(ssize_t x = 0; x < ssize_t(len); x++) {
                                s.push_back(i+x >= 0 ? block[i+x] : memory[m + i+x]);
                            }
                        }
                    }

                    // possibly make a reference
                    if(global_pos >= ssize_t(next[l])) {
                        assert(global_pos >= 0);

                        // lookup fingerprint in top-k structure
                        typename TopK::FilterIndex slot;
                        if(topk[l]->find(fp[l], len, slot)) {
                            // found it, make a reference
                            assert(src[l][slot] < size_t(global_pos));
                            refs[l].push_back(Ref{size_t(global_pos), src[l][slot]});
                            next[l] = size_t(global_pos) + len;

                            if constexpr(DEBUG) {
                                if(omp_get_num_threads() == 1) {
                                    std::cout << "\ti=" << global_pos << ": found string \"" << s << "\" (length " << len
                                        << ", fingerprint 0x" << std::hex << fp[l] << std::dec << ") in slot " << slot << ", last seen at position " << src[l][slot] << std::endl;
                                }
                            }
                        }
                    }

                    // possibly enter the fingerprint in the top-k data structure
                    if(global_pos >= 0 && should_sample(fp[l], len >> sample_rsh)) {
                        ++num_sampled[l];

                        typename TopK::FilterIndex slot;
                        if(topk[l]->insert(fp[l], len, slot)) {
                            src[l][slot] = global_pos;
                            if constexpr(DEBUG) {
                                if(omp_get_num_threads() == 1) {
                                    std::cout << "\ti=" << global_pos << ": inserted string \"" << s << "\" (length " << len
                                        << ", fingerprint 0x" << std::hex << fp[l] << std::dec << ") into slot " << slot << std::endl;
                                }
                            }
                        }
                    }
                }
            }
            t.stop();
            t_process += t.elapsed_time_millis();
        }

        // encode block
        {
            pm::Stopwatch t;
            t.start();

            if constexpr(DEBUG) {
                std::cout << "encoding block " << blocknum << " ..." << std::endl;
            }

            size_t cur[num_lens];
            for(size_t l = 0; l < num_lens; l++) cur[l] = 0;

            // TODO: currently, we effectively skip references crossing block boundaries
            // we should really start at j = -max_len, but then we also have to make sure that the previous block is only encoded up to blocksize-max_len
            size_t j = 0;
            size_t const block_offs = blocknum * window;
            while(j < blocksize) {
                // find the position at which to encode a reference next, and of what length
                size_t ref_pos = block_offs + blocksize;
                size_t ref_src;
                size_t ref_l;
                for(size_t l_ = num_lens; l_ > 0; l_--) {
                    auto const l = l_ - 1;

                    // advance to next reference
                    auto& x = cur[l];
                    while(x < refs[l].size() && refs[l][x].pos < block_offs + j) {
                        ++x;
                    }

                    auto const len = get_len(l, len_exp_min);
                    if(x < refs[l].size() && refs[l][x].pos + len <= ref_pos) {
                        ref_pos = refs[l][x].pos;
                        ref_src = refs[l][x].src;
                        ref_l = l;
                    }
                }

                // advance to the next reference, simply emitting all characters on the way
                assert(ref_pos >= block_offs);
                ref_pos -= block_offs;

                while(j < ref_pos) {
                    ++num_literals;
                    auto const c = (char)block[j];
                    if constexpr(PROTOCOL) std::cout << "i=" << (block_offs + j) << ": " << display(c) << std::endl;
                    *out++ = c;
                    if(c == SIGNAL) write_uint(out, 0, REF_BYTES); // nb: make SIGNAL decodable
                    ++j;
                }

                if(j < blocksize) {
                    // encode reference
                    auto const len = get_len(ref_l, len_exp_min);
                    ++num_refs;

                    total_len += len;
                    longest = std::max(longest, len);

                    assert(block_offs + j > ref_src);
                    auto const dist = block_offs + j - ref_src;

                    total_dist += dist;
                    furthest = std::max(dist, furthest);

                    if constexpr(PROTOCOL) std::cout << "i=" << (block_offs + j) << ": (" << ref_src << ", " << len << ")" << std::endl;
                    *out++ = SIGNAL;
                    write_uint(out, dist, REF_BYTES);
                    write_uint(out, ref_l, 1);
                    j += len;
                }
            }
            t.stop();
            t_encode += (size_t)t.elapsed_time_millis();
        }

        // cleanup refs
        for(size_t l = 0; l < num_lens; l++) {
            refs[l].clear();
        }

        // advance to next block
        ++blocknum;
    }

    // stats
    size_t total_sampled = 0;
    for(size_t l = 0; l < num_lens; l++) {
        total_sampled += num_sampled[l];
    }

    auto const num_phrases = num_refs + num_literals;
    result.add("time_process", t_process);
    result.add("time_encode", t_encode);
    result.add("total_sampled", total_sampled);
    result.add("phrases_total", num_phrases);
    result.add("phrases_ref", num_refs);
    result.add("phrases_literal", num_literals);
    result.add("phrases_longest", longest);
    result.add("phrases_furthest", furthest);
    result.add("phrases_avg_ref_len", std::round(100.0 * ((double)total_len / (double)num_refs)) / 100.0);
    result.add("phrases_avg_ref_dist", std::round(100.0 * ((double)total_dist / (double)num_refs)) / 100.0);
}

template<typename TopK, iopp::InputIterator<char> In, std::output_iterator<char> Out>
void topk_decompress_psample(In in, In const end, Out out) {
    uint64_t const magic = read_uint(in, 8);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }

    auto const len_exp_min = read_uint(in, 1);

    std::string s;
    while(in != end) {
        auto const c = *in++;
        if(c == SIGNAL) {
            auto const delta = read_uint(in, REF_BYTES);
            if(delta == 0) {
                // we decoded a signal literal
                if constexpr(PROTOCOL) std::cout << "i=" << s.size() << ": " << display(SIGNAL) << std::endl;
                s.push_back(SIGNAL);
            } else {
                // copy characters
                assert(delta <= s.size());

                auto const l = read_uint(in, 1);
                auto const len = get_len(l, len_exp_min);

                auto src = s.size() - delta;
                if constexpr(PROTOCOL) std::cout << "i=" << s.size() << ": (" << src << ", " << len << ")" << std::endl;

                for(size_t i = 0; i < len; i++) {
                    s.push_back(s[src++]);
                }
            }
        } else {
            if constexpr(PROTOCOL) std::cout << "i=" << s.size() << ": " << display(c) << std::endl;
            s.push_back(c);
        }
    }

    // emit
    for(auto c : s) {
        *out++ = c;
    }
}
