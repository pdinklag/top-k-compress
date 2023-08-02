#include <phrase_block_reader.hpp>
#include <phrase_block_writer.hpp>

#include <pm/result.hpp>

#include <omp.h>

#include <limits>
#include <queue>

#include <display.hpp>
#include <topk_strings.hpp>
#include <rolling_karp_rabin.hpp>

#include <tdc/util/concepts.hpp>
#include <tlx/container/ring_buffer.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'_') << 24 |
    ((uint64_t)'M') << 16 |
    ((uint64_t)'D') << 8 |
    ((uint64_t)'L');

constexpr bool DEBUG = false;
constexpr bool PROTOCOL = false;

using TopK = TopKStrings<true>;
using Index = TopK::Index;

constexpr size_t rolling_fp_base = (1ULL << 16) - 39;

struct ModelBuilder {
    Index k;
    size_t max_len;
    size_t num_lens;
    size_t len_exp_min;

    std::unique_ptr<std::unique_ptr<TopK>[]> topk;
    std::unique_ptr<tlx::RingBuffer<char>[]> buffer;
    std::unique_ptr<RollingKarpRabin[]> hash;
    std::unique_ptr<uint64_t[]> fp;
    std::unique_ptr<std::unique_ptr<size_t[]>[]> ref;

    size_t get_len(size_t const i) const {
        return 1ULL << (i + len_exp_min);
    }

    size_t model_size(size_t const i) const {
        return topk[i]->size();
    }

    ModelBuilder(Index const k, size_t const len_exp_min, size_t const len_exp_max, size_t const sketch_rows, size_t const sketch_columns)
        : k(k),
          len_exp_min(len_exp_min),
          max_len(1ULL << len_exp_max),
          num_lens(len_exp_max - len_exp_min + 1),
          topk(std::make_unique<std::unique_ptr<TopK>[]>(num_lens)),
          buffer(std::make_unique<tlx::RingBuffer<char>[]>(num_lens)),
          hash(std::make_unique<RollingKarpRabin[]>(num_lens)),
          fp(std::make_unique<uint64_t[]>(num_lens)),
          ref(std::make_unique<std::unique_ptr<size_t[]>[]>(num_lens))
    {
        for(size_t i = 0; i < num_lens; i++) {
            size_t const len = get_len(i);
            Index _k, _cols;
            switch(len) {
                case 1:
                    _k = std::min(k, Index(0x1'00));
                    _cols = 0;
                    break;
                case 2:
                    _k = std::min(k, Index(0x1'00'00));
                    _cols = 0;
                    break;
                default:
                    _k = k;
                    _cols = sketch_columns;
                    break;
            }

            topk[i] = std::make_unique<TopK>(_k, sketch_rows, _cols);
            buffer[i] = tlx::RingBuffer<char>(len);
            hash[i] = RollingKarpRabin(len, rolling_fp_base);
            fp[i] = 0;
            ref[i] = std::make_unique<size_t[]>(_k);
        }
    }

    void reset_buffers() {
        for(size_t i = 0; i < num_lens; i++) {
            buffer[i].clear();
            fp[i] = 0;
        }
    }
};

template<tdc::InputIterator<char> In>
size_t underflow(In& in, In const& end, char* buffer, size_t const bufsize) {
    size_t read = 0;
    while(in != end && read < bufsize) {
        buffer[read++] = *in++;
    }
    return read;
}

template<typename RewindFunc, iopp::BitSink Out>
void topk_compress_model(RewindFunc rewind, Out out, size_t const len_exp_min, size_t const len_exp_max, size_t const k, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size, pm::Result& result) {
    // buffers
    ModelBuilder b(k, len_exp_min, len_exp_max, sketch_rows, sketch_columns);

    size_t const rbufsize = k;
    auto rbuf = std::make_unique<char[]>(rbufsize);

    // pass 1 -- build model
    std::cout << "building model ..." << std::endl;
    
    {
        auto [in, end] = rewind();
        
        auto pos = std::make_unique<size_t[]>(b.num_lens);
        for(size_t i = 0; i < b.num_lens; i++) {
            pos[i] = 0;
        }

        while(size_t read = underflow(in, end, rbuf.get(), rbufsize)) {
            std::cout << "\tprocessing next " << read << " bytes ..." << std::endl;

            // process each length in parallel
            #pragma omp parallel for
            for(size_t i = 0; i < b.num_lens; i++) {
                size_t const len = b.get_len(i);

                // process characters
                for(size_t j = 0; j < read; j++) {
                    // pop first from ring buffer if needed
                    char drop;
                    if(pos[i] >= len) {
                        drop = b.buffer[i].front();
                        b.buffer[i].pop_front();
                    } else {
                        drop = char(0);
                    }

                    // push to ring buffer
                    auto const c = rbuf[j];
                    b.buffer[i].push_back(c);

                    // roll hash
                    b.fp[i] = b.hash[i].roll(b.fp[i], drop, c);

                    // potentially register string
                    if(pos[i] + 1 >= len) {
                        Index slot;
                        if(b.topk[i]->insert(b.fp[i], len, slot)) {
                            b.ref[i][slot] = pos[i] + 1 - len;
                        }
                    }

                    // increment
                    ++pos[i];
                }
            }
        }
    }

    // pass 2 -- encode model
    {
        // sort reference positions
        std::cout << "sorting reference positions ..." << std::endl;
        auto sorted_refs = std::make_unique<std::vector<std::pair<size_t, Index>>[]>(b.num_lens);

        #pragma omp parallel for
        for(size_t i = 0; i < b.num_lens; i++) {
            sorted_refs[i].reserve(b.model_size(i));
            for(Index j = 0; j < b.model_size(i); j++) {
                sorted_refs[i].emplace_back(b.ref[i][j], j);
            }
            std::sort(sorted_refs[i].begin(), sorted_refs[i].end(), [](auto a, auto b){ return a.first < b.first; });
        }

        // gather model strings
        std::cout << "gathering model strings ..." << std::endl;
        auto strs = std::make_unique<std::unique_ptr<std::string[]>[]>(b.num_lens);
        for(size_t i = 0; i < b.num_lens; i++) {
            strs[i] = std::make_unique<std::string[]>(b.model_size(i));
        }

        b.reset_buffers();
        auto [in, end] = rewind();

        auto pos = std::make_unique<size_t[]>(b.num_lens);
        auto next = std::make_unique<size_t[]>(b.num_lens);
        for(size_t i = 0; i < b.num_lens; i++) {
            pos[i] = 0;
            next[i] = 0;
        }

        while(size_t read = underflow(in, end, rbuf.get(), rbufsize)) {
            std::cout << "\tprocessing next " << read << " bytes ..." << std::endl;
            #pragma omp parallel for
            for(size_t i = 0; i < b.num_lens; i++) {
                size_t const len = b.get_len(i);
                for(size_t j = 0; j < read; j++) {
                    auto const next_ref = sorted_refs[i][next[i]].first;
                    if(next[i] < b.model_size(i) && pos[i] == next_ref) {
                        auto const next_slot = sorted_refs[i][next[i]].second;
                        if(b.topk[i]->freq(next_slot) > 1) {
                            strs[i][next_slot] = std::string(rbuf.get() + j, len);
                        }
                        ++next[i];
                    }
                    ++pos[i];
                }
            }
        }

        // encode
        out.write(len_exp_min, 8);
        out.write(len_exp_max, 8);
        auto w = PhraseBlockWriter(out, block_size);
        for(size_t i = 0; i < b.num_lens; i++) {
            out.write(b.model_size(i), 32);
            for(size_t j = 0; j < b.model_size(i); j++) {
                for(char c : strs[i][j]) {
                    w.write_literal(c);
                }
            }
        }
        w.flush();
    }

    // pass 3 -- encode stream
    {
        b.reset_buffers();
        auto [in, end] = rewind();
    }
}
