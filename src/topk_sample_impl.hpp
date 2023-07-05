#include <pm/result.hpp>

#include <limits>

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
    ((uint64_t)'S') << 16 |
    ((uint64_t)'M') << 8 |
    ((uint64_t)'P');

constexpr bool DEBUG = false;

using TopK = TopKStrings<true>;

constexpr size_t rolling_fp_base = (1ULL << 16) - 39;

using Index = uint32_t;
constexpr size_t BYTE_BITS = 8;
constexpr size_t CHAR_BITS = 8;
constexpr size_t REF_BITS = std::numeric_limits<Index>::digits;

constexpr char SIGNAL = '$';

struct Buffers {
    size_t max_len;
    size_t num_lens;
    size_t len_exp_min_;

    tlx::RingBuffer<char> buffer;
    std::unique_ptr<std::unique_ptr<TopK>[]> topk;
    std::unique_ptr<RollingKarpRabin[]> hash;
    std::unique_ptr<uint64_t[]> fp;

    Buffers(size_t const len_exp_min, size_t const len_exp_max, size_t const k, size_t const sketch_rows, size_t const sketch_columns)
        : num_lens(len_exp_max - len_exp_min + 1),
          max_len(1ULL << len_exp_max),
          len_exp_min_(len_exp_min),
          buffer(max_len),
          topk(std::make_unique<std::unique_ptr<TopK>[]>(num_lens)),
          hash(std::make_unique<RollingKarpRabin[]>(num_lens)),
          fp(std::make_unique<uint64_t[]>(num_lens)) {

        // init top-k
        {
            size_t num = k >> 1;
            size_t cols = sketch_columns >> 1;

            for(size_t i = 0; i < num_lens; i++) {
                if(num == 0 || cols == 0) {
                    std::cerr << "please choose a greater k and/or number of sketch columns" << std::endl;
                    return;
                }

                topk[i] = std::make_unique<TopK>(num, sketch_rows, cols);

                num >>= 1;
                cols >>= 1;
            }
        }

        // init hashing
        for(size_t i = 0; i < num_lens; i++) {
            hash[i] = RollingKarpRabin(get_len(i), rolling_fp_base);
            fp[i] = 0;
        }
    }

    size_t get_len(size_t const i) const {
        return 1ULL << (i + len_exp_min_);
    }

    char push(char const c) {
        auto const buffer_full = (buffer.size() == buffer.max_size());
        auto const drop = buffer_full ? buffer[0] : char(0);

        if(buffer_full) buffer.pop_front();
        buffer.push_back(c);

        return drop;
    }

    void roll_hash(size_t const i, char const c, char const drop) {
        auto const len = get_len(i);
        if(len == max_len) {
            fp[i] = hash[i].roll(fp[i], drop, c);
        } else {
            assert(len < max_len);
            char const x = buffer.size() > len ? buffer[buffer.size() - len - 1] : 0;
            fp[i] = hash[i].roll(fp[i], x, c);
        }
    }
};

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_sample(In begin, In const& end, Out out, size_t const sample_exp, size_t const len_exp_min, size_t const len_exp_max, size_t const k, size_t const sketch_rows, size_t const sketch_columns, pm::Result& result) {
    assert(len_exp_max >= len_exp_min);
    assert(len_exp_max <= 31);

    auto const sample_mask = (1ULL << sample_exp) - 1;

    size_t num_refs = 0;
    size_t num_literals = 0;
    size_t longest = 0;
    size_t total_len = 0;

    // initialize encoding
    out.write(MAGIC, 64);
    out.write(sample_exp, BYTE_BITS);
    out.write(len_exp_min, BYTE_BITS);
    out.write(len_exp_max, BYTE_BITS);
    out.write(k, REF_BITS);
    out.write(sketch_rows, BYTE_BITS);
    out.write(sketch_columns, REF_BITS);

    // init buffers
    Buffers b(len_exp_min, len_exp_max, k, sketch_rows, sketch_columns);

    // process
    size_t pos = 0; // the current position in the input
    size_t next = 0; // the next position that must be encoded

    while(begin != end) {
        // read and push next character
        auto const c = *begin++;
        auto const drop = b.push(c);

        // try and find a reference
        for(size_t i_ = b.num_lens; i_ > 0; i_--) {
            auto const i = i_ - 1;
            size_t const len = b.get_len(i);
            if(pos >= len) {
                // we have read enough characters to do meaningful things

                // lookup
                TopK::Index slot;
                if(pos >= next && b.topk[i]->find(b.fp[i], len, slot)) {
                    // we found it
                    if constexpr(DEBUG) std::cout << "pos=" << pos << ": found [" << (pos - len) << " .. " << pos - 1 << "] = 0x" << std::hex << b.fp[i] << " / " << std::dec << len << " at slot #" << slot << std::endl;
                    next += len;
                    ++num_refs;

                    out.write(SIGNAL, CHAR_BITS);
                    out.write(slot + 1, REF_BITS);
                    out.write(i, BYTE_BITS);

                    longest = std::max(longest, len);
                    total_len += len;

                    // increase its frequency
                    b.topk[i]->insert(b.fp[i], len);
                }

                if((b.fp[i] & sample_mask) == 0) {
                    // this is a sampling position, sample the current state
                    // which represents the string s[pos-len .. pos-1]
                    // if constexpr(DEBUG) std::cout << "pos=" << pos << ": sample [" << (pos - len) << " .. " << pos - 1 << "] = 0x" << std::hex << fp[i] << " / " << std::dec << len << std::endl;
                    b.topk[i]->insert(b.fp[i], len);
                }
            }

            // update the rolling hash
            b.roll_hash(i, c, drop);
        }

        // potentially encode literal
        if(pos >= next) {
            out.write(c, CHAR_BITS);
            if(c == SIGNAL) out.write(0, REF_BITS); // nb: make signal characters decodable
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
    uint64_t const magic = in.read(64);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }

    // init decoding
    size_t const sample_exp = in.read(BYTE_BITS);
    size_t const len_exp_min = in.read(BYTE_BITS);
    size_t const len_exp_max = in.read(BYTE_BITS);
    size_t const k = in.read(REF_BITS);
    size_t const sketch_rows = in.read(BYTE_BITS);
    size_t const sketch_columns = in.read(REF_BITS);

    // init buffers
    Buffers b(len_exp_min, len_exp_max, k, sketch_rows, sketch_columns);

    // other than the encoder, we also need to keep reference positions
    std::unique_ptr<Index[]> ref[b.num_lens];
    {
        auto num_slots = k >> 1;
        for(size_t i = 0; i < b.num_lens; i++) {
            ref[i] = std::make_unique<Index[]>(num_slots);
            num_slots >>= 1;
        }
    }

    // decode
    std::string s;
    auto emit = [&](char const c){
        // TODO: update top-k, ref and fingerprints
        s.push_back(c);
    };
    
    while(in) {
        char const c = in.read(CHAR_BITS);
        if(c == SIGNAL) {
            auto const slot = in.read(REF_BITS);
            if(slot > 0) {
                // we decoded an actual reference, copy characters
                auto const i = in.read(BYTE_BITS);
                auto const len = b.get_len(i);

                auto const src = ref[i][slot];
                for(size_t j = 0; j < len; j++) {
                    emit(s[src + j]);
                }
            } else {
                // we decoded a signal character
                emit(SIGNAL);
            }
        } else {
            // we decoded a simple literal
            emit(c);
        }
    }

    // emit
    for(char const c : s) {
        *out++ = c;
    }
}
