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
constexpr bool PROTOCOL = true;

using TopK = TopKStrings<true>;

constexpr size_t rolling_fp_base = (1ULL << 16) - 39;

using Index = uint32_t;
constexpr size_t BYTE_BITS = 8;
constexpr size_t CHAR_BITS = 8;
constexpr size_t REF_BITS = std::numeric_limits<Index>::digits;

constexpr char SIGNAL = '$';

template<bool use_sss>
struct Buffers {
    size_t sample;
    size_t max_len;
    size_t num_lens;
    size_t len_exp_min;

    tlx::RingBuffer<char> buffer;
    size_t total;
    std::unique_ptr<std::unique_ptr<TopK>[]> topk;
    std::unique_ptr<RollingKarpRabin[]> hash;
    std::unique_ptr<uint64_t[]> fp;

    // nb: we use an alternative definition of the SSS that looks for minimizers of the tau most recent fingerprints,
    // (rather than looking forward as in the original definition)
    struct Synchronizer {
        size_t sample;
        tlx::RingBuffer<uint64_t> recent;
        uint64_t min;
        size_t total;

        Synchronizer() {
        }

        Synchronizer(size_t const sample) : sample(sample), recent(sample), min(UINT64_MAX), total(0) {
        }

        void push(uint64_t const fp) {
            if(total >= sample) {
                auto const pop = recent.front();
                recent.pop_front();

                if(pop == min) {
                    // scan for new minimum
                    assert(recent.size() == sample - 1);

                    min = UINT64_MAX;
                    for(size_t i = 0; i < sample - 1; i++) {
                        min = std::min(min, recent[i]);
                    }
                }
            }

            recent.push_back(fp);
            ++total;
            min = std::min(min, fp);
        }
    };

    std::unique_ptr<Synchronizer[]> sync;

    Buffers(size_t const sample_exp, size_t const len_exp_min, size_t const len_exp_max, size_t const k, size_t const sketch_rows, size_t const sketch_columns)
        : sample(1ULL << sample_exp),
          num_lens(len_exp_max - len_exp_min + 1),
          max_len(1ULL << len_exp_max),
          len_exp_min(len_exp_min),
          buffer(max_len),
          total(0),
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

        // init fingerprint history for sss
        if constexpr(use_sss) {
            sync = std::make_unique<Synchronizer[]>(num_lens);
            for(size_t i = 0; i < num_lens; i++) {
                sync[i] = Synchronizer(sample);
            }
        }
    }

    size_t get_len(size_t const i) const {
        return 1ULL << (i + len_exp_min);
    }

    char push(char const c) {
        auto const buffer_full = (total >= max_len);
        auto const drop = buffer_full ? buffer[0] : char(0);

        if(buffer_full) buffer.pop_front();
        buffer.push_back(c);
        ++total;

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

        // update SSS
        if constexpr(use_sss) {
            sync[i].push(fp[i]);
        }
    }

    bool is_sampling_pos(size_t const i) const {
        if constexpr(use_sss) {
            return fp[i] == sync[i].min;
        } else {
            return (fp[i] & (sample - 1)) == 0;
        }
    }
};

template<bool use_sss, tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_sample(In begin, In const& end, Out out, size_t const sample_exp, size_t const len_exp_min, size_t const len_exp_max, size_t const k, size_t const sketch_rows, size_t const sketch_columns, pm::Result& result) {
    assert(len_exp_max >= len_exp_min);
    assert(len_exp_max <= 31);
    assert(sample_exp < 64);

    auto const sample = (1ULL << sample_exp);
    auto const sample_mask = sample - 1;

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
    Buffers<use_sss> b(sample, len_exp_min, len_exp_max, k, sketch_rows, sketch_columns);

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
                    if constexpr(PROTOCOL) std::cout << "pos=" << pos << ": (#" << slot << ", " << len << ")" << std::endl;

                    longest = std::max(longest, len);
                    total_len += len;
                }

                if(b.is_sampling_pos(i)) {
                    // this is a sampling position, sample the current state
                    // which represents the string s[pos-len .. pos-1]
                    if constexpr(DEBUG) std::cout << "pos=" << pos << ": sample [" << (pos - len) << " .. " << pos - 1 << "] = 0x" << std::hex << b.fp[i] << " / " << std::dec << len;
                    if(b.topk[i]->insert(b.fp[i], len, slot)) {
                        if constexpr(DEBUG) std::cout << " -> slot #" << slot;
                    } else {
                        if constexpr(DEBUG) std::cout << " -> not frequent";
                    }
                    if constexpr(DEBUG) std::cout << std::endl;
                }
            }

            // update the rolling hash
            b.roll_hash(i, c, drop);
        }

        // potentially encode literal
        if(pos >= next) {
            if constexpr(PROTOCOL) std::cout << "pos=" << pos << ": " << display(c) << std::endl;
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

template<bool use_sss, iopp::BitSource In, std::output_iterator<char> Out>
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
    Buffers<use_sss> b(1ULL << sample_exp, len_exp_min, len_exp_max, k, sketch_rows, sketch_columns);

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
    size_t pos = 0;
    auto emit = [&](char const c){
        s.push_back(c);

        auto const drop = b.push(c);
        for(size_t i_ = b.num_lens; i_ > 0; i_--) {
            auto const i = i_ - 1;
            size_t const len = b.get_len(i);
            if(pos >= len && b.is_sampling_pos(i)) {
                if constexpr(DEBUG) std::cout << "pos=" << pos << ": sample [" << (pos - len) << " .. " << pos - 1 << "] = 0x" << std::hex << b.fp[i] << " / " << std::dec << len;
                Index slot;
                if(b.topk[i]->insert(b.fp[i], len, slot)) {
                    ref[i][slot] = pos - len;
                    if constexpr(DEBUG) std::cout << " -> slot #" << slot;
                } else {
                    if constexpr(DEBUG) std::cout << " -> not frequent";
                }
                if constexpr(DEBUG) std::cout << std::endl;
            }
            b.roll_hash(i, c, drop);
        }

        ++pos;
    };
    
    while(in) {
        char const c = in.read(CHAR_BITS);
        if(c == SIGNAL) {
            auto const x = in.read(REF_BITS);
            if(x > 0) {
                // we decoded an actual reference, copy characters
                auto const slot = x - 1;
                auto const i = in.read(BYTE_BITS);
                auto const len = b.get_len(i);
                if constexpr(PROTOCOL) std::cout << "pos=" << pos << ": (#" << slot << ", " << len << ")" << std::endl;

                auto const src = ref[i][slot];
                if constexpr(DEBUG) std::cout << "pos=" << pos << ": decode [" << src << " .. " << src + len - 1 << "] = 0x" << std::hex << b.fp[i] << " / " << std::dec << len << " from slot #" << slot << std::endl;
                for(size_t j = 0; j < len; j++) {
                    emit(s[src + j]);
                }
            } else {
                // we decoded a signal character
                emit(SIGNAL);
                if constexpr(PROTOCOL) std::cout << "pos=" << pos << ": " << display(SIGNAL) << std::endl;
            }
        } else {
            // we decoded a simple literal
            emit(c);
            if constexpr(PROTOCOL) std::cout << "pos=" << pos << ": " << display(c) << std::endl;
        }
    }

    // emit
    for(char const c : s) {
        *out++ = c;
    }
}
