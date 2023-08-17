#pragma once

#include <cassert>
#include <memory>

#include <code.hpp>
#include <iopp/concepts.hpp>
#include <rans_byte.h>

namespace rans_internal {
    constexpr size_t MAX_NUM_SYMBOLS = 256;
    constexpr size_t BYTE_BITS = 8;

    inline static void compute_cumulative_freqs(uint32_t const* freqs, uint32_t* cum_freqs) {
        cum_freqs[0] = 0;
        for(size_t i = 0; i < MAX_NUM_SYMBOLS; i++) {
            cum_freqs[i+1] = cum_freqs[i] + freqs[i];
        }
    }

    // from ryg_rans
    inline static void normalize_freqs(uint32_t* freqs, uint32_t* cum_freqs, uint32_t const target_total) {
        uint32_t cur_total = cum_freqs[MAX_NUM_SYMBOLS];
        assert(cur_total > 0);
        
        // resample distribution based on cumulative freqs
        for(size_t i = 1; i <= MAX_NUM_SYMBOLS; i++) {
            cum_freqs[i] = ((uint64_t)target_total * cum_freqs[i]) / cur_total;
        }

        // if we nuked any non-0 frequency symbol to 0, we need to steal
        // the range to make the frequency nonzero from elsewhere.
        //
        // this is not at all optimal, i'm just doing the first thing that comes to mind.
        for(size_t i=0; i < MAX_NUM_SYMBOLS; i++) {
            if (freqs[i] && cum_freqs[i+1] == cum_freqs[i]) {
                // symbol i was set to zero freq

                // find best symbol to steal frequency from (try to steal from low-freq ones)
                uint32_t best_freq = ~0u;
                int best_steal = -1;
                for (size_t j = 0; j < MAX_NUM_SYMBOLS; j++) {
                    uint32_t freq = cum_freqs[j+1] - cum_freqs[j];
                    if (freq > 1 && freq < best_freq) {
                        best_freq = freq;
                        best_steal = j;
                    }
                }
                assert(best_steal != -1);

                // and steal from it!
                if (best_steal < i) {
                    for (size_t j = best_steal + 1; j <= i; j++) {
                        cum_freqs[j]--;
                    }
                } else {
                    assert(best_steal > i);
                    for (size_t j = i + 1; j <= best_steal; j++) {
                        cum_freqs[j]++;
                    }
                }
            }
        }

        // calculate updated freqs and make sure we didn't screw anything up
        assert(cum_freqs[0] == 0 && cum_freqs[MAX_NUM_SYMBOLS] == target_total);
        for (int i=0; i < MAX_NUM_SYMBOLS; i++) {
            if (freqs[i] == 0) {
                assert(cum_freqs[i+1] == cum_freqs[i]);
            } else {
                assert(cum_freqs[i+1] > cum_freqs[i]);
            }

            // calc updated freq
            freqs[i] = cum_freqs[i+1] - cum_freqs[i];
        }
    }
}

template<iopp::BitSink Sink>
void rans_encode(Sink& sink, uint8_t const* data, size_t const n, uint32_t const prob_bits = 14) {
    assert(prob_bits >= 8);
    static constexpr auto MAX_NUM_SYMBOLS = rans_internal::MAX_NUM_SYMBOLS;

    // initialize frequencies
    uint32_t freqs[MAX_NUM_SYMBOLS];
    for(size_t x = 0; x < MAX_NUM_SYMBOLS; x++) {
        freqs[x] = 0;
    }

    // count frequencies
    for(size_t i = 0; i < n; i++) {
        ++freqs[data[i]];
    }

    // compute cumulative frequencies
    uint32_t cum_freqs[MAX_NUM_SYMBOLS + 1];
    rans_internal::compute_cumulative_freqs(freqs, cum_freqs);

    // normalize
    auto const prob_scale = 1U << prob_bits;
    rans_internal::normalize_freqs(freqs, cum_freqs, prob_scale);

    // encode
    {
        // we need prob_bits to encode a frequency
        // to encode the corresponding symbols, we encode them as deltas from the previous symbol
        uintmax_t prev = 0;
        for(uintmax_t x = 0; x < MAX_NUM_SYMBOLS; x++) {
            if(freqs[x] != 0) {
                code::EliasDelta::encode(sink, x - prev + 1);
                code::Binary::encode(sink, freqs[x] - 1, prob_bits);
                prev = x;
            }
        }

        // "end of file"
        code::EliasDelta::encode(sink, MAX_NUM_SYMBOLS - prev + 1);
    }

    // initialize rANS symbols
    RansEncSymbol esyms[MAX_NUM_SYMBOLS];
    for(size_t x = 0; x < MAX_NUM_SYMBOLS; x++) {
        RansEncSymbolInit(&esyms[x], cum_freqs[x], freqs[x], prob_bits);
    }

    // initialize state
    RansState state;
    RansEncInit(&state);

    // encode data
    auto buffer = std::make_unique<uint8_t[]>(n);
    uint8_t* const end = buffer.get() + n;
    uint8_t* p = end;

    uint8_t const* s = data + n - 1;
    for(size_t i = 0; i < n; i++) {
        auto const x = *s--;
        RansEncPutSymbol(&state, &p, &esyms[x]);
    }
    RansEncFlush(&state, &p);

    // emit
    auto const num_enc_bytes = end - p;
    assert(num_enc_bytes < n);
    code::Binary::encode(sink, num_enc_bytes, code::Universe(n));
    while(p < end) {
        code::Binary::encode(sink, *p++, rans_internal::BYTE_BITS);
    }
}

template<iopp::BitSource Src, std::output_iterator<uint8_t> Out>
void rans_decode(Src& src, size_t const n, Out out, uint32_t const prob_bits = 14) {
    assert(prob_bits >= 8);
    static constexpr auto MAX_NUM_SYMBOLS = rans_internal::MAX_NUM_SYMBOLS;

    // initialize frequencies
    uint32_t freqs[MAX_NUM_SYMBOLS];
    for(size_t x = 0; x < MAX_NUM_SYMBOLS; x++) {
        freqs[x] = 0;
    }

    // decode frequencies
    auto x = code::EliasDelta::decode(src) - 1;
    while(x < MAX_NUM_SYMBOLS) {
        freqs[x] = code::Binary::decode(src, prob_bits) + 1;
        x += code::EliasDelta::decode(src) - 1;
    }

    // compute cumulative frequencies
    uint32_t cum_freqs[MAX_NUM_SYMBOLS + 1];
    rans_internal::compute_cumulative_freqs(freqs, cum_freqs);

    // already normalized
    auto const prob_scale = 1U << prob_bits;
    assert(cum_freqs[MAX_NUM_SYMBOLS] == prob_scale);

    // brute-force (but fast) cumulative to symbol table
    auto cum2sym = std::make_unique<uint8_t[]>(prob_scale);
    for(size_t x = 0; x < MAX_NUM_SYMBOLS; x++) {
        for(size_t i = cum_freqs[x]; i < cum_freqs[x+1]; i++) {
            cum2sym[i] = x;
        }
    }

    // initialize rANS symbols
    RansDecSymbol dsyms[MAX_NUM_SYMBOLS];
    for(size_t x = 0; x < MAX_NUM_SYMBOLS; x++) {
        RansDecSymbolInit(&dsyms[x], cum_freqs[x], freqs[x]);
    }

    // initialize buffer
    auto const num_dec_bytes = code::Binary::decode(src, code::Universe(n));
    assert(num_dec_bytes < n);
    auto buffer = std::make_unique<uint8_t[]>(num_dec_bytes);
    for(size_t i = 0; i < num_dec_bytes; i++) {
        buffer[i] = code::Binary::decode(src, rans_internal::BYTE_BITS);
    }

    // initialize state
    uint8_t* p = buffer.get();

    RansState state;
    RansDecInit(&state, &p);

    // decode
    for(size_t i = 0; i < n; i++) {
        auto const x = cum2sym[RansDecGet(&state, prob_bits)];
        *out++ = (uint8_t)x;
        RansDecAdvanceSymbol(&state, &p, &dsyms[x], prob_bits);
    }
}

