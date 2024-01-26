#include <algorithm>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>

#include <libsais64.h>
#include <lz77/factor.hpp>
#include <sdsl/suffix_arrays.hpp>
#include <pm.hpp>

/**
 * \brief Computes an exact Lempel-Ziv 77 factorization of the input by simulating the longest previous factor (LPF) array
 * 
 * The algorithm first computes the suffix array, its inverse and the LCP array, and then uses it to simulate a scan of the LPF array
 * to compute greedily the Lempel-Ziv 77 parse.
 * 
 * In the case of multiple sources being eligible for a factor, tie breaking is done based on the lexicographic order.
 * In other words, the factorization is neither leftmost nor rightmost.
 */
class LPFSuccinctFactorizer {
public:
    using Factor = lz77::Factor;

private:
    static constexpr size_t MAX_SIZE_32BIT = 1ULL << 31;

    static size_t lce(std::string const& t, size_t const i, size_t const j) {
        auto const n = t.length();

        size_t l = 0;
        while(i + l < n && j + l < n && t[i + l] == t[j + l]) ++l;

        return l;
    }

    size_t min_ref_len_;

public:
    LPFSuccinctFactorizer() : min_ref_len_(2) {
    }

    template<std::output_iterator<Factor> Output>
    void factorize(std::string const& t, Output out) {
        // construct suffix array
        size_t const n = t.length();
        std::cout << "n=" << n << std::endl;
        std::cout << "construct csa ...";
        std::cout.flush();

        pm::MallocCounter mem;
        pm::Stopwatch sw;

        sw.start(); mem.start();
        sdsl::csa_wt<> sa_succinct;
        sdsl::construct_im(sa_succinct, t, 1);
        auto const& isa = sa_succinct.isa;
        sw.stop(); mem.stop();
        std::cout << " time=" << long(sw.elapsed_time_millis()/1000.0) << "s, peak=" << mem.peak() << ", final=" << mem.count() << std::endl;

        std::cout << "construct suffix array ...";
        std::cout.flush();

        sw.start(); mem.start();
        auto sa = std::make_unique<int64_t[]>(n+1);
        libsais64((uint8_t const*)t.data(), sa.get(), n+1, 0, nullptr);
        sw.stop(); mem.stop();
        std::cout << " time=" << long(sw.elapsed_time_millis()/1000.0) << "s, peak=" << mem.peak() << ", final=" << mem.count() << std::endl;

        // factorize
        sw.start();
        std::cout << "factorize" << std::endl;
        size_t prog_next = 0;
        size_t const prog_step = n / 100;
        size_t z=0;
        for(size_t i = 0; i < n;) {
            if(i >= prog_next) {
                sw.pause();
                std::cout << "\ti=" << i << " (" << 100.0 * double(i) / double(n) << "%), z=" << z << ", time=" << long(sw.elapsed_time_millis()/1000.0) << "s" << std::endl;
                prog_next += prog_step;
                sw.resume();
            }
            ++z;

            // get SA position for suffix i
            size_t const cur_pos = isa[i];

            // compute PSV and NSV
            ssize_t psv_pos = (ssize_t)cur_pos - 1;
            while (psv_pos >= 0 && sa[psv_pos] > i) {
                --psv_pos;
            }
            size_t const psv_lcp = psv_pos >= 0 ? lce(t, i, (size_t)sa[psv_pos]) : 0;

            size_t nsv_pos = cur_pos + 1;
            while(nsv_pos < n && sa[nsv_pos] > i) {
                ++nsv_pos;
            }
            size_t const nsv_lcp = nsv_pos < n ? lce(t, i, (size_t)sa[nsv_pos]) : 0;

            //select maximum
            size_t const max_lcp = std::max(psv_lcp, nsv_lcp);
            if(max_lcp >= min_ref_len_) {
                ssize_t const max_pos = (max_lcp == psv_lcp) ? psv_pos : nsv_pos;
                assert(max_pos >= 0);
                assert(max_pos < n);
                assert(sa[max_pos] < i);
                
                // emit reference
                *out++ = Factor(i - sa[max_pos], max_lcp);
                i += max_lcp; //advance
            } else {
                // emit literal
                *out++ = Factor(t[i]);
                ++i; //advance
            }
        }
    }

    /**
     * \brief Reports the minimum length of a referencing factor
     * 
     * If a referencing factor is shorter than this length, a literal factor is emitted instead
     * 
     * \return the minimum reference length
     */
    size_t min_reference_length() const { return min_ref_len_; }

    /**
     * \brief Sets the minimum length of a referencing factor
     * 
     * If a referencing factor is shorter than this length, a literal factor is emitted instead
     * 
     * \param min_ref_len the minimum reference length
     */
    void min_reference_length(size_t min_ref_len) { min_ref_len_ = min_ref_len; }
};
