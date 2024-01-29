#include <algorithm>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iterator>
#include <memory>

#include <libsais64.h>
#include <lz77/factor.hpp>
#include <pm.hpp>

/**
 * \brief Computes an exact Lempel-Ziv 77 factorization of the input by simulating the longest previous factor (LPF) array
 */
class LPFSemiExternalFactorizer {
public:
    using Factor = lz77::Factor;

private:
    static constexpr size_t MAX_SIZE_32BIT = 1ULL << 31;

    static size_t lce(std::string_view const& t, size_t const i, size_t const j) {
        auto const n = t.length();

        size_t l = 0;
        while(i + l < n && j + l < n && t[i + l] == t[j + l]) ++l;

        return l;
    }

    static void write5(iopp::FileOutputStream& out, int64_t const v) {
        out.write((char const*)&v, 5);
    }

    static int64_t read5(iopp::FileInputStream& in) {
        int64_t v = 0;
        in.read((char*)&v, 5);
        return v;
    }

    size_t min_ref_len_;

public:
    LPFSemiExternalFactorizer() : min_ref_len_(2) {
    }

    template<std::contiguous_iterator Input, std::output_iterator<Factor> Output>
    requires (sizeof(std::iter_value_t<Input>) == 1)
    void factorize(Input begin, Input const& end, Output out) {
        std::string_view const t(begin, end);
        size_t const n = t.size();
        std::cout << "loaded input: n=" << n << std::endl;

        pm::Stopwatch sw;

        auto const work_file_sa = std::filesystem::current_path() / "_work_sa";
        auto const work_file_isa = std::filesystem::current_path() / "_work_isa";

        // allocate working memory
        std::cout << "allocating working memory" << std::endl;
        auto work_mem = std::make_unique<int64_t[]>(n);

        // construct suffix array
        {
            auto* sa = work_mem.get();

            std::cout << "construct suffix array ... ";
            std::cout.flush();
            sw.start();
            libsais64((uint8_t const*)t.data(), sa, n, 0, nullptr);
            sw.stop();
            std::cout << long(sw.elapsed_time_millis()/1000.0) << "s" << std::endl;

            // externalize suffix array
            std::cout << "externalize suffix array ... ";
            std::cout.flush();
            sw.start();
            {
                auto sa_out = iopp::FileOutputStream(work_file_sa);
                for(size_t i = 0; i < n; i++) write5(sa_out, sa[i]);
            }
            sw.stop();
            std::cout << long(sw.elapsed_time_millis()/1000.0) << "s" << std::endl;
        }

        // construct inverse suffix array and load suffix array
        {
            auto* isa = work_mem.get();

            std::cout << "construct inverse suffix array ... ";
            std::cout.flush();
            sw.start();
            {
                auto sa_in = iopp::FileInputStream(work_file_sa);
                for(size_t i = 0; i < n; i++) isa[read5(sa_in)] = i;
            }
            sw.stop();
            std::cout << long(sw.elapsed_time_millis()/1000.0) << "s" << std::endl;

            // externalize suffix array and load sufix array
            std::cout << "externalize inverse suffix array ... ";
            std::cout.flush();
            sw.start();
            {
                auto isa_out = iopp::FileOutputStream(work_file_isa);
                for(size_t i = 0; i < n; i++) write5(isa_out, isa[i]);
            }
            sw.stop();
            std::cout << long(sw.elapsed_time_millis()/1000.0) << "s" << std::endl;
        }

        // reload suffix array
        auto* sa = work_mem.get();
        {
            std::cout << "reload suffix array ... ";
            std::cout.flush();
            sw.start();
            {
                auto sa_in = iopp::FileInputStream(work_file_sa);
                for(size_t i = 0; i < n; i++) sa[i] = read5(sa_in);
            }
            sw.stop();
            std::cout << long(sw.elapsed_time_millis()/1000.0) << "s" << std::endl;

            std::filesystem::remove(work_file_sa);
        }

        // stream inverse suffix array
        auto isa_in = iopp::FileInputStream(work_file_isa);

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
            size_t const cur_pos = read5(isa_in);

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
                for(size_t j = 1; j < max_lcp; j++) read5(isa_in); // skip
            } else {
                // emit literal
                *out++ = Factor(t[i]);
                ++i; //advance
            }
        }

        // discard inverse suffix array
        isa_in = iopp::FileInputStream();
        std::filesystem::remove(work_file_isa);
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
