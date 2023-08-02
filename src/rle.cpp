#include "compressor_base.hpp"

#include <runs.hpp>

struct Compressor : public CompressorBase {
    Compressor() : CompressorBase("rle", "Run-length compression") {
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "rle");
        CompressorBase::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".rle";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        using Index = uint32_t;

        // read input into RAM
        std::string s;
        {
            auto beg = in.begin();
            auto const end = in.end();
            while(beg != end) {
                s.push_back(*beg++);
            }
            s.shrink_to_fit();
        }

        // compute runs
        auto const n = s.length();
        std::vector<linear_time_runs::run_type<Index>> runs;
        {
            std::cout << "mapping characters ..." << std::endl;

            // we map each character to its value + 1, so that we can use 0 as a true sentinel
            auto s16 = std::make_unique<uint16_t[]>(n+2);
            s16[0] = 0;
            for(size_t i = 0; i < n; i++) {
                s16[i + 1] = uint16_t(uint8_t(s[i])) + 1;
            }
            s16[n+1] = 0;

            std::cout << "computing runs ..." << std::endl;
            runs = linear_time_runs::compute_all_runs<uint16_t, Index>(s16.get(), n+2);
        }

        // output
        std::cout << "producing output ..." << std::endl;
        size_t const num_runs = runs.size();
        size_t i = 0;
        size_t next_run = 0;

        auto out_it = iopp::StreamOutputIterator(out);
        while(i < n) {
            // advance to next relevant run
            while(next_run < num_runs && runs[next_run].start - 1 < i) {
                ++next_run;
            }

            if(next_run < num_runs && runs[next_run].start - 1 == i) {
                static constexpr size_t SIGNAL_SIZE = 1;
                static constexpr size_t PERIOD_SIZE = 1;
                static constexpr size_t EXPONENT_SIZE = 1;
                static constexpr size_t REMAINDER_SIZE = 1;
                static constexpr size_t RUN_ENC_SIZE = SIGNAL_SIZE + PERIOD_SIZE + EXPONENT_SIZE + REMAINDER_SIZE;

                auto const& run = runs[next_run];
                if(run.period + RUN_ENC_SIZE < run.length) {
                    // encode run
                    auto const exponent = run.length / run.period;
                    auto const remainder = run.length % run.period;
                    // std::cout << "i=" << i << ": encoding run (period=" << run.period << ", exponent=" << exponent << ", remainder=" << remainder << ")" << std::endl;

                    for(size_t j = 0; j < run.period; j++) {
                        *out_it++ = s[i + j];
                    }
                    *out_it++ = '#'; // signal
                    *out_it++ = (char)run.period;
                    *out_it++ = (char)exponent;
                    *out_it++ = (char)remainder;

                    i += run.length;
                    continue;
                }
            }

            *out_it++ = s[i];
            ++i;
        }
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        std::abort();
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
