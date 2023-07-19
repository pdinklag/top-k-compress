#include "compressor_base.hpp"

constexpr size_t RL_MIN = 3;
constexpr size_t RL_MAX = 255 + RL_MIN;
constexpr size_t RL_BITS = 8;

constexpr size_t CHAR_BITS = 8;

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
        auto bitout = iopp::bitwise_output_to(out);

        auto beg = in.begin();
        auto end = in.end();
        if(beg == end) return;
        
        // read first character
        char prev = *beg++;
        bitout.write(prev, CHAR_BITS);

        bool run = false;
        size_t rl = 0;

        size_t num_runs = 0;
        size_t longest = 0;
        size_t total = 0;

        auto encode_run = [&](){
            // TODO: split if longer than max
            if(rl >= RL_MIN) {
                longest = std::max(longest, rl);
                total += rl;
                ++num_runs;

                for(size_t i = 0; i < RL_MIN - 1; i++) {
                    bitout.write(prev, CHAR_BITS);
                }
                bitout.write(rl - RL_MIN, RL_BITS);
            } else {
                for(size_t i = 0; i < rl - 1; i++) {
                    bitout.write(prev, CHAR_BITS);
                }
            }
        };

        while(beg != end) {
            char const c = *beg++;
            if(c == prev) {
                if(run) {
                    ++rl;
                } else {
                    run = true;
                    rl = 2;
                }
            } else {
                if(run) {
                    encode_run();
                    run = false;
                }
                bitout.write(c, CHAR_BITS);
            }
            prev = c;
        }
        if(run) encode_run();

        result.add("run_num", num_runs);
        result.add("run_longest", longest);
        result.add("run_avg_len", std::round(100.0 * ((double)total / (double)num_runs)) / 100.0);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        auto bitin = iopp::bitwise_input_from(in.begin(), in.end());
        auto dec = iopp::StreamOutputIterator(out);

        /*
        PhraseBlockReader reader(bitin);
        while(bitin) {
            *dec++ = reader.read_literal();
        }
        */
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
