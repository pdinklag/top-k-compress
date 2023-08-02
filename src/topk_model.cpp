#include "topk_compressor.hpp"
#include "topk_model_impl.hpp"

#include <tdc/util/si_iec_literals.hpp>

struct Compressor : public TopkCompressor {
    uint64_t len_exp_min = 2;
    uint64_t len_exp_max = 6;

    Compressor() : TopkCompressor("topk-sample", "Samples strings in expectedly regular synchronizing intervals and uses them as a top-k dictionary") {
        param("min", len_exp_min, "len_exp_min");
        param("max", len_exp_max, "len_exp_max");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "topk-model");
        result.add("len_exp_min", len_exp_min);
        result.add("len_exp_max", len_exp_max);
        CompressorBase::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".topkmodel";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        auto rewind = [&](){
            in.seekg(0, std::ios::beg);
            return std::make_pair(in.begin(), in.end());
        };
        topk_compress_model(rewind, iopp::bitwise_output_to(out), len_exp_min, len_exp_max, k, sketch_rows, sketch_columns, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        std::abort();
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
