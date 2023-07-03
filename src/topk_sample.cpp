#include "compressor_base.hpp"
#include "topk_sample_impl.hpp"

#include <tdc/util/si_iec_literals.hpp>

struct Compressor : public CompressorBase {
    uint64_t k = 1'000'000;
    uint64_t sketch_rows = 2;
    uint64_t sketch_columns = 1'000'000;
    uint64_t sample = 64;
    uint64_t len_exp_min = 2;
    uint64_t len_exp_max = 6;

    Compressor() : CompressorBase("lzend-blockwise", "Implements blockwise LZ-End (basically top-0 LZ-End)") {
        
        param('k', "num-frequent", k, "The number of frequent substrings to maintain.");
        param('r', "sketch-rows", sketch_rows, "The number of rows in the Count-Min sketch.");
        param('c', "sketch-columns", sketch_columns, "The total number of columns in each Count-Min row.");
        param('s', "sample", sample, "Sample at these positions");
        param("min", len_exp_min, "len_exp_min");
        param("max", len_exp_max, "len_exp_max");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "lzend-blockwise");
        result.add("k", k);
        result.add("sketch_columns", sketch_columns);
        result.add("sketch_rows", sketch_rows);
        result.add("sample", sample);
        result.add("len_exp_min", len_exp_min);
        result.add("len_exp_max", len_exp_max);
        CompressorBase::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".lzendblock";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_compress_sample(in.begin(), in.end(), iopp::bitwise_output_to(out), sample, len_exp_min, len_exp_max, k, sketch_rows, sketch_columns, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_decompress_sample(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}