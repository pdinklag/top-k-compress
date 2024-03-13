#include "../compressor_base.hpp"
#include "topk_sample_impl.hpp"

#include <si_iec_literals.hpp>
#include <archive/cm/topk_strings_count_min.hpp>

struct Compressor : public CompressorBase {
    uint64_t k = 1'000'000;
    uint64_t sketch_rows = 2;
    uint64_t sketch_columns = 1'000'000;
    uint64_t sample_exp = 7;
    uint64_t len_exp_min = 2;
    uint64_t len_exp_max = 6;
    uint64_t min_dist = 0;

    Compressor() : CompressorBase("topk-sample-cm", "Samples strings in expectedly regular synchronizing intervals and uses them as a top-k dictionary") {
        param('k', "num-frequent", k, "The number of frequent substrings to maintain.");
        param('r', "sketch-rows", sketch_rows, "The number of rows in the Count-Min sketch.");
        param('c', "sketch-columns", sketch_columns, "The total number of columns in each Count-Min row.");
        param('s', "sample", sample_exp, "sample_exp");
        param("min", len_exp_min, "len_exp_min");
        param("max", len_exp_max, "len_exp_max");
        param("dist", min_dist, "The minimum distance of references.");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "topk-sample-cm");
        result.add("k", k);
        result.add("sketch_columns", sketch_columns);
        result.add("sketch_rows", sketch_rows);
        result.add("sample_exp", sample_exp);
        result.add("len_exp_min", len_exp_min);
        result.add("len_exp_max", len_exp_max);
        result.add("min_dist", min_dist);
        CompressorBase::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".topksamplecm";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_sample::compress<TopKStringsCountMin<>, false>(in.begin(), in.end(), iopp::StreamOutputIterator(out), sample_exp, len_exp_min, len_exp_max, min_dist, k, sketch_rows, sketch_columns, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_sample::decompress<TopKStringsCountMin<>, false>(in.begin(), in.end(), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
