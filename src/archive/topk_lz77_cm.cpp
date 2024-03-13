#include "topk_compressor_cm.hpp"
#include "../topk_lz77_impl.hpp"

#include <archive/cm/topk_prefixes_count_min.hpp>

using Topk = TopKPrefixesCountMin<>;

struct Compressor : public TopkCompressorCountMin {
    uint64_t window = 8;
    unsigned int threshold = 2;

    Compressor() : TopkCompressorCountMin("topk-lz77-cm", "Factorizes (small) windows to use as relevant strings in a Misra-Gries top-k data structure.") {
        param('w', "window", window, "The window size.");
        param('t', "threshold", threshold, "The minimum reference length");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "topk-lz77-cm");
        TopkCompressorCountMin::init_result(result);
        result.add("window", window);
        result.add("threshold", threshold);
    }

    virtual std::string file_ext() override {
        return ".topklz77cm";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_lz77::compress<Topk>(in.begin(), in.end(), iopp::bitwise_output_to(out), threshold, k, window, sketch_columns, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_lz77::decompress<Topk>(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
