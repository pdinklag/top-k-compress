#include "topk_compressor.hpp"
#include "topk_plz77_impl.hpp"

#include <topk_prefixes_misra_gries.hpp>

using Topk = TopKPrefixesMisraGries<>;

struct Compressor : public TopkCompressor {
    uint64_t window = 8;
    unsigned int threshold = 2;

    Compressor() : TopkCompressor("topk-plz77-mg", "Factorizes (small) windows to use as relevant strings in a Misra-Gries top-k data structure.") {
        param('w', "window", window, "The window size.");
        param('t', "threshold", threshold, "The minimum reference length");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "topk-plz77-mg");
        TopkCompressor::init_result(result);
        result.add("window", window);
        result.add("threshold", threshold);
    }

    virtual std::string file_ext() override {
        return ".topkplz77mg";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_compress_plz77<Topk>(in.begin(), in.end(), iopp::bitwise_output_to(out), threshold, k, window, sketch_rows, sketch_columns, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        std::cerr << "not implemented" << std::endl;
        std::abort();
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
