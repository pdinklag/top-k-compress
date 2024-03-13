#include "topk_compressor.hpp"
#include "topk_lz77_impl.hpp"

#include <topk_prefixes_misra_gries.hpp>

using Topk = TopKPrefixesMisraGries<>;

struct Compressor : public TopkCompressor {
    uint64_t window = 1_Mi;
    unsigned int threshold = 2;

    Compressor() : TopkCompressor("topk-lz77", "Best of both worlds approach to blockwise LZ77 and top-k LZ78.") {
        param('w', "window", window, "The window size.");
        param('t', "threshold", threshold, "The minimum reference length");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "topk-lz77");
        TopkCompressor::init_result(result);
        result.add("window", window);
        result.add("threshold", threshold);
    }

    virtual std::string file_ext() override {
        return ".topklz77";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_lz77::compress<Topk>(in.begin(), in.end(), iopp::bitwise_output_to(out), threshold, k, window, max_freq, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_lz77::decompress<Topk>(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
