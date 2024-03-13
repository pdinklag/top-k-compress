#include "../topk_compressor.hpp"
#include "topk_lzend_impl.hpp"

#include <si_iec_literals.hpp>

struct Compressor : public TopkCompressor {
    uint64_t window = 1_Mi;

    Compressor() : TopkCompressor("topk-lzend", "Implements the top-k LZEnd compression algorithm") {
        param('w', "window", window, "The window size.");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "topk-lzend");
        result.add("window", window);
        TopkCompressor::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".lzendtopk";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_lzend::compress<true>(in.begin(), in.end(), iopp::bitwise_output_to(out), window, k, sketch_rows, sketch_columns, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_lzend::decompress<true>(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
