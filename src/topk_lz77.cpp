#include "topk_compressor.hpp"
#include "topk_lz77_impl.hpp"

struct Compressor : public TopkCompressor {
    uint64_t window = 8;
    unsigned int threshold = 2;

    Compressor() : TopkCompressor("topk-lz77", "Factorizes (small) windows to use as relevant strings in a top-k data structure.") {
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
        topk_compress_lz77(in.begin(), in.end(), iopp::bitwise_output_to(out), threshold, k, window, sketch_rows, sketch_columns, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_decompress_lz77(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
