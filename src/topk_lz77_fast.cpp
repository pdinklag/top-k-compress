#include "topk_compressor.hpp"
#include "topk_lz77_impl.hpp"

struct Compressor : public TopkCompressor {
    uint64_t window = 8;
    unsigned int threshold = 2;

    Compressor() : TopkCompressor("topk-lz77", "Implements the top-k LZ77 compression algorithm") {
        param('w', "window", window, "The window size.");
        param('t', "threshold", threshold, "The minimum reference length");
    }

    virtual std::string file_ext() override {
        return ".lz77f";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out) override {
        topk_compress_lz77<true>(in.begin(), in.end(), iopp::bitwise_output_to(out), k, window, sketch_count, sketch_rows, sketch_columns, block_size, threshold);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out) override {
        lz77like_decompress(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
