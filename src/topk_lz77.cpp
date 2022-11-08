#include "topk_compressor.hpp"
#include "topk_lz77_impl.hpp"

struct Compressor : public TopkCompressor {
    uint64_t window = 8;

    Compressor() : TopkCompressor("topk-lz77", "Implements the top-k LZ77 compression algorithm") {
        param('w', "window", window, "The window size.");
    }

    virtual std::string file_ext() override {
        return ".lz77";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out) override {
        topk_compress_lz77(in.begin(), in.end(), iopp::bitwise_output_to(out), k, window, sketch_count, sketch_rows, sketch_columns, block_size);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out) override {
        std::cerr << "not implemented" << std::endl;
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
