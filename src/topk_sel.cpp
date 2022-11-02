#include "topk_compressor.hpp"
#include "topk_sel_impl.hpp"

struct Compressor : public TopkCompressor {
    uint64_t window = 8;

    Compressor() : TopkCompressor("topk-sel", "Implements the selective top-k compression algorithm") {
        param('w', "window", window, "The window size.");
    }

    virtual std::string file_ext() override {
        return ".topk";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out) override {
        topk_compress_sel(in.begin(), in.end(), iopp::bitwise_output_to(out), k, window, sketch_count, sketch_rows, sketch_columns, block_size);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out) override {
        topk_decompress_sel(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
