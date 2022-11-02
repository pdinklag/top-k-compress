#include "topk_compressor.hpp"
#include "topk_lz78_impl.hpp"

struct TopkLZ78Compressor : public TopkCompressor {
    TopkLZ78Compressor() : TopkCompressor("topk-lz78", "Implements the top-k LZ78 algorithm") {
    }

    virtual std::string file_ext() override {
        return ".lz78";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out) override {
        topk_compress_lz78(in.begin(), in.end(), iopp::bitwise_output_to(out), k, sketch_count, sketch_rows, sketch_columns, block_size);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out) override {
        topk_decompress_lz78(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    TopkLZ78Compressor c;
    return Application::run(c, argc, argv);
}
