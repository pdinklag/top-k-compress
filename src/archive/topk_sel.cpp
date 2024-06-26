#include "topk_compressor_cm.hpp"
#include "topk_sel_impl.hpp"

struct Compressor : public TopkCompressorCountMin {
    uint64_t window = 8;

    Compressor() : TopkCompressorCountMin("topk-sel", "Implements the selective top-k compression algorithm") {
        param('w', "window", window, "The window size.");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "topk-sel");
        TopkCompressorCountMin::init_result(result);
        result.add("window", window);
    }

    virtual std::string file_ext() override {
        return ".topk";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_sel::compress(in.begin(), in.end(), iopp::bitwise_output_to(out), k, window, sketch_rows, sketch_columns, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_sel::decompress(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
