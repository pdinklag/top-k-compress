#include "topk_compressor.hpp"
#include "topk_twopass_impl.hpp"

#include <topk_prefixes_misra_gries.hpp>

struct Compressor : public TopkCompressor {
    uint64_t ignored_ = 0;

    Compressor() : TopkCompressor("topk-twopass", "two passes") {
        param('w', "window", ignored_, "Ignored, provided only for interoperability.");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "topk-twopass");
        TopkCompressor::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".topk2pass";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_twopass::compress(in, iopp::bitwise_output_to(out), k, sketch_columns, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        std::abort();
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
