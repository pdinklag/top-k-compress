#include "topk_compressor.hpp"
#include "topk_lz78_impl.hpp"

#include <topk_prefixes_misra_gries.hpp>

struct Compressor : public TopkCompressor {
    uint64_t ignored_ = 0;

    Compressor() : TopkCompressor("topk-lz78-mg", "Implements the top-k LZ78 compression algorithm") {
        param('w', "window", ignored_, "Ignored, provided only for interoperability.");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "topk-lz78-mg");
        TopkCompressor::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".topklz78mg";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_compress_lz78<TopKPrefixesMisraGries<>>(in.begin(), in.end(), iopp::bitwise_output_to(out), k, sketch_rows, sketch_columns, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_decompress_lz78<TopKPrefixesMisraGries<>>(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
