#include "topk_compressor_cm.hpp"
#include "../topk_lz78_impl.hpp"

#include <archive/cm/topk_prefixes_count_min.hpp>

struct Compressor : public TopkCompressorCountMin {
    uint64_t ignored_ = 0;

    Compressor() : TopkCompressorCountMin("topk-lz78-cm", "Implements the top-k LZ78 compression algorithm") {
        param('w', "window", ignored_, "Ignored, provided only for interoperability.");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "topk-lz78-cm");
        TopkCompressorCountMin::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".topklz78cm";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_lz78::compress<TopKPrefixesCountMin<>>(in.begin(), in.end(), iopp::bitwise_output_to(out), k, sketch_columns, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_lz78::decompress<TopKPrefixesCountMin<>>(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
