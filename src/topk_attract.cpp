#include "topk_compressor.hpp"
#include "topk_attract_impl.hpp"

struct Compressor : public TopkCompressor {
    Compressor() : TopkCompressor("topk-attract", "k-attractor experiment.") {
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "topk-attract");
        TopkCompressor::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".topkattract";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_attract::compress(in.begin(), in.end(), iopp::bitwise_output_to(out), k, sketch_columns, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        std::abort();
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
