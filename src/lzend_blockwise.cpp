#include "compressor_base.hpp"
#include "topk_lzend_impl.hpp"

#include <tdc/util/si_iec_literals.hpp>

struct Compressor : public CompressorBase {
    uint64_t window = 1_Mi;

    Compressor() : CompressorBase("lzend-blockwise", "Implements blockwise LZ-End (basically top-0 LZ-End)") {
        param('w', "window", window, "The window size.");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "lzend-blockwise");
        result.add("window", window);
        CompressorBase::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".lzendblock";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_lzend_compress<false>(in.begin(), in.end(), iopp::bitwise_output_to(out), window, 1, 1, 1, 1, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        topk_lzend_decompress<false>(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
