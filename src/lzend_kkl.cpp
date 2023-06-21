#include "compressor_base.hpp"
#include "lzend_kk_impl.hpp"

#include <tdc/util/si_iec_literals.hpp>

struct Compressor : public CompressorBase {
    uint64_t window = 1_Mi;

    Compressor() : CompressorBase("lzend-kkl", "LZEnd compression via Kempa & Kosolobov (prefer local)") {
        param('w', "window", window, "The window size.");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "lzend-kk");
        result.add("window", window);
        CompressorBase::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".lzendkkl";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        lzend_kk_compress<true>(in.begin(), in.end(), iopp::bitwise_output_to(out), window, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        lzend_kk_decompress(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
