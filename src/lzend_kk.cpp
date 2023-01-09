#include "compressor_base.hpp"
#include "lzend_kk_impl.hpp"

struct Compressor : public CompressorBase {
    Compressor() : CompressorBase("lzend-kk", "LZEnd compression via Kempa & Kosolobov") {
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "lzend-kk");
        CompressorBase::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".lzendkk";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        lzend_kk_compress(in.begin(), in.end(), iopp::bitwise_output_to(out), block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        // TODO
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}

