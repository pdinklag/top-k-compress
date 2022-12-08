#include "compressor_base.hpp"
#include "lzend_impl.hpp"

struct Compressor : public CompressorBase {
    Compressor() : CompressorBase("lzend", "LZEnd compression") {
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "lzend");
        CompressorBase::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".lzend";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        lzend_compress(in.begin(), in.end(), iopp::bitwise_output_to(out), block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        lzend_decompress(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}

