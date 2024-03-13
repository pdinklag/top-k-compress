#include "compressor_base.hpp"
#include "lz77_blockwise_impl.hpp"

#include <si_iec_literals.hpp>

struct Compressor : public CompressorBase {
    uint64_t window = 1_Mi;
    unsigned int threshold = 2;

    Compressor() : CompressorBase("lz77-blockwise", "Blockwise LZ77 compression") {
        param('w', "window", window, "The window size.");
        param('t', "threshold", threshold, "The minimum reference length");
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "lz77-blockwise");
        result.add("window", window);
        result.add("threshold", threshold);
        
        CompressorBase::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".lz77block";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        lz77_blockwise::compress(in.begin(), in.end(), iopp::bitwise_output_to(out), threshold, window, block_size, result);
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        lz77_blockwise::decompress(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}

