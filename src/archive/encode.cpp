#include "../compressor_base.hpp"

#include <block_coding.hpp>

struct Compressor : public CompressorBase {
    Compressor() : CompressorBase("encode", "Encodes the input as a baseline compressor") {
    }

    virtual void init_result(pm::Result& result) override {
        result.add("algo", "encode");
        CompressorBase::init_result(result);
    }

    virtual std::string file_ext() override {
        return ".encode";
    }

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        auto bitout = iopp::bitwise_output_to(out);

        BlockEncoder enc(bitout, block_size);
        enc.register_huffman();
        auto beg = in.begin();
        auto end = in.end();
        while(beg != end) {
            enc.write_char(0, *beg++);
        }
        enc.flush();
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        auto bitin = iopp::bitwise_input_from(in.begin(), in.end());
        auto _out = iopp::StreamOutputIterator(out);

        BlockDecoder dec(bitin);
        dec.register_huffman();
        while(bitin) {
            *_out++ = dec.read_char(0);
        }
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
