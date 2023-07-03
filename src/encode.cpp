#include "compressor_base.hpp"

#include <phrase_block_reader.hpp>
#include <phrase_block_writer.hpp>

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

        PhraseBlockWriter writer(bitout, block_size);
        auto beg = in.begin();
        auto end = in.end();
        while(beg != end) {
            writer.write_literal(*beg++);
        }
        writer.flush();
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        auto bitin = iopp::bitwise_input_from(in.begin(), in.end());
        auto _out = iopp::StreamOutputIterator(out);

        PhraseBlockReader reader(bitin);
        while(bitin) {
            *_out++ = reader.read_literal();
        }
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
