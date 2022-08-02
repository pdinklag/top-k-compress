#include <iostream>
#include <filesystem>

#include <tdc/framework/application.hpp>
#include <tdc/io/file_input_stream.hpp>
#include <tdc/io/file_output_stream.hpp>
#include <tdc/io/stream_input_iterator.hpp>
#include <tdc/io/stream_output_iterator.hpp>
#include <tdc/io/util.hpp>

#include "vitter87.hpp"

using namespace tdc::framework;

struct Options : public Entity {
    bool decode = false;

    Options() : Entity("vitter87", "Encode a file using Vitter's dynamic Huffman codes from 1987") {
        param('d', "decode", decode, "Decode the input file.");
    }
};

Options options;

int main(int argc, char** argv) {
    Application app(options, argc, argv);
    if(app) {
        if(!app.args().empty()) {
            if(options.decode) {
                tdc::io::FileInputStream fis(app.args()[0]);
                tdc::io::FileOutputStream fos(app.args()[0] + ".dec");
                auto in = tdc::io::bitwise_input_from(fis);
                auto out = tdc::io::StreamOutputIterator(fos);

                Vitter87<unsigned char> huff(256);

                size_t const n = in.read(64);
                std::cout << "decoding n=" << n << " bytes" << std::endl;

                auto receive = [&](){ return in.read_bit(); };

                for(size_t i = 0; i < n; i++) {
                    auto const c = huff.receive_and_decode(receive);
                    
                    *out++ = (char)c;
                    huff.update(c);
                }
            } else {
                size_t const n = std::filesystem::file_size(app.args()[0]);
                std::cout << "encoding n=" << n << " bytes" << std::endl;
                std::cout.flush();

                tdc::io::FileInputStream fis(app.args()[0]);
                tdc::io::FileOutputStream fos(app.args()[0] + ".dhuff");
                auto out = tdc::io::bitwise_output_to(fos);
                out.write(n, 64);

                Vitter87<unsigned char> huff(256);

                size_t bits = 0;
                for(char const c : fis) {
                    auto const code = huff.encode_and_transmit(c);
                    bits += code.length;

                    out.write(code.word, code.length);
                    huff.update(c);
                }
                std::cout << "=> " << bits << " bits written" << std::endl;
            }
        } else {
            app.print_usage(options);
        }
    }
    return 0;
}
