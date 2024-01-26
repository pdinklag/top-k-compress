#include <iostream>
#include <filesystem>

#include <oocmd.hpp>

#include <iopp/bitwise_io.hpp>
#include <iopp/file_input_stream.hpp>
#include <iopp/file_output_stream.hpp>
#include <iopp/stream_input_iterator.hpp>
#include <iopp/stream_output_iterator.hpp>

#include <archive/vitter87.hpp>

using namespace oocmd;

struct Options : public ConfigObject {
    bool decode = false;

    Options() : ConfigObject("vitter87", "Encode a file using Vitter's dynamic Huffman codes from 1987") {
        param('d', "decode", decode, "Decode the input file.");
    }
};

Options options;

int main(int argc, char** argv) {
    Application app(options, argc, argv);
    if(app) {
        if(!app.args().empty()) {
            if(options.decode) {
                iopp::FileInputStream fis(app.args()[0]);
                iopp::FileOutputStream fos(app.args()[0] + ".dec");
                auto in = iopp::bitwise_input_from(fis);
                auto out = iopp::StreamOutputIterator(fos);

                Vitter87<unsigned char> huff(256);

                size_t const n = in.read(64);
                std::cout << "decoding n=" << n << " bytes" << std::endl;

                auto receive = [&](){ return in.read(); };

                for(size_t i = 0; i < n; i++) {
                    auto const c = huff.receive_and_decode(receive);
                    
                    *out++ = (char)c;
                    huff.update(c);
                }
            } else {
                size_t const n = std::filesystem::file_size(app.args()[0]);
                std::cout << "encoding n=" << n << " bytes" << std::endl;
                std::cout.flush();

                iopp::FileInputStream fis(app.args()[0]);
                iopp::FileOutputStream fos(app.args()[0] + ".dhuff");
                auto out = iopp::bitwise_output_to(fos);
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
