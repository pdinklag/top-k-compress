#include <tdc/framework/application.hpp>

#include <iopp/bitwise_io.hpp>
#include <iopp/file_input_stream.hpp>
#include <iopp/file_output_stream.hpp>
#include <iopp/stream_input_iterator.hpp>
#include <iopp/stream_output_iterator.hpp>

#include "top_k_compress.hpp"

using namespace tdc::framework;

struct Options : public Entity {
    std::string output;
    bool decompress = false;
    bool raw = false;
    bool huffman = false;
    uint64_t k = 1'000'000;
    uint64_t window = 8;
    uint64_t sketch_count = 1;
    uint64_t sketch_rows = 2;
    uint64_t sketch_columns = 1'000'000;

    Options() : Entity("top-k-compress", "Compression using top-k substrings") {
        param('o', "out", output, "The output filename.");
        param('d', "decompress", decompress, "Decompress the input file; all other parameters are ignored.");
        param('k', "num-frequent", k, "The number of frequent substrings to maintain.");
        param('w', "window", window, "The window size.");
        param('s', "sketch-count", sketch_count, "The number of Count-Min sketches to distribute to..");
        param('r', "sketch-rows", sketch_rows, "The number of rows in the Count-Min sketch.");
        param('c', "sketch-columns", sketch_columns, "The total number of columns in each Count-Min row.");
        param("huff", huffman, "Use dynamic Huffman coding for phrases.");
        param("raw", raw, "Omit the header in the output file -- cannot be decompressed!");
    }
};

Options options;

int main(int argc, char** argv) {
    Application app(options, argc, argv);
    if(app) {
        if(!app.args().empty()) {
            auto const& input = app.args()[0];
            if(options.decompress) {
                if(options.output.empty()) {
                    options.output = input + ".dec";
                }

                iopp::FileInputStream fis(input);
                iopp::FileOutputStream fos(options.output);
                top_k_decompress(iopp::bitwise_input_from(fis), iopp::StreamOutputIterator(fos));
            } else {
                if(options.output.empty()) {
                    if(options.huffman) {
                        options.output = input + ".topkh";
                    } else {
                        options.output = input + ".topk";
                    }
                }

                iopp::FileInputStream fis(input);
                iopp::FileOutputStream fos(options.output);
                top_k_compress(fis.begin(), fis.end(), iopp::bitwise_output_to(fos), options.raw, options.k, options.window, options.sketch_count, options.sketch_rows, options.sketch_columns, options.huffman);
            }
        } else {
            app.print_usage(options);
        }
    }
    return 0;
}
