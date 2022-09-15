#include <tdc/framework/application.hpp>

#include <iopp/bitwise_io.hpp>
#include <iopp/file_input_stream.hpp>
#include <iopp/file_output_stream.hpp>
#include <iopp/stream_input_iterator.hpp>
#include <iopp/stream_output_iterator.hpp>

#include "topk_exhaustive.hpp"
#include "topk_lz78.hpp"
#include "topk_selective.hpp"

using namespace tdc::framework;

struct Options : public Entity {
    std::string output;
    bool decompress = false;
    bool raw = false;
    bool huffman = false;
    bool lz78 = false;
    bool selective = false;
    uint64_t k = 1'000'000;
    uint64_t window = 8;
    uint64_t sketch_count = 1;
    uint64_t sketch_rows = 2;
    uint64_t sketch_columns = 1'000'000;
    uint64_t block_size = 65'536;

    Options() : Entity("top-k-compress", "Compression using top-k substrings") {
        param('o', "out", output, "The output filename.");
        param('d', "decompress", decompress, "Decompress the input file; all other parameters are ignored.");
        param('k', "num-frequent", k, "The number of frequent substrings to maintain.");
        param('w', "window", window, "The window size.");
        param('s', "sketch-count", sketch_count, "The number of Count-Min sketches to distribute to..");
        param('r', "sketch-rows", sketch_rows, "The number of rows in the Count-Min sketch.");
        param('c', "sketch-columns", sketch_columns, "The total number of columns in each Count-Min row.");
        param('x', "sel", selective, "Do a more selective parsing.");
        param('b', "block-size", block_size, "The block size for encoding.");
        param('z', "lz78", lz78, "Produce an LZ78 parsing.");
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
                if(options.lz78) {
                    topk_decompress_lz78(iopp::bitwise_input_from(fis.begin(), fis.end()), iopp::StreamOutputIterator(fos));
                } else {
                    if(options.selective) {
                        std::cerr << "not implemented" << std::endl;
                        std::abort();
                    } else {
                        topk_decompress_exh(iopp::bitwise_input_from(fis.begin(), fis.end()), iopp::StreamOutputIterator(fos));
                    }
                }
            } else {
                if(options.output.empty()) {
                    if(options.lz78) {
                        options.output = input + ".lz78";
                    } else {
                        if(options.selective) {
                            options.output = input + ".sel";
                        } else {
                            options.output = input + ".exh";
                        }
                    }
                }

                {
                    iopp::FileInputStream fis(input);
                    iopp::FileOutputStream fos(options.output);
                    if(options.lz78) {
                        topk_compress_lz78(fis.begin(), fis.end(), iopp::bitwise_output_to(fos), options.raw, options.k, options.sketch_count, options.sketch_rows, options.sketch_columns, options.block_size);
                    } else {
                        if(options.selective) {
                            topk_compress_sel(fis.begin(), fis.end(), iopp::bitwise_output_to(fos), options.raw, options.k, options.window, options.sketch_count, options.sketch_rows, options.sketch_columns, options.block_size);
                        } else {
                            topk_compress_exh(fis.begin(), fis.end(), iopp::bitwise_output_to(fos), options.raw, options.k, options.window, options.sketch_count, options.sketch_rows, options.sketch_columns, options.block_size);
                        }
                    }
                }
                std::cout << "n'=" << std::filesystem::file_size(options.output) << std::endl;
            }
        } else {
            app.print_usage(options);
        }
    }
    return 0;
}
