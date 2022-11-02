#include <oocmd.hpp>

#include <iopp/file_input_stream.hpp>
#include <iopp/file_output_stream.hpp>

#include <iopp/bitwise_io.hpp>
#include <iopp/stream_input_iterator.hpp>
#include <iopp/stream_output_iterator.hpp>

using namespace oocmd;

struct TopkCompressor : public ConfigObject {
    std::string output;
    bool decompress_flag = false;

    uint64_t k = 1'000'000;
    uint64_t sketch_count = 1;
    uint64_t sketch_rows = 2;
    uint64_t sketch_columns = 1'000'000;
    uint64_t block_size = 65'536;

    TopkCompressor(std::string&& type_name, std::string&& desc) : ConfigObject(std::move(type_name), std::move(desc)) {
        param('o', "out", output, "The output filename.");
        param('d', "decompress", decompress_flag, "Decompress the input file rather than compressing it.");
        param('k', "num-frequent", k, "The number of frequent substrings to maintain.");
        param('s', "sketch-count", sketch_count, "The number of Count-Min sketches to distribute to..");
        param('r', "sketch-rows", sketch_rows, "The number of rows in the Count-Min sketch.");
        param('c', "sketch-columns", sketch_columns, "The total number of columns in each Count-Min row.");
        param('b', "block-size", block_size, "The block size for encoding.");
    }

    virtual std::string file_ext() = 0;

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out) = 0;

    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out) = 0;

    int run(Application const& app) {
        if(!app.args().empty()) {
            auto const& input = app.args()[0];
            if(output.empty()) {
                output = input + (decompress_flag ? ".dec" : file_ext());
            }

            {
                iopp::FileInputStream fis(input);
                iopp::FileOutputStream fos(output);
                if(decompress_flag) {
                    decompress(fis, fos);
                } else {
                    compress(fis, fos);
                }
            }
            return 0;
        } else {
            app.print_usage(*this);
            return -1;
        }
    }
};
