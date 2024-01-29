#include <oocmd.hpp>

#include <iopp/file_input_stream.hpp>
#include <iopp/file_output_stream.hpp>

#include <iopp/bitwise_io.hpp>
#include <iopp/stream_input_iterator.hpp>
#include <iopp/stream_output_iterator.hpp>

#include <cmath>

#include <pm/malloc_counter.hpp>
#include <pm/stopwatch.hpp>

#include <pm/result.hpp>

using namespace oocmd;

struct CompressorBase : public ConfigObject {
    std::string input;
    std::string output;
    bool decompress_flag = false;

    uint64_t block_size = 4'096;
    uint64_t prefix = UINTMAX_MAX;

    CompressorBase(std::string&& type_name, std::string&& desc) : ConfigObject(std::move(type_name), std::move(desc)) {
        param('o', "out", output, "The output filename.");
        param('d', "decompress", decompress_flag, "Decompress the input file rather than compressing it.");
        param('b', "block-size", block_size, "The block size for encoding.");
        param('p', "prefix", prefix, "The prefix of the input file to consider.");
    }

    virtual void init_result(pm::Result& result) {
        result.add("block_size", block_size);
    }

    virtual std::string file_ext() = 0;

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) = 0;

    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) = 0;

    int run(Application const& app) {
        if(!app.args().empty()) {
            input = app.args()[0];
            if(output.empty()) {
                output = input + (decompress_flag ? ".dec" : file_ext());
            }

            pm::Result result;
            result.add("file", std::filesystem::path(input).filename().string());
            result.add("n", std::min(std::filesystem::file_size(input), prefix));
            this->init_result(result);

            {
                iopp::FileInputStream fis(input, 0, prefix);
                iopp::FileOutputStream fos(output);

                pm::MallocCounter m;
                m.start();

                pm::Stopwatch t;
                t.start();

                if(decompress_flag) {
                    decompress(fis, fos, result);
                } else {
                    compress(fis, fos, result);
                }

                t.stop();
                result.add("time", (uint64_t)std::round(t.elapsed_time_millis()));

                m.stop();
                result.add("mem_peak", m.peak());
            }

            result.add("nout", std::filesystem::file_size(output));
            result.sort();
            std::cout << result.str() << std::endl;
            return 0;
        } else {
            app.print_usage(*this);
            return -1;
        }
    }
};
