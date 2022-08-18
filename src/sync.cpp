#include <iostream>
#include <filesystem>

#include <tdc/framework/application.hpp>

#include <iopp/bitwise_io.hpp>
#include <iopp/file_input_stream.hpp>
#include <iopp/file_output_stream.hpp>
#include <iopp/stream_input_iterator.hpp>
#include <iopp/stream_output_iterator.hpp>

#include <tlx/container/ring_buffer.hpp>

#include "rolling_karp_rabin.hpp"

using namespace tdc::framework;

struct Options : public Entity {
    size_t fp_window = 32;
    size_t fp_mod = 512;

    Options() : Entity("vitter87", "Encode a file using Vitter's dynamic Huffman codes from 1987") {
        param('f', "fp-window", fp_window, "The fingerprinting window.");
        param('m', "fp-mod", fp_mod, "The expected synchronization frequency.");
    }
};

Options options;

int main(int argc, char** argv) {
    Application app(options, argc, argv);
    if(app) {
        if(!app.args().empty()) {
            auto const& input = app.args()[0];
            iopp::FileInputStream fis(input);

            size_t num_sync = 0;
            uint64_t fp;
            RollingKarpRabin hash(options.fp_window, (1ULL<<9) - 3);
            tlx::RingBuffer<char> rb(options.fp_window);

            for(char const c : fis) {
                char out = 0;
                if(rb.size() == rb.max_size()) {
                    out = rb.front();
                    rb.pop_front();
                }

                rb.push_back(c);
                fp = hash.roll(fp, out, c);
                if((fp % options.fp_mod) == 0) {
                    ++num_sync;
                }
            }

            std::cout << "num_sync=" << num_sync << std::endl;
        } else {
            app.print_usage(options);
        }
    }
    return 0;
}
