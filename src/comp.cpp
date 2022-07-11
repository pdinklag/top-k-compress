#include <tdc/framework/application.hpp>
#include <tdc/io/file_input_stream.hpp>
#include <tdc/io/stream_input_iterator.hpp>
#include "top_k_compress.hpp"

using namespace tdc::framework;

struct Options : public Entity {
    uint64_t k = 8;
    uint64_t window = 4;
    uint64_t sketch_rows = 2;
    uint64_t sketch_columns = 8;

    Options() : Entity("top-k-compress", "Compression using top-k substrings") {
        param('k', "num-frequent", k, "The number of frequent substrings to maintain.");
        param('w', "window", window, "The window size.");
        param('d', "sketch-rows", sketch_rows, "The number of rows in the Count-Min sketch.");
        param('c', "sketch-columns", sketch_columns, "The number of columns in each Count-Min row.");
    }
};

Options options;

int main(int argc, char** argv) {
    Application app(options, argc, argv);
    if(app) {
        if(!app.args().empty()) {
            tdc::io::FileInputStream fis(app.args()[0]);
            top_k_compress(fis.begin(), fis.end(), options.k, options.window, options.sketch_rows, options.sketch_columns);
        } else {
            app.print_usage(options);
        }
    }
    return 0;
}
