#include <tdc/framework/application.hpp>
#include <tdc/io/file_input_stream.hpp>
#include <tdc/io/stream_input_iterator.hpp>
#include "top_k_compress.hpp"

using namespace tdc::framework;

struct Options : public Entity {
    Options() : Entity("top-k-compress", "Compression using top-k substrings") {
    }
};

Options options;

int main(int argc, char** argv) {
    Application app(options, argc, argv);
    if(app) {
        if(!app.args().empty()) {
            tdc::io::FileInputStream fis(app.args()[0]);
            top_k_compress(fis.begin(), fis.end());
        } else {
            std::cerr << "no input file" << std::endl;
        }
    }
    return 0;
}
