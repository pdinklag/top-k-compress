#include <iostream>

#include <iopp/file_input_stream.hpp>
#include <iopp/file_output_stream.hpp>

int main(int argc, char** argv) {
    if(argc < 2) {
        std::cerr << "usage: " << argv[0] << " [FILE]" << std::endl;
        return -1;
    }

    iopp::FileInputStream fis(argv[1]);
    auto const outfilename = std::string(argv[1]) + ".no0";
    iopp::FileOutputStream fos(outfilename);
    for(char const c : fis) {
        fos.put(c ? c : (char)255);
    }
    fos.flush();
}
