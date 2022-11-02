#include <iostream>

#include <iopp/file_input_stream.hpp>

int main(int argc, char** argv) {
    if(argc < 2) {
        std::cerr << "usage: " << argv[0] << " [FILE]" << std::endl;
        return -1;
    }

    size_t hist[256] = { 0 };

    iopp::FileInputStream fis(argv[1]);
    for(char const c : fis) {
        ++hist[(unsigned char)c];
    }

    for(size_t i = 0; i < 256; i++) {
        std::cout << i << "\t" << hist[i];
        if(i >= 32 && i < 127) std::cout << "\t# '" << (char)i << "'";
        std::cout << std::endl;
    }
}
