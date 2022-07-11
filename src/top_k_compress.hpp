#include <iostream>
#include <tdc/util/concepts.hpp>

template<tdc::InputIterator<char> In>
void top_k_compress(In p, In const end) {
    while(p != end) {
        std::cout << *p++;
    }
    std::cout << std::endl;
}
