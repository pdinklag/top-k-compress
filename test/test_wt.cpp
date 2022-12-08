#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

#include <index/wavelet_tree.hpp>

#include <string>

TEST_SUITE("wavelet_tree") {
    using namespace std::string_literals;
    
    std::string const input =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus aliquet in turpis vitae mattis.\0"
        "Etiam nunc nibh, ornare in tincidunt quis, iaculis eget orci. Morbi viverra maximus quam vel feugiat. "
        "Nulla est augue, vehicula eu ante non, dapibus dignissim purus. Donec at viverra est. Sed a rhoncus lectus. "
        "Maecenas a purus nisi. Donec aliquet dignissim tempor. Donec interdum pulvinar massa, sit amet finibus ante volutpat aliquet. "
        "Aliquam eget purus sed ex ornare imperdiet vel in lorem. Cras accumsan egestas malesuada. "
        "Phasellus mauris eros, congue non feugiat porttitor, commodo at quam. Vestibulum cursus enim ullamcorper tristique mattis.\0"s;
    
    TEST_CASE("rank") {
        WaveletTree<> wt(input.begin(), input.end());
        
        REQUIRE(wt.length() == input.length());
        
        size_t r[256] = {0};
        for(size_t i = 0; i < input.length(); i++) {
            ++r[size_t(input[i])];
            
            for(size_t c = 0; c < 256; c++) {
                REQUIRE(wt.rank(char(c), i) == r[c]);
            }
        }
    }
}
