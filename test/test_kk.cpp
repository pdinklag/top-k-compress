#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

#include <kk_trie.hpp>

TEST_SUITE("kempa_kosolobov_2017") {
    TEST_CASE("rst") {
        using Index = uint32_t;

        for(Index y = 0; y <= 4095; y++) {
            for(Index x = y + 1; x <= 4096; x++) {
                auto const i = max_i_rst(x, y);
                auto const p = rst(x, i);

                // make sure p > y
                REQUIRE(p > y);

                // make sure that i is maximal
                auto const p2 = rst(x, i+1);
                REQUIRE(p2 <= y);
            }
        }
    }
}
