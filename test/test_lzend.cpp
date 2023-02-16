#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

#include <lzend_parsing.hpp>
#include <lzend_rev_phrase_trie.hpp>

TEST_SUITE("kempa_kosolobov_2017") {
    TEST_CASE("lzend") {
        LZEndParsing<char, uint32_t> parsing;
        parsing.emplace_back(0, 1, 'a'); // a
        parsing.emplace_back(0, 1, 'b'); // b
        parsing.emplace_back(2, 3, 'b'); // abb
        parsing.emplace_back(3, 5, 'a'); // babba
        parsing.emplace_back(4, 8, 'a'); // bbbabbaa

        REQUIRE(parsing.size() == 5);
        REQUIRE(parsing.length() == 18);
        REQUIRE(parsing.phrase_at(0) == 1);
        REQUIRE(parsing.phrase_at(1) == 2);
        REQUIRE(parsing.phrase_at(2) == 3);
        REQUIRE(parsing.phrase_at(3) == 3);
        REQUIRE(parsing.phrase_at(4) == 3);
        REQUIRE(parsing.phrase_at(5) == 4);
        REQUIRE(parsing.phrase_at(6) == 4);
        REQUIRE(parsing.phrase_at(7) == 4);
        REQUIRE(parsing.phrase_at(8) == 4);
        REQUIRE(parsing.phrase_at(9) == 4);
        REQUIRE(parsing.phrase_at(10) == 5);
        REQUIRE(parsing.phrase_at(11) == 5);
        REQUIRE(parsing.phrase_at(12) == 5);
        REQUIRE(parsing.phrase_at(13) == 5);
        REQUIRE(parsing.phrase_at(14) == 5);
        REQUIRE(parsing.phrase_at(15) == 5);
        REQUIRE(parsing.phrase_at(16) == 5);
        REQUIRE(parsing.phrase_at(17) == 5);

        std::string s;
        s.reserve(18);
        {
            parsing.extract(std::back_inserter(s), 0, 18);
            REQUIRE(s == "ababbbabbabbbabbaa");
        }
        s.clear();
        {
            parsing.extract_phrase(std::back_inserter(s), 3);
            REQUIRE(s == "abb");
        }
        s.clear();
        {
            parsing.extract_phrase(std::back_inserter(s), 4);
            REQUIRE(s == "babba");
        }
        s.clear();
        {
            parsing.extract_phrase(std::back_inserter(s), 5);
            REQUIRE(s == "bbbabbaa");
        }
    }

    TEST_CASE("trie") {
        LZEndParsing<char, uint32_t> parsing;
        parsing.emplace_back(0, 1, 'a'); // a
        parsing.emplace_back(0, 1, 'b'); // b
        parsing.emplace_back(2, 3, 'b'); // abb
        parsing.emplace_back(3, 5, 'a'); // babba
        parsing.emplace_back(4, 8, 'a'); // bbbabbaa

        LZEndRevPhraseTrie<char, uint32_t> trie(parsing);
        
        // the concatenated phrases, reversed
        FPStringView<char> f1("a");
        REQUIRE(trie.insert(f1) == 1);
        REQUIRE(trie.approx_find_phr(f1) == 1);

        FPStringView<char> f2("ba");
        REQUIRE(trie.insert(f2) == 2);
        REQUIRE(trie.approx_find_phr(f2) == 2);

        FPStringView<char> fx("xxx");
        REQUIRE(trie.approx_find_phr(fx) == 0);

        /*
        FPStringView<char> f3("bbaba");
        REQUIRE(trie.insert(f3) == 3);

        FPStringView<char> f4("abbabbbaba");
        REQUIRE(trie.insert(f4) == 4);

        FPStringView<char> f5("aabbabbbabbabbbaba");
        REQUIRE(trie.insert(f5) == 5);
        */
    }

    TEST_CASE("rst") {
        using Index = uint32_t;
        static constexpr Index MAX = 256; //4096;

        for(Index y = 0; y < MAX; y++) {
            for(Index x = y + 1; x <= MAX; x++) {
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
