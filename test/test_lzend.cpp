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
        using String = FPStringView<char>;

        LZEndParsing<char, uint32_t> parsing;
        LZEndRevPhraseTrie<char, uint32_t> trie(parsing);

        // ensure that we don't find any bogus in an empty trie
        {
            String fx("xxx");
            REQUIRE(trie.approx_find_phr(fx) == 0);
        }

        parsing.emplace_back(0, 1, 'a'); // 1 - a
        parsing.emplace_back(0, 1, 'b'); // 2 - b
        parsing.emplace_back(1, 2, 'b'); // 3 - ab
        parsing.emplace_back(2, 3, 'b'); // 4 - abb
        parsing.emplace_back(1, 2, 'a'); // 5 - ba
        parsing.emplace_back(5, 6, 'a'); // 6 - babbbaa
        
        // insert concatenated phrases, reversed
        String f1("a");
        REQUIRE(trie.insert(f1) == 1);
        
        String f2("ba");
        REQUIRE(trie.insert(f2) == 2);

        String f3("baba");
        REQUIRE(trie.insert(f3) == 3);

        String f4("bbababa");
        REQUIRE(trie.insert(f4) == 4);

        String f5("abbbababa");
        REQUIRE(trie.insert(f5) == 5);

        String f6("aabbbababbbababa");
        REQUIRE(trie.insert(f6) == 6);

        // ensure that we don't find any bogus
        {
            String fx("xxx");
            REQUIRE(trie.approx_find_phr(fx) == 0);
        }

        // try to find reverse phrases back
        REQUIRE(trie.approx_find_phr(f1) == 1);
        REQUIRE(trie.approx_find_phr(f2) == 2);
        REQUIRE(trie.approx_find_phr(f3) == 3);
        REQUIRE(trie.approx_find_phr(f4) == 4);
        REQUIRE(trie.approx_find_phr(f5) == 5);
        REQUIRE(trie.approx_find_phr(f6) == 6);

        // try to find suffixes
        {
            String rsuf("bab");
            REQUIRE(trie.approx_find_phr(rsuf) == 3);
        }
        {
            String rsuf("bb");
            REQUIRE(trie.approx_find_phr(rsuf) == 4);
        }
        {
            String rsuf("ab");
            REQUIRE(trie.approx_find_phr(rsuf) == 5);
        }
        {
            String rsuf("aa");
            REQUIRE(trie.approx_find_phr(rsuf) == 6);
        }
        {
            String rsuf("a");
            auto const p = trie.approx_find_phr(rsuf);
            REQUIRE((p == 1 || p == 5 || p == 6));
        }
        {
            String rsuf("b");
            auto const p = trie.approx_find_phr(rsuf);
            REQUIRE((p == 2 || p == 3 || p == 4));
        }
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

    TEST_CASE("fp") {
        using String = FPStringView<char>;

        String s("abaaababaaab");

        {
            String x("aba");
            auto const fp_x = x.fingerprint(x.length() - 1);
            REQUIRE(s.fingerprint(0,2) == fp_x);
            REQUIRE(s.fingerprint(1,3) != fp_x);
            REQUIRE(s.fingerprint(2,4) != fp_x);
            REQUIRE(s.fingerprint(3,5) != fp_x);
            REQUIRE(s.fingerprint(4,6) == fp_x);
            REQUIRE(s.fingerprint(5,7) != fp_x);
            REQUIRE(s.fingerprint(6,8) == fp_x);
        }

        {
            String y("abaaab");
            auto const fp_y = y.fingerprint(y.length() - 1);
            REQUIRE(s.fingerprint(0,5) == fp_y);
            REQUIRE(s.fingerprint(1,5) != fp_y);
            REQUIRE(s.fingerprint(6,11) == fp_y);
        }
    }
}
