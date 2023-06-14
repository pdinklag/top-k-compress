#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

#include <lzend_parsing.hpp>
#include <lzend_rev_phrase_trie.hpp>

TEST_SUITE("kempa_kosolobov_2017") {
    TEST_CASE("lzend") {
        LZEndParsing<char, uint32_t> parsing;
        parsing.emplace_back(0, 1, 'a'); // 1 - a       => rev suffix: a
        parsing.emplace_back(0, 1, 'b'); // 2 - b       => rev suffix: ba
        parsing.emplace_back(1, 2, 'b'); // 3 - ab      => rev suffix: baba
        parsing.emplace_back(2, 3, 'b'); // 4 - abb     => rev suffix: bbababa
        parsing.emplace_back(2, 2, 'a'); // 5 - ba      => rev suffix: abbbababa
        parsing.emplace_back(5, 7, 'a'); // 6 - babbbaa => rev suffix: aabbbababbbababa

        REQUIRE(parsing.size() == 6);
        REQUIRE(parsing.length() == 16);
        REQUIRE(parsing.phrase_at(0) == 1);
        REQUIRE(parsing.phrase_at(1) == 2);
        REQUIRE(parsing.phrase_at(2) == 3);
        REQUIRE(parsing.phrase_at(3) == 3);
        REQUIRE(parsing.phrase_at(4) == 4);
        REQUIRE(parsing.phrase_at(5) == 4);
        REQUIRE(parsing.phrase_at(6) == 4);
        REQUIRE(parsing.phrase_at(7) == 5);
        REQUIRE(parsing.phrase_at(8) == 5);
        REQUIRE(parsing.phrase_at(9) == 6);
        REQUIRE(parsing.phrase_at(10) == 6);
        REQUIRE(parsing.phrase_at(11) == 6);
        REQUIRE(parsing.phrase_at(12) == 6);
        REQUIRE(parsing.phrase_at(13) == 6);
        REQUIRE(parsing.phrase_at(14) == 6);
        REQUIRE(parsing.phrase_at(15) == 6);

        {
            std::string s;
            parsing.decode_rev(std::back_inserter(s), 1, 1);
            REQUIRE(s == "a");
        }
        {
            std::string s;
            parsing.decode_rev(std::back_inserter(s), 2, 2);
            REQUIRE(s == "ba");
        }
        {
            std::string s;
            parsing.decode_rev(std::back_inserter(s), 3, 4);
            REQUIRE(s == "baba");
        }
        {
            std::string s;
            parsing.decode_rev(std::back_inserter(s), 4, 7);
            REQUIRE(s == "bbababa");
        }
        {
            std::string s;
            parsing.decode_rev(std::back_inserter(s), 5, 9);
            REQUIRE(s == "abbbababa");
        }
        {
            std::string s;
            parsing.decode_rev(std::back_inserter(s), 6, 16);
            REQUIRE(s == "aabbbababbbababa");
        }
    }

    TEST_CASE("trie") {
        using FPString = FPStringView<char>;

        LZEndParsing<char, uint32_t> parsing;
        LZEndRevPhraseTrie<char, uint32_t> trie(parsing);

        // ensure that we don't find any bogus in an empty trie
        {
            FPString fx("xxx");
            REQUIRE(trie.approx_find_phr(fx, 0, fx.length()) == 0);
        }

        parsing.emplace_back(0, 1, 'a'); // 1 - a       => rev suffix: a
        parsing.emplace_back(0, 1, 'b'); // 2 - b       => rev suffix: ba
        parsing.emplace_back(1, 2, 'b'); // 3 - ab      => rev suffix: baba
        parsing.emplace_back(2, 3, 'b'); // 4 - abb     => rev suffix: bbababa
        parsing.emplace_back(2, 2, 'a'); // 5 - ba      => rev suffix: abbbababa
        parsing.emplace_back(5, 7, 'a'); // 6 - babbbaa => rev suffix: aabbbababbbababa
        
        // insert concatenated phrases, reversed
        FPString f1("a");
        trie.insert(f1, 0, f1.length());
        
        FPString f2("ba");
        trie.insert(f2, 0, f2.length());

        FPString f3("baba");
        trie.insert(f3, 0, f3.length());

        FPString f4("bbababa");
        trie.insert(f4, 0, f4.length());

        FPString f5("abbbababa");
        trie.insert(f5, 0, f5.length());

        FPString f6("aabbbababbbababa");
        trie.insert(f6, 0, f6.length());

        // ensure that we don't find any bogus
        {
            FPString fx("xxx");
            REQUIRE(trie.approx_find_phr(fx, 0, fx.length()) == 0);
        }

        // try to find reverse phrases back
        REQUIRE(trie.approx_find_phr(f1, 0, f1.length()) == 1);
        REQUIRE(trie.approx_find_phr(f2, 0, f2.length()) == 2);
        REQUIRE(trie.approx_find_phr(f3, 0, f3.length()) == 3);
        REQUIRE(trie.approx_find_phr(f4, 0, f4.length()) == 4);
        REQUIRE(trie.approx_find_phr(f5, 0, f5.length()) == 5);
        REQUIRE(trie.approx_find_phr(f6, 0, f6.length()) == 6);

        // substring variants
        REQUIRE(trie.approx_find_phr(f6, f6.length() - f1.length(), f1.length()) == 1);
        REQUIRE(trie.approx_find_phr(f6, f6.length() - f2.length(), f2.length()) == 2);
        REQUIRE(trie.approx_find_phr(f6, f6.length() - f3.length(), f3.length()) == 3);
        REQUIRE(trie.approx_find_phr(f6, f6.length() - f4.length(), f4.length()) == 4);
        REQUIRE(trie.approx_find_phr(f6, f6.length() - f5.length(), f5.length()) == 5);

        // try to find suffixes
        {
            FPString rsuf("bab");
            REQUIRE(trie.approx_find_phr(rsuf, 0, rsuf.length()) == 3);
        }
        {
            FPString rsuf("bb");
            REQUIRE(trie.approx_find_phr(rsuf, 0, rsuf.length()) == 4);
        }
        {
            FPString rsuf("ab");
            REQUIRE(trie.approx_find_phr(rsuf, 0, rsuf.length()) == 5);
        }
        {
            FPString rsuf("aa");
            REQUIRE(trie.approx_find_phr(rsuf, 0, rsuf.length()) == 6);
        }
        {
            FPString rsuf("a");
            auto const p = trie.approx_find_phr(rsuf, 0, rsuf.length());
            REQUIRE((p == 1 || p == 5 || p == 6));
        }
        {
            FPString rsuf("b");
            auto const p = trie.approx_find_phr(rsuf, 0, rsuf.length());
            REQUIRE((p == 2 || p == 3 || p == 4));
        }
        {
            // substring variants
            REQUIRE(trie.approx_find_phr(f6, 4, 3) == 3); // substring "bab"
            REQUIRE(trie.approx_find_phr(f6, 10, 3) == 3); // substring "bab"
            REQUIRE(trie.approx_find_phr(f6, 12, 3) == 3); // substring "bab"
            REQUIRE(trie.approx_find_phr(f6, 2, 2) == 4); // substring "bb"
            REQUIRE(trie.approx_find_phr(f6, 3, 2) == 4); // substring "bb"
            REQUIRE(trie.approx_find_phr(f6, 8, 2) == 4); // substring "bb"
            REQUIRE(trie.approx_find_phr(f6, 9, 2) == 4); // substring "bb"
            REQUIRE(trie.approx_find_phr(f6, 1, 2) == 5); // substring "ab"
            REQUIRE(trie.approx_find_phr(f6, 5, 2) == 5); // substring "ab"
            REQUIRE(trie.approx_find_phr(f6, 7, 2) == 5); // substring "ab"
            REQUIRE(trie.approx_find_phr(f6, 11, 2) == 5); // substring "ab"
            REQUIRE(trie.approx_find_phr(f6, 13, 2) == 5); // substring "ab"
            REQUIRE(trie.approx_find_phr(f6, 0, 2) == 6); // substring "aa"
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
        using FPString = FPStringView<char>;

        SUBCASE("substrings") {
            FPString s("abaaababaaab");
            {
                FPString x("aba");
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
                FPString y("abaaab");
                auto const fp_y = y.fingerprint(y.length() - 1);
                REQUIRE(s.fingerprint(0,5) == fp_y);
                REQUIRE(s.fingerprint(1,5) != fp_y);
                REQUIRE(s.fingerprint(6,11) == fp_y);
            }
        }
        
        SUBCASE("append") {
            std::string const s = "asdfb??3227ZabfewajeAAFFfjfb32j3b1baQPbabaf__as+a+aewf#2fajwsfwqejfgbharea";

            // compute fingerprint of s using single character appending
            uint64_t fp_single = 0;
            for(size_t i = 0; i < s.length(); i++) {
                fp_single = FPString::append(fp_single, s[i]);
            }

            // now compute it using two substrings of half the size
            uint64_t fp_prefix = 0;
            for(size_t i = 0; i < s.length() / 2; i++) {
                fp_prefix = FPString::append(fp_prefix, s[i]);
            }

            uint64_t fp_suffix = 0;
            for(size_t i = s.length() / 2; i < s.length(); i++) {
                fp_suffix = FPString::append(fp_suffix, s[i]);
            }

            uint64_t fp_combined = FPString::append(fp_prefix, fp_suffix, s.length() - s.length() / 2);
            REQUIRE(fp_combined == fp_single);
        }
    }
}
