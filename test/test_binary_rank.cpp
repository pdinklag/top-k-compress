#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

#include <bit>
#include <random>

#include <index/binary_rank.hpp>

TEST_SUITE("binary_rank") {
    TEST_CASE("rank1") {
        size_t const N = 10'000'000;
        size_t const SEED = 777;
        
        auto bits = std::make_unique<uint64_t[]>(N);
        bits[0] = 0;
        bits[1] = UINT64_MAX;
        {
            std::mt19937 gen(SEED);
            std::uniform_int_distribution<uint64_t> rand(0, UINT64_MAX);
            for(size_t i = 2; i < N; i++) bits[i] = rand(gen);
        }
        
        auto rank = BinaryRank(std::move(bits), N);
        
        {
            std::mt19937 gen(~SEED);
            std::uniform_int_distribution<size_t> query(0, N-1);
            
            size_t r = 0;
            size_t j = 0;
            uint64_t x;
            for(size_t i = 0; i < N; i++) {
                if(i % 64 == 0) {
                    x = rank.bits()[j++];
                }
                
                r += x & 1;
                x >>= 1;
                
                REQUIRE(rank.rank1(i) == r);
            }
        }
    }
}
