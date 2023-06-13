#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

#include <cstddef>
#include <index/dynamic_universe_sampling.hpp>

TEST_SUITE("pred") {
    constexpr size_t U = 1'000'000;
    constexpr size_t b = 16;    

    template<typename T>
    static void INSERT(T& ds, uint32_t const key) {
        ds.insert(key, uint32_t(key));
    }

    template<typename T>
    static void REMOVE(T& ds, uint32_t const key) {
        ds.remove(key);
    }

    template<typename T>
    static void REQUIRE_PRED_NEXIST(T const& ds, uint32_t const key) {
        auto r = ds.predecessor(key);
        REQUIRE(!r.exists);
    }

    template<typename T>
    static void REQUIRE_PRED(T const& ds, uint32_t const key, uint32_t const expected) {
        auto r = ds.predecessor(key);
        REQUIRE(r.exists);
        REQUIRE(r.key == expected);
        REQUIRE(r.value == expected);
    }

    template<typename T>
    static void REQUIRE_SUCC_NEXIST(T const& ds, uint32_t const key) {
        auto r = ds.successor(key);
        REQUIRE(!r.exists);
    }

    template<typename T>
    static void REQUIRE_SUCC(T const& ds, uint32_t const key, uint32_t const expected) {
        auto r = ds.successor(key);
        REQUIRE(r.exists);
        REQUIRE(r.key == expected);
        REQUIRE(r.value == expected);
    }

    template<typename T>
    static void REQUIRE_CONTAINS(T const& ds, uint32_t const key) {
        REQUIRE(ds.contains(key));
    }

    template<typename T>
    static void REQUIRE_NCONTAINS(T const& ds, uint32_t const key) {
        REQUIRE(!ds.contains(key));
    }

    TEST_CASE("predecessor") {   
        DynamicUniverseSampling<uint32_t, uint32_t, b> ds(U); 
        INSERT(ds, 5);
        INSERT(ds, 17);
        INSERT(ds, 19);
        INSERT(ds, 128);
        INSERT(ds, 900);
        INSERT(ds, 65535);
        INSERT(ds, 65555); // crossed bucket boundary
        INSERT(ds, 131400); // crossed bucket boundary

        REQUIRE_PRED_NEXIST(ds, 0);
        REQUIRE_PRED_NEXIST(ds, 4);
        REQUIRE_PRED(ds, 5, 5);
        REQUIRE_PRED(ds, 6, 5);
        REQUIRE_PRED(ds, 16, 5);
        REQUIRE_PRED(ds, 17, 17);
        REQUIRE_PRED(ds, 18, 17);
        REQUIRE_PRED(ds, 19, 19);
        REQUIRE_PRED(ds, 64, 19);
        REQUIRE_PRED(ds, 127, 19);
        REQUIRE_PRED(ds, 899, 128);
        REQUIRE_PRED(ds, 65534, 900);
        REQUIRE_PRED(ds, 65535, 65535);
        REQUIRE_PRED(ds, 65536, 65535);
        REQUIRE_PRED(ds, 65554, 65535);
        REQUIRE_PRED(ds, 65555, 65555);
        REQUIRE_PRED(ds, 131399, 65555);
        REQUIRE_PRED(ds, 131400, 131400);
        REQUIRE_PRED(ds, U-1, 131400);
    }

    TEST_CASE("successor") {
        DynamicUniverseSampling<uint32_t, uint32_t, b> ds(U);
        INSERT(ds, 5);
        INSERT(ds, 17);
        INSERT(ds, 19);
        INSERT(ds, 128);
        INSERT(ds, 900);
        INSERT(ds, 65535);
        INSERT(ds, 65555); // crossed bucket boundary
        INSERT(ds, 131400); // crossed bucket boundary

        REQUIRE_SUCC(ds, 0, 5);
        REQUIRE_SUCC(ds, 4, 5);
        REQUIRE_SUCC(ds, 5, 5);
        REQUIRE_SUCC(ds, 6, 17);
        REQUIRE_SUCC(ds, 16, 17);
        REQUIRE_SUCC(ds, 17, 17);
        REQUIRE_SUCC(ds, 18, 19);
        REQUIRE_SUCC(ds, 19, 19);
        REQUIRE_SUCC(ds, 20, 128);
        REQUIRE_SUCC(ds, 127, 128);
        REQUIRE_SUCC(ds, 129, 900);
        REQUIRE_SUCC(ds, 350, 900);
        REQUIRE_SUCC(ds, 899, 900);
        REQUIRE_SUCC(ds, 901, 65535);
        REQUIRE_SUCC(ds, 9000, 65535);
        REQUIRE_SUCC(ds, 65536, 65555);
        REQUIRE_SUCC(ds, 65556, 131400);
        REQUIRE_SUCC_NEXIST(ds, 131401);
        REQUIRE_SUCC_NEXIST(ds, U-1);
    }

    TEST_CASE("insert") {
        DynamicUniverseSampling<uint32_t, uint32_t, b> ds(U);
        REQUIRE_PRED_NEXIST(ds, 317'362);
        REQUIRE_SUCC_NEXIST(ds, 5);
        INSERT(ds, 783'281);
        REQUIRE_PRED_NEXIST(ds, 317'362);
        REQUIRE_SUCC(ds, 5, 783'281);
        INSERT(ds, 372'444);
        REQUIRE_PRED_NEXIST(ds, 317'362);
        REQUIRE_SUCC(ds, 5, 372'444);
        INSERT(ds, 388'123);
        REQUIRE_PRED_NEXIST(ds, 317'362);
        REQUIRE_SUCC(ds, 5, 372'444);
        INSERT(ds, 2);
        REQUIRE_PRED(ds, 317'362, 2);
        REQUIRE_PRED_NEXIST(ds, 1);
        REQUIRE_SUCC(ds, 5, 372'444);
        INSERT(ds, 100'000);
        REQUIRE_PRED(ds, 317'362, 100'000);
        REQUIRE_PRED(ds, 99'999, 2);
        REQUIRE_SUCC(ds, 5, 100'000);
        REQUIRE_PRED_NEXIST(ds, 1);
        INSERT(ds, 317'363);
        REQUIRE_PRED(ds, 317'362, 100'000);
        REQUIRE_SUCC(ds, 5, 100'000);
        INSERT(ds, 317'362);
        REQUIRE_PRED(ds, 317'362, 317'362);
        REQUIRE_SUCC(ds, 5, 100'000);
        REQUIRE_SUCC_NEXIST(ds, 783'282);
    }

    TEST_CASE("remove") {
        DynamicUniverseSampling<uint32_t, uint32_t, b> ds(U);
        INSERT(ds, 783'281);
        INSERT(ds, 372'444);
        INSERT(ds, 388'123);
        INSERT(ds, 2);
        INSERT(ds, 100'000);
        INSERT(ds, 317'363);
        INSERT(ds, 317'362);

        REQUIRE_PRED(ds, 100'000, 100'000);
        REQUIRE_SUCC(ds, 100'000, 100'000);
        REMOVE(ds, 100'000);
        REQUIRE_PRED(ds, 100'000, 2);
        REQUIRE_SUCC(ds, 100'000, 317'362);
        REMOVE(ds, 2);
        REQUIRE_PRED_NEXIST(ds, 100'000);
        REQUIRE_SUCC(ds, 100'000, 317'362);
        REMOVE(ds, 317'362);
        REQUIRE_SUCC(ds, 100'000, 317'363);
        REQUIRE_SUCC(ds, 2, 317'363);
    }

    TEST_CASE("contains") {   
        DynamicUniverseSampling<uint32_t, uint32_t, b> ds(U); 
        INSERT(ds, 5);
        INSERT(ds, 17);
        INSERT(ds, 19);
        INSERT(ds, 128);
        INSERT(ds, 900);
        INSERT(ds, 65535);
        INSERT(ds, 65555); // crossed bucket boundary
        INSERT(ds, 131400); // crossed bucket boundary

        REQUIRE_CONTAINS(ds, 5);
        REQUIRE_CONTAINS(ds, 17);
        REQUIRE_CONTAINS(ds, 19);
        REQUIRE_CONTAINS(ds, 128);
        REQUIRE_CONTAINS(ds, 900);
        REQUIRE_CONTAINS(ds, 65535);
        REQUIRE_CONTAINS(ds, 65555);
        REQUIRE_CONTAINS(ds, 131400);

        REQUIRE_NCONTAINS(ds, 7);
        REQUIRE_NCONTAINS(ds, 65536);
        REQUIRE_NCONTAINS(ds, 65534);

        REMOVE(ds, 5);
        REQUIRE_NCONTAINS(ds, 5);
    }
}
