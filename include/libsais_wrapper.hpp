#pragma once

#include <concepts>
#include <iterator>
#include <memory>
#include <tuple>

#include <libsais.h>
#include <libsais64.h>

template<std::contiguous_iterator TextReadAccess>
requires (sizeof(std::iter_value_t<TextReadAccess>) == 1)
auto sa_isa_lcp_u32(TextReadAccess begin, TextReadAccess end) {
    auto const text = std::string_view(begin, end);
    auto const n = text.length();

    auto sa = std::make_unique<uint32_t[]>(n);
    auto lcp = std::make_unique<uint32_t[]>(n);
    auto isa = std::make_unique<uint32_t[]>(n);

    libsais((uint8_t const*)text.data(), (int32_t*)sa.get(), n, 0, nullptr);
    libsais_plcp((uint8_t const*)text.data(), (int32_t const*)sa.get(), (int32_t*)isa.get(), n);
    libsais_lcp((int32_t const*)isa.get(), (int32_t const*)sa.get(), (int32_t*)lcp.get(), n);
    for(uint32_t i = 0; i < n; i++) isa[sa[i]] = i;

    return std::tuple(std::move(sa), std::move(isa), std::move(lcp));
}

template<std::contiguous_iterator TextReadAccess>
requires (sizeof(std::iter_value_t<TextReadAccess>) == 1)
auto sa_isa_lcp_u64(TextReadAccess begin, TextReadAccess end) {
    auto const text = std::string_view(begin, end);
    auto const n = text.length();

    auto sa = std::make_unique<uint64_t[]>(n);
    auto lcp = std::make_unique<uint64_t[]>(n);
    auto isa = std::make_unique<uint64_t[]>(n);

    libsais64((uint8_t const*)text.data(), (int64_t*)sa.get(), n, 0, nullptr);
    libsais64_plcp((uint8_t const*)text.data(), (int64_t const*)sa.get(), (int64_t*)isa.get(), n);
    libsais64_lcp((int64_t const*)isa.get(), (int64_t const*)sa.get(), (int64_t*)lcp.get(), n);
    for(uint64_t i = 0; i < n; i++) isa[sa[i]] = i;

    return std::tuple(std::move(sa), std::move(isa), std::move(lcp));
}

