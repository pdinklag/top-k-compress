#pragma once

#include <cstdint>
#include <iomanip>
#include <sstream>

inline std::string display(char const x) {
    std::ostringstream oss;
    if(x >= 32 && x < 127) {
        oss << "'" << x << "'";
    } else {
        oss << "0x" << std::hex << std::uppercase << (unsigned int)uint8_t(x);
    }
    return oss.str();
}

inline std::string display_inline(char const x) {
    std::ostringstream oss;
    if(x >= 32 && x < 127) {
        oss << x;
    } else {
        oss << "<0x" << std::hex << std::uppercase << (unsigned int)uint8_t(x) << ">";
    }
    return oss.str();
}
