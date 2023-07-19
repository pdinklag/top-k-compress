#pragma once

#include <iomanip>
#include <sstream>

inline std::string display(char const x) {
    std::ostringstream oss;
    if(x >= 32 && x < 127) {
        oss << "'" << x << "'";
    } else {
        oss << "0x" << std::hex << std::uppercase << (unsigned)x;
    }
    return oss.str();
}

inline std::string display_inline(char const x) {
    std::ostringstream oss;
    if(x >= 32 && x < 127) {
        oss << x;
    } else {
        oss << "<0x" << std::hex << std::uppercase << (unsigned)x << ">";
    }
    return oss.str();
}
