#include <block_coding.hpp>

namespace lzend {

constexpr TokenType TOK_REF = 0;
constexpr TokenType TOK_LEN = 1;
constexpr TokenType TOK_LITERAL = 2;

void setup_encoding(BlockEncodingBase& enc) {
    enc.register_binary(SIZE_MAX); // TOK_REF
    enc.register_huffman();        // TOK_LEN
    enc.register_huffman();        // TOK_LITERAL
}

template<bool protocol, iopp::BitSource In, std::output_iterator<char> Out>
void decompress_offline(In in, Out out, uint64_t const expected_magic) {
    uint64_t const magic = in.read(64);
    if(magic != expected_magic) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << expected_magic << ")" << std::endl;
        std::abort();
    }
    
    std::string s;
    std::vector<size_t> factors;
    
    BlockDecoder dec(in);
    setup_encoding(dec);
    while(in) {
        auto const q = dec.read_uint(TOK_REF);
        auto const len = (q > 0) ? dec.read_uint(TOK_LEN) : 0;

        if(len > 0) {
            auto p = factors[q-1] + 1 - len;
            for(size_t i = 0; i < len; i++) {
                s.push_back(s[p++]);
            }
        }
        
        if(in) {
            auto const c = dec.read_char(TOK_LITERAL);
            factors.push_back(s.length());
            s.push_back(c);

            if constexpr(protocol) {
                std::cout << "factor #" << factors.size() << ": i=" << (s.size() - len - 1) << ", (" << q << ", " << len << ", " << display(c) << ")" << std::endl;
            }
        } else {
            if constexpr(protocol) {
                std::cout << "factor #" << factors.size() << ": i=" << (s.size() - len - 1) << ", (" << q << ", " << len << ", <EOF>)" << std::endl;
            }
        }
    }

    // output
    std::copy(s.begin(), s.end(), out);
}

}
