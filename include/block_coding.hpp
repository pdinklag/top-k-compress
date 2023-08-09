#pragma once

#include <code.hpp>
#include <range.hpp>
#include <iopp/concepts.hpp>

#include <cstdint>
#include <memory>
#include <vector>

using Token = uintmax_t;
using TokenType = uint8_t;

enum TokenEncoding {
    Binary,
    Huffman,
};

struct TokenParams {
    TokenEncoding encoding;
    Token max;

    inline TokenParams() : max(std::numeric_limits<Token>::max()), encoding(TokenEncoding::Binary) {
    }
};

class TokenBuffer {
private:
    // setup
    TokenParams params_;

    // encode buffer
    std::vector<Token> tokens_;
    Range range_;

    // encoding phase
    using HuffmanTree = code::HuffmanTree<uint8_t>;
    using HuffmanTable = decltype(std::declval<HuffmanTree>().table());

    HuffmanTree huff_tree_;
    HuffmanTable huff_table_;

    code::Universe universe_;
    size_t next_;

public:
    TokenBuffer() {
    }

    void push_back(Token const token) {
        #ifndef NDEBUG
        switch(params_.encoding) {
            case TokenEncoding::Huffman:
                assert(token <= 255);
                break;
        }
        #endif

        tokens_.push_back(token);
        range_.contain(token);
    }

    template<iopp::BitSink Sink>
    void prepare_encode(Sink& sink) {
        if(params_.encoding == TokenEncoding::Huffman) {
            // Huffman codes
            huff_tree_ = HuffmanTree(tokens_.begin(), tokens_.end());
            huff_tree_.encode(sink);

            huff_table_ = huff_tree_.table();
            huff_tree_ = HuffmanTree(); // discard
        } else {
            // Binary codes

            // first, write a single bit indicating if there are any tokens at all
            sink.write(!tokens_.empty());
            if(!tokens_.empty()) {
                if(params_.max <= 1) {
                    // a universe of single bits
                    universe_ = code::Universe::binary();
                } else {
                    // a larger universe
                    code::Binary::encode(sink, range_.min, code::Universe(params_.max));
                    code::Binary::encode(sink, range_.max, code::Universe(range_.min, params_.max));
                    universe_ = range_.universe();
                }
            }
        }
        next_ = 0;
    }

    template<iopp::BitSink Sink>
    void encode_next(Sink& sink) {
        assert(next_ < tokens_.size());

        auto const token = tokens_[next_++];
        if(params_.encoding == TokenEncoding::Huffman) {
            code::Huffman::encode(sink, (uint8_t)token, huff_table_);
        } else {
            code::Binary::encode(sink, token, universe_);
        }
    }

    template<iopp::BitSource Src>
    void prepare_decode(Src& src) {
        if(params_.encoding == TokenEncoding::Huffman) {
            // Huffman codes
            huff_tree_ = HuffmanTree(src);
        } else {
            // Binary codes
            bool const any = src.read();
            if(any) {
                if(params_.max <= 1) {
                    // a universe of single bits
                    universe_ = code::Universe::binary();
                } else {
                    // a universe defined by min and max
                    auto const min = code::Binary::decode(src, code::Universe(params_.max));
                    auto const max = code::Binary::decode(src, code::Universe(min, params_.max));
                    universe_ = code::Universe(min, max);
                }
            }
        }
    }

    template<iopp::BitSource Src>
    uintmax_t decode_next(Src& src) {
        if(params_.encoding == TokenEncoding::Huffman) {
            return code::Huffman::decode(src, huff_tree_.root());
        } else {
            return code::Binary::decode(src, universe_);
        }
    }

    auto& params() {
        return params_;
    }

    void clear() { 
        tokens_.clear();
        range_.reset();
    }

    void print_stats() {
        #ifndef NDEBUG
        // basic stats
        {
            double const n = double(tokens_.size());
            double min = std::numeric_limits<double>::max();
            double max = std::numeric_limits<double>::min();
            double sum = 0;
            for(auto x : tokens_) {
                auto const d = double(x);
                sum += d;
                min = std::min(min, d);
                max = std::max(max, d);
            }

            double const avg = sum / n;
            double qdsum = 0.0;
            for(auto x : tokens_) {
                double const d = double(x) - avg;
                qdsum += d * d;
            }
            double const var = qdsum / (n - 1.0);
            double const stddev = sqrt(var);
            std::cout << "\t\tn=" << n << ", min=" << min << ", max=" << max << ", avg=" << avg << ", stddev=" << stddev << std::endl;
        }

        if(params_.encoding == TokenEncoding::Huffman) {
            // print histogram
            size_t hist[256];
            for(size_t c = 0; c < 256; c++) hist[c] = 0;

            for(auto c : tokens_) {
                ++hist[(uint8_t)c];
            }

            for(size_t c = 0; c < 256; c++) {
                if(hist[c]) std::cout << "\t\t0x" << std::hex << c << std::dec << " -> " << hist[c] << std::endl;
            }

            std::cout << "\t\ttokens:" << std::endl;
            for(auto c : tokens_) {
                std::cout << c << ",";
            }
            std::cout << std::endl;
        }
        #endif
    }
};

class BlockEncodingBase {
private:
    size_t num_types_;
    std::unique_ptr<TokenBuffer[]> tokens_;

protected:
    BlockEncodingBase(TokenType const num_types) : num_types_(num_types), tokens_(std::make_unique<TokenBuffer[]>(num_types_)) {
    }

    TokenBuffer& tokens(TokenType const type) { return tokens_[type]; }

    size_t num_types() const { return num_types_; }

public:
    auto& params(TokenType const type) {
        return tokens_[type].params();
    }
};

template<iopp::BitSink Sink>
class BlockEncoder : public BlockEncodingBase {
private:
    Sink* sink_;
    size_t max_block_size_;

    std::vector<TokenType> token_types_;

    size_t cur_tokens_;
    bool print_stats_;

    void overflow() {
        // write block header
        assert(cur_tokens_ > 0);
        assert(cur_tokens_ <= max_block_size_);

        bool const small_block = cur_tokens_ < max_block_size_;
        sink_->write(small_block);
        if(small_block) code::Binary::encode(*sink_, cur_tokens_ - 1, code::Universe(max_block_size_));

        // print block stats
        #ifndef NDEBUG
        if(print_stats_) {
            std::cout << "BLOCK STATS" << std::endl;
            for(size_t j = 0; j < num_types(); j++) {
                std::cout << "\ttoken type " << j << ":" << std::endl;
                tokens(j).print_stats();
            }
            std::cout << std::endl;
        }
        #endif

        for(size_t j = 0; j < num_types(); j++) {
            tokens(j).prepare_encode(*sink_);
        }

        // write tokens
        for(auto j : token_types_) {
            tokens(j).encode_next(*sink_);
        }

        // 
        for(size_t j = 0; j < num_types(); j++) {
            tokens(j).clear();
        }
        token_types_.clear();
        cur_tokens_ = 0;
    }

public:
    BlockEncoder(Sink& sink, TokenType const num_types, size_t const max_block_size, bool print_stats = false)
        : BlockEncodingBase(num_types),
          sink_(&sink),
          max_block_size_(max_block_size),
          cur_tokens_(0),
          print_stats_(print_stats) {
    
        token_types_.reserve(max_block_size_);

        // header
        code::Binary::encode(sink, max_block_size_, code::Universe::of<uint32_t>());
    }

    void write_uint(TokenType const type, Token const token) {
        token_types_.push_back(type);
        tokens(type).push_back(token);

        ++cur_tokens_;
        if(cur_tokens_ >= max_block_size_) {
            assert(cur_tokens_ == max_block_size_);
            overflow();
        }
    }

    void write_char(TokenType const type, char const c) {
        write_uint(type, Token((uint8_t)c));
    }

    void flush() {
        if(cur_tokens_ > 0) overflow();
    }
};

template<iopp::BitSource Src>
class BlockDecoder : public BlockEncodingBase {
private:
    Src* src_;
    size_t max_block_size_;
    
    size_t cur_block_size_;
    size_t next_token_;

    void underflow() {
        if(*src_) {
            bool const small_block = src_->read();
            cur_block_size_ = small_block ? (code::Binary::decode(*src_, code::Universe(max_block_size_)) + 1) : max_block_size_;

            for(size_t j = 0; j < num_types(); j++) {
                tokens(j).clear();
                tokens(j).prepare_decode(*src_);
            }
        } else {
            cur_block_size_ = 0;
        }
        next_token_ = 0;
    }

public:
    BlockDecoder(Src& src, TokenType const num_types)
        : BlockEncodingBase(num_types),
          src_(&src),
          cur_block_size_(0),
          next_token_(0) {

        // header
        max_block_size_ = code::Binary::decode(src, code::Universe::of<uint32_t>());
    }

    uintmax_t read_uint(TokenType const type) {
        if(next_token_ >= cur_block_size_) {
            underflow();
        }

        ++next_token_;
        return tokens(type).decode_next(*src_);
    }

    char read_char(TokenType const type) {
        return (char)read_uint(type);
    }
};
