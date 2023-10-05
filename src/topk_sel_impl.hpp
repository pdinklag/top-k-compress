#include <tlx/container/ring_buffer.hpp>

#include <old/topk_substrings.hpp>
#include <old/topk_trie_node.hpp>
#include <topk_header.hpp>
#include <block_coding.hpp>
#include <pm/result.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'S') << 24 |
    ((uint64_t)'E') << 16 |
    ((uint64_t)'L') << 8 |
    ((uint64_t)'#');

constexpr bool DEBUG = false;
constexpr bool PROTOCOL = false;

constexpr size_t MIN_REF = 1;

constexpr TokenType TOK_TRIE_REF = 0;
constexpr TokenType TOK_LITERAL = 1;

using Topk = TopKSubstrings<TopkTrieNode<>, true>;

void setup_encoding(BlockEncodingBase& enc, size_t const k) {
    enc.register_binary(k-1);        // TOK_TRIE_REF
    enc.register_binary(255, false); // TOK_LITERAL
}

template<iopp::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_sel(In begin, In const& end, Out out, size_t const k, size_t const window_size, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size, pm::Result& result) {
    // write header
    TopkHeader header(k, window_size, num_sketches, sketch_rows, sketch_columns);
    header.encode(out, MAGIC);

    // initialize compression
    // - frequent substring 0 is reserved to indicate a literal character
    Topk topk(k - 1, num_sketches, sketch_rows, sketch_columns);

    // initialize encoding
    BlockEncoder enc(out, block_size);
    setup_encoding(enc, k);

    struct NewNode {
        size_t index;
        size_t pos;
    };
    tlx::RingBuffer<NewNode> new_nodes((window_size * (window_size + 1)) / 2); // w(w+1)/2 is the maximum number of nodes we can possibly create within a window

    Topk::StringState s[window_size];
    Topk::StringState match[window_size];
    for(size_t j = 0; j < window_size; j++) {
        s[j] = topk.empty_string();
        match[j] = topk.empty_string();
    }
    size_t longest = 0;

    size_t num_frequent = 0;
    size_t num_literal = 0;

    size_t max_freq_len = 0;
    size_t max_freq_val = 0;
    size_t total_freq_len = 0;
    size_t total_freq_val = 0;

    size_t i = 0;
    size_t next_phrase = 0;

    auto handle = [&](char const c, size_t len) {
        if constexpr(DEBUG) {
            std::cout << "read next character: " << display(c) << ", i=" << i << ", next_phrase=" << next_phrase << ", longest=" << longest << std::endl;
        }

        // update the w cursors and find the maximum current match
        size_t const num_active_windows = std::min(window_size, i + 1);
        for(size_t j = 0; j < num_active_windows; j++) {
            if(s[j].frequent) {
                s[j] = topk.extend(s[j], c);
                if(s[j].frequent) {
                    match[j] = s[j];
                }
                if(s[j].new_node) {
                    assert(i + 1 >= s[j].len);
                    new_nodes.push_back({ s[j].node, i + 1 - s[j].len });
                }
            }
        }

        // test if our buffers are full
        if(i + 1 >= window_size) {
            // decide whether something must be encoded now
            assert(i + 1 - window_size <= next_phrase); // if this doesn't hold, we missed something
            if(i + 1 - window_size == next_phrase) {
                if constexpr(DEBUG) std::cout << "- [ENCODE] ";

                // our longest phrase is now exactly w long; encode whatever is possible
                auto phrase_index = match[longest].node;
                auto phrase_len = match[longest].len;

                if(phrase_len >= MIN_REF && phrase_index > 0) {
                    // check if the phrase node was only recently created
                    for(size_t j = 0; j < new_nodes.size(); j++) {
                        auto const& recent = new_nodes[j];
                        if(recent.index == phrase_index) {
                            // it has - determine the maximum possible length for the phrase
                            size_t const max_len = next_phrase - recent.pos;
                            if(phrase_len > max_len) {
                                // phrase is too long, limit
                                phrase_index = topk.limit(match[longest], max_len);
                                phrase_len = max_len;

                                if constexpr(DEBUG) {
                                    std::cout << "(LIMITED from index=" << phrase_index << ", length=" << phrase_len << ") ";
                                }
                            }
                            break;
                        }
                    }

                    if constexpr(DEBUG) {
                        std::cout << "frequent phrase: index=" << phrase_index << ", length=" << phrase_len << std::endl;
                    }
                    if constexpr(PROTOCOL) {
                        std::cout << "(" << phrase_index << ") / " << phrase_len << std::endl;
                    }

                    assert(phrase_index > 0);
                    enc.write_uint(TOK_TRIE_REF, phrase_index);

                    ++num_frequent;
                    next_phrase += phrase_len;

                    total_freq_len += phrase_len;
                    max_freq_len = std::max(max_freq_len, (size_t)phrase_len);

                    total_freq_val += phrase_index;
                    max_freq_val = std::max(max_freq_val, (size_t)phrase_index);
                } else {
                    auto const x = s[longest].first;

                    if constexpr(DEBUG) {
                        std::cout << "literal phrase: " << display(x) << std::endl;
                    }
                    if constexpr(PROTOCOL) {
                        std::cout << display(x) << std::endl;
                    }

                    enc.write_uint(TOK_TRIE_REF, 0);
                    enc.write_char(TOK_LITERAL, x);

                    ++num_literal;
                    ++next_phrase;
                }
            }

            // advance longest
            topk.drop_out(s[longest]);
            s[longest] = topk.empty_string();
            match[longest] = topk.empty_string();
            longest = (longest + 1) % window_size;
        }

        // discard outdated entries in new_nodes
        if(i + 1 >= 2 * window_size) {
            auto const first_pos = i + 1 - 2 * window_size;
            while(!new_nodes.empty() && new_nodes.front().pos < first_pos) {
                new_nodes.pop_front();
            }
        }

        // advance position
        ++i;
    };
    
    while(begin != end) {
        // read next character
        handle(*begin++, window_size);
    }

    // pad trailing zeroes (is this needed?)
    for(size_t x = 0; x < window_size - 1; x++) {
        handle(0, window_size - 1 - x);
    }

    enc.flush();

    topk.print_debug_info();
    
    result.add("phrases_total", num_frequent + num_literal);
    result.add("phrases_ref", num_frequent);
    result.add("phrases_literal", num_literal);
    result.add("phrases_longest", max_freq_len);
    result.add("phrases_furthest", max_freq_val);
    result.add("phrases_avg_len", std::round(100.0 * ((double)total_freq_len / (double)num_frequent)) / 100.0);
    result.add("phrases_avg_dist", std::round(100.0 * ((double)total_freq_val / (double)num_frequent)) / 100.0);

    // topk.dump();
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void topk_decompress_sel(In in, Out out) {
    // decode header
    TopkHeader header(in, MAGIC);
    auto const k = header.k;
    auto const window_size = header.window_size;
    auto const num_sketches = header.num_sketches;
    auto const sketch_rows = header.sketch_rows;
    auto const sketch_columns = header.sketch_columns;

    // initialize decompression
    // - frequent substring 0 is reserved to indicate a literal character
    Topk topk(k - 1, num_sketches, sketch_rows, sketch_columns);

    // initialize decoding
    BlockDecoder dec(in);
    setup_encoding(dec, k);

    Topk::StringState s[window_size];
    for(size_t j = 0; j < window_size; j++) {
        s[j] = topk.empty_string();
    }
    size_t longest = 0;
    size_t n = 0;

    auto handle = [&](char const c){
        // emit character
        *out++ = c;

        // update the w cursors and find the maximum current match
        size_t const num_active_windows = std::min(window_size, n + 1);
        for(size_t j = 0; j < num_active_windows; j++) {
            if(s[j].frequent) s[j] = topk.extend(s[j], c);
        }

        if(n + 1 >= window_size) {
            // advance longest
            topk.drop_out(s[longest]);
            s[longest] = topk.empty_string();
            longest = (longest + 1) % window_size;
        }

        // advance position
        ++n;
    };

    char frequent_string[window_size];
    size_t num_frequent = 0;
    size_t num_literal = 0;

    while(in) {
        auto const p = dec.read_uint(TOK_TRIE_REF);

        if(p) {
            // decode frequent phrase
            ++num_frequent;

            auto const len = topk.get(p, frequent_string);
            if constexpr(PROTOCOL) {
                std::cout << "(" << p << ") / " << len << std::endl;
            }

            for(size_t i = 0; i < len; i++) {
                handle(frequent_string[i]);
            }
        } else {
            // decode literal phrase
            ++num_literal;
            char const c = dec.read_char(TOK_LITERAL);
            if constexpr(PROTOCOL) {
                std::cout << display(c) << std::endl;
            }
            handle(c);
        }
    }
}
