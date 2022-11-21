#include "topk_common.hpp"
#include <tlx/container/ring_buffer.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'S') << 24 |
    ((uint64_t)'E') << 16 |
    ((uint64_t)'L') << 8 |
    ((uint64_t)'#');

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_sel(In begin, In const& end, Out out, size_t const k, size_t const window_size, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size) {
    using namespace tdc::code;

    pm::MallocCounter malloc_counter;
    malloc_counter.start();

    // write header
    TopkHeader header(k, window_size, num_sketches, sketch_rows, sketch_columns, false);
    header.encode(out, MAGIC);

    // initialize compression
    // - frequent substring 0 is reserved to indicate a literal character
    using Topk = TopKSubstrings<TopkTrieNode<>>;
    Topk topk(k, num_sketches, sketch_rows, sketch_columns);

    // initialize encoding
    PhraseBlockWriter writer(out, block_size);

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
    size_t total_freq_len = 0;

    size_t i = 0;
    size_t next_phrase = 0;

    auto handle = [&](char const c, size_t len) {
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
                // our longest phrase is now exactly w long; encode whatever is possible
                auto phrase_index = match[longest].node;
                auto phrase_len = match[longest].len;

                if(phrase_len >= 1) {
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
                            }
                            break;
                        }
                    }

                    assert(phrase_index > 0);
                    writer.write_ref(phrase_index);

                    ++num_frequent;
                    next_phrase += phrase_len;

                    total_freq_len += phrase_len;
                    max_freq_len = std::max(max_freq_len, (size_t)phrase_len);
                } else {
                    auto const x = s[longest].first;
                    writer.write_ref(0);
                    writer.write_literal(x);

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
            while(!new_nodes.empty() && new_nodes.front().pos < i + 1 - 2 * window_size) {
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

    writer.flush();
    malloc_counter.stop();

    topk.print_debug_info();
    std::cout << "mem_peak=" << malloc_counter.peak() << std::endl;
    std::cout << "parse"
        << " n=" << (i - window_size + 1)
        << ": num_frequent=" << num_frequent
        << ", num_literal=" << num_literal
        << " -> total phrases: " << (num_frequent + num_literal)
        << ", longest_ref=" << max_freq_len
        << ", avg_ref_len=" << ((double)total_freq_len / (double)num_frequent)
        << std::endl;
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void topk_decompress_sel(In in, Out out) {
    using namespace tdc::code;

    // decode header
    TopkHeader header(in, MAGIC);
    auto const k = header.k;
    auto const window_size = header.window_size;
    auto const num_sketches = header.num_sketches;
    auto const sketch_rows = header.sketch_rows;
    auto const sketch_columns = header.sketch_columns;
    auto const huffman_coding = header.huffman_coding;

    // initialize decompression
    // - frequent substring 0 is reserved to indicate a literal character
    using Topk = TopKSubstrings<TopkTrieNode<>>;
    Topk topk(k, num_sketches, sketch_rows, sketch_columns);

    // initialize decoding
    PhraseBlockReader reader(in);

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
            assert(s[longest].len == window_size);

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
        auto const p = reader.read_ref();
        if(p) {
            // decode frequent phrase
            ++num_frequent;

            auto const len = topk.get(p, frequent_string);
            for(size_t i = 0; i < len; i++) {
                handle(frequent_string[i]);
            }
        } else {
            // decode literal phrase
            ++num_literal;
            char const c = reader.read_literal();
            handle(c);
        }
    }

    // debug
    std::cout << "decompress"
        << " k=" << k
        << " w=" << window_size
        << " s=" << num_sketches
        << " c=" << sketch_columns
        << " r=" << sketch_rows
        << " huffman=" << huffman_coding
        << ": n=" << n
        << ", num_frequent=" << num_frequent
        << ", num_literal=" << num_literal
        << " -> total phrases: " << (num_frequent + num_literal)
        << std::endl;
}
