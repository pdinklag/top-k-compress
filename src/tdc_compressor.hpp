#include "compressor_base.hpp"
#include "lzlike_decompress.hpp"

#include <tdc/lz/factor.hpp>

#include <phrase_block_writer.hpp>

#include <iterator>
#include <vector>

struct TdcCompressor : public CompressorBase {
    TdcCompressor(std::string&& type_name, std::string&& desc) : CompressorBase(std::move(type_name), std::move(desc)) {
    }

    virtual std::vector<tdc::lz::Factor> factorize(iopp::FileInputStream& in) = 0;

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        // compute factorization
        auto factors = factorize(in);

        // initialize encoding
        auto bitout = iopp::bitwise_output_to(out);
        bitout.write(LZLIKE_MAGIC, 64);
        PhraseBlockWriter writer(bitout, block_size, true);

        size_t n = 0;
        for(auto const& f : factors) {
            if(f.is_literal()) {
                writer.write_len(0);
                writer.write_literal(f.literal());
            } else {
                writer.write_len(f.len);
                writer.write_ref(f.src);
            }
            n += f.num_literals();
        }
        writer.flush();
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        lz77like_decompress(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};
