#include "compressor_base.hpp"
#include "lzlike_decompress.hpp"

#include <tdc/lz/factor.hpp>

#include <phrase_block_writer.hpp>
#include <iopp/util/output_iterator_base.hpp>

#include <iterator>
#include <vector>

struct TdcCompressor : public CompressorBase {
    using Factor = tdc::lz::Factor;

    struct FactorWriter : iopp::OutputIteratorBase<Factor> {
        using IteratorBase = OutputIteratorBase<Factor>;

        // type declarations required to satisfy std::output_iterator<Item>
        using IteratorBase::iterator_category;
        using IteratorBase::difference_type;
        using IteratorBase::value_type;
        using IteratorBase::pointer;
        using IteratorBase::reference;

        using WriteFunc = std::function<void(Factor)>;
        WriteFunc write_func_;

        FactorWriter(WriteFunc write_func) : write_func_(write_func) {
        }

        // item access
        using IteratorBase::operator*;

        // postfix incrementation
        auto operator++(int) { return iopp::LatentWriter<Factor, FactorWriter>(*this); }

        // prefix incrementation
        FactorWriter& operator++() {
            auto& item = **this;
            write_func_(item);
            return *this;
        }
    };

    TdcCompressor(std::string&& type_name, std::string&& desc) : CompressorBase(std::move(type_name), std::move(desc)) {
    }

    virtual void factorize(iopp::FileInputStream& in, FactorWriter& out) = 0;

    virtual void compress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        // initialize encoding
        auto bitout = iopp::bitwise_output_to(out);
        bitout.write(LZLIKE_MAGIC, 64);
        PhraseBlockWriter writer(bitout, block_size, true);

        FactorWriter factor_writer([&](Factor f){
            if(f.is_literal()) {
                writer.write_len(0);
                writer.write_literal(f.literal());
            } else {
                writer.write_len(f.len);
                writer.write_ref(f.src);
            }
        });

        factorize(in, factor_writer);
        writer.flush();
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        lz77like_decompress(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};
