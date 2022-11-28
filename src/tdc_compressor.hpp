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

        size_t num_ref = 0;
        size_t num_literal = 0;
        size_t longest = 0;
        size_t total_ref_len = 0;
        size_t furthest = 0;
        size_t total_ref_dist = 0;

        FactorWriter factor_writer([&](Factor f){
            if(f.is_literal()) {
                ++num_literal;

                writer.write_len(0);
                writer.write_literal(f.literal());
            } else {
                ++num_ref;

                total_ref_dist += f.src;
                furthest = std::max(furthest, (size_t)f.src);
                total_ref_len += f.len;
                longest = std::max(longest, (size_t)f.len);

                writer.write_len(f.len);
                writer.write_ref(f.src);
            }
        });

        factorize(in, factor_writer);
        writer.flush();

        result.add("phrases_total", num_ref + num_literal);
        result.add("phrases_ref", num_ref);
        result.add("phrases_literal", num_literal);
        result.add("phrases_longest", longest);
        result.add("phrases_furthest", furthest);
        result.add("phrases_avg_len", std::round(100.0 * ((double)total_ref_len / (double)num_ref)) / 100.0);
        result.add("phrases_avg_dist", (uint64_t)std::round((double)total_ref_dist / (double)num_ref));
    }
    
    virtual void decompress(iopp::FileInputStream& in, iopp::FileOutputStream& out, pm::Result& result) override {
        lz77like_decompress(iopp::bitwise_input_from(in.begin(), in.end()), iopp::StreamOutputIterator(out));
    }
};
