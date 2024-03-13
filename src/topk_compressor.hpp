#include "compressor_base.hpp"
#include <si_iec_literals.hpp>

struct TopkCompressor : public CompressorBase {
    uint64_t k = 1_Mi;
    uint64_t max_freq = 1_Ki;

    TopkCompressor(std::string&& type_name, std::string&& desc) : CompressorBase(std::move(type_name), std::move(desc)) {
        param('k', "num-frequent", k, "The number of frequent substrings to maintain.");
        param('c', "max-freq", max_freq, "The maximum frequency of a frequent pattern.");
    }

    virtual void init_result(pm::Result& result) {
        CompressorBase::init_result(result);
        result.add("k", k);
        result.add("max_freq", max_freq);
    }
};
