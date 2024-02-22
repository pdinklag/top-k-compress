#include "compressor_base.hpp"
#include <si_iec_literals.hpp>

struct TopkCompressor : public CompressorBase {
    uint64_t k = 1_Mi;
    uint64_t sketch_rows = 2;
    uint64_t sketch_columns = 1_Mi;

    TopkCompressor(std::string&& type_name, std::string&& desc) : CompressorBase(std::move(type_name), std::move(desc)) {
        param('k', "num-frequent", k, "The number of frequent substrings to maintain.");
        param('r', "sketch-rows", sketch_rows, "The number of rows in the Count-Min sketch.");
        param('c', "sketch-columns", sketch_columns, "The total number of columns in each Count-Min row.");
    }

    virtual void init_result(pm::Result& result) {
        CompressorBase::init_result(result);
        result.add("k", k);
        result.add("sketch_columns", sketch_columns);
    }
};
