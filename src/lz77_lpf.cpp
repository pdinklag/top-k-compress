#include "tdc_compressor.hpp"

#include <tdc/lz/lpf_factorizer.hpp>

struct Compressor : public TdcCompressor {
    unsigned int threshold = 2;

    Compressor() : TdcCompressor("lz77-lpf", "Computes the exact LZ77 factorization using the LPF array") {
        param('t', "threshold", threshold, "The minimum reference length");
    }

    virtual void init_result(pm::Result& result) override {
        TdcCompressor::init_result(result);
        result.add("algo", "lz77-lpf");
        result.add("threshold", threshold);
    }

    virtual std::string file_ext() override {
        return ".lpf";
    }

    virtual void factorize(iopp::FileInputStream& in, FactorWriter& out) override {
        // fully read file into RAM
        std::string s;
        std::copy(in.begin(), in.end(), std::back_inserter(s));

        tdc::lz::LPFFactorizer factorizer;
        factorizer.min_reference_length(threshold);
        factorizer.factorize(s.begin(), s.end(), out);
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
