#include "tdc_compressor.hpp"

#include <tdc/lz/gzip9_factorizer.hpp>

struct Compressor : public TdcCompressor {
    Compressor() : TdcCompressor("gzip-tdc", "GZip provided by tudocomp2") {
    }

    virtual void init_result(pm::Result& result) override {
        TdcCompressor::init_result(result);
        result.add("algo", "gzip-tdc");
    }

    virtual std::string file_ext() override {
        return ".gzip";
    }

    virtual std::vector<tdc::lz::Factor> factorize(iopp::FileInputStream& in) override {
        std::vector<tdc::lz::Factor> factors;
        tdc::lz::Gzip9Factorizer factorizer;
        factorizer.factorize(in.begin(), in.end(), std::back_inserter(factors));
        return factors;
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
