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

    virtual void factorize(iopp::FileInputStream& in, FactorWriter& out) override {
        tdc::lz::Gzip9Factorizer factorizer;
        factorizer.factorize(in.begin(), in.end(), out);
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}