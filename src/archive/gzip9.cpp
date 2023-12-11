#include "../lz77_compressor.hpp"

#include <lz77/gzip9_factorizer.hpp>

struct Compressor : public Lz77Compressor {
    Compressor() : Lz77Compressor("gzip-tdc", "GZip provided by tudocomp2") {
    }

    virtual void init_result(pm::Result& result) override {
        Lz77Compressor::init_result(result);
        result.add("algo", "gzip-tdc");
    }

    virtual std::string file_ext() override {
        return ".gzip";
    }

    virtual void factorize(iopp::FileInputStream& in, FactorWriter& out) override {
        lz77::Gzip9Factorizer factorizer;
        factorizer.factorize(in.begin(), in.end(), out);
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
