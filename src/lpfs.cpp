#include "lz77_compressor.hpp"

#include <lpf_semi_external_factorizer.hpp>

struct Compressor : public Lz77Compressor {
    unsigned int threshold = 2;

    Compressor() : Lz77Compressor("lpfs", "Computes the exact LZ77 factorization using the LPF array") {
        param('t', "threshold", threshold, "The minimum reference length");
    }

    virtual void init_result(pm::Result& result) override {
        Lz77Compressor::init_result(result);
        result.add("algo", "lpfs");
        result.add("threshold", threshold);
    }

    virtual std::string file_ext() override {
        return ".lpfs";
    }

    virtual void factorize(iopp::FileInputStream& in, FactorWriter& out) override {
        // fully read file into RAM
        std::string s;
        std::copy(in.begin(), in.end(), std::back_inserter(s));
        s.shrink_to_fit();

        LPFSemiExternalFactorizer factorizer;
        factorizer.min_reference_length(threshold);
        factorizer.factorize(s.begin(), s.end(), out);
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
