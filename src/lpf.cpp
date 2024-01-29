#include "lz77_compressor.hpp"

#include <filesystem>
#include <lz77/lpf_factorizer.hpp>

struct Compressor : public Lz77Compressor {
    unsigned int threshold = 2;

    Compressor() : Lz77Compressor("lz77-lpf", "Computes the exact LZ77 factorization using the LPF array") {
        param('t', "threshold", threshold, "The minimum reference length");
    }

    virtual void init_result(pm::Result& result) override {
        Lz77Compressor::init_result(result);
        result.add("algo", "lz77-lpf");
        result.add("threshold", threshold);
    }

    virtual std::string file_ext() override {
        return ".lpf";
    }

    virtual void factorize(iopp::FileInputStream& in, FactorWriter& out) override {
        // fully read file into RAM
        std::string s;
        s.reserve(std::filesystem::file_size(input));
        std::copy(in.begin(), in.end(), std::back_inserter(s));

        lz77::LPFFactorizer factorizer;
        factorizer.min_reference_length(threshold);
        factorizer.factorize(s.begin(), s.end(), out);
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
