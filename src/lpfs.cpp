#include "lz77_compressor.hpp"

#include <lpf_semi_external_factorizer.hpp>

struct Compressor : public Lz77Compressor {
    using Factor = lz77::Factor;

    unsigned int threshold = 2;
    bool keep_index = false;

    Compressor() : Lz77Compressor("lpfs", "Computes the exact LZ77 factorization using the LPF array") {
        param('t', "threshold", threshold, "The minimum reference length");
        param("keep-index", keep_index, "Keep the suffix array and its inverse");
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
        s.reserve(std::filesystem::file_size(input));
        std::copy(in.begin(), in.end(), std::back_inserter(s));

        LPFSemiExternalFactorizer factorizer;
        factorizer.min_reference_length(threshold);

        if(keep_index) {
            factorizer.sa_path(input + ".sa5");
            factorizer.isa_path(input + ".isa5");
        }

        factorizer.factorize(s.begin(), s.end(), out, keep_index);
    }
};

int main(int argc, char** argv) {
    Compressor c;
    return Application::run(c, argc, argv);
}
