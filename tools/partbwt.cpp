#include <string_view>

#include <oocmd.hpp>

#include <display.hpp>
#include <iopp/load_file.hpp>
#include <tdc/text/util.hpp>

using namespace oocmd;

struct Options : public ConfigObject {
    size_t block = 4096;

    Options() : ConfigObject("partbwt", "Partial BWT test") {
        param('b', "block", block, "The block size.");
    }
};

Options options;

size_t bwt_runs(std::string_view const& s, uint32_t const* sa, size_t const n) {
    size_t r = 0;
    char prev = 0;

    for(size_t i = 0; i < n; i++) {
        auto const sa_i = sa[i];
        auto const c = (sa_i > 0) ? s[sa_i - 1] : s[s.length() - 1];
        if(i == 0 || c != prev) {
            ++r;
        }
        prev = c;
    }
    return r;
}

int main(int argc, char** argv) {
    Application app(options, argc, argv);
    if(app) {
        if(!app.args().empty()) {
            std::cout << "reading input file ..." << std::endl;
            auto const& input = app.args()[0];
            auto const s = iopp::load_file_str(input);
            auto const n = s.length();

            std::cout << "computing suffix array ..." << std::endl;
            auto const sa = tdc::text::suffix_array_u32(s.begin(), s.end());

            std::cout << "computing inverse ..." << std::endl;
            auto const isa = std::make_unique<uint32_t[]>(n);
            for(size_t i = 0; i < n; i++) {
                isa[sa[i]] = i;
            }

            auto const partial_sa = std::make_unique<uint32_t[]>(options.block);
            for(size_t i = 0; i < n; i += options.block) {
                auto const blocksize = std::min(size_t(options.block), n - i);
                std::cout << "processing block #" << (i/options.block) << " at i=" << i << ", size=" << blocksize << " ..." << std::endl;
 
                auto const block = std::string_view(s).substr(i, blocksize);

                std::cout << "\tcomputing partial suffix array ..." << std::endl;
                for(size_t j = 0; j < blocksize; j++) {
                    auto const x = sa[isa[i + j]];
                    assert(x >= i);
                    assert(x < i + blocksize);
                    partial_sa[j] = x - i;
                }

                auto const partial_r = bwt_runs(block, partial_sa.get(), blocksize);
                std::cout << "\t-> runs in partial BWT: " << partial_r << std::endl;

                std::cout << "\tcomputing block suffix array ..." << std::endl;
                auto const block_sa = tdc::text::suffix_array_u32(block.begin(), block.end());
                auto const block_r = bwt_runs(block, block_sa.get(), blocksize);
                std::cout << "\t-> runs in block BWT: " << block_r << std::endl;
            }
        } else {
            app.print_usage(options);
            return -1;
        }
    }
    return 0;

    if(argc < 2) {
        std::cerr << "usage: " << argv[0] << " [FILE]" << std::endl;
        return -1;
    }


    

}
