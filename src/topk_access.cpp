#include <oocmd.hpp>
#include <pm.hpp>
#include <iopp/load_file.hpp>

#include "topk_access.hpp"
#include <si_iec_literals.hpp>

using namespace oocmd;

struct Options : public ConfigObject {
    uint64_t k = 1_Mi;

    Options() : ConfigObject("acccess", "Access experiments") {
        param('k', "num-frequent", k, "The number of frequent substrings to maintain.");
    }
};

Options options;

int main(int argc, char** argv) {
    Application app(options, argc, argv);
    if(app) {
        if(app.args().size() == 1) {
            // construct
            std::cout << "constructing top-k access ..." << std::endl;
            pm::Stopwatch sw;
            pm::MallocCounter mem;

            mem.start();
            sw.start();
            TopKAccess text(app.args()[0], options.k);
            sw.stop();
            mem.stop();

            auto const t_topk_construct = (uintmax_t)sw.elapsed_time_millis();
            auto const mem_topk_peak = mem.peak();
            auto const mem_topk_final = mem.count();

            // decode one by one
            std::cout << "decoding top-k access (one by one) ..." << std::endl;
            std::string dec;
            sw.start();
            {
                size_t n = text.length();
                
                dec.reserve(n);

                for(size_t i = 0; i < n; i++) {
                    dec.push_back(text[i]);
                }
            }
            sw.stop();
            auto const t_topk_decode_1by1 = (uintmax_t)sw.elapsed_time_millis();

            {
                iopp::FileOutputStream fos(app.args()[0] + ".dec");
                fos.write(dec.data(), dec.length());
            }

            // comparison: load file and copy string
            std::cout << "copying std::string one by one ..." << std::endl;
            auto const str = iopp::load_file_str(app.args()[0]);
            sw.start();
            {
                std::string cpy;
                cpy.reserve(str.length());
                for(auto const c : str) cpy.push_back(c);
            }
            sw.stop();
            auto const t_str_decode_1by1 = (uintmax_t)sw.elapsed_time_millis();

            // print stats
            pm::Result r;
            r.add("n", text.length());
            r.add("h", text.height());
            r.add("z", text.num_phrases());
            r.add("t_topk_construct", t_topk_construct);
            r.add("t_topk_decode_1by1", t_topk_decode_1by1);
            r.add("t_str_copy_1by1", t_str_decode_1by1);
            r.add("mem_topk_peak", mem_topk_peak);
            r.add("mem_topk_final", mem_topk_final);

            r.print();
            return 0;
        } else {
            app.print_usage(options);
        }
    }
    return -1;
}
