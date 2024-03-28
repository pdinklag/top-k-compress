#include <oocmd.hpp>
#include <pm.hpp>

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
            std::cout << "constructing ..." << std::endl;
            pm::Stopwatch sw;
            pm::MallocCounter mem;

            mem.start();
            sw.start();
            TopKAccess text(app.args()[0], options.k);
            sw.stop();
            mem.stop();

            auto const t_construct = (uintmax_t)sw.elapsed_time_millis();
            auto const mem_peak = mem.peak();
            auto const mem_text = mem.count();

            // decode one by one
            std::cout << "decoding one by one ..." << std::endl;
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
            auto const t_decode_1by1 = (uintmax_t)sw.elapsed_time_millis();

            {
                iopp::FileOutputStream fos(app.args()[0] + ".dec");
                fos.write(dec.data(), dec.length());
            }

            pm::Result r;
            r.add("n", text.length());
            r.add("z", text.num_phrases());
            r.add("time_construct", t_construct);
            r.add("time_decode_1by1", t_decode_1by1);
            r.add("mem_peak", mem_peak);
            r.add("mem_text", mem_text);

            r.print();
            return 0;
        } else {
            app.print_usage(options);
        }
    }
    return -1;
}
