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
            pm::Stopwatch sw;
            pm::MallocCounter mem;

            mem.start();
            sw.start();
            TopKAccess text(app.args()[0], options.k);
            sw.stop();
            mem.stop();

            pm::Result r;
            r.add("num_phrases", text.num_phrases());
            r.add("time_construct", (uintmax_t)sw.elapsed_time_millis());
            r.add("mem_peak", mem.peak());
            r.add("mem_text", mem.count());

            r.print();
            return 0;
        } else {
            app.print_usage(options);
        }
    }
    return -1;
}
