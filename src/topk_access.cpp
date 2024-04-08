#include <oocmd.hpp>
#include <pm.hpp>
#include <iopp/load_file.hpp>

#include "topk_access.hpp"
#include <si_iec_literals.hpp>

using namespace oocmd;

struct Options : public ConfigObject {
    uint64_t k = 1_Mi;
    bool decode_benchmark = false;

    Options() : ConfigObject("acccess", "Access experiments") {
        param('k', "num-frequent", k, "The number of frequent substrings to maintain.");
        param("dec", decode_benchmark, "Decode the access data structure.");
    }
};

Options options;

int main(int argc, char** argv) {
    Application app(options, argc, argv);
    if(app) {
        if(app.args().size() == 1) {
            pm::Stopwatch sw;
            pm::MallocCounter mem;
            pm::Result r;

            // construct
            std::cout << "constructing top-k access ..." << std::endl;

            mem.start();
            sw.start();
            TopKAccess text(app.args()[0], options.k);
            sw.stop();
            mem.stop();

            auto const t_topk_construct = (uintmax_t)sw.elapsed_time_millis();
            auto const mem_topk_peak = mem.peak();
            auto const mem_topk_final = mem.count();

            r.add("n", text.length());
            r.add("h", text.height());
            r.add("z", text.num_phrases());
            r.add("z_literal", text.num_literals());
            r.add("t_topk_construct", t_topk_construct);
            r.add("mem_topk_peak", mem_topk_peak);
            r.add("mem_topk_final", mem_topk_final);
            
            {
                auto a = text.alloc_size();
                r.add("alloc", a.total());
                r.add("alloc_parent", a.parent);
                r.add("alloc_inlabel", a.inlabel);
                r.add("alloc_parsing", a.parsing);
                r.add("alloc_literal", a.literal);
                r.add("alloc_start", a.start);
                r.add("alloc_start_rank", a.start_rank);
            }

            if(options.decode_benchmark) {
                // decode one by one
                std::cout << "decoding top-k access (one by one) ..." << std::endl;
                {
                    std::string dec;
                    sw.start();
                    {
                        size_t const n = text.length();
                        dec.reserve(n);

                        for(size_t i = 0; i < n; i++) {
                            dec.push_back(text[i]);
                        }
                    }
                    sw.stop();
                    auto const t_topk_decode_1by1 = (uintmax_t)sw.elapsed_time_millis();

                    uintmax_t chk_decode_1by1 = 0;
                    for(auto const c : dec) chk_decode_1by1 += (uint8_t)c;

                    r.add("t_topk_decode_1by1", t_topk_decode_1by1);
                    r.add("chk_decode_1by1", chk_decode_1by1);
                }

                // decode using iterator
                std::cout << "decoding top-k access (iterator) ..." << std::endl;
                {
                    std::string dec;
                    sw.start();
                    {
                        dec.reserve(text.length());
                        for(auto const c : text) dec.push_back(c);
                    }
                    sw.stop();
                    auto const t_topk_decode_iter = (uintmax_t)sw.elapsed_time_millis();

                    uintmax_t chk_decode_iter = 0;
                    for(auto const c : dec) chk_decode_iter += (uint8_t)c;

                    r.add("t_topk_decode_iter", t_topk_decode_iter);
                    r.add("chk_decode_iter", chk_decode_iter);
                }

                // comparison: load file and copy string
                std::cout << "copying std::string one by one ..." << std::endl;
                {
                    auto const str = iopp::load_file_str(app.args()[0]);
                    sw.start();
                    {
                        std::string cpy;
                        cpy.reserve(str.length());
                        for(auto const c : str) cpy.push_back(c);
                    }
                    sw.stop();
                    auto const t_str_copy = (uintmax_t)sw.elapsed_time_millis();

                    uintmax_t chk_str = 0;
                    for(auto const c : str) chk_str += (uint8_t)c;

                    r.add("t_str_copy", t_str_copy);
                    r.add("chk_str", chk_str);
                }                
            }

            r.print();
            return 0;
        } else {
            app.print_usage(options);
        }
    }
    return -1;
}
