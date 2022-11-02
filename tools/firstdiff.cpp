#include <oocmd.hpp>
#include <iopp/file_input_stream.hpp>

using namespace oocmd;

struct Options : public ConfigObject {
    bool one_based = false;
    
    Options() : ConfigObject("firstdiff", "Find the first position at which two input files differ") {
        param("one", one_based, "Make the output one-based instead of zero-based.");
    }
};

Options options;

int main(int argc, char** argv) {
    Application app(options, argc, argv);
    if(app) {
        if(app.args().size() == 2) {
            iopp::FileInputStream fis1(app.args()[0]);
            iopp::FileInputStream fis2(app.args()[1]);

            size_t i = 0;

            auto it1 = fis1.begin();
            auto it2 = fis2.begin();
            while(it1 != fis1.end() && it2 != fis2.end()) {
                if(*it1 != *it2) {
                    break;
                }

                ++i;
                ++it1;
                ++it2;
            }
            
            if(it1 == fis1.end() && it2 == fis2.end()) {
                std::cout << "the input files are equal" << std::endl;
            } else {
                std::cout << "the input files first differ at position i=" << (options.one_based ? (i+1) : i) << " (" << (options.one_based ? "one-based" : "zero-based") << ")" << std::endl;
            }
            return 0;
        } else {
            app.print_usage(options);
        }
    }
    return -1;
}
