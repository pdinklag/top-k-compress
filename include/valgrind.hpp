#if __has_include(<valgrind/callgrind.h>)

#include <valgrind/callgrind.h>

#else

#define CALLGRIND_START_INSTRUMENTATION
#define CALLGRIND_STOP_INSTRUMENTATION
#define CALLGRIND_TOGGLE_COLLECT

#endif
