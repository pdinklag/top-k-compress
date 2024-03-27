add_executable(lz77-blockwise lz77_blockwise.cpp)
target_link_libraries(lz77-blockwise lz77 topk word-packing)

add_executable(lz78 lz78.cpp)
target_link_libraries(lz78 topk)

add_library(topk INTERFACE)
target_link_libraries(topk INTERFACE code iopp oocmd pm-malloc rans unordered_dense)

add_executable(lpf lpf.cpp)
target_link_libraries(lpf lz77 topk)

add_executable(lpfs lpfs.cpp)
target_link_libraries(lpfs lz77 topk)
target_link_options(lpfs PRIVATE -fopenmp)

find_library(TBB_LIBRARY tbb)
if(TBB_LIBRARY)
    target_compile_definitions(lpfs PRIVATE PARALLEL_ISA)
    target_link_libraries(lpfs ${TBB_LIBRARY})
endif()

add_executable(topk-attract topk_attract.cpp)
target_link_libraries(topk-attract topk word-packing)

add_executable(topk-lz77 topk_lz77.cpp)
target_link_libraries(topk-lz77 lz77 topk word-packing)

add_executable(topk-lz78 topk_lz78.cpp)
target_link_libraries(topk-lz78 topk)

add_executable(topk-twopass topk_twopass.cpp)
target_link_libraries(topk-twopass topk word-packing)

add_executable(topk-psample topk_psample.cpp)
target_link_libraries(topk-psample topk)

add_executable(topk-access topk_access.cpp)
target_link_libraries(topk-access topk ordered word-packing)

option(BUILD_ALL "build ALL algorithms, even those no longer relevant" OFF)
message(STATUS "BUILD_ALL=${BUILD_ALL}")

if(BUILD_ALL)
    # rarely used
    add_executable(gzip9 archive/gzip9.cpp)
    target_link_libraries(gzip9 lz77 topk)
    
    # Count-Min variants
    add_executable(topk-lz77-cm archive/topk_lz77_cm.cpp)
    target_link_libraries(topk-lz77-cm lz77 topk word-packing)
    
    add_executable(topk-lz78-cm archive/topk_lz78_cm.cpp)
    target_link_libraries(topk-lz78-cm topk)
    
    add_executable(topk-psample-cm archive/topk_psample_cm.cpp)
    target_link_libraries(topk-psample-cm topk)
    
    add_executable(topk-sample-cm archive/topk_sample_cm.cpp)
    target_link_libraries(topk-sample-cm topk)

    add_executable(topk-sample archive/topk_sample.cpp)
    target_link_libraries(topk-sample topk)

    # archive of older, no longer really relevant implementations
    add_executable(encode archive/encode.cpp)
    target_link_libraries(encode topk)

    add_executable(rle archive/rle.cpp)
    target_link_libraries(rle topk)

    add_executable(topk-weiner-lz77 archive/topk_weiner_lz77.cpp)
    target_link_libraries(topk-weiner-lz77 topk)

    add_executable(topk-weiner-lz77-fast archive/topk_weiner_lz77_fast.cpp)
    target_link_libraries(topk-weiner-lz77-fast topk)

    add_executable(topk-lzend archive/topk_lzend.cpp)
    target_link_libraries(topk-lzend topk lz77 ordered word-packing)

    add_executable(lzend archive/lzend.cpp)
    target_link_libraries(lzend topk lz77 ordered word-packing)

    add_executable(lzend-kk archive/lzend_kk.cpp)
    target_link_libraries(lzend-kk topk lz77 ordered word-packing)

    add_executable(lzend-kkl archive/lzend_kkl.cpp)
    target_link_libraries(lzend-kkl topk lz77 ordered word-packing)

    add_executable(lzend-blockwise archive/lzend_blockwise.cpp)
    target_link_libraries(lzend-blockwise topk lz77 ordered word-packing)

    add_executable(topk-sel archive/topk_sel.cpp)
    target_link_libraries(topk-sel topk)
endif()
