add_executable(lz77-blockwise lz77_blockwise.cpp)
target_link_libraries(lz77-blockwise topk word-packing)

add_library(topk INTERFACE)
target_link_libraries(topk INTERFACE tdc ordered tlx unordered_dense pm-malloc rans)

add_executable(topk-lz77 topk_lz77.cpp)
target_link_libraries(topk-lz77 topk word-packing)

add_executable(topk-lz78 topk_lz78.cpp)
target_link_libraries(topk-lz78 topk)

add_executable(topk-psample topk_psample.cpp)
target_link_libraries(topk-psample topk word-packing)

add_executable(topk-sample topk_sample.cpp)
target_link_libraries(topk-sample topk word-packing)

add_executable(topk-sel topk_sel.cpp)
target_link_libraries(topk-sel topk)

option(BUILD_ALL "build ALL algorithms, even those no longer relevant" OFF)
message(STATUS "BUILD_ALL=${BUILD_ALL}")

if(BUILD_ALL)
    # rarely used
    add_executable(lpf lpf.cpp)
    target_link_libraries(lpf topk)
    
    add_executable(lz78 lz78.cpp)
    target_link_libraries(lz78 topk)

    add_executable(gzip-tdc archive/gzip_tdc.cpp)
    target_link_libraries(gzip-tdc topk)
    
    # archive of older, no longer really relevant implementations
    add_executable(encode archive/encode.cpp)
    target_link_libraries(encode topk)

    add_executable(rle archive/rle.cpp)
    target_link_libraries(rle topk)

    add_executable(topk-sss archive/topk_sss.cpp)
    target_link_libraries(topk-sss topk word-packing)

    add_executable(topk-weiner-lz77 archive/topk_weiner_lz77.cpp)
    target_link_libraries(topk-weiner-lz77 topk)

    add_executable(topk-weiner-lz77-fast archive/topk_weiner_lz77_fast.cpp)
    target_link_libraries(topk-weiner-lz77-fast topk)

    add_executable(topk-lzend archive/topk_lzend.cpp)
    target_link_libraries(topk-lzend topk word-packing)

    add_executable(lzend archive/lzend.cpp)
    target_link_libraries(lzend topk word-packing)

    add_executable(lzend-kk archive/lzend_kk.cpp)
    target_link_libraries(lzend-kk topk word-packing)

    add_executable(lzend-kkl archive/lzend_kkl.cpp)
    target_link_libraries(lzend-kkl topk word-packing)

    add_executable(lzend-blockwise archive/lzend_blockwise.cpp)
    target_link_libraries(lzend-blockwise topk word-packing)
endif()
