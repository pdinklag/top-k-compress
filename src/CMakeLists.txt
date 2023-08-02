add_library(topk INTERFACE)
target_link_libraries(topk INTERFACE tdc tlx unordered_dense pm-malloc)

add_executable(encode encode.cpp)
target_link_libraries(encode topk)

add_executable(rle rle.cpp)
target_link_libraries(rle topk)

add_executable(topk-lz77 topk_lz77.cpp)
target_link_libraries(topk-lz77 topk)

add_executable(topk-lz77-fast topk_lz77_fast.cpp)
target_link_libraries(topk-lz77-fast topk)

add_executable(topk-lz78 topk_lz78.cpp)
target_link_libraries(topk-lz78 topk)

add_executable(topk-lzend topk_lzend.cpp)
target_link_libraries(topk-lzend topk word-packing)

add_executable(topk-model topk_model.cpp)
target_link_libraries(topk-model topk word-packing)

add_executable(topk-sample topk_sample.cpp)
target_link_libraries(topk-sample topk word-packing)

add_executable(topk-sel topk_sel.cpp)
target_link_libraries(topk-sel topk)

add_executable(topk-sss topk_sss.cpp)
target_link_libraries(topk-sss topk word-packing)

add_executable(lz77-lpf lz77_lpf.cpp)
target_link_libraries(lz77-lpf topk)

add_executable(gzip-tdc gzip_tdc.cpp)
target_link_libraries(gzip-tdc topk)

add_executable(lz78 lz78.cpp)
target_link_libraries(lz78 topk)

add_executable(lzend lzend.cpp)
target_link_libraries(lzend topk word-packing)

add_executable(lzend-kk lzend_kk.cpp)
target_link_libraries(lzend-kk topk word-packing)

add_executable(lzend-kkl lzend_kkl.cpp)
target_link_libraries(lzend-kkl topk word-packing)

add_executable(lzend-blockwise lzend_blockwise.cpp)
target_link_libraries(lzend-blockwise topk word-packing)
