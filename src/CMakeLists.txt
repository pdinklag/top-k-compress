add_library(topk INTERFACE)
target_link_libraries(topk INTERFACE tdc tlx unordered_dense pm-malloc)

add_executable(topk-exh topk_exh.cpp)
target_link_libraries(topk-exh topk)

add_executable(topk-lz77 topk_lz77.cpp)
target_link_libraries(topk-lz77 topk)

add_executable(topk-lz78 topk_lz78.cpp)
target_link_libraries(topk-lz78 topk)

add_executable(topk-sel topk_sel.cpp)
target_link_libraries(topk-sel topk)
