add_subdirectory(tudocomp2)

add_library(word-packing INTERFACE)
target_include_directories(word-packing INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/word-packing/include)

add_library(unordered_dense INTERFACE)
target_include_directories(unordered_dense INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/unordered_dense/include)
