add_subdirectory(code)
add_subdirectory(iopp)
add_subdirectory(lz77)
add_subdirectory(oocmd)
add_subdirectory(ordered)
add_subdirectory(pm)
add_subdirectory(word-packing)

add_library(rans INTERFACE)
target_include_directories(rans INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/ryg_rans)

add_library(sdsl INTERFACE)
target_include_directories(sdsl INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/sdsl-lite/include)

add_library(unordered_dense INTERFACE)
target_include_directories(unordered_dense INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/unordered_dense/include)
