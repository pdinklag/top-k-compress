add_executable(test-binary-rank test_binary_rank.cpp)
target_include_directories(test-binary-rank PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test-binary-rank PRIVATE word-packing tdc)
add_test(test-binary-rank ${CMAKE_CURRENT_BINARY_DIR}/test-binary-rank)

add_executable(test-lzend test_lzend.cpp)
target_include_directories(test-lzend PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test-lzend PRIVATE tdc tlx unordered_dense)
add_test(test-lzend ${CMAKE_CURRENT_BINARY_DIR}/test-lzend)

add_executable(test-pred test_pred.cpp)
target_include_directories(test-pred PRIVATE ${CMAKE_SOURCE_DIR}/include)
#target_link_libraries(test-pred PRIVATE)
add_test(test-pred ${CMAKE_CURRENT_BINARY_DIR}/test-pred)

add_executable(test-wavelet-tree test_wt.cpp)
target_include_directories(test-wavelet-tree PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test-wavelet-tree PRIVATE word-packing tdc)
add_test(test-wavelet-tree ${CMAKE_CURRENT_BINARY_DIR}/test-wavelet-tree)
