add_executable(test-binary-rank test_binary_rank.cpp)
target_link_directories(test-binary-rank PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test-binary-rank PRIVATE word-packing tdc)
add_test(test-binary-rank ${CMAKE_CURRENT_BINARY_DIR}/test-binary-rank)

add_executable(test-kk test_kk.cpp)
target_link_directories(test-kk PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test-kk PRIVATE tdc tlx unordered_dense)
add_test(test-kk ${CMAKE_CURRENT_BINARY_DIR}/test-kk)

add_executable(test-wavelet-tree test_wt.cpp)
target_link_directories(test-wavelet-tree PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test-wavelet-tree PRIVATE word-packing tdc)
add_test(test-wavelet-tree ${CMAKE_CURRENT_BINARY_DIR}/test-wavelet-tree)
