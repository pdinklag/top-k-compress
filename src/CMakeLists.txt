add_executable(comp comp.cpp)
target_link_libraries(comp tdc tlx unordered_dense ips4o pm-malloc)

add_executable(lz78 lz78.cpp)
target_link_libraries(lz78 tdc)

add_executable(vitter87 vitter87.cpp)
target_link_libraries(vitter87 tdc)

add_executable(hist hist.cpp)
target_link_libraries(hist tdc)

add_executable(sync sync.cpp)
target_link_libraries(sync tdc tlx)
