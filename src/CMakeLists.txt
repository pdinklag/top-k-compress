add_executable(comp comp.cpp)
target_link_libraries(comp tdc tlx unordered_dense pm-malloc)

add_executable(lz78 lz78.cpp)
target_link_libraries(lz78 tdc)
