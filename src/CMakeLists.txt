add_executable(comp comp.cpp)
target_link_libraries(comp tdc tlx)

add_executable(hist hist.cpp)
target_link_libraries(hist tdc)
