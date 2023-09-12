option(BUILD_TOOLS "build tools" OFF)
message(STATUS "BUILD_TOOLS=${BUILD_TOOLS}")

if(BUILD_TOOLS)
    add_executable(firstdiff firstdiff.cpp)
    target_link_libraries(firstdiff tdc)

    add_executable(hist hist.cpp)
    target_link_libraries(hist tdc)

    add_executable(sync sync.cpp)
    target_link_libraries(sync tdc tlx)

    add_executable(vitter87 vitter87.cpp)
    target_link_libraries(vitter87 tdc)
endif()
