option(BUILD_TOOLS "build tools" OFF)
message(STATUS "BUILD_TOOLS=${BUILD_TOOLS}")

if(BUILD_TOOLS)
    add_executable(firstdiff firstdiff.cpp)
    target_link_libraries(firstdiff iopp oocmd)

    add_executable(hist hist.cpp)
    target_link_libraries(hist iopp)
endif()
