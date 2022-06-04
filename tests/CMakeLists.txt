##
# @file CMakeLists.txt
# @brief
# tests目录下cmake文件
#
# @author niexw
# @email xiaowen.nie.cn@gmail.com
#
include_directories(${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)

# catch要求打开异常
string(REGEX REPLACE "-fno-exceptions" "" CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})

if (WIN32)
    set(TEST test.cc db/integerTest.cc db/checksumTest.cc db/fileTest.cc
    db/datatypeTest.cc db/timestampTest.cc db/recordTest.cc db/bufferTest.cc
    db/schemaTest.cc db/blockTest.cc db/tableTest.cc)
    add_executable(utest ${TEST})
    add_dependencies(utest dbimpl)
    target_link_libraries(utest dbimpl)

elseif (Linux)
    set(TEST test.cc db/fieldTest.cc)
    add_executable(utest ${TEST})
    add_dependencies(utest dbimpl)
    target_link_libraries(utest dbimpl)

endif()


