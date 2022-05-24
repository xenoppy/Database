##
# @file CMakeLists.txt
# @brief
# src目录的cmake文件
#
# @author niexw
# @email xiaowen.nie.cn@gmail.com
#
include_directories(${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)

set(LIB_DB_IMPL integer.cc file.cc datatype.cc timestamp.cc record.cc block.cc
    schema.cc buffer.cc table.cc)
add_library(dbimpl STATIC ${LIB_DB_IMPL})
# set(CMAKE_C_FLAGS "/D EXPORT ${CMAKE_C_FLAGS}")
# set(CMAKE_CXX_FLAGS "/D EXPORT ${CMAKE_CXX_FLAGS}")
