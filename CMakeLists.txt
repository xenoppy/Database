##
# @file CMakeLists.txt
# @brief
# db项目的根CMakeLists.txt
#
# @author niexw
# @email xiaowen.nie.cn@gmail.com
#
project(db)
cmake_minimum_required(VERSION 3.0)
message(STATUS "### Begin to configure project db ###")

# 检测cmake运行操作系统/CPU/编译器
message(STATUS "Generator: ${CMAKE_GENERATOR}")
message(STATUS "Building OS: ${CMAKE_SYSTEM_NAME}")
message(STATUS "Building Processor: ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "Compiler: ${CMAKE_C_COMPILER_ID}")

# 打开verbose选项
set(CMAKE_VERBOSE_MAKEFILE on)

# 编译模式，Release/Debug，-DCMAKE_BUILD_TYPE=Release|Debug
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug) # Debug Release
endif()
string(TOLOWER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE)
if (CMAKE_BUILD_TYPE MATCHES "debug")
    set(CMAKE_BUILD_TYPE Debug)
else()
    set(CMAKE_BUILD_TYPE Release)
endif()
message(STATUS "Building mode: ${CMAKE_BUILD_TYPE}")

# c/c++按照11标准
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED OFF)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
message(STATUS "C/C++ standard: ${CMAKE_CXX_STANDARD}")

# 设置编译结果输出路径
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG "${PROJECT_BINARY_DIR}/bin")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE "${PROJECT_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_DEBUG "${PROJECT_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_RELEASE "${PROJECT_BINARY_DIR}/bin")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG "${PROJECT_BINARY_DIR}/bin")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE "${PROJECT_BINARY_DIR}/bin")

# 设置MSVC编译选项
if (CMAKE_C_COMPILER_ID MATCHES "MSVC")
    message(STATUS "Visual Studio C++ compiler version: ${CMAKE_C_COMPILER_VERSION}")
    if(CMAKE_C_COMPILER_VERSION VERSION_LESS "19")
        message(FATAL_ERROR "The least version of msvc is 19")
    endif()

    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /WX /EHsc /Oy- /utf-8 /FI ${PROJECT_SOURCE_DIR}/include/db/config.h")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} /D DEBUG /Zi /Od")
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /WX /EHsc /Oy- /utf-8 /FI ${PROJECT_SOURCE_DIR}/include/db/config.h")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /D DEBUG /Zi /Od")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")

    set(CMAKE_LINKER_FLAGS "/NXCOMPAT /INCREMENTAL:NO /DYNAMICBASE")
    set(CMAKE_LINKER_FLAGS_DEBUG "/DEBUG /NXCOMPAT /INCREMENTAL:NO /OPT:REF /OPT:ICF /VERBOSE:ICF /VERBOSE:REF")
    set(CMAKE_LINKER_FLAGS_RELEASE "/NXCOMPAT /INCREMENTAL:NO /DYNAMICBASE")
    set(CMAKE_EXE_LINKER_FLAGS ${CMAKE_LINKER_FLAGS})
    set(CMAKE_MODULE_LINKER_FLAGS ${CMAKE_LINKER_FLAGS})
    set(CMAKE_SHARED_LINKER_FLAGS ${CMAKE_LINKER_FLAGS})
    set(CMAKE_EXE_LINKER_FLAGS_DEBUG ${CMAKE_LINKER_FLAGS_DEBUG})
    set(CMAKE_MODULE_LINKER_FLAGS_DEBUG ${CMAKE_LINKER_FLAGS_DEBUG})
    set(CMAKE_SHARED_LINKER_FLAGS_DEBUG ${CMAKE_LINKER_FLAGS_DEBUG})
    set(CMAKE_EXE_LINKER_FLAGS_RELEASE ${CMAKE_LINKER_FLAGS_RELEASE})
    set(CMAKE_MODULE_LINKER_FLAGS_RELEASE ${CMAKE_LINKER_FLAGS_RELEASE})
    set(CMAKE_SHARED_LINKER_FLAGS_RELEASE ${CMAKE_LINKER_FLAGS_RELEASE})

    # Windows下的make.bat命令
    if (CMAKE_BUILD_TYPE MATCHES "Release")
        file(WRITE build/remake.bat "devenv db.sln /rebuild Release /project all_build")
        file(WRITE build/make.bat "devenv db.sln /build Release /project all_build")
    else()
        file(WRITE build/remake.bat "devenv db.sln /rebuild Debug /project all_build")
        file(WRITE build/make.bat "devenv db.sln /build Debug /project all_build")
    endif()
endif()

# gcc/g++ options
if (CMAKE_C_COMPILER_ID MATCHES "GNU")
    if(CMAKE_C_COMPILER_VERSION VERSION_LESS "6.3")
        message(FATAL_ERROR "The least version of gcc is 6.3")
    endif()

    set(CMAKE_C_FLAGS                     "-Wall -Werror -Wno-attributes -include db/config.h")
    set(CMAKE_C_FLAGS_DEBUG               "-g -DDEBUG")
    set(CMAKE_C_FLAGS_RELEASE             "-O3")
    set(CMAKE_C_FLAGS_MINSIZEREL          "-Os")
    set(CMAKE_C_RELWITHDEBINFO_RELEASE    "-O2")
    set(CMAKE_CXX_FLAGS                   "-Wall -Werror -Wno-attributes -include db/config.h -ftemplate-backtrace-limit=0 -fno-exceptions")
    set(CMAKE_CXX_FLAGS_DEBUG             "-g -DDEBUG")
    set(CMAKE_CXX_FLAGS_RELEASE           "-O3 -fno-strict-aliasing")
    set(CMAKE_CXX_FLAGS_MINSIZEREL        "-Os")
    set(CMAKE_CXX_RELWITHDEBINFO_RELEASE  "-O2")

    set(CMAKE_EXE_LINKER_FLAGS_DEBUG      "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
    set(CMAKE_MODULE_LINKER_FLAGS_DEBUG   "${CMAKE_MODULE_LINKER_FLAGS_DEBUG} -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
    set(CMAKE_SHARED_LINKER_FLAGS_DEBUG   "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
    set(CMAKE_EXE_LINKER_FLAGS_RELEASE    "${CMAKE_EXE_LINKER_FLAGS_RELEASE} -s -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
    set(CMAKE_MODULE_LINKER_FLAGS_RELEASE "${CMAKE_MODULE_LINKER_FLAGS_RELEASE} -s -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
    set(CMAKE_SHARED_LINKER_FLAGS_RELEASE "${CMAKE_SHARED_LINKER_FLAGS_RELEASE} -s -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
endif()

#
# export CC=/usr/bin/clang
# export CXX=/usr/bin/clang++
#
if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    if(CMAKE_C_COMPILER_VERSION VERSION_LESS "3.5")
        message(FATAL_ERROR "The least version of clang is 3.5")
    endif()

    set(CMAKE_C_FLAGS                     "-Wall -Werror -include db/config.h -Wno-atomic-alignment ")
    set(CMAKE_C_FLAGS_DEBUG               "-g -DDEBUG")
    set(CMAKE_C_FLAGS_MINSIZEREL          "-Os")
    set(CMAKE_C_FLAGS_RELEASE             "-O3")
    set(CMAKE_C_FLAGS_RELWITHDEBINFO      "-O2")
    set(CMAKE_CXX_FLAGS                   "-Wall -Werror -include db/config.h  -fno-exceptions -Wno-delete-non-virtual-dtor -Wno-atomic-alignment -ftemplate-backtrace-limit=0")
    set(CMAKE_CXX_FLAGS_DEBUG             "-g -DDEBUG")
    set(CMAKE_CXX_FLAGS_MINSIZEREL        "-Os")
    set(CMAKE_CXX_FLAGS_RELEASE           "-O3")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO    "-O2")

    set(CMAKE_EXE_LINKER_FLAGS_DEBUG      "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
    set(CMAKE_MODULE_LINKER_FLAGS_DEBUG   "${CMAKE_MODULE_LINKER_FLAGS_DEBUG} -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
    set(CMAKE_SHARED_LINKER_FLAGS_DEBUG   "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
    set(CMAKE_EXE_LINKER_FLAGS_RELEASE    "${CMAKE_EXE_LINKER_FLAGS_RELEASE} -s -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
    set(CMAKE_MODULE_LINKER_FLAGS_RELEASE "${CMAKE_MODULE_LINKER_FLAGS_RELEASE} -s -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
    set(CMAKE_SHARED_LINKER_FLAGS_RELEASE "${CMAKE_SHARED_LINKER_FLAGS_RELEASE} -s -Bsymbolic -Bsymbolic-functions -Wl,--no-undefined")
endif()

# 操作系统
if (${CMAKE_SYSTEM_NAME} MATCHES Linux)
    set(Linux "Linux")
endif()
if (${CMAKE_SYSTEM_NAME} MATCHES FreeBSD)
    set(FreeBSD "FreeBSD")
endif()

# 检查数据类型
include(CheckTypeSize)
check_type_size(long SIZEOF_LONG)
check_type_size(wchar_t SIZEOF_WCHAR_T)

# 生成config.h文件
configure_file(
    ${CMAKE_SOURCE_DIR}/config.h.in
    ${CMAKE_SOURCE_DIR}/include/db/config.h
    NEWLINE_STYLE UNIX
)

# 指定编译子目录
add_subdirectory(src)
add_subdirectory(tests)

message(STATUS "### Done ###")
