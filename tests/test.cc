////
// @file test.cc
// @brief
// 采用catch2作为单元测试方案，需要一个main函数，这里定义。
//
// @author niexw
// @email xiaowen.nie.cn@gmail.com
//
#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

int main(int argc, char *argv[])
{
    int result = Catch::Session().run(argc, argv);
    return result;
}
