////
// @file timestampTest.cc
// @brief
// 时戳单元测试
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/timestamp.h>
using namespace db;

TEST_CASE("db/timestamp.h")
{
    SECTION("now")
    {
        TimeStamp ts;
        ts.now();
        REQUIRE(sizeof(ts.stamp_) == 8);

        long long buf;
        ts.store(&buf);
        TimeStamp ts1;
        ts1.retrieve(buf);

        // char buf2[512];
        // ts1.toString(buf2, 512);
        // printf("%s\n", buf2);

        // const char *buf3 = "1900_04_17-09:39:45.461121";
        // ts.fromString(buf3);
        // char buf2[512];
        // ts.toString(buf2, 512);
        // printf("%s\n", buf2);
    }
}