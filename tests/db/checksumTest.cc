////
// @file checksumTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <string.h>
#include <db/checksum.h>
using namespace db;

TEST_CASE("db/checksum.h")
{
    SECTION("checksum")
    {
        unsigned char buf[4096];
        unsigned short sum = checksum(buf, 4096 - 2);
        memcpy(buf + 4096 - 2, &sum, 2);
        sum = checksum(buf, 4096);
        REQUIRE(sum == 0);

        unsigned int sum32 = checksum32(buf, 4096 - 4);
        memcpy(buf + 4096 - 4, &sum32, 4);
        sum32 = checksum32(buf, 4096);
        REQUIRE(sum32 == 0);
    }
}