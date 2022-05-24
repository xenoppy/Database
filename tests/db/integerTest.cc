////
// @file integerTest.cc
// @brief
// 测试压缩整数
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/integer.h>
using namespace db;

TEST_CASE("db/integer.h")
{
    SECTION("integer")
    {
        Integer it;
        it.set(3);
        REQUIRE(it.value_ == 3);
        REQUIRE(it.get() == 3);
        REQUIRE(it.size() == 1);

        it.set(0x40);
        REQUIRE(it.size() == 2);
        it.set(0x3FFF);
        REQUIRE(it.size() == 2);
        it.set(0x4000);
        REQUIRE(it.size() == 4);
        it.set(0x3FFFFFFF);
        REQUIRE(it.size() == 4);
        it.set(0x40000000);
        REQUIRE(it.size() == 8);
        it.set(0x3FFFFFFFFFFFFFFF);
        REQUIRE(it.size() == 8);
        it.set(0x4000000000000000);
        REQUIRE(it.size() == -1);
    }

    SECTION("encode")
    {
        Integer it;
        it.set(3);
        char x1;
        REQUIRE(it.encode(&x1, 1));
        REQUIRE(x1 == 3);

        it.set(0x80);
        unsigned short x2;
        REQUIRE(it.encode((char *) &x2, 2));
        REQUIRE(x2 == 0x8040);

        it.set(0x4000);
        unsigned int x3;
        REQUIRE(it.encode((char *) &x3, 4));
        REQUIRE(x3 == 0x00400080);

        it.set(0x40000000);
        unsigned long long x4;
        REQUIRE(it.encode((char *) &x4, 8));
        REQUIRE(x4 == 0x00000040000000C0);

        it.set(0x4000000000000000);
        REQUIRE(!it.encode((char *) &x4, 8));
    }

    SECTION("decode")
    {
        Integer it;
        it.set(3);
        char x1;
        it.encode(&x1, 1);
        it.decode(&x1, 1);
        REQUIRE(x1 == 3);

        it.set(0x80);
        unsigned short x2;
        it.encode((char *) &x2, 2);
        it.set(0);
        REQUIRE(it.decode((char *) &x2, 2));
        REQUIRE(it.get() == 0x80);

        it.set(0x4000);
        unsigned int x3;
        it.encode((char *) &x3, 4);
        it.set(0);
        REQUIRE(it.decode((char *) &x3, 4));
        REQUIRE(it.get() == 0x4000);

        it.set(0x40000000);
        unsigned long long x4;
        it.encode((char *) &x4, 8);
        it.set(0);
        REQUIRE(it.decode((char *) &x4, 8));
        REQUIRE(it.get() == 0x40000000);
    }
}