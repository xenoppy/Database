////
// @file datatypeTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/datatype.h>
using namespace db;

TEST_CASE("db/datatype.h")
{
    SECTION("find&less")
    {
        DataType *dt_char = findDataType("CHAR");
        DataType *dt_varchar = findDataType("VARCHAR");
        DataType *dt_tinyint = findDataType("TINYINT");
        DataType *dt_smallint = findDataType("SMALLINT");
        DataType *dt_int = findDataType("INT");
        DataType *dt_bigint = findDataType("BIGINT");

        REQUIRE(dt_char);
        REQUIRE(dt_varchar);
        REQUIRE(dt_tinyint);
        REQUIRE(dt_smallint);
        REQUIRE(dt_int);
        REQUIRE(dt_bigint);

        REQUIRE(dt_char->size == 65535);
        REQUIRE(dt_varchar->size == -65535);
        REQUIRE(dt_tinyint->size == 1);
        REQUIRE(dt_smallint->size == 2);
        REQUIRE(dt_int->size == 4);
        REQUIRE(dt_bigint->size == 8);

        const char *hello = "hello";
        const char *hello2 = "hello2";
        REQUIRE(dt_char->less((unsigned char*)hello, (unsigned int)strlen(hello), (unsigned char*)hello2, (unsigned int)strlen(hello2)));
        REQUIRE(!dt_char->less((unsigned char*)hello2, (unsigned int)strlen(hello2), (unsigned char*)hello, (unsigned int)strlen(hello)));
        REQUIRE(dt_varchar->less((unsigned char*)hello, (unsigned int)strlen(hello), (unsigned char*)hello2, (unsigned int)strlen(hello2)));
        REQUIRE(!dt_varchar->less((unsigned char*)hello2, (unsigned int)strlen(hello2), (unsigned char*)hello, (unsigned int)strlen(hello)));
        char a=1;
        char b=2;
        REQUIRE(dt_tinyint->less((unsigned char*)&a,1,(unsigned char*)&b,1));
        REQUIRE(!dt_tinyint->less((unsigned char*)&b,1,(unsigned char*)&a,1));

        short c=0x0100;
        short d=0x0200;
        REQUIRE(dt_smallint->less((unsigned char*)&c,2,(unsigned char*)&d,2));
        REQUIRE(!dt_smallint->less((unsigned char*)&d,2,(unsigned char*)&c,2));

        int e=0x01000000;
        int f=0x02000000;
        REQUIRE(dt_int->less((unsigned char*)&e,4,(unsigned char*)&f,4));
        REQUIRE(!dt_int->less((unsigned char*)&f,4,(unsigned char*)&e,4));

        long long g=0x0100000000000000;
        long long h=0x0200000000000000;
        REQUIRE(dt_bigint->less((unsigned char*)&g,8,(unsigned char*)&h,8));
        REQUIRE(!dt_bigint->less((unsigned char*)&h,8,(unsigned char*)&g,8));
    }
}