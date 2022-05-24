////
// @file fileTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/file.h>
#include <db/buffer.h>
#include <db/schema.h>
using namespace db;

TEST_CASE("db/file.h")
{
    // 初始化全部变量
    dbInit();

    const char *hello = "hello, world\n";

    SECTION("open")
    {
        // 测试创建
        File file;
        int ret = file.open("table.db");
        REQUIRE(ret == S_OK);
        file.close();
        REQUIRE(file.handle_ == INVALID_HANDLE_VALUE);

        // 测试打开
        ret = file.open("table.db");
        REQUIRE(ret == S_OK);
        file.close();
        REQUIRE(file.handle_ == INVALID_HANDLE_VALUE);
    }

    SECTION("write")
    {
        File file;
        file.open("table.db");

        int ret = file.write(0, hello, strlen(hello));
        REQUIRE(ret == S_OK);

        file.close();
    }

    SECTION("length")
    {
        File file;
        file.open("table.db");

        unsigned long long len = 0;
        int ret = file.length(len);
        REQUIRE(ret == S_OK);
        REQUIRE(len == strlen(hello));

        file.close();
    }

    SECTION("read")
    {
        File file;
        file.open("table.db");

        char buffer[20];
        int ret = file.read(0, buffer, strlen(hello));
        REQUIRE(ret == S_OK);
        int iret = strncmp(buffer, hello, strlen(hello));
        REQUIRE(iret == 0);

        file.close();
    }

    SECTION("remove")
    {
        int ret = File::remove("table.db");
        REQUIRE(ret == S_OK);
    }

    SECTION("open")
    {
        File *meta = kFiles.open(Schema::META_FILE);
        REQUIRE(meta);
    }
}