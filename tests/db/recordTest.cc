////
// @file recordTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/record.h>
using namespace db;

TEST_CASE("db/record.h")
{
    SECTION("size")
    {
        std::vector<struct iovec> iov(4);

        const char *table = "table.db";

        int type = 1;
        const char *hello = "hello, worl";
        size_t length = strlen(hello) + 1;

        iov[0].iov_base = (void *) table;
        iov[0].iov_len = strlen(table) + 1;
        iov[1].iov_base = &type;
        iov[1].iov_len = sizeof(int);
        iov[2].iov_base = (void *) hello;
        iov[2].iov_len = strlen(hello) + 1;
        iov[3].iov_base = &length;
        iov[3].iov_len = sizeof(size_t);

        size_t ret = Record::size(iov);
        REQUIRE(ret == 39);

        unsigned char buffer[80];
        Record record;
        record.attach(buffer, 80);
        unsigned char header = 0x48;
        bool sret = record.set(iov, &header);
        REQUIRE(sret);

        REQUIRE(record.length() == 39);
        REQUIRE(record.allocLength() == 40);
        REQUIRE(record.fields() == 4);
        REQUIRE(record.startOfoffsets() == 2);
        REQUIRE(record.startOfFields() == 6);

        REQUIRE(buffer[0] == header); // 头部
        REQUIRE(buffer[1] == 39);     // 记录长度
        REQUIRE(buffer[5] == 0);      // 第0个field偏移量
        REQUIRE(buffer[4] == 9);      // 第1个field偏移量
        REQUIRE(buffer[3] == 13);     // 第2个field偏移量
        REQUIRE(buffer[2] == 25);     // 第3个field偏移量

        REQUIRE(strlen(table) + 1 == 9 - 0);    // 第0个field的长度
        REQUIRE(sizeof(int) == 13 - 9);         // 第1个field的长度
        REQUIRE(strlen(hello) + 1 == 25 - 13);  // 第2个field的长度
        REQUIRE(sizeof(size_t) == 39 - 25 - 6); // 第3个field的长度

        // get
        std::vector<struct iovec> iov2(4);
        char b1[16];
        iov2[0].iov_base = (void *) b1;
        iov2[0].iov_len = 16;
        int type2;
        iov2[1].iov_base = &type2;
        iov2[1].iov_len = sizeof(int);
        char b2[16];
        iov2[2].iov_base = (void *) b2;
        iov2[2].iov_len = 16;
        size_t length2;
        iov2[3].iov_base = &length2;
        iov2[3].iov_len = sizeof(size_t);
        unsigned char header2;
        bool bret = record.get(iov2, &header2);
        REQUIRE(bret);
        REQUIRE(strncmp(b1, table, strlen(table)) == 0);
        REQUIRE(iov2[0].iov_len == strlen(table) + 1);
        REQUIRE(type2 == type);
        REQUIRE(iov2[1].iov_len == sizeof(int));
        REQUIRE(strncmp(b2, hello, strlen(hello)) == 0);
        REQUIRE(iov2[2].iov_len == strlen(hello) + 1);
        REQUIRE(length == length2);
        REQUIRE(iov2[3].iov_len == sizeof(size_t));

        // ref
        std::vector<struct iovec> iov3(4);
        bret = record.ref(iov3, &header2);
        REQUIRE(bret);
        REQUIRE(header2 == header); // 头部
        REQUIRE(memcmp(table, iov3[0].iov_base, iov3[0].iov_len) == 0);
        REQUIRE(memcmp(&type, iov3[1].iov_base, iov3[1].iov_len) == 0);
        REQUIRE(memcmp(hello, iov3[2].iov_base, iov3[2].iov_len) == 0);
        REQUIRE(memcmp(&length, iov3[3].iov_base, iov3[3].iov_len) == 0);

        // getByIndex
        unsigned int l = 16;
        bret = record.getByIndex(b1, &l, 0);
        REQUIRE(bret);
        REQUIRE(strncmp(b1, table, strlen(table)) == 0);
        REQUIRE(l == 9);
        l = 4;
        bret = record.getByIndex((char *) &type2, &l, 1);
        REQUIRE(bret);
        REQUIRE(type2 == type);
        REQUIRE(l == 4);
        l = 16;
        bret = record.getByIndex(b2, &l, 2);
        REQUIRE(bret);
        REQUIRE(strncmp(b2, hello, strlen(hello)) == 0);
        REQUIRE(l == 12);
        l = 8;
        bret = record.getByIndex((char *) &length2, &l, 3);
        REQUIRE(bret);
        REQUIRE(length2 == length);
        REQUIRE(l == 8);

        // refByIndex
        unsigned char *pb;
        bret = record.refByIndex(&pb, &l, 0);
        REQUIRE(bret);
        REQUIRE(strncmp((const char *) pb, table, strlen(table)) == 0);
        REQUIRE(l == 9);
        bret = record.refByIndex(&pb, &l, 1);
        REQUIRE(bret);
        REQUIRE(memcmp(pb, &type, sizeof(type)) == 0);
        REQUIRE(l == 4);
        bret = record.refByIndex(&pb, &l, 2);
        REQUIRE(bret);
        REQUIRE(strncmp((const char *) pb, hello, strlen(hello)) == 0);
        REQUIRE(l == 12);
        bret = record.refByIndex(&pb, &l, 3);
        REQUIRE(bret);
        REQUIRE(memcmp(pb, &length, sizeof(length)) == 0);
        REQUIRE(l == 8);
    }
}