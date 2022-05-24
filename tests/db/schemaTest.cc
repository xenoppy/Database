////
// @file schemaTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/schema.h>
using namespace db;

TEST_CASE("db/schema.h")
{
    SECTION("relationinfo")
    {
        // 填充关系
        RelationInfo relation;
        relation.path = "table.dat";

        // id char(20) varchar
        FieldInfo field;
        field.name = "id";
        field.index = 0;
        field.length = 8;
        field.type = findDataType("BIGINT");
        relation.fields.push_back(field);

        field.name = "phone";
        field.index = 1;
        field.length = 20;
        field.type = findDataType("CHAR");
        relation.fields.push_back(field);

        field.name = "name";
        field.index = 2;
        field.length = -255;
        field.type = findDataType("VARCHAR");
        relation.fields.push_back(field);

        relation.count = 3;
        relation.key = 0;

        int total = relation.iovSize();
        REQUIRE(total == 3 * 4 + 7);

        Schema schema;
        std::vector<struct iovec> iov(total);
        schema.initIov("table", relation, iov);

        REQUIRE(iov[0].iov_len == strlen("table") + 1);
        REQUIRE(
            strncmp((const char *) iov[0].iov_base, "table", iov[0].iov_len) ==
            0);

        REQUIRE(iov[1].iov_len == strlen("table.dat") + 1);
        REQUIRE(
            strncmp(
                (const char *) iov[1].iov_base, "table.dat", iov[1].iov_len) ==
            0);

        REQUIRE(iov[2].iov_len == 2);
        unsigned short count = *((unsigned short *) iov[2].iov_base);
        REQUIRE(count == 3);

        REQUIRE(iov[3].iov_len == 2);
        unsigned short type = *((unsigned short *) iov[3].iov_base);
        REQUIRE(type == 0);

        REQUIRE(iov[4].iov_len == 4);
        unsigned int key = *((unsigned int *) iov[4].iov_base);
        REQUIRE(key == 0);
    }

    SECTION("create")
    {
        // 填充关系
        RelationInfo relation;
        relation.path = "table.dat";

        // id char(20) varchar
        FieldInfo field;
        field.name = "id";
        field.index = 0;
        field.length = 8;
        field.type = findDataType("BIGINT");
        relation.fields.push_back(field);

        field.name = "phone";
        field.index = 1;
        field.length = 20;
        field.type = findDataType("CHAR");
        relation.fields.push_back(field);

        field.name = "name";
        field.index = 2;
        field.length = -255;
        field.type = findDataType("VARCHAR");
        relation.fields.push_back(field);

        relation.count = 3;
        relation.key = 0;

        int ret = kSchema.create("table", relation);
        REQUIRE(ret == S_OK);

        std::pair<Schema::TableSpace::iterator, bool> bret =
            kSchema.lookup("table");
        REQUIRE(bret.second);
        REQUIRE(bret.first->second.count == 3);
        REQUIRE(strcmp(bret.first->second.fields[0].name.c_str(), "id") == 0);
    }
}