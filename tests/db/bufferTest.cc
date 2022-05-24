////
// @file bufferTest.cc
// @brief
// buffer单元测试
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/buffer.h>
#include <db/file.h>
#include <db/block.h>
using namespace db;

TEST_CASE("db/buffer.h")
{
    SECTION("init")
    {
        kBuffer.init(&kFiles);
        REQUIRE(kBuffer.idles() == 256 * 1024 * 1024 / BLOCK_SIZE);

        BufDesp *bd = kBuffer.borrow(Schema::META_FILE, 0);
        REQUIRE(bd);
        REQUIRE(bd->buffer);
        REQUIRE(bd->ref.load() == 1);
        kBuffer.releaseBuf(bd);
        REQUIRE(bd->ref.load() == 0);
    }
}