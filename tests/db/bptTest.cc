#include "../catch.hpp"
#include <db/block.h>
#include <db/record.h>
#include <db/buffer.h>
#include <db/file.h>
#include <db/table.h>
#include "db/bpt.h"

using namespace db;
TEST_CASE("db/bpt.h")
{
    SECTION("index_search")
    {
        Table table;
        table.open("table");
        bplus_tree btree;
        btree.set_table(&table);
        //空树
        char l[8];
        std::pair<bool,unsigned int>ret=btree.index_search(&l,8);
        REQUIRE(ret.first==false);
        //构建一个根节点
        unsigned int newindex=table.allocate(1);
        SuperBlock super;
        BufDesp*desp=kBuffer.borrow(table.name_.c_str(),0);
        super.attach(desp->buffer);
        super.setIndexroot(newindex);
        desp->relref();
       
        IndexBlock index;
        index.setTable(&table);
        BufDesp*desp2=kBuffer.borrow(table.name_.c_str(),newindex);
        index.attach(desp2->buffer);
        index.setMark(1);
         //手动插入记录
        long long key = 7;
        unsigned int left2 = table.allocate(1);
        DataType *type = findDataType("BIGINT");
        DataType *type2 = findDataType("INT");
        std::vector<struct iovec> iov(2);
        type->htobe(&key);
        type2->htobe(&left2);
        iov[0].iov_base = &key;
        iov[0].iov_len = 8;
        iov[1].iov_base = &left2;
        iov[1].iov_len = 4;
        index.insertRecord(iov);
        type2->betoh(&left2);

        key = 11;
        unsigned int mid2 = table.allocate(1);
        type = findDataType("BIGINT");
        type2 = findDataType("INT");
        type->htobe(&key);
        type2->htobe(&mid2);
        iov[0].iov_base = &key;
        iov[0].iov_len = 8;
        iov[1].iov_base = &mid2;
        iov[1].iov_len = 4;
        index.insertRecord(iov);
        type2->betoh(&mid2);
        //根节点是叶子节点
        ret=btree.index_search(&key,8);
        REQUIRE(ret.first==true);
        REQUIRE(ret.second==newindex);
        index.setMark(0);
        //构建两层树
        unsigned int right2 = table.allocate(1);
        index.setNext(right2);
        //搜索左边
        index.detach();
        desp2=kBuffer.borrow(table.name_.c_str(),left2);
        index.attach(desp2->buffer);
        index.setMark(1);

        key=5;
        type->htobe(&key);
        ret=btree.index_search(&key,8);
        REQUIRE(ret.first==true);
        REQUIRE(ret.second==left2);
        //搜索中间
        index.detach();
        desp2=kBuffer.borrow(table.name_.c_str(),mid2);
        index.attach(desp2->buffer);
        index.setMark(1);

        key=9;
        type->htobe(&key);
        ret=btree.index_search(&key,8);
        REQUIRE(ret.first==true);
        REQUIRE(ret.second==mid2);
        //搜索右边
        index.detach();
        desp2=kBuffer.borrow(table.name_.c_str(),right2);
        index.attach(desp2->buffer);
        index.setMark(1);

        key=13;
        type->htobe(&key);
        ret=btree.index_search(&key,8);
        REQUIRE(ret.first==true);
        REQUIRE(ret.second==right2);
    }

}