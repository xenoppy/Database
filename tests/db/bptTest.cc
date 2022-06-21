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
        //打开表
        Table table;
        table.open("table");
        bplus_tree btree;
        btree.set_table(&table);
        //读超级块
        SuperBlock super;
        BufDesp*desp=kBuffer.borrow(table.name_.c_str(),0);
        super.attach(desp->buffer);
        desp->relref();
        //空树搜索
        REQUIRE(table.indexCount()==0);
        REQUIRE(super.getIndexroot()==0);
        char l[8];
        std::pair<bool,unsigned int>ret=btree.index_search(&l,8);
        REQUIRE(ret.first==false);
        //构建一个根节点
        unsigned int newindex=table.allocate(1);
        REQUIRE(table.indexCount()==1);
        super.setIndexroot(newindex);
        //读根节点，根节点设置为叶子节点
        IndexBlock index;
        index.setTable(&table);
        BufDesp*desp2=kBuffer.borrow(table.name_.c_str(),newindex);
        index.attach(desp2->buffer);
        index.setMark(1);
        //给根节点手动插入记录
        //记录1
        long long key = 7;
        unsigned int left2 = table.allocate(1);
        REQUIRE(table.indexCount()==2);
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
        //记录2
        key = 11;
        unsigned int mid2 = table.allocate(1);
        REQUIRE(table.indexCount()==3);
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
        //根节点设为非叶子节点
        index.setMark(0);
        //第3个指针
        unsigned int right2 = table.allocate(1);
        index.setNext(right2);
        //搜索左边，先把左节点设为叶子节点
        index.detach();
        desp2=kBuffer.borrow(table.name_.c_str(),left2);
        index.attach(desp2->buffer);
        index.setMark(1);

        key=5;
        type->htobe(&key);
        ret=btree.index_search(&key,8);
        REQUIRE(ret.first==true);
        REQUIRE(ret.second==left2);
        //搜索中间，把中间节点设为叶子节点
        index.detach();
        desp2=kBuffer.borrow(table.name_.c_str(),mid2);
        index.attach(desp2->buffer);
        index.setMark(1);

        key=9;
        type->htobe(&key);
        ret=btree.index_search(&key,8);
        REQUIRE(ret.first==true);
        REQUIRE(ret.second==mid2);
        //搜索右边，把右节点设为叶子节点
        index.detach();
        desp2=kBuffer.borrow(table.name_.c_str(),right2);
        index.attach(desp2->buffer);
        index.setMark(1);

        key=13;
        type->htobe(&key);
        ret=btree.index_search(&key,8);
        REQUIRE(ret.first==true);
        REQUIRE(ret.second==right2);
        //清空手动建的树
        table.deallocate(newindex,1);
        table.deallocate(left2,1);
        table.deallocate(mid2,1);
        table.deallocate(right2,1);
        super.setIndexroot(0);
        REQUIRE(super.getIndexroot()==0);
        REQUIRE(table.indexCount()==0);

    }

}