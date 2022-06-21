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
        BufDesp *desp = kBuffer.borrow(table.name_.c_str(), 0);
        super.attach(desp->buffer);
        desp->relref();
        //空树搜索
        REQUIRE(table.indexCount() == 0);
        REQUIRE(super.getIndexroot() == 0);
        char l[8];
        std::pair<bool, unsigned int> ret = btree.index_search(&l, 8);
        REQUIRE(ret.first == false);
        //构建一个根节点
        unsigned int newindex = table.allocate(1);
        REQUIRE(table.indexCount() == 1);
        super.setIndexroot(newindex);
        //读根节点，根节点设置为叶子节点
        IndexBlock index;
        index.setTable(&table);
        BufDesp *desp2 = kBuffer.borrow(table.name_.c_str(), newindex);
        index.attach(desp2->buffer);
        index.setMark(1);
        //给根节点手动插入记录
        //记录1
        long long key = 7;
        unsigned int left2 = table.allocate(1);
        REQUIRE(table.indexCount() == 2);
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
        REQUIRE(table.indexCount() == 3);
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
        ret = btree.index_search(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == newindex);
        REQUIRE(btree.route.empty());
        //根节点设为非叶子节点
        index.setMark(0);
        //第3个指针
        unsigned int right2 = table.allocate(1);
        index.setNext(right2);
        //搜索左边，先把左节点设为叶子节点
        index.detach();
        desp2 = kBuffer.borrow(table.name_.c_str(), left2);
        index.attach(desp2->buffer);
        index.setMark(1);
        key = 5;
        type->htobe(&key);
        ret = btree.index_search(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == left2);
        int route = btree.route.top();
        REQUIRE(route == newindex);
        REQUIRE(btree.route.size() == 1);
        btree.route.pop();
        //搜索中间，把中间节点设为叶子节点
        index.detach();
        desp2 = kBuffer.borrow(table.name_.c_str(), mid2);
        index.attach(desp2->buffer);
        index.setMark(1);

        key = 9;
        type->htobe(&key);
        ret = btree.index_search(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == mid2);
        route = btree.route.top();
        REQUIRE(route == newindex);
        REQUIRE(btree.route.size() == 1);
        btree.route.pop();
        //搜索右边，把右节点设为叶子节点
        index.detach();
        desp2 = kBuffer.borrow(table.name_.c_str(), right2);
        index.attach(desp2->buffer);
        index.setMark(1);

        key = 13;
        type->htobe(&key);
        ret = btree.index_search(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == right2);
        route = btree.route.top();
        REQUIRE(route == newindex);
        REQUIRE(btree.route.size() == 1);
        btree.route.pop();

        key = 11;
        type->htobe(&key);
        ret = btree.index_search(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == right2);
        route = btree.route.top();
        REQUIRE(route == newindex);
        REQUIRE(btree.route.size() == 1);
        btree.route.pop();

        key = 7;
        type->htobe(&key);
        ret = btree.index_search(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == mid2);
        route = btree.route.top();
        REQUIRE(route == newindex);
        REQUIRE(btree.route.size() == 1);
        btree.route.pop();
        //清空手动建的树
        table.deallocate(newindex, 1);
        table.deallocate(left2, 1);
        table.deallocate(mid2, 1);
        table.deallocate(right2, 1);
        super.setIndexroot(0);
        REQUIRE(super.getIndexroot() == 0);
        REQUIRE(table.indexCount() == 0);
    }
    SECTION("index_insert")
    {
        //打开表
        Table table;
        table.open("table");
        bplus_tree btree;
        btree.set_table(&table);
        //读超级块
        SuperBlock super;
        BufDesp *desp = kBuffer.borrow(table.name_.c_str(), 0);
        super.attach(desp->buffer);
        desp->relref();
        super.setOrder(5);
        REQUIRE(super.getOrder() == 5);
        REQUIRE(table.indexCount() == 0);
        REQUIRE(super.getIndexroot() == 0);
        //构建index根节点
        unsigned int newindex = table.allocate(1);
        REQUIRE(table.indexCount() == 1);
        super.setIndexroot(newindex);
        //目前有6个数据块
        unsigned int data0 = table.allocate(0);
        unsigned int data1 = table.allocate(0);
        unsigned int data2 = table.allocate(0);
        unsigned int data3 = table.allocate(0);
        unsigned int data4 = table.allocate(0);
        unsigned int data5 = table.allocate(0);
        //读根节点,设为非叶子节点
        IndexBlock index;
        index.setTable(&table);
        BufDesp *desp2 = kBuffer.borrow(table.name_.c_str(), newindex);
        index.attach(desp2->buffer);
        index.setMark(0);
        //给根节点手动插入记录
        /*                        10
                          5|8              10|15|16|17

        */
        //根节点记录1
        long long key = 10;
        unsigned int left2 = table.allocate(1);
        REQUIRE(table.indexCount() == 2);
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
        //根节点next指向第二层第二个节点
        unsigned int right2 = table.allocate(1);
        index.setNext(right2);
        //第二层第一个节点
        index.detach();
        desp2 = kBuffer.borrow(table.name_.c_str(), left2);
        index.attach(desp2->buffer);
        index.setMark(1);
        //第二层第一个节点记录1
        key = 5;
        type->htobe(&key);

        type2->htobe(&data0);
        iov[0].iov_base = &key;
        iov[0].iov_len = 8;
        iov[1].iov_base = &data0;
        iov[1].iov_len = 4;
        index.insertRecord(iov);
        type2->betoh(&data0);
        //第二层第一个节点记录2
        key = 8;
        type->htobe(&key);

        type2->htobe(&data1);
        iov[0].iov_base = &key;
        iov[0].iov_len = 8;
        iov[1].iov_base = &data1;
        iov[1].iov_len = 4;
        index.insertRecord(iov);
        type2->betoh(&data1);
        //第二层第二个节点

        index.detach();
        desp2 = kBuffer.borrow(table.name_.c_str(), right2);
        index.attach(desp2->buffer);
        index.setMark(1);

        //第二层第二个节点记录1
        key = 10;
        type->htobe(&key);

        type2->htobe(&data2);
        iov[0].iov_base = &key;
        iov[0].iov_len = 8;
        iov[1].iov_base = &data2;
        iov[1].iov_len = 4;
        index.insertRecord(iov);
        type2->betoh(&data2);
        //第二层第二个节点记录2
        key = 15;
        type->htobe(&key);

        type2->htobe(&data3);
        iov[0].iov_base = &key;
        iov[0].iov_len = 8;
        iov[1].iov_base = &data3;
        iov[1].iov_len = 4;
        index.insertRecord(iov);
        type2->betoh(&data3);
        //第二层第二个节点记录3
        key = 16;
        type->htobe(&key);

        type2->htobe(&data4);
        iov[0].iov_base = &key;
        iov[0].iov_len = 8;
        iov[1].iov_base = &data4;
        iov[1].iov_len = 4;
        index.insertRecord(iov);
        type2->betoh(&data4);
        //第二层第二个节点记录4
        key = 17;
        type->htobe(&key);

        type2->htobe(&data5);
        iov[0].iov_base = &key;
        iov[0].iov_len = 8;
        iov[1].iov_base = &data5;
        iov[1].iov_len = 4;
        index.insertRecord(iov);
        type2->betoh(&data5);
        index.detach();
        //浅浅搜索一下，此处只找该节点所在的叶子节点，之后再在节点内部查找
        //搜索key为5的节点，
        key = 5;
        type->htobe(&key);
        std::pair<bool, unsigned int> ret = btree.index_search(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == left2);
        int route = btree.route.top();
        REQUIRE(route == newindex);
        REQUIRE(btree.route.size() == 1);
        btree.route.pop();
        //搜索key为16的节点，
        key = 16;
        type->htobe(&key);
        ret = btree.index_search(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == right2);
        route = btree.route.top();
        REQUIRE(route == newindex);
        REQUIRE(btree.route.size() == 1);
        btree.route.pop();
        //搜索key为10的节点
        key = 10;
        type->htobe(&key);
        ret = btree.index_search(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == right2);
        route = btree.route.top();
        REQUIRE(route == newindex);
        REQUIRE(btree.route.size() == 1);
        btree.route.pop();
        //测试重复节点的插入，应该失败
        key = 10;
        type->htobe(&key);
        unsigned int data6 = table.allocate(0);
        unsigned int insert_ret = btree.insert(&key, 8, data6);
        REQUIRE(insert_ret == 1); //失败返回值
        //插入到节点7，应该变成这样：
        /*                        10
                        5|7|8              10|15|16|17

      */
        key = 7;
        type->htobe(&key);
        unsigned int data7 = table.allocate(0);
        insert_ret = btree.insert(&key, 8, data7);
        ret = btree.index_search(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == left2);
        index.detach();
        desp2 = kBuffer.borrow(table.name_.c_str(), left2);
        index.attach(desp2->buffer);
        ret = index.searchRecord(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == 1);
        REQUIRE(table.indexCount() == 3);
        //插入到节点11，应该变成这样：
        /*                        10
                        5|7|8              10|11|15|16|17
      之后将分裂成：
                                  10
                        5|7|8              10|11      15|16|17
      */
        key = 11;
        type->htobe(&key);
        unsigned int data8 = table.allocate(0);
        insert_ret = btree.insert(&key, 8, data8);
        ret = btree.index_search(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == right2);
        index.detach();
        //判断是否正确分裂
        key = 11;
        type->htobe(&key);
        desp2 = kBuffer.borrow(table.name_.c_str(), right2);
        index.attach(desp2->buffer);
        ret = index.searchRecord(&key, 8);
        REQUIRE(ret.first == true);
        REQUIRE(ret.second == 1);
        REQUIRE(table.indexCount() == 4);
        REQUIRE(index.getSlots() == 2);
        //在分裂出来的新节点中寻找节点16，17
        unsigned int next2 = index.getNext();
        index.detach();
        desp2 = kBuffer.borrow(table.name_.c_str(), next2);
        index.attach(desp2->buffer);
        REQUIRE(index.getSlots() == 3);
        key = 16;
        type->htobe(&key);
        ret = index.searchRecord(&key, 8);
        REQUIRE(ret.second == 0);
        REQUIRE(ret.first == true);
        key = 17;
        type->htobe(&key);
        ret = index.searchRecord(&key, 8);
        REQUIRE(ret.second == 1);
        REQUIRE(ret.first == true);
        //清空手动建的树
        table.deallocate(newindex, 1);
        table.deallocate(left2, 1);
        table.deallocate(right2, 1);
        table.deallocate(next2, 1);
        table.deallocate(data0, 0);
        table.deallocate(data1, 0);
        table.deallocate(data2, 0);
        table.deallocate(data3, 0);
        table.deallocate(data4, 0);
        table.deallocate(data5, 0);
        table.deallocate(data6, 0);
        table.deallocate(data7, 0);
        table.deallocate(data8, 0);
        super.setIndexroot(0);
        REQUIRE(super.getIndexroot() == 0);
        REQUIRE(table.indexCount() == 0);
    }
}