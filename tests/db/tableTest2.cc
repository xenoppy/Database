////
// @file tableTest.cc
// @brief
// 测试存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/table.h>
#include <db/block.h>
#include <db/buffer.h>
using namespace db;

namespace {
    void dump_index(unsigned int root, Table &table)
{
    // test indexroot
    SuperBlock super;
    BufDesp *desp = kBuffer.borrow(table.name_.c_str(), 0);
    super.attach(desp->buffer);
    desp->relref();
    int indexroot = super.getIndexroot();
    // 打印所有记录，检查是否正确
    std::queue<unsigned int> index_blocks;
    index_blocks.push(root);
    while (!index_blocks.empty()) {
        IndexBlock index;
        unsigned int now = index_blocks.front();
        index_blocks.pop();
        BufDesp *desp = kBuffer.borrow(table.name_.c_str(), now);
        index.attach(desp->buffer);
        index.setTable(&table);
        for (unsigned short i = 0; i < index.getSlots(); ++i) {
            Slot *slot = index.getSlotsPointer() + i;
            Record record;
            record.attach(
                index.buffer_ + be16toh(slot->offset), be16toh(slot->length));

            unsigned char *pkey;
            unsigned int len;
            long long key;
            record.refByIndex(&pkey, &len, 0);
            memcpy(&key, pkey, len);
            key = be64toh(key);

            unsigned char *pvalue;
            unsigned int value;
            unsigned int value_len;
            record.refByIndex(&pvalue, &value_len, 1);
            memcpy(&value, pvalue, value_len);
            value = be32toh(value);
            printf(
                "key=%lld, offset=%d,blocknum=%d, blkid=%d\n",
                key,
                be16toh(slot->offset),
                table.dataCount(),
                now);
            if (index.getMark() != 1) { index_blocks.push(value); }
        }
        if (index.getMark() != 1) {
            int tmp = index.getNext();
            index_blocks.push(tmp);
        }
    } //读超级块
    printf(
        "total indexs=%d,rootindex=%d,orders=%d,height=%d\n",
        table.indexCount(),
        indexroot,
        super.getOrder(),
        super.getHeight());
}
void dump(Table &table)
{
    // 打印所有记录，检查是否正确
    int rcount = 0;
    int bcount = 0;
    for (Table::BlockIterator bi = table.beginblock(); bi != table.endblock();
         ++bi, ++bcount) {
        for (unsigned short i = 0; i < bi->getSlots(); ++i, ++rcount) {
            Slot *slot = bi->getSlotsPointer() + i;
            Record record;
            record.attach(
                bi->buffer_ + be16toh(slot->offset), be16toh(slot->length));

            unsigned char *pkey;
            unsigned int len;
            long long key;
            record.refByIndex(&pkey, &len, 0);
            memcpy(&key, pkey, len);
            key = be64toh(key);
            printf(
                "key=%lld, offset=%d, rcount=%d, blkid=%d\n",
                key,
                be16toh(slot->offset),
                rcount,
                bcount);
        }
    }
    printf("total records=%zd\n", table.recordCount());
}
 bool check(Table &table)
{
    int rcount = 0;
    int bcount = 0;
    long long okey = 0;
    for (Table::BlockIterator bi = table.beginblock(); bi != table.endblock();
         ++bi, ++bcount) {
        for (DataBlock::RecordIterator ri = bi->beginrecord();
             ri != bi->endrecord();
             ++ri, ++rcount) {
            unsigned char *pkey;
            unsigned int len;
            long long key;
            ri->refByIndex(&pkey, &len, 0);
            memcpy(&key, pkey, len);
            key = be64toh(key);
            if (okey >= key) {
                dump(table);
                printf("check error %d\n", rcount);
                return true;
            }
            okey = key;
        }
    }
    return false;
}
} // namespace

TEST_CASE("db/table.h2")
{
   
    SECTION("insert2")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;



        //补充树
        Table::BlockIterator bi = table.beginblock();
        long long key;
        bplus_tree btp;
        btp.set_table(&table);
        key=htobe64(3);
        btp.insert(&key,8,bi->getSelf());
        bi.release();
        // 准备添加
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char addr[128];

        // 构造一个记录
        nid = rand();
        type->htobe(&nid);
        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;

        int ret = table.insert(1, iov);
        REQUIRE(ret == S_OK);

        bi = table.beginblock();
        REQUIRE(bi.bufdesp->blockid == 1);
        REQUIRE(bi->getSelf() == 1);
        unsigned short count1 = bi->getSlots();
        ++bi;
        REQUIRE(bi->getNext() == 0);
        unsigned short count2 = bi->getSlots();
        REQUIRE(count1 + count2 == 96);
        REQUIRE(count1 + count2 == table.recordCount());
        REQUIRE(!check(table));

        // dump(table);
        // bi = table.beginblock();
        // bi->shrink();
        // dump(table);
        // bi->reorder(type, 0);
        // dump(table);
        // REQUIRE(!check(table));
    }

    // 再插入10000条记录
    SECTION("insert3")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;
        // 准备添加
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char addr[128];

        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;

        int count = 96;
        int count2 = 0;
        for (int i = 0; i < 10000; ++i) {
            nid =rand();
            type->htobe(&nid);
            // locate位置
            unsigned int blkid =
                table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len);
            // 插入记录
            int ret = table.insert(blkid, iov);
            if (ret == S_OK) ++count;
            SuperBlock super;
            BufDesp*desp=kBuffer.borrow(table.name_.c_str(),0);
            super.attach(desp->buffer);
            desp->relref();
            //dump_index(super.getIndexroot(),table);
        }


        for (Table::BlockIterator bi = table.beginblock();
             bi != table.endblock();
             ++bi)
            count2 += bi->getSlots();
        REQUIRE(count == count2);
        REQUIRE(count == table.recordCount());
        REQUIRE(table.idleCount() == 0);

        REQUIRE(!check(table));
    }
SECTION("remove")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;
        //获取超级块
        BufDesp *bd_super = kBuffer.borrow(table.name_.c_str(), 0);
        SuperBlock superblock;
        superblock.attach(bd_super->buffer);
        bd_super->relref();
        unsigned int blkid = superblock.getFirst();
        //删除第一条记录
        long long id = htobe64(5);
        size_t current = table.recordCount();
        table.remove(blkid, &id, (unsigned int) sizeof(id));
        // dump(table);
        REQUIRE(table.recordCount() == current - 1);
        //将删除的补回去
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char addr[128];
        nid = 5;
        type->htobe(&nid);
        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;
        table.insert(blkid, iov);

        //在一个新的block中删除
        DataBlock next;
        next.setTable(&table);
        blkid = table.allocate(0);
        BufDesp *bd2 = kBuffer.borrow(table.name_.c_str(), blkid);
        next.attach(bd2->buffer);
        //在这个block上添加一条记录
        table.insert(blkid, iov);
        //将该Block加到链表第二个
        Table::BlockIterator bi;
        Table::BlockIterator bi2;
        bi = table.beginblock();
        bi2 = table.beginblock();
        bi2++;
        bi->setNext(blkid);
        next.setNext(bi2->getSelf());
        Table::BlockIterator bi3;
        bi3 = table.beginblock();
        bi3++;
        REQUIRE(bi3->getSelf() == blkid);
        
        // dump(table);
        //删除只有一个记录的块
        int current_idle = table.idleCount();
        int current_data = table.dataCount();
        size_t current_record = table.recordCount();
        table.remove(blkid, &id, (unsigned int) sizeof(id));
        bi3 = table.beginblock();
        bi3++;
        REQUIRE(bi3->getSelf() != blkid);
        REQUIRE(table.idleCount() == current_idle + 1);
        REQUIRE(table.dataCount() == current_data - 1);
        REQUIRE(table.recordCount() == current_record - 1);
        // dump(table);

        //查无此记录
        id = htobe64(1);
        int ret = table.remove(table.first_, &id, (unsigned int) sizeof(id));
        REQUIRE(ret == ENOENT);
    }
    SECTION("update")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;
        //获取超级块,获得第一个blk的id
        BufDesp *bd_super = kBuffer.borrow(table.name_.c_str(), 0);
        SuperBlock superblock;
        superblock.attach(bd_super->buffer);
        bd_super->relref();
        unsigned int blkid = superblock.getFirst();
        // 更新记录
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        phone[1] = '0';
        char addr[128];
        nid = 3;
        type->htobe(&nid);
        iov[0].iov_base = &nid;
        iov[0].iov_len = 8;
        iov[1].iov_base = phone;
        iov[1].iov_len = 20;
        iov[2].iov_base = (void *) addr;
        iov[2].iov_len = 128;
        //不变长更新
        int ret = table.update(blkid, iov);
        REQUIRE(ret == S_OK);
        int current_idle = table.idleCount();
        int current_data = table.dataCount();
        // dump(table);
        //变长更新
        DataBlock data;
        BufDesp *bd = kBuffer.borrow(table.name_.c_str(), blkid);
        data.attach(bd->buffer);
        iov[2].iov_len = data.getFreeSize() + 135;
        ret = table.update(blkid, iov);
        REQUIRE(table.idleCount() == current_idle - 1);
        REQUIRE(table.dataCount() == current_data + 1);
        // dump(table);

        //更新不存在的记录
        nid = 1;
        type->htobe(&nid);
        iov[0].iov_base = &nid;
        ret = table.update(blkid, iov);
        REQUIRE(ret == ENOENT);
    }
}
