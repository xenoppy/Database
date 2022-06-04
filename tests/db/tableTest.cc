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

TEST_CASE("db/table.h")
{
    SECTION("less")
    {
        Buffer::BlockMap::key_compare compare;
        const char *table = "hello";
        std::pair<const char *, unsigned int> key1(table, 1);
        const char *table2 = "hello";
        std::pair<const char *, unsigned int> key2(table2, 1);
        bool ret = !compare(key1, key2);
        REQUIRE(ret);
        ret = !compare(key2, key1);
        REQUIRE(ret);
    }

    SECTION("open")
    {
        // NOTE: schemaTest.cc中创建
        Table table;
        int ret = table.open("table");
        REQUIRE(ret == S_OK);
        REQUIRE(table.name_ == "table");
        REQUIRE(table.maxid_ == 1);
        REQUIRE(table.idle_ == 0);
        REQUIRE(table.first_ == 1);
        REQUIRE(table.info_->key == 0);
        REQUIRE(table.info_->count == 3);
    }
   
    SECTION("bi")
    {
        Table table;
        int ret = table.open("table");
        REQUIRE(ret == S_OK);

        Table::BlockIterator bi = table.beginblock();
        REQUIRE(bi.block.table_ == &table);
        REQUIRE(bi.block.buffer_);

        unsigned int blockid = bi->getSelf();
        REQUIRE(blockid == 1);
        REQUIRE(blockid == bi.bufdesp->blockid);
        REQUIRE(bi.bufdesp->ref == 1);

        Table::BlockIterator bi1 = bi;
        REQUIRE(bi.bufdesp->ref == 2);
        bi.bufdesp->relref();

        ++bi;
        bool bret = bi == table.endblock();
        REQUIRE(bret);
    }

    SECTION("locate")
    {
        Table table;
        table.open("table");

        long long id = htobe64(5);
        int blkid = table.locate(&id, sizeof(id));
        REQUIRE(blkid == 1);
        id = htobe64(1);
        blkid = table.locate(&id, sizeof(id));
        REQUIRE(blkid == 1);
        id = htobe64(32);
        blkid = table.locate(&id, sizeof(id));
        REQUIRE(blkid == 1);
    }
    
    // 插满一个block
    SECTION("insert")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;

        // 检查表记录
        long long records = table.recordCount();
        REQUIRE(records == 0);
        Table::BlockIterator bi = table.beginblock();
        REQUIRE(bi->getSlots() == 4); // 已插入4条记录，但表上没记录
        bi.release();
        // 修正表记录
        BufDesp *bd = kBuffer.borrow("table", 0);
        REQUIRE(bd);
        SuperBlock super;
        super.attach(bd->buffer);
        super.setRecords(4);
        super.setDataCounts(1);
        REQUIRE(!check(table));

        // table = id(BIGINT)+phone(CHAR[20])+name(VARCHAR)
        // 准备添加
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        char addr[128];

        // 先填充
        int i, ret;
        for (i = 0; i < 91; ++i) {
            // 构造一个记录
            nid = rand();
            // printf("key=%lld\n", nid);
            type->htobe(&nid);
            iov[0].iov_base = &nid;
            iov[0].iov_len = 8;
            iov[1].iov_base = phone;
            iov[1].iov_len = 20;
            iov[2].iov_base = (void *) addr;
            iov[2].iov_len = 128;

            // locate位置
            unsigned int blkid =
                table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len);
            // 插入记录
            ret = table.insert(blkid, iov);
            if (ret == EEXIST) { printf("id=%lld exist\n", be64toh(nid)); }
            if (ret == EFAULT) break;
        }
        // 这里测试表明再插入到91条记录后出现分裂
        REQUIRE(i + 4 == table.recordCount());
        REQUIRE(!check(table));
        //dump(table);
    }


    SECTION("split")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;

        Table::BlockIterator bi = table.beginblock();

        unsigned short slot_count = bi->getSlots();
        size_t space = 162; // 前面的记录大小

        // 测试split，考虑插入位置在一半之前
        unsigned short index = (slot_count / 2 - 10); // 95/2-10
        std::pair<unsigned short, bool> ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == 47); // 95/2=47，同时将新表项算在内
        REQUIRE(ret.second);

        // 在后半部分
        index = (slot_count / 2 + 10); // 95/2+10
        ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == 48); // 95/2=47，同时将新表项算在内
        REQUIRE(!ret.second);

        // 在中间的位置上
        index = slot_count / 2; // 47
        ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == 47); // 95/2=47，同时将新表项算在内
        REQUIRE(ret.second);

        // 在中间后一个位置上
        index = slot_count / 2 + 1; // 48
        ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == 48); // 95/2=47，同时将新表项算在内
        REQUIRE(!ret.second);

        // 考虑space大小，超过一半
        space = BLOCK_SIZE / 2;
        index = (slot_count / 2 - 10); // 95/2-10
        ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == index); // 未将新插入记录考虑在内
        REQUIRE(!ret.second);

        // space>1/2，位置在后部
        index = (slot_count / 2 + 10); // 95/2+10
        ret = bi->splitPosition(space, index);
        REQUIRE(ret.first == 48); // 95/2=47，同时将新表项算在内
        REQUIRE(!ret.second);

        // 测试说明，这个分裂策略可能使得前一半小于1/2
    }

    SECTION("allocate")
    {
        Table table;
        table.open("table");

        REQUIRE(table.dataCount() == 1);
        REQUIRE(table.idleCount() == 0);

        REQUIRE(table.maxid_ == 1);
        unsigned int blkid = table.allocate();
        REQUIRE(table.maxid_ == 2);
        REQUIRE(table.dataCount() == 2);

        Table::BlockIterator bi = table.beginblock();
        REQUIRE(bi.bufdesp->blockid == 1);
        ++bi;
        REQUIRE(bi == table.endblock()); // 新分配block未插入数据链
        REQUIRE(table.idle_ == 0);       // 也未放在空闲链上
        bi.release();

        // 回收该block
        table.deallocate(blkid);
        REQUIRE(table.idleCount() == 1);
        REQUIRE(table.dataCount() == 1);
        REQUIRE(table.idle_ == blkid);

        // 再从idle上分配
        blkid = table.allocate();
        REQUIRE(table.idleCount() == 0);
        REQUIRE(table.maxid_ == 2);
        REQUIRE(table.idle_ == 0);
        table.deallocate(blkid);
        REQUIRE(table.idleCount() == 1);
        REQUIRE(table.dataCount() == 1);
    }

    SECTION("insert2")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;

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

        Table::BlockIterator bi = table.beginblock();
        REQUIRE(bi.bufdesp->blockid == 1);
        REQUIRE(bi->getSelf() == 1);
        REQUIRE(bi->getNext() == 2);
        unsigned short count1 = bi->getSlots();
        ++bi;
        REQUIRE(bi->getSelf() == 2);
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
            nid = rand();
            type->htobe(&nid);
            // locate位置
            unsigned int blkid =
                table.locate(iov[0].iov_base, (unsigned int) iov[0].iov_len);
            // 插入记录
            int ret = table.insert(blkid, iov);
            if (ret == S_OK) ++count;
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
        BufDesp*bd_super=kBuffer.borrow(table.name_.c_str(),0);
        SuperBlock superblock;
        superblock.attach(bd_super->buffer);
        unsigned int blkid=superblock.getFirst();
        //删除第一条记录
        long long id= htobe64(5);
        size_t current=table.recordCount();
        table.remove(blkid,&id,(unsigned int) sizeof(id));
        //dump(table);
        REQUIRE(table.recordCount()==current-1);
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
        table.insert(blkid,iov);
        
        //在一个新的block中删除
        DataBlock next;
        next.setTable(&table);
        blkid = table.allocate();
        BufDesp *bd2 = kBuffer.borrow(table.name_.c_str(), blkid);
        next.attach(bd2->buffer);
        //在这个block上添加一条记录
        table.insert(blkid,iov);
        //将该Block加到链表第二个
        Table::BlockIterator bi;
        Table::BlockIterator bi2;
        bi = table.beginblock();
        bi2= table.beginblock();
        bi2++;
        bi->setNext(blkid);
        next.setNext(bi2->getSelf());
        Table::BlockIterator bi3;
        bi3=table.beginblock();
        bi3++;
        REQUIRE(bi3->getSelf()==blkid);
        REQUIRE(table.dataCount()==blkid);
        //dump(table);
        //删除只有一个记录的块
        int current_idle=table.idleCount();
        int current_data=table.dataCount();
        size_t current_record=table.recordCount();
        table.remove(blkid,&id,(unsigned int) sizeof(id));
        bi3=table.beginblock();
        bi3++;
        REQUIRE(bi3->getSelf()!=blkid);
        REQUIRE(table.idleCount()==current_idle+1);
        REQUIRE(table.dataCount()==current_data-1);
        REQUIRE(table.recordCount()==current_record-1);
        //dump(table);

        //查无此记录
        id= htobe64(1);
        int ret=table.remove(table.first_,&id,(unsigned int) sizeof(id));
        REQUIRE(ret==ENOENT);
    }
    SECTION("update")
    {
        Table table;
        table.open("table");
        DataType *type = table.info_->fields[table.info_->key].type;
        //获取超级块,获得第一个blk的id
        BufDesp*bd_super=kBuffer.borrow(table.name_.c_str(),0);
        SuperBlock superblock;
        superblock.attach(bd_super->buffer);
        unsigned int blkid=superblock.getFirst();
        // 更新记录
        std::vector<struct iovec> iov(3);
        long long nid;
        char phone[20];
        phone[1]='0';
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
        int ret=table.update(blkid,iov);
        REQUIRE(ret==S_OK);
        int current_idle=table.idleCount();
        int current_data=table.dataCount();
        //dump(table);
        //变长更新
        DataBlock data;
        BufDesp*bd=kBuffer.borrow(table.name_.c_str(),blkid);
        data.attach(bd->buffer);
        iov[2].iov_len = data.getFreeSize()+135;
        ret=table.update(blkid,iov);
        REQUIRE(table.idleCount()==current_idle-1);
        REQUIRE(table.dataCount()==current_data+1);
        //dump(table);

        //更新不存在的
        nid = 1;
        type->htobe(&nid);
        iov[0].iov_base = &nid;
        ret=table.update(blkid,iov);
        REQUIRE(ret==ENOENT);
    }
}
