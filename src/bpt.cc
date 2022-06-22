#include "db/bpt.h"
#include "db/block.h"
#include "db/table.h"
#include "db/buffer.h"
#include <stdlib.h>

#include <list>
#include <algorithm>
#pragma warning(disable : 4996)
using std::swap;
using std::binary_search;
using std::lower_bound;
using std::upper_bound;

namespace db {
unsigned int bplus_tree::index_create(IndexBlock *preindex)
{
    IndexBlock next;
    /*新建一个IndexBlock*/
    unsigned int nextid = table_->allocate(1);
    BufDesp *desp = kBuffer.borrow(table_->name_.c_str(), nextid);
    next.attach(desp->buffer);
    next.setNext(preindex->getNext());
    preindex->setNext(next.getSelf());
    next.detach();
    return nextid;
}
void bplus_tree::insert_to_index(
    void *key,
    size_t key_len,
    unsigned int newblockid)
{
    //获取超级块
    SuperBlock super;
    BufDesp *desp2 = kBuffer.borrow(table_->name_.c_str(), 0);
    super.attach(desp2->buffer);
    desp2->relref();
    //读取键和值各自的数据类型
    unsigned int key_index = table_->info_->key;
    DataType *key_type = table_->info_->fields[key_index].type;
    DataType *value_type = findDataType("INT");
    unsigned int value_len = 4;
    //向上传递的键和值
    void *upper_key = new char[key_len];
    memcpy(upper_key,key,key_len);
    unsigned int upper_value = newblockid;
    while (!route.empty()) {
        //获取堆栈顶部
        unsigned int parent = route.top();
        route.pop();
        //读取IndexBlock
        IndexBlock now;
        BufDesp *desp = kBuffer.borrow(table_->name_.c_str(), parent);
        now.attach(desp->buffer);
        now.setTable(table_);
        //包装成iov
        key_type->htobe(upper_key);
        value_type->htobe(&upper_value);
        std::vector<struct iovec> iov(2);
        iov[0].iov_base = upper_key;
        iov[0].iov_len = key_len;
        iov[1].iov_base = &upper_value;
        iov[1].iov_len = value_len;
        value_type->betoh(&upper_value);
        //插入
        std::pair<bool, unsigned int> ret = now.insertRecord(iov);
        //插入为最后一个
        if (ret.second == now.getSlots()) {
            now.deallocate(ret.second);
            unsigned int tmpvalue = now.getNext();
            value_type->htobe(&tmpvalue);
            iov[0].iov_base = upper_key;
            iov[0].iov_len = key_len;
            iov[1].iov_base = &tmpvalue;
            iov[1].iov_len = value_len;     
            now.setNext(upper_value);
        }
        //插入位置不是最后一个
        else {
            Record next;
            now.refslots(ret.second + 1, next);
            void *next_key = new char[key_len];
            unsigned int next_value;

            next.getByIndex((char *) next_key, (unsigned int *) &key_len, 0);
            next.getByIndex(
                (char *) &next_value, (unsigned int *) &value_len, 1);

            now.deallocate(ret.second);
            now.deallocate(ret.second);

            std::vector<struct iovec> iovpre(2);
            iovpre[0].iov_base = upper_key;
            iovpre[0].iov_len = key_len;
            iovpre[1].iov_base = &next_value;
            iovpre[1].iov_len = value_len;
            std::vector<struct iovec> iovnext(2);
            iovnext[0].iov_base = next_key;
            iovnext[0].iov_len = key_len;
            iovnext[1].iov_base = &upper_value;
            iovnext[1].iov_len = value_len;
            now.insertRecord(iovpre);
            now.insertRecord(iovnext);
        }
        //判断是否需要分裂
        if (now.getSlots() == super.getOrder() - 2) //分裂并向上传递
        {
            // index已经满了，需要分裂。分裂分三步：1.新建一个indexblock；2.将数据进行转移；3.往将中间的节点插入父节点parent
            // 1.新建一个indexblock
            IndexBlock next;
            unsigned int nextid=index_create(&now);
            BufDesp*desp=kBuffer.borrow(table_->name_.c_str(),nextid);
            next.setTable(table_);
            // 2.将数据进行转移,并插入
            unsigned short point = now.getSlots() / 2;
            while (now.getSlots() > point) {
                Record record;
                now.refslots(point, record); //从原数据块中拿到record
                next.copyRecord(key_len,record);     // copy到next数据块中
                now.deallocate(point);
            }
            Record record;
            now.refslots(point, record); //从原数据块中拿到record
            record.getByIndex(
                (char *) &upper_value, (unsigned int *) &value_len, 1);
            value_type->betoh(&upper_value);
            next.setNext(now.getNext());
            now.setNext(upper_value);
        } else {
            reset_route();
            return;
        }
    }
    return;
}
unsigned int bplus_tree::insert(void *key, size_t key_len, unsigned int value)
{
    // TODO:空树
    //清空路径栈
    reset_route();
    //读取超级块
    SuperBlock superblock;
    BufDesp *desp = kBuffer.borrow(table_->name_.c_str(), 0);
    superblock.attach(desp->buffer);
    desp->relref();
    //获取数据类型
    unsigned int pkey = table_->info_->key;
    DataType *type = table_->info_->fields[pkey].type;
    DataType *type2 = findDataType("INT");
    //寻找leaf位置，并读入内存
    IndexBlock leaf;
    std::pair<bool, unsigned short> ret1 = index_search(key, key_len);
    BufDesp *desp2 = kBuffer.borrow(table_->name_.c_str(), ret1.second);
    leaf.attach(desp2->buffer);
    leaf.setTable(table_);
    //确认是否已存在相同主键的记录项。
    std::pair<bool, unsigned short> ret2 = leaf.searchRecord(key, key_len);
    if (ret2.first == true) { return 1; }
    //包装成iov
    void *tmpkey = key;
    unsigned int tmpvalue = value;
    type->htobe(tmpkey);
    type2->htobe(&tmpvalue);
    std::vector<struct iovec> iov(2);
    iov[0].iov_base = tmpkey;
    iov[0].iov_len = key_len;
    iov[1].iov_base = &tmpvalue;//该值为暂存值
    iov[1].iov_len = 4;
    type->betoh(tmpkey);

    //判断indexblock是否已经满了是否需要分裂
    if (leaf.getSlots() == superblock.getOrder() - 1) {
        // index已经满了，需要分裂。分裂分三步：1.新建一个indexblock；2.将数据进行转移；3.往将中间的节点插入父节点parent
        // 1.新建一个indexblock，并维护链表
        IndexBlock next;
        unsigned int nextid = index_create(&leaf);
        BufDesp *desp = kBuffer.borrow(table_->name_.c_str(), nextid);
        next.attach(desp->buffer);
        next.setTable(table_);
        next.setMark(1);
        // 2.将数据转移到新indexblock,并插入节点
        unsigned short point = (leaf.getSlots() + 1) / 2;
        // point放在下一个节点，插入的放在这一个节点
        //从原数据块中拿到record,并将键值取出来，放在新的Record
        if (ret2.second <= point) 
        {
            while (leaf.getSlots() >= point) {
                Record record;
                leaf.refslots(point, record); 
                next.copyRecord(key_len,record);
                leaf.deallocate(point);
            } 
            leaf.insertRecord(iov);
        } else // point放在这一个节点，插入的放在下一个节点
        {
            while (leaf.getSlots() > point) {
                Record record;
                leaf.refslots(point, record); 
                next.copyRecord(key_len,record);
                leaf.deallocate(point);
            }
            next.insertRecord(iov);
        }
        // 3.往将中间的节点插入父节点parent
        Record record;
        next.refslots(0, record);
        void *point_key = new char[key_len];
        record.getByIndex((char *) point_key, (unsigned int *) &key_len, 0);
        type->betoh(point_key);
        insert_to_index(point_key, key_len, next.getSelf());

    } else {
        leaf.insertRecord(iov); //直接插入
    }
    type->betoh(&key);
    return 0;
}
std::pair<bool, unsigned int>
bplus_tree::index_search(void *key, size_t key_len)
{
    if (!table_->indexCount()) return {false, 1};
    SuperBlock super;
    BufDesp *desp = kBuffer.borrow(table_->name_.c_str(), 0);
    super.attach(desp->buffer);
    unsigned int root = super.getIndexroot();
    desp->relref();
    IndexBlock index;
    index.setTable(table_);
    BufDesp *desp2 = kBuffer.borrow(table_->name_.c_str(), root);
    index.attach(desp2->buffer);

    unsigned int target = root;
    while (index.getMark() != 1) {
        route.push(index.getSelf());
        std::pair<bool, unsigned short> ret = index.searchRecord(key, key_len);
        if (ret.first) ret.second++;
        if (ret.second >= index.getSlots()) //跳转到右指针
        {
            target = index.getNext();
            index.detach();
            desp2 = kBuffer.borrow(table_->name_.c_str(), target);
            index.attach(desp2->buffer);
        } else //找到对应位置后，跳转到左指针
        {
            Record record;
            index.refslots(ret.second, record);
            unsigned int length = 4;
            record.getByIndex((char *) &target, &length, 1);
            target = be32toh(target);
            index.detach();
            desp2 = kBuffer.borrow(table_->name_.c_str(), target);
            index.attach(desp2->buffer);
        }
    }
    return {true, target};
}
} // namespace db