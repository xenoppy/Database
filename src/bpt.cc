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
void bplus_tree::index_create(IndexBlock *preindex, IndexBlock &nextindex)
{
    IndexBlock next;
    /*新建一个IndexBlock*/
    table_->allocate(BLOCK_TYPE_INDEX, next);
    nextindex = next;
    nextindex.setNext(preindex->getNext());
}
void bplus_tree::insert_to_index() {}
unsigned int bplus_tree::insert(void *key, size_t key_len, unsigned int *value)
{
    IndexBlock parent, leaf;
    SuperBlock superblock;
    /*！待实现！通过bplus_tree的table_找到superblock并将其attach到内存中*/
    /*！待实现！通过key，search到那个叶子节点和其parent，并从buffer中把他们映射到内存,attch上去了*/
    //确认是否已存在相同主键的记录项。
    std::pair<bool, unsigned short> ret = leaf.searchRecord(key, key_len);
    if (ret.first == false) { return 1; }
    //判断是否indexblock是否已经满了是否需要分裂
    if (leaf.getSlots() == superblock.getOrder()) {
        // index已经满了，需要分裂。分裂分三步：1.新建一个indexblock；2.将数据进行转移；3.往将中间的节点插入父节点parent
        // 1.新建一个indexblock
        IndexBlock next;
        index_create(&leaf, next);
        // 2.将数据进行转移
        unsigned short point = leaf.getSlots() / 2;
        /*！待实现！比较参数传入的key与leaf节点的第slot[point]哪个在前哪个在后。若插入的记录在后，则将（point+1）及之后的记录转移到新叶子节点，并把待插入记录插入到新叶子节点。反之则将point及之后的记录转移到新叶子节点,并把待插入记录插入到原叶子节点*/
        // 3.往将中间的节点插入父节点parent
        /*！待实现！实际上就是把新叶子节点的第一个记录插入到父节点中。此处父节点采用标准库的堆来记录，插入到父节点的过程需递归完成。用insert_index()*/
    }
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
        std::pair<bool, unsigned short> ret = index.searchRecord(key, key_len);
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