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

void bplus_tree::clear_tree(unsigned int root)
{
    int indexroot =root;
    // 打印所有记录，检查是否正确
    std::queue<unsigned int> index_blocks;//用于寻找所有节点
    std::queue<unsigned int> index_blocks2;//用于记录所有节点
    index_blocks.push(indexroot);
    index_blocks2.push(indexroot);
    while (!index_blocks.empty()) {
        IndexBlock index;
        unsigned int now = index_blocks.front();
        index_blocks.pop();
        BufDesp *desp = kBuffer.borrow(table_->name_.c_str(), now);
        index.attach(desp->buffer);
        index.setTable(table_);
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
            if (index.getMark() != 1) { 
            index_blocks.push(value); 
            index_blocks2.push(value);
            }
        }
        if (index.getMark() != 1) {
            int tmp = index.getNext();
            index_blocks.push(tmp);
            index_blocks2.push(tmp);
        }
    }
    while (!index_blocks2.empty())
    {
        table_->deallocate(index_blocks2.front(),1);
        index_blocks2.pop();
    }
}

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
/*
    
*/
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
    memcpy(upper_key, key, key_len);
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
        if (ret.second == now.getSlots() - 1) {
            now.deallocate(ret.second);
            unsigned int tmpvalue = now.getNext();
            value_type->htobe(&tmpvalue);
            iov[0].iov_base = upper_key;
            iov[0].iov_len = key_len;
            iov[1].iov_base = &tmpvalue;
            iov[1].iov_len = value_len;
            now.setNext(upper_value);
            now.insertRecord(iov);
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
            value_type->htobe(&upper_value);
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
        if (now.getSlots() == super.getOrder()) //分裂并向上传递
        {
            // index已经满了，需要分裂。分裂分三步：1.新建一个indexblock；2.将数据进行转移；3.往将中间的节点插入父节点parent
            // 1.新建一个indexblock
            IndexBlock next;
            unsigned int nextid = table_->allocate(1);
            BufDesp *desp = kBuffer.borrow(table_->name_.c_str(), nextid);
            next.attach(desp->buffer);
            next.setTable(table_);
            // 2.将数据进行转移,并插入
            unsigned short point = (now.getSlots() + 1) / 2;
            while (now.getSlots() > point) {
                Record record;
                now.refslots(point, record); //从原数据块中拿到record
                next.copyRecord(key_len, record); // copy到next数据块中
                now.deallocate(point);
            }
            Record record;
            now.refslots(point - 1, record); //从原数据块中拿到record
            record.getByIndex(
                (char *) &upper_value, (unsigned int *) &value_len, 1);
            record.getByIndex((char *) upper_key, (unsigned int *) &key_len, 0);
            value_type->betoh(&upper_value);
            next.setNext(now.getNext());
            now.setNext(upper_value);
            now.deallocate(point - 1);
            upper_value = next.getSelf();
            //判断是否当前分裂节点是否为根节点
            if (parent == super.getIndexroot()) {
                unsigned int newroot_id;
                IndexBlock newroot;
                newroot_id = table_->allocate(1);
                BufDesp *desp =
                    kBuffer.borrow(table_->name_.c_str(), newroot_id);
                newroot.attach(desp->buffer);
                super.setIndexroot(newroot_id);
                newroot.setNext(now.getSelf());
                route.push(newroot_id);
                super.setHeight(super.getHeight() + 1);
            }
        } else {
            reset_route();
            return;
        }
    }
    return;
}
unsigned int bplus_tree::insert(void *key, size_t key_len, unsigned int value)
{
    //读取超级块
    SuperBlock superblock;
    BufDesp *desp = kBuffer.borrow(table_->name_.c_str(), 0);
    superblock.attach(desp->buffer);
    desp->relref();
    //获取数据类型
    unsigned int pkey = table_->info_->key;
    DataType *type = table_->info_->fields[pkey].type;
    DataType *type2 = findDataType("INT");

    //包装成iov
    void *tmpkey = new char [key_len];
    memcpy(tmpkey,key,key_len);
    unsigned int tmpvalue = value;
    type2->htobe(&tmpvalue);
    std::vector<struct iovec> iov(2);
    iov[0].iov_base = (unsigned int*)tmpkey;
    iov[0].iov_len = key_len;
    iov[1].iov_base = &tmpvalue; //该值为暂存值
    iov[1].iov_len = 4;

    // 若目前索引B+树为空树
    if (superblock.getIndexroot() == 0) {
        unsigned int newindex = table_->allocate(1);
        superblock.setIndexroot(newindex);
        superblock.setIndexLeaf(newindex);
        IndexBlock index;
        index.setTable(table_);
        BufDesp *desp2 = kBuffer.borrow(table_->name_.c_str(), newindex);
        index.attach(desp2->buffer);
        index.setMark(1);
        index.insertRecord(iov);
        return 0;
    }

    //清空路径栈
    reset_route();
    //寻找leaf位置，并读入内存
    IndexBlock leaf;
    std::pair<bool, unsigned short> ret1 = index_search(key, key_len);
    BufDesp *desp2 = kBuffer.borrow(table_->name_.c_str(), ret1.second);
    leaf.attach(desp2->buffer);
    leaf.setTable(table_);
    //确认是否已存在相同主键的记录项。
    std::pair<bool, unsigned short> ret2 = leaf.searchRecord(key, key_len);
    if (ret2.first == true) { return 1; }

    //判断indexblock是否已经满了是否需要分裂
    if (leaf.getSlots() == superblock.getOrder() - 1) {
        // index已经满了，需要分裂。分裂分三步：1.新建一个indexblock；2.将数据进行转移；3.往将中间的记录插入父节点parent
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
        if (ret2.second <= point - 1) {
            while (leaf.getSlots() >= point) {
                Record record;
                leaf.refslots(point - 1, record);
                next.copyRecord(key_len, record);
                leaf.deallocate(point - 1);
            }
            leaf.insertRecord(iov);
        } else // point放在这一个节点，插入的放在下一个节点
        {
            while (leaf.getSlots() > point) {
                Record record;
                leaf.refslots(point, record);
                next.copyRecord(key_len, record);
                leaf.deallocate(point);
            }
            next.insertRecord(iov);
        }
        // 3.往将中间的节点插入父节点parent
        if (leaf.getSelf() == superblock.getIndexroot()) {
            unsigned int newroot_id;
            IndexBlock newroot;
            newroot_id = table_->allocate(1);
            BufDesp *desp = kBuffer.borrow(table_->name_.c_str(), newroot_id);
            newroot.attach(desp->buffer);
            superblock.setIndexroot(newroot_id);
            newroot.setNext(leaf.getSelf());
            route.push(newroot_id);
            superblock.setHeight(superblock.getHeight() + 1);
        }
        Record record;
        next.refslots(0, record);
        void *point_key = new char[key_len];
        record.getByIndex((char *) point_key, (unsigned int *) &key_len, 0);

        insert_to_index(point_key, key_len, next.getSelf());

    } else {
        leaf.insertRecord(iov); //直接插入
    }
    return 0;
}

unsigned int bplus_tree::search(void *key, size_t key_len)
{
    std::pair<bool, unsigned int> ret = index_search(key, key_len);
    Record record;
    IndexBlock index;
    BufDesp *desp = kBuffer.borrow(table_->name_.c_str(), ret.second);
    index.attach(desp->buffer);
    index.setTable(table_);
    std::pair<bool, unsigned int> ret2 = index.searchRecord(key, key_len);
    if (ret2.first == false) return  0;
    index.refslots(ret2.second, record);
    unsigned int value;
    unsigned int len = 4;
    record.getByIndex((char *) &value, &len, 1);
    value = be32toh(value);
    return value;
}

std::pair<bool, unsigned int>
bplus_tree::index_search(void* key, size_t key_len)
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
        if (ret.first) ret.second++;//ret.second标识了通过第几个指针
        routeslot.push(ret.second);
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

int bplus_tree::remove(void* key, size_t key_len)
{
    DataType *type2 = findDataType("INT");
    //获取超块
    SuperBlock superblock;
    BufDesp* desp = kBuffer.borrow(table_->name_.c_str(), 0);
    superblock.attach(desp->buffer);
    desp->relref();

    //若当前索引B+树为空树，异常，返回1
    if (superblock.getIndexroot() == 0) {
        return 1;
    }

    //寻找leaf位置，读入内存
    reset_routeslot();
    reset_route();
    IndexBlock leaf;
    std::pair<bool, unsigned int> ret = index_search(key, key_len);
    if (ret.first == false)return 1;
    BufDesp* desp2 = kBuffer.borrow(table_->name_.c_str(), ret.second);
    leaf.attach(desp2->buffer);
    leaf.setTable(table_);

    //查找叶子节点是否有对应的记录，如果没有则无法删除，返回1
    std::pair<bool, unsigned int> ret2 = leaf.searchRecord(key, key_len);
    if (ret2.first == false) return 1;
    //设定最小limit
    unsigned short order = superblock.getOrder();
    unsigned short min_n = (order + 1) / 2 - 1;

    //删除对应记录
    leaf.deallocate(ret2.second);

    //如果满足要求直接返回,不满足要求就继续borrow或merge
    if (leaf.getSlots()<min_n) {
        bool borrowed = false;
        //首先尝试从左借
        borrowed = borrow_key(false, leaf, min_n);
        //没借到就从右借
        if (!borrowed) {
            borrowed = borrow_key(true, leaf, min_n);
        }
        //还没借到就merge
        if (!borrowed) {
            unsigned int parentid;
            unsigned short preslot;
            parentid = route.top();
            preslot = routeslot.top();
            IndexBlock parent;
            BufDesp* parentdesp = kBuffer.borrow(table_->name_.c_str(), parentid);
            parent.attach(parentdesp->buffer);
            parent.setTable(table_);

            unsigned short parent_n = parent.getSlots();
            std::pair<void*, size_t> upper_key;
            
            if (preslot == parent_n ) {
                //向左merge：如果该叶子节点是最后一个元素则merge | prev | leaf |
                //获取prev节点
                Record record;//父节点的一条记录，该记录指向prev
                parent.refslots(preslot - 1, record);
                //从record的第1个域中取出value，存进prev_id，标识prev的块号
                unsigned int prev_id;//前兄弟节点块号
                unsigned int value_len = 4;//值的长度
                record.getByIndex((char*)&prev_id, &value_len, 1);
                type2->betoh(&prev_id);
                //获取prev节点
                IndexBlock prev;
                BufDesp* prev_desp = kBuffer.borrow(table_->name_.c_str(), prev_id);
                prev.attach(prev_desp->buffer);
                prev.setTable(table_);

                
                //首先把leaf记录全部copy到prev
                for (int i = 0;i < leaf.getSlots();i++) {
                    Record record_move;
                    leaf.refslots(i, record_move);
                    prev.copyRecord(key_len, record_move);
                }
                //记录索引节点需要删除的key
                Record target;
                leaf.refslots(0, target);
                void* key_to_delete = new char[key_len];
                target.getByIndex((char*)key_to_delete, (unsigned int*)&key_len, 0);
                upper_key.first = key_to_delete;
                upper_key.second = key_len;



                //更新叶子节点链表
                prev.setNext(0);
                
                // //父节点中应被删除,修改的记录
                // Record to_be_deleted;
                // Record to_be_updated;
                // parent.refslots(preslot - 1, to_be_deleted);
                // parent.refslots(preslot, to_be_updated);
                // //取出要被更新记录的key和要被删除记录的value
                // void* update_key = new char[key_len];
                // to_be_updated.getByIndex((char*)update_key, (unsigned int*)&key_len, 0);
                // unsigned int delete_value;
                // unsigned int value_len = 4;
                // to_be_deleted.getByIndex((char*)&delete_value, &value_len, 1);
                // //删除记录
                // parent.deallocate(preslot - 1);
                // //更新记录（删除+插入）
                // parent.deallocate(preslot);
                // std::vector<struct iovec> iov_update(2);
                // iov_update[0].iov_base = update_key;
                // iov_update[0].iov_len = key_len;
                // iov_update[1].iov_base = &delete_value;
                // iov_update[1].iov_len = 4;
                // parent.insertRecord(iov_update);
                
                //释放leaf节点
                table_->deallocate(leaf.getSelf(), 1);
            }
            else {
                //其他情况都向右merge：merge | leaf | next |
                //向右merge
                //获取next_id
                unsigned int next_id;//next兄弟节点块号
                if (preslot == parent_n-1) {
                    next_id = parent.getNext();
                }
                else {
                    Record record;//父节点的一条记录，该记录指向next
                    parent.refslots(preslot + 1, record);
                    //从record的第1个域中取出value，存进prev_id，标识prev的块号

                    unsigned int value_len = 4;//值的长度
                    record.getByIndex((char*)&next_id, &value_len, 1);
                    type2->betoh(&next_id);
                }

                //获取next节点
                IndexBlock next;
                BufDesp* next_desp = kBuffer.borrow(table_->name_.c_str(), next_id);
                next.attach(next_desp->buffer);
                next.setTable(table_);

                
                //首先把leaf记录全部copy到next
                for (int i = 0;i < leaf.getSlots();i++) {
                    Record record_move;
                    leaf.refslots(i, record_move);
                    next.copyRecord(key_len, record_move);
                }

                //记录索引节点需要删除的key(next节点的第一条记录对应的key)
                
                Record target;
                next.refslots(0, target);
                void* key_to_delete = new char[key_len];
                target.getByIndex((char*)key_to_delete, (unsigned int*)&key_len, 0);
                upper_key.first = key_to_delete;
                upper_key.second = key_len;
                
                //更新叶子节点链表
                if (preslot != 0) {//不在最左边
                    //获取prev节点
                    Record record;//父节点的一条记录，该记录指向prev
                    parent.refslots(preslot - 1, record);
                    unsigned int prev_id;//前兄弟节点块号
                    unsigned int value_len = 4;//值的长度
                    record.getByIndex((char*)&prev_id, &value_len, 1);
                    type2->betoh(&prev_id);
                    IndexBlock prev;
                    BufDesp* prev_desp = kBuffer.borrow(table_->name_.c_str(), prev_id);
                    prev.attach(prev_desp->buffer);
                    prev.setTable(table_);
                    prev.setNext(leaf.getNext());
                }
                else {
                    superblock.setIndexLeaf(next_id);
                }

                
                // //父节点中应被删除,修改的记录
                // Record to_be_deleted;
                // Record to_be_updated;
                // parent.refslots(preslot-1, to_be_deleted);
                // parent.refslots(preslot, to_be_updated);
                // //取出要被更新记录的key和要被删除记录的value
                // void* update_key = new char[key_len];
                // to_be_updated.getByIndex((char*)update_key, (unsigned int*)&key_len, 0);
                // unsigned int delete_value;
                // unsigned int value_len = 4;
                // to_be_deleted.getByIndex((char*)&delete_value, &value_len, 1);
                // //删除记录
                // parent.deallocate(preslot - 1);
                // //更新记录（删除+插入）
                // parent.deallocate(preslot);
                // std::vector<struct iovec> iov_update(2);
                // iov_update[0].iov_base = update_key;
                // iov_update[0].iov_len = key_len;
                // iov_update[1].iov_base = &delete_value;
                // iov_update[1].iov_len = 4;
                // parent.insertRecord(iov_update);
                
                //释放leaf节点
                table_->deallocate(leaf.getSelf(), 1);
            }
            
            //开始移除索引节点
            remove_from_index(upper_key);

            
        }

    }
    return 0;
}

bool bplus_tree::borrow_key(bool from_right,  IndexBlock& borrower, unsigned short limit) {
    //回溯父节点,从栈顶取路径的上一个节点和对应的slot
    DataType *type2 = findDataType("INT");
    unsigned int parentid;
    unsigned short preslot;
    parentid = route.top();
    preslot = routeslot.top();
    IndexBlock parent;
    BufDesp* parentdesp = kBuffer.borrow(table_->name_.c_str(), parentid);
    parent.attach(parentdesp->buffer);
    parent.setTable(table_);

    //根据借的方向取得lender节点
    if (from_right) {//从右借
        //首先看有无右兄弟节点，没有就直接返回
        int parent_n = parent.getSlots();
        if (preslot<=parent_n-1) {
            //获取lender的索引记录,即为后一个slot对应的记录
            unsigned int lender_id;//lender块号
            
            //有可能lender的块号存在next中,总之获取lender_id
            if (preslot == parent.getSlots()-1) {
                lender_id = parent.getNext();
            }
            else {
                //父节点的一条记录，该记录指向lender
                Record record;
                parent.refslots(preslot + 1, record);
                unsigned int value_len = 4;//值的长度
                record.getByIndex((char*)&lender_id, &value_len, 1);
                type2->betoh(&lender_id);
            }

            //获取lender节点
            IndexBlock lender;
            BufDesp* lender_desp = kBuffer.borrow(table_->name_.c_str(), lender_id);
            lender.attach(lender_desp->buffer);
            lender.setTable(table_);
            //检查lender是否够借，不够借直接返回
            unsigned short lender_n = lender.getSlots();
            if (lender_n > limit) {
                //将最左的记录copy到borrower中
                Record to_be_borrowed;
                Record to_get_newkey;
                lender.refslots(0, to_be_borrowed);
                lender.refslots(1, to_get_newkey);
                //从表中获取到主键的长度
                unsigned int pkey = table_->info_->key;
                size_t key_len = table_->info_->fields[pkey].length;
                //获取右节点最右新记录的主键
                void* new_key = new char[key_len];
                to_get_newkey.getByIndex((char*)new_key, (unsigned int*)&key_len, 0);
                //记录复制到borrower，从lender中删除
                borrower.copyRecord(key_len, to_be_borrowed);
                lender.deallocate(0);
                //修改父节点对应指针key值
                Record origin;//preslot对应的记录
                parent.refslots(preslot, origin);
                unsigned int value_len = 4;//值的长度
                unsigned int origin_value;
                origin.getByIndex((char*)&origin_value, &value_len, 1);
                
                //type2->htobe(&origin_value);
                std::vector<struct iovec> iov_update(2);
                iov_update[0].iov_base = new_key;
                iov_update[0].iov_len = key_len;
                iov_update[1].iov_base = &origin_value;
                iov_update[1].iov_len = 4;
                parent.deallocate(preslot);
                parent.insertRecord(iov_update);
                return true;
            }
        }
        return false;
    }
    else {//从左借
        //首先看有无左兄弟节点，没有就直接返回
        if (preslot != 0) {
            //获取lender的索引记录,即为前一个slot对应的记录
            Record record;//父节点的一条记录，该记录指向lender
            parent.refslots(preslot - 1, record);
            //从record的第1个域中取出value，存进lender_id，标识lender的块号
            unsigned int lender_id;//lender块号
            unsigned int value_len = 4;//值的长度
            record.getByIndex((char*)&lender_id, &value_len, 1);
            type2->betoh(&lender_id);
            //获取lender节点
            IndexBlock lender;
            BufDesp* lender_desp = kBuffer.borrow(table_->name_.c_str(), lender_id);
            lender.attach(lender_desp->buffer);
            lender.setTable(table_);
            //检查lender是否够借，不够借直接返回
            unsigned short lender_n = lender.getSlots();
            if (lender_n > limit) {
                //将最右的记录copy到borrower中
                Record to_be_borrowed;
                lender.refslots(lender_n-1, to_be_borrowed);
                //从表中获取到主键的长度
                unsigned int pkey = table_->info_->key;
                size_t key_len = table_->info_->fields[pkey].length;
                //获取移动位置的记录的主键
                void* update_key = new char[key_len];
                to_be_borrowed.getByIndex((char*)update_key, (unsigned int*)&key_len, 0);
                //记录复制到borrower，从lender中删除
                borrower.copyRecord(key_len, to_be_borrowed);
                lender.deallocate(lender_n-1);
                //修改父节点对应指针key值
                Record origin;//preslot对应的记录
                parent.refslots(preslot-1, origin);
                unsigned int value_len = 4;//值的长度
                unsigned int origin_value;
                origin.getByIndex((char*)&origin_value, &value_len, 1);
                
                
                std::vector<struct iovec> iov_update(2);
                iov_update[0].iov_base = update_key;
                iov_update[0].iov_len = key_len;
                iov_update[1].iov_base = &origin_value;
                iov_update[1].iov_len = 4;
                parent.deallocate(preslot - 1);
                parent.insertRecord(iov_update);
                return true;
            }
        }
        return false;
    }
}

//索引节点删除记录暂未实现
void bplus_tree::remove_from_index(std::pair<void*, size_t>) {
    
}
} // namespace db