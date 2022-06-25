////
// @file table.cc
// @brief
// 实现存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/table.h>

namespace db {

Table::BlockIterator::BlockIterator()
    : bufdesp(nullptr)
{}
Table::BlockIterator::~BlockIterator()
{
    if (bufdesp) kBuffer.releaseBuf(bufdesp);
}
Table::BlockIterator::BlockIterator(const BlockIterator &other)
    : block(other.block)
    , bufdesp(other.bufdesp)
{
    if (bufdesp) bufdesp->addref();
}

// 前置操作
Table::BlockIterator &Table::BlockIterator::operator++()
{
    if (block.buffer_ == nullptr) return *this;
    unsigned int blockid = block.getNext();
    kBuffer.releaseBuf(bufdesp);
    if (blockid) {
        bufdesp = kBuffer.borrow(block.table_->name_.c_str(), blockid);
        block.attach(bufdesp->buffer);
    } else
        block.buffer_ = nullptr;
    return *this;
}
// 后置操作
Table::BlockIterator Table::BlockIterator::operator++(int)
{
    BlockIterator tmp(*this);
    if (block.buffer_ == nullptr) return *this;
    unsigned int blockid = block.getNext();
    kBuffer.releaseBuf(bufdesp);
    if (blockid) {
        bufdesp = kBuffer.borrow(block.table_->name_.c_str(), blockid);
        block.attach(bufdesp->buffer);
    } else
        block.buffer_ = nullptr;
    return tmp;
}
// 数据块指针
DataBlock *Table::BlockIterator::operator->() { return &block; }
void Table::BlockIterator::release()
{
    bufdesp->relref();
    block.detach();
}

int Table::open(const char *name)
{
    // 查找table
    std::pair<Schema::TableSpace::iterator, bool> bret = kSchema.lookup(name);
    if (!bret.second) return EEXIST; // 表不存在
    // 填充结构
    name_ = name;
    info_ = &bret.first->second;
    // 加载超块
    SuperBlock super;
    BufDesp *desp = kBuffer.borrow(name, 0);
    super.attach(desp->buffer);

    // 获取元数据
    maxid_ = super.getMaxid();
    idle_ = super.getIdle();
    first_ = super.getFirst();

    //设置索引阶数
    /*
    unsigned int pkey = this->info_->key;
    DataType *type = this->info_->fields[pkey].type;
    DataType *type2 = findDataType("INT");
    void *tmpkey = new char;
    __int64 key_len = this->info_->fields[pkey].length;
    unsigned int tmpvalue;
    std::vector<struct iovec> iov(2);
    iov[0].iov_base = tmpkey;
    iov[0].iov_len = key_len;
    iov[1].iov_base = &tmpvalue; //该值为暂存值
    iov[1].iov_len = 4;
    size_t one_record_size =
        ALIGN_TO_SIZE(Record::size(iov)) + ALIGN_TO_SIZE(sizeof(Slot));
    unsigned int Record_Count =
        (BLOCK_SIZE - sizeof(IndexHeader) - sizeof(Trailer)) 
        (unsigned int) one_record_size;*/
    super.setOrder(200);
    // 释放超块
    super.detach();
    desp->relref();
    return S_OK;
}

unsigned int Table::allocate(int BlockType)
{
    // 空闲链上有block
    DataBlock data;
    IndexBlock index;
    SuperBlock super;
    BufDesp *desp;

    if (idle_) {
        // 读idle块，获得下一个空闲块
        desp = kBuffer.borrow(name_.c_str(), idle_);
        data.attach(desp->buffer);
        unsigned int next = data.getNext();
        data.detach();
        desp->relref();

        // 读超块，设定空闲块
        desp = kBuffer.borrow(name_.c_str(), 0);
        super.attach(desp->buffer);
        super.setIdle(next);
        super.setIdleCounts(super.getIdleCounts() - 1);
        if (BlockType == 0) {
            super.setDataCounts(super.getDataCounts() + 1);
        } else {
            super.setIndexcounts(super.getIndexcounts() + 1);
        }
        super.setChecksum();
        super.detach();
        kBuffer.writeBuf(desp);
        desp->relref();

        unsigned int current = idle_;
        idle_ = next;

        desp = kBuffer.borrow(name_.c_str(), current);
        if (BlockType == 0) {
            data.attach(desp->buffer);
            data.clear(1, current, BLOCK_TYPE_DATA);
        } else {
            index.attach(desp->buffer);
            index.clear(1, current, BLOCK_TYPE_INDEX, 0);
        }

        desp->relref();
        return current;
    }

    // 没有空闲块
    ++maxid_;
    // 读超块，设定空闲块
    desp = kBuffer.borrow(name_.c_str(), 0);
    super.attach(desp->buffer);
    super.setMaxid(maxid_);
    if (BlockType == 0) {
        super.setDataCounts(super.getDataCounts() + 1);
    } else {
        super.setIndexcounts(super.getIndexcounts() + 1);
    }
    super.setChecksum();
    super.detach();
    kBuffer.writeBuf(desp);
    desp->relref();
    // 初始化数据块
    desp = kBuffer.borrow(name_.c_str(), maxid_);
    if (BlockType == 0) {
        data.attach(desp->buffer);
        data.clear(1, maxid_, BLOCK_TYPE_DATA);
    } else {
        index.attach(desp->buffer);
        index.clear(1, maxid_, BLOCK_TYPE_INDEX, 0);
    }
    desp->relref();
    data.detach();
    index.detach();

    return maxid_;
}

void Table::deallocate(unsigned int blockid, int BlockType)
{
    // 读idle块，获得下一个空闲块
    DataBlock data;
    IndexBlock index;
    BufDesp *desp = kBuffer.borrow(name_.c_str(), blockid);
    data.attach(desp->buffer);
    data.setNext(idle_);
    data.setChecksum();
    data.detach();
    kBuffer.writeBuf(desp);
    desp->relref();

    // 读超块，设定空闲块
    SuperBlock super;
    desp = kBuffer.borrow(name_.c_str(), 0);
    super.attach(desp->buffer);
    super.setIdle(blockid);
    super.setIdleCounts(super.getIdleCounts() + 1);
    if (BlockType == 0)
        super.setDataCounts(super.getDataCounts() - 1);
    else
        super.setIndexcounts(super.getIndexcounts() - 1);
    super.setChecksum();
    super.detach();
    kBuffer.writeBuf(desp);
    desp->relref();

    // 设定自己
    idle_ = blockid;
}

Table::BlockIterator Table::beginblock()
{
    // 通过超块找到第1个数据块的id
    BlockIterator bi;
    bi.block.table_ = this;

    // 获取第1个blockid
    BufDesp *bd = kBuffer.borrow(name_.c_str(), 0);
    SuperBlock super;
    super.attach(bd->buffer);
    unsigned int blockid = super.getFirst();
    kBuffer.releaseBuf(bd);

    bi.bufdesp = kBuffer.borrow(name_.c_str(), blockid);
    bi.block.attach(bi.bufdesp->buffer);
    return bi;
}

Table::BlockIterator Table::endblock()
{
    BlockIterator bi;
    bi.block.table_ = this;
    return bi;
}

unsigned int Table::locate(void *keybuf, unsigned int len)
{
    unsigned int key = info_->key;
    DataType *type = info_->fields[key].type;

    BlockIterator prev = beginblock();
    for (BlockIterator bi = beginblock(); bi != endblock(); ++bi) {
        // 获取第1个记录
        Record record;
        bi->refslots(0, record);

        // 与参数比较
        unsigned char *pkey;
        unsigned int klen;
        record.refByIndex(&pkey, &klen, key);
        bool bret = type->less(pkey, klen, (unsigned char *) keybuf, len);
        if (bret) {
            prev = bi;
            continue;
        }
        // 要排除相等的情况
        bret = type->less((unsigned char *) keybuf, len, pkey, klen);
        if (bret)
            return prev->getSelf(); //
        else
            return bi->getSelf(); // 相等
    }
    return prev->getSelf();
}

int Table::insert(unsigned int blkid, std::vector<struct iovec> &iov)
{
    SuperBlock super;
    BufDesp *bd3= kBuffer.borrow(name_.c_str(),0);
    super.attach(bd3->buffer);
    bd3->relref();
    DataBlock data;
    data.setTable(this);

    // 从buffer中借用
    BufDesp *bd = kBuffer.borrow(name_.c_str(), blkid);
    data.attach(bd->buffer);
    // 尝试插入
    std::pair<bool, unsigned short> ret = data.insertRecord(iov);
    if (ret.first) {
        kBuffer.writeBuf(bd);   //写入文件
        kBuffer.releaseBuf(bd); // 释放buffer
        // 修改表头统计
        bd = kBuffer.borrow(name_.c_str(), 0);
        super.setRecords(super.getRecords() + 1);
        bd->relref();
        kBuffer.writeBuf(bd);
        return S_OK; // 插入成功
    } else if (ret.second == (unsigned short) -1) {
        kBuffer.releaseBuf(bd); // 释放buffer
        return EEXIST;          // key已经存在
    } else {
        // 分裂block
        unsigned short insert_position = ret.second;
        std::pair<unsigned short, bool> split_position =
            data.splitPosition(Record::size(iov), insert_position);
        // 先分配一个block
        DataBlock next;
        next.setTable(this);
        blkid = allocate(0);
        BufDesp *bd2 = kBuffer.borrow(name_.c_str(), blkid);
        next.attach(bd2->buffer);

        // 移动记录到新的block上
        while (data.getSlots() > split_position.first) {
            Record record;
            data.refslots(
                split_position.first, record); //从原数据块中拿到record
            next.copyRecord(record);           // copy到next数据块中
            data.deallocate(split_position.first);
        }
        // 插入新记录，不需要再重排顺序
        if (split_position.second)
            data.insertRecord(iov);
        else
            next.insertRecord(iov);
        // 维持数据链
        next.setNext(data.getNext());
        data.setNext(next.getSelf());
        bd2->relref();
        //更新btree
        unsigned int pkey = this->info_->key;
        Record record;
        next.refslots(0, record);
        void *key = new char[iov[info_->key].iov_len];
        bplus_tree bpt;
        bpt.set_table(this);
        record.getByIndex(
            (char *) key, (unsigned int *) &iov[info_->key].iov_len, pkey);
        bpt.insert(key, iov[info_->key].iov_len, blkid);
        //
        super.setRecords(super.getRecords() + 1);
        bd->relref();
        kBuffer.writeBuf(bd);
        kBuffer.writeBuf(bd2);
        return S_OK;
    }
}
int Table::remove(unsigned int blkid, void *keybuf, unsigned int len)
{
    DataBlock data;
    data.setTable(this);
    // 从buffer中借用
    BufDesp *bd = kBuffer.borrow(name_.c_str(), blkid);
    data.attach(bd->buffer);
    //定位record，标记为tomestone
    std::pair<bool, unsigned short> ret =
        data.searchRecord(keybuf, (size_t) len);
    if (ret.first == false) return ENOENT;
    data.deallocate(ret.second);
    kBuffer.writeBuf(bd);
    //修改表头统计
    SuperBlock super;
    BufDesp *bd2 = kBuffer.borrow(name_.c_str(), 0);
    super.attach(bd2->buffer);
    super.setRecords(super.getRecords() - 1);
    bd2->relref();
    kBuffer.writeBuf(bd2);
    // block已空的状态
    if (data.getSlots() == 0) {
        //将block移出链表,放回idle
        //定位到前一个
        DataBlock pre;
        pre.setTable(this);
        BufDesp *bd3 = kBuffer.borrow(name_.c_str(), super.getFirst());
        pre.attach(bd3->buffer);
        while (pre.getNext() != blkid) {
            bd3 = kBuffer.borrow(name_.c_str(), pre.getNext());
            pre.attach(bd3->buffer);
        }
        //维护链表
        unsigned int next = data.getNext();
        pre.setNext(next);
        //回收Block
        deallocate(blkid, 0);
        kBuffer.writeBuf(bd3);
    }

    return S_OK;
}

int Table::update(unsigned int blkid, std::vector<struct iovec> &iov)
{
    DataBlock data;
    data.setTable(this);
    // 从buffer中借用
    BufDesp *bd = kBuffer.borrow(name_.c_str(), blkid);
    data.attach(bd->buffer);
    //更新Record
    std::pair<bool, unsigned short> ret = data.updateRecord(iov);
    //直接更新失败，需要处理
    if (ret.first == false) {
        //查无此Record
        if (ret.second == (unsigned short) -1) return ENOENT;
        //考虑变长因素，新更新的Record太大，需要分裂
        else {
            // 分裂block
            unsigned short insert_position = ret.second;
            std::pair<unsigned short, bool> split_position =
                data.splitPosition(Record::size(iov), insert_position);
            // 先分配一个block
            DataBlock next;
            next.setTable(this);
            blkid = allocate(0);
            BufDesp *bd2 = kBuffer.borrow(name_.c_str(), blkid);
            next.attach(bd2->buffer);

            // 移动记录到新的block上
            while (data.getSlots() > split_position.first) {
                Record record;
                data.refslots(split_position.first, record);
                next.copyRecord(record);
                data.deallocate(split_position.first);
            }
            // 插入新记录，不需要再重排顺序
            if (split_position.second)
                data.insertRecord(iov);
            else
                next.insertRecord(iov);
            // 维持数据链
            next.setNext(data.getNext());
            data.setNext(next.getSelf());
            BufDesp *bd3 = kBuffer.borrow(name_.c_str(), 0);
            //
            SuperBlock super;
            super.attach(bd->buffer);
            super.setRecords(super.getRecords() + 1);
            bd3->relref();
            kBuffer.writeBuf(bd3);
            kBuffer.writeBuf(bd2);
            kBuffer.writeBuf(bd);
            return S_OK;
        }
    }
    return S_OK;
}

size_t Table::recordCount()
{
    BufDesp *bd = kBuffer.borrow(name_.c_str(), 0);
    SuperBlock super;
    super.attach(bd->buffer);
    size_t count = super.getRecords();
    kBuffer.releaseBuf(bd);
    return count;
}

unsigned int Table::dataCount()
{
    BufDesp *bd = kBuffer.borrow(name_.c_str(), 0);
    SuperBlock super;
    super.attach(bd->buffer);
    unsigned int count = super.getDataCounts();
    kBuffer.releaseBuf(bd);
    return count;
}

unsigned int Table::idleCount()
{
    BufDesp *bd = kBuffer.borrow(name_.c_str(), 0);
    SuperBlock super;
    super.attach(bd->buffer);
    unsigned int count = super.getIdleCounts();
    kBuffer.releaseBuf(bd);
    return count;
}
unsigned int Table::indexCount()
{
    BufDesp *bd = kBuffer.borrow(name_.c_str(), 0);
    SuperBlock super;
    super.attach(bd->buffer);
    unsigned int count = super.getIndexcounts();
    kBuffer.releaseBuf(bd);
    return count;
}
} // namespace db