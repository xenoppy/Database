////
// @file block.cc
// @brief
// 实现block
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <algorithm>
#include <cmath>
#include <db/block.h>
#include <db/record.h>
#include <db/table.h>

namespace db {

DataBlock::RecordIterator::RecordIterator()
    : block(nullptr)
    , index(0)
{}
DataBlock::RecordIterator::~RecordIterator() {}
DataBlock::RecordIterator::RecordIterator(const RecordIterator &other)
    : block(other.block)
    , record(other.record)
    , index(other.index)
{}

DataBlock::RecordIterator &DataBlock::RecordIterator::operator++()
{
    if (block == nullptr || block->getSlots() == 0) return *this;
    index = (++index) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return *this;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return *this;
}
DataBlock::RecordIterator DataBlock::RecordIterator::operator++(int)
{
    RecordIterator tmp(*this);
    if (block == nullptr || block->getSlots() == 0) return tmp;
    index = (++index) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return tmp;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return tmp;
}
DataBlock::RecordIterator &DataBlock::RecordIterator::operator--()
{
    if (block == nullptr || block->getSlots() == 0) return *this;
    index = (index + block->getSlots()) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return *this;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return *this;
}
DataBlock::RecordIterator DataBlock::RecordIterator::operator--(int)
{
    RecordIterator tmp(*this);
    if (block == nullptr || block->getSlots() == 0) return tmp;
    index = (index + block->getSlots()) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return tmp;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return tmp;
}
Record *DataBlock::RecordIterator::operator->() { return &record; }
DataBlock::RecordIterator &DataBlock::RecordIterator::operator+=(int step)
{
    if (block == nullptr || block->getSlots() == 0) return *this;
    index = (index + step) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return *this;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return *this;
}
DataBlock::RecordIterator &DataBlock::RecordIterator::operator-=(int step)
{
    if (block == nullptr || block->getSlots() == 0) return *this;
    index = (index + step + block->getSlots()) % (block->getSlots() + 1);
    if (index == block->getSlots()) {
        record.detach();
        return *this;
    }
    Slot *slots = block->getSlotsPointer();
    record.attach(
        block->buffer_ + be16toh(slots[index].offset),
        be16toh(slots[index].length));
    return *this;
}

void SuperBlock::clear(unsigned short spaceid)
{
    // 清buffer
    ::memset(buffer_, 0, SUPER_SIZE);
    SuperHeader *header = reinterpret_cast<SuperHeader *>(buffer_);

    // 设置magic number
    header->magic = MAGIC_NUMBER;
    // 设定spaceid
    setSpaceid(spaceid);
    // 设定类型
    setType(BLOCK_TYPE_SUPER);
    // 设定时戳
    setTimeStamp();
    // 设定数据块
    setFirst(0);
    // 设定maxid
    setMaxid(0);
    // 设定self
    setSelf();
    // 设定空闲块，缺省从1开始
    setIdle(0);
    // 设定记录数目
    setRecords(0);
    // 设定数据块个数
    setDataCounts(0);
    // 设定空闲块个数
    setIdleCounts(0);
    // 设定空闲空间
    setFreeSpace(sizeof(SuperHeader));
    // 设置checksum
    setChecksum();
}

void MetaBlock::clear(
    unsigned short spaceid,
    unsigned int self,
    unsigned short type)
{
    // 清buffer
    ::memset(buffer_, 0, BLOCK_SIZE);
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);

    // 设定magic
    header->magic = MAGIC_NUMBER;
    // 设定spaceid
    setSpaceid(spaceid);
    // 设定类型
    setType(type);
    // 设定空闲块
    setNext(0);
    // 设置本块id
    setSelf(self);
    // 设定时戳
    setTimeStamp();
    // 设定slots
    setSlots(0);
    // 设定freesize
    setFreeSize(BLOCK_SIZE - sizeof(MetaHeader) - sizeof(Trailer));
    // 设定freespace
    setFreeSpace(sizeof(MetaHeader));
    // 设定校验和
    setChecksum();
}

// TODO: 如果record非full，直接分配，不考虑slot
std::pair<unsigned char *, bool>
MetaBlock::allocate(unsigned short space, unsigned short index)
{
    bool need_reorder = false;
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
    space = ALIGN_TO_SIZE(space); // 先将需要空间数对齐8B

    // 计算需要分配的空间，需要考虑到分配Slot的问题
    unsigned short demand_space = space;
    unsigned short freesize = getFreeSize(); // block当前的剩余空间
    unsigned short current_trailersize = getTrailerSize();
    unsigned short demand_trailersize =
        (getSlots() + 1) * sizeof(Slot) + sizeof(int);
    if (current_trailersize < demand_trailersize)
        demand_space += ALIGN_TO_SIZE(sizeof(Slot)); // 需要的空间数目

    // 该block空间不够
    if (freesize < demand_space)
        return std::pair<unsigned char *, bool>(nullptr, false);

    // 如果freespace空间不够，先回收删除的记录
    unsigned short freespacesize = getFreespaceSize();
    // freespace的空间要减去要分配的slot的空间
    if (current_trailersize < demand_trailersize)
        freespacesize -= ALIGN_TO_SIZE(sizeof(Slot));
    // NOTE: 这里这里没法reorder，才分配还未填充记录
    if (freespacesize < demand_space) {
        shrink();
        need_reorder = true;
    }

    // 从freespace分配空间
    unsigned char *ret = buffer_ + getFreeSpace();

    // 增加slots计数
    unsigned short old = getSlots();
    unsigned short total = std::min<unsigned short>(old, index);
    setSlots(old + 1);
    // 在slots[]顶部增加一个条目
    Slot *new_position = getSlotsPointer();
    for (unsigned short i = 0; i < total; ++i, ++new_position)
        *new_position = *(new_position + 1);
    new_position = getSlotsPointer() + index;
    new_position->offset = htobe16(getFreeSpace());
    new_position->length = htobe16(space);

    // 设定空闲空间大小
    setFreeSize(getFreeSize() - demand_space);
    // 设定freespace偏移量
    setFreeSpace(getFreeSpace() + space);

    return std::pair<unsigned char *, bool>(ret, need_reorder);
}

// TODO: 需要考虑record非full的情况
void MetaBlock::deallocate(unsigned short index)
{
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);

    // 计算需要删除的记录的槽位
    Slot *pslot = reinterpret_cast<Slot *>(
        buffer_ + BLOCK_SIZE - sizeof(int) -
        sizeof(Slot) * (getSlots() - index));
    Slot slot;
    slot.offset = be16toh(pslot->offset);
    slot.length = be16toh(pslot->length);

    // 设置tombstone
    Record record;
    unsigned char *space = buffer_ + slot.offset;
    record.attach(space, 8); // 只使用8个字节
    record.die();

    // 挤压slots[]
    for (unsigned short i = index; i > 0; --i) {
        Slot *from = pslot;
        --from;
        pslot->offset = from->offset;
        pslot->length = from->length;
        pslot = from;
    }

    // 回收slots[]空间
    unsigned short previous_trailersize = getTrailerSize();
    setSlots(getSlots() - 1);
    unsigned short current_trailersize = getTrailerSize();
    // 要把slots[]回收的空间加回来
    if (previous_trailersize > current_trailersize)
        slot.length += previous_trailersize - current_trailersize;
    // 修改freesize
    setFreeSize(getFreeSize() + slot.length);
}

void MetaBlock::shrink()
{
    MetaHeader *header = reinterpret_cast<MetaHeader *>(buffer_);
    Slot *slots = getSlotsPointer();

    // 按照偏移量重新排序slots[]函数
    struct OffsetSort
    {
        bool operator()(const Slot &x, const Slot &y)
        {
            return be16toh(x.offset) < be16toh(y.offset);
        }
    };
    OffsetSort osort;
    std::sort(slots, slots + getSlots(), osort);

    // 枚举所有record，然后向前移动
    unsigned short offset = sizeof(MetaHeader);
    unsigned short space = 0;
    for (unsigned short i = 0; i < getSlots(); ++i) {
        unsigned short len = be16toh((slots + i)->length);
        unsigned short off = be16toh((slots + i)->offset);
        if (offset < off) memmove(buffer_ + offset, buffer_ + off, len);
        (slots + i)->offset = htobe16(offset);
        offset += len;
        space += len;
    }

    // 设定freespace
    setFreeSpace(offset);
    // 计算freesize
    setFreeSize(BLOCK_SIZE - sizeof(MetaHeader) - getTrailerSize() - space);
}

unsigned short DataBlock::searchRecord(void *buf, size_t len)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);

    // 获取key位置
    RelationInfo *info = table_->info_;
    unsigned int key = info->key;

    // 调用数据类型的搜索
    return info->fields[key].type->search(buffer_, key, buf, len);
}

std::pair<unsigned short, bool>
DataBlock::splitPosition(size_t space, unsigned short index)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(buffer_);
    RelationInfo *info = table_->info_;
    unsigned int key = info->key;
    static const unsigned short BlockHalf =
        (BLOCK_SIZE - sizeof(DataHeader) - 8) / 2; // 一半的大小

    // 枚举所有记录
    unsigned short count = getSlots();
    size_t half = 0;
    Slot *slots = getSlotsPointer();
    bool included = false;
    unsigned short i;
    for (i = 0; i < count; ++i) {
        // 如果是index，则将需要插入的记录空间算在内
        if (i == index) {
            // 这里的计算并不精确，没有准确考虑slot的大小，但只算一半没有太大的误差。
            half += ALIGN_TO_SIZE(space) + sizeof(Slot);
            if (half > BlockHalf)
                break;
            else
                included = true;
        }

        // fallthrough, i != index
        half += be16toh(slots[i].length);
        if (half > BlockHalf) break;
    }
    return std::pair<unsigned short, bool>(i, included);
}

unsigned short DataBlock::requireLength(std::vector<struct iovec> &iov)
{
    size_t length = ALIGN_TO_SIZE(Record::size(iov)); // 对齐8B后的长度
    size_t trailer =
        ALIGN_TO_SIZE((getSlots() + 1) * sizeof(Slot) + sizeof(unsigned int)) -
        ALIGN_TO_SIZE(
            getSlots() * sizeof(Slot) +
            sizeof(unsigned int)); // trailer新增部分
    return (unsigned short) (length + trailer);
}

std::pair<bool, unsigned short>
DataBlock::insertRecord(std::vector<struct iovec> &iov)
{
    RelationInfo *info = table_->info_;
    unsigned int key = info->key;
    DataType *type = info->fields[key].type;

    // 先确定插入位置
    unsigned short index =
        type->search(buffer_, key, iov[key].iov_base, iov[key].iov_len);

    // 比较key
    Record record;
    if (index < getSlots()) {
        Slot *slots = getSlotsPointer();
        record.attach(
            buffer_ + be16toh(slots[index].offset),
            be16toh(slots[index].length));
        unsigned char *pkey;
        unsigned int len;
        record.refByIndex(&pkey, &len, key);
        if (memcmp(pkey, iov[key].iov_base, len) == 0) // key相等不能插入
            return std::pair<bool, unsigned short>(false, -1);
    }

    // 如果block空间足够，插入
    size_t blen = getFreeSize(); // 该block的富余空间
    unsigned short actlen = (unsigned short) Record::size(iov);
    unsigned short alignlen = ALIGN_TO_SIZE(actlen);
    unsigned short trailerlen =
        ALIGN_TO_SIZE((getSlots() + 1) * sizeof(Slot) + sizeof(unsigned int)) -
        ALIGN_TO_SIZE(getSlots() * sizeof(Slot) + sizeof(unsigned int));
    if (blen < actlen + trailerlen)
        return std::pair<bool, unsigned short>(false, index);

    // 分配空间
    std::pair<unsigned char *, bool> alloc_ret = allocate(actlen, index);
    // 填写记录
    record.attach(alloc_ret.first, actlen);
    unsigned char header = 0;
    record.set(iov, &header);
    // 重新排序
    if (alloc_ret.second) reorder(type, key);

    return std::pair<bool, unsigned short>(true, index);
}

bool DataBlock::copyRecord(Record &record)
{
    // 判断剩余空间是否足够
    size_t blen = getFreespaceSize(); // 该block的富余空间
    unsigned short actlen = (unsigned short) record.allocLength();
    unsigned short trailerlen =
        ALIGN_TO_SIZE((getSlots() + 1) * sizeof(Slot) + sizeof(unsigned int)) -
        ALIGN_TO_SIZE(getSlots() * sizeof(Slot) + sizeof(unsigned int));
    if (blen < actlen + trailerlen) return false;

    // 分配空间，然后copy
    std::pair<unsigned char *, bool> alloc_ret = allocate(actlen, getSlots());
    memcpy(alloc_ret.first, record.buffer_, actlen);

#if 0
    // 重新排序，最后才重拍？
    RelationInfo *info = table_->info_;
    unsigned int key = info->key;
    DataType *type = info->fields[key].type;
    reorder(type, key); // 最后才重排？
#endif
    return true;
}

DataBlock::RecordIterator DataBlock::beginrecord()
{
    RecordIterator ri;
    ri.block = this;
    ri.index = 0;

    if (getSlots()) {
        Slot *slots = getSlotsPointer();
        ri.record.attach(
            buffer_ + be16toh(slots[0].offset), be16toh(slots[0].length));
    }
    return ri;
}
DataBlock::RecordIterator DataBlock::endrecord()
{
    RecordIterator ri;
    ri.block = this;
    ri.index = getSlots();
    return ri;
}

} // namespace db
