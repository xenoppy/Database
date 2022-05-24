////
// @file datatype.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <algorithm>
#include <db/datatype.h>
#include <db/block.h>
#include <db/endian.h>

namespace db {

// 匿名空间
namespace {
struct CharCompare
{
    unsigned char *buffer; // buffer指针
    unsigned int key;      // 键的位置

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);
        unsigned short y = be16toh(sy.offset);

        // 引用两条记录
        Record rx, ry;
        rx.attach(buffer + x, 8);
        ry.attach(buffer + y, 8);
        std::vector<struct iovec> iovrx;
        std::vector<struct iovec> iovry;
        unsigned char xheader;
        unsigned char yheader;
        rx.ref(iovrx, &xheader);
        ry.ref(iovry, &yheader);

        // 得到x，y
        const char *xchar = (const char *) iovrx[key].iov_base;
        const char *ychar = (const char *) iovry[key].iov_base;

        // CHAR长度固定
        size_t xsize = iovrx[key].iov_len; // 字符串长度
        return strncmp(xchar, ychar, xsize) < 0;
    }
};

struct VarCharCompare
{
    unsigned char *buffer; // buffer指针
    unsigned int key;      // 键的位置

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);
        unsigned short y = be16toh(sy.offset);

        // 引用两条记录
        Record rx, ry;
        rx.attach(buffer + x, 8);
        ry.attach(buffer + y, 8);
        std::vector<struct iovec> iovrx;
        std::vector<struct iovec> iovry;
        unsigned char xheader;
        unsigned char yheader;
        rx.ref(iovrx, &xheader);
        ry.ref(iovry, &yheader);

        // 得到x，y
        const char *xchar = (const char *) iovrx[key].iov_base;
        const char *ychar = (const char *) iovry[key].iov_base;
        size_t xsize = iovrx[key].iov_len; // x字符串长度
        size_t ysize = iovry[key].iov_len; // y字符串长度

        // 比较字符串
        int ret = strncmp(xchar, ychar, xsize);
        if (ret != 0)
            return ret == -1 ? true : false; // 已经有结果
        else if (xsize == ysize)             // 相同
            return false;
        else if (xsize < ysize)
            return true;
        else
            return false;
    }
};

struct TinyIntCompare
{
    unsigned char *buffer; // buffer指针
    unsigned int key;      // 键的位置

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);
        unsigned short y = be16toh(sy.offset);

        // 引用两条记录
        Record rx, ry;
        rx.attach(buffer + x, 8);
        ry.attach(buffer + y, 8);
        std::vector<struct iovec> iovrx;
        std::vector<struct iovec> iovry;
        unsigned char xheader;
        unsigned char yheader;
        rx.ref(iovrx, &xheader);
        ry.ref(iovry, &yheader);

        // 得到x，y
        unsigned char ix = *((const unsigned char *) iovrx[key].iov_base);
        unsigned char iy = *((const unsigned char *) iovry[key].iov_base);

        return ix < iy;
    }
};

struct SmallIntCompare
{
    unsigned char *buffer; // buffer指针
    unsigned int key;      // 键的位置

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);
        unsigned short y = be16toh(sy.offset);

        // 引用两条记录
        Record rx, ry;
        rx.attach(buffer + x, 8);
        ry.attach(buffer + y, 8);
        std::vector<struct iovec> iovrx;
        std::vector<struct iovec> iovry;
        unsigned char xheader;
        unsigned char yheader;
        rx.ref(iovrx, &xheader);
        ry.ref(iovry, &yheader);

        // 得到x，y
        unsigned short ix =
            be16toh(*((const unsigned short *) iovrx[key].iov_base));
        unsigned short iy =
            be16toh(*((const unsigned short *) iovry[key].iov_base));

        return ix < iy;
    }
};

struct IntCompare
{
    unsigned char *buffer; // buffer指针
    unsigned int key;      // 键的位置

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);
        unsigned short y = be16toh(sy.offset);

        // 引用两条记录
        Record rx, ry;
        rx.attach(buffer + x, 8);
        ry.attach(buffer + y, 8);
        std::vector<struct iovec> iovrx;
        std::vector<struct iovec> iovry;
        unsigned char xheader;
        unsigned char yheader;
        rx.ref(iovrx, &xheader);
        ry.ref(iovry, &yheader);

        // 得到x，y
        unsigned int ix =
            be32toh(*((const unsigned int *) iovrx[key].iov_base));
        unsigned int iy =
            be32toh(*((const unsigned int *) iovry[key].iov_base));

        return ix < iy;
    }
};

struct BigIntCompare
{
    unsigned char *buffer; // buffer指针
    unsigned int key;      // 键的位置

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);
        unsigned short y = be16toh(sy.offset);

        // 引用两条记录
        Record rx, ry;
        rx.attach(buffer + x, 8);
        ry.attach(buffer + y, 8);
        std::vector<struct iovec> iovrx;
        std::vector<struct iovec> iovry;
        unsigned char xheader;
        unsigned char yheader;
        rx.ref(iovrx, &xheader);
        ry.ref(iovry, &yheader);

        // 得到x，y
        unsigned long long ix =
            be64toh(*((const unsigned long long *) iovrx[key].iov_base));
        unsigned long long iy =
            be64toh(*((const unsigned long long *) iovry[key].iov_base));

        return ix < iy;
    }
};

struct CharCompare2
{
    unsigned char *buffer; // buffer指针
    const char *val;       // 搜索值
    size_t size;           // val长度
    unsigned int key;      // 键的位置

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);

        // 引用两条记录
        Record rx;
        rx.attach(buffer + x, 8);
        std::vector<struct iovec> iovrx;
        unsigned char xheader;
        rx.ref(iovrx, &xheader);

        // 得到x
        const char *xchar = (const char *) iovrx[key].iov_base;

        // CHAR长度固定
        size_t xsize = iovrx[key].iov_len; // 字符串长度
        return strncmp(xchar, val, xsize) < 0;
    }
};

struct VarCharCompare2
{
    unsigned char *buffer; // buffer指针
    const char *val;       // 搜索值
    size_t size;           // 字符串长度
    unsigned int key;      // 键的位置

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);

        // 引用两条记录
        Record rx;
        rx.attach(buffer + x, 8);
        std::vector<struct iovec> iovrx;
        unsigned char xheader;
        rx.ref(iovrx, &xheader);

        // 得到x
        const char *xchar = (const char *) iovrx[key].iov_base;
        size_t xsize = iovrx[key].iov_len; // x字符串长度

        // 比较字符串
        int ret = strncmp(xchar, val, xsize);
        if (ret != 0)
            return ret == -1 ? true : false; // 已经有结果
        else if (xsize == size)              // 相同
            return false;
        else if (xsize < size)
            return true;
        else
            return false;
    }
};

struct TinyIntCompare2
{
    unsigned char *buffer; // buffer指针
    unsigned int key;      // 键的位置
    unsigned char val;     // 搜索键

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);

        // 引用两条记录
        Record rx;
        rx.attach(buffer + x, 8);
        std::vector<struct iovec> iovrx;
        unsigned char xheader;
        rx.ref(iovrx, &xheader);

        // 得到x
        unsigned char ix = *((const unsigned char *) iovrx[key].iov_base);

        return ix < val;
    }
};

struct SmallIntCompare2
{
    unsigned char *buffer; // buffer指针
    unsigned int key;      // 键的位置
    unsigned short val;    // 搜索键值

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);

        // 引用两条记录
        Record rx;
        rx.attach(buffer + x, 8);
        std::vector<struct iovec> iovrx;
        unsigned char xheader;
        rx.ref(iovrx, &xheader);

        // 得到x
        unsigned short ix =
            be16toh(*((const unsigned short *) iovrx[key].iov_base));

        return ix < val;
    }
};

struct IntCompare2
{
    unsigned char *buffer; // buffer指针
    unsigned int key;      // 键的位置
    unsigned int val;      // 搜索键值

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);

        // 引用两条记录
        Record rx;
        rx.attach(buffer + x, 8);
        std::vector<struct iovec> iovrx;
        unsigned char xheader;
        rx.ref(iovrx, &xheader);

        // 得到x
        unsigned int ix =
            be32toh(*((const unsigned int *) iovrx[key].iov_base));

        return ix < val;
    }
};

struct BigIntCompare2
{
    unsigned char *buffer;  // buffer指针
    unsigned long long val; // 搜索键值
    unsigned int key;       // 键的位置

    bool operator()(const Slot &sx, const Slot &sy)
    {
        // 先转化为主机字节序
        unsigned short x = be16toh(sx.offset);

        // 引用两条记录
        Record rx;
        rx.attach(buffer + x, 8);
        std::vector<struct iovec> iovrx;
        unsigned char xheader;
        rx.ref(iovrx, &xheader);

        // 得到x，y
        unsigned long long ix =
            be64toh(*((const unsigned long long *) iovrx[key].iov_base));

        return ix < val;
    }
};
} // namespace

static void CharSort(unsigned char *block, unsigned int key)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    CharCompare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static void VarCharSort(unsigned char *block, unsigned int key)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    VarCharCompare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static void TinyIntSort(unsigned char *block, unsigned int key)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    TinyIntCompare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static void SmallIntSort(unsigned char *block, unsigned int key)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    SmallIntCompare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static void IntSort(unsigned char *block, unsigned int key)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    IntCompare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static void BigIntSort(unsigned char *block, unsigned int key)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    BigIntCompare compare;
    compare.buffer = block;
    compare.key = key;

    std::sort(slots, slots + count, compare);
}

static unsigned short
CharSearch(unsigned char *block, unsigned int key, void *val, size_t len)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    CharCompare2 compare;
    compare.buffer = block;
    compare.key = key;
    compare.val = (const char *) val;
    compare.size = len;

    // 搜索值放在compare.val中，-1只是占位
    Slot dump;
    Slot *low = std::lower_bound(slots, slots + count, dump, compare);
    Slot *start =
        (Slot *) (block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));
    return (unsigned short) (low - start);
}

static unsigned short
VarCharSearch(unsigned char *block, unsigned int key, void *val, size_t len)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    VarCharCompare2 compare;
    compare.buffer = block;
    compare.key = key;
    compare.val = (const char *) val;
    compare.size = len;

    // 搜索值放在compare.val中，-1只是占位
    Slot dump;
    Slot *low = std::lower_bound(slots, slots + count, dump, compare);
    Slot *start =
        (Slot *) (block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));
    return (unsigned short) (low - start);
}

static unsigned short
TinyIntSearch(unsigned char *block, unsigned int key, void *val, size_t len)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    TinyIntCompare2 compare;
    compare.buffer = block;
    compare.key = key;
    compare.val = *(reinterpret_cast<unsigned char *>(val));

    // 搜索值放在compare.val中，-1只是占位
    Slot dump;
    Slot *low = std::lower_bound(slots, slots + count, dump, compare);
    Slot *start =
        (Slot *) (block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));
    return (unsigned short) (low - start);
}

static unsigned short
SmallIntSearch(unsigned char *block, unsigned int key, void *val, size_t len)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    SmallIntCompare2 compare;
    compare.buffer = block;
    compare.key = key;
    compare.val = be16toh(*(reinterpret_cast<unsigned short *>(val)));

    // 搜索值放在compare.val中，-1只是占位
    Slot dump;
    Slot *low = std::lower_bound(slots, slots + count, dump, compare);
    Slot *start =
        (Slot *) (block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));
    return (unsigned short) (low - start);
}

static unsigned short
IntSearch(unsigned char *block, unsigned int key, void *val, size_t len)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    IntCompare2 compare;
    compare.buffer = block;
    compare.key = key;
    compare.val = be32toh(*(reinterpret_cast<unsigned int *>(val)));

    // 搜索值放在compare.val中，-1只是占位
    Slot dump;
    Slot *low = std::lower_bound(slots, slots + count, dump, compare);
    Slot *start =
        (Slot *) (block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));
    return (unsigned short) (low - start);
}

static unsigned short
BigIntSearch(unsigned char *block, unsigned int key, void *val, size_t len)
{
    DataHeader *header = reinterpret_cast<DataHeader *>(block);
    unsigned count = be16toh(header->slots);
    Slot *slots = reinterpret_cast<Slot *>(
        block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));

    BigIntCompare2 compare;
    compare.buffer = block;
    compare.key = key;
    compare.val = be64toh(*(reinterpret_cast<unsigned long long *>(val)));

    // 搜索值放在compare.val中，-1只是占位
    Slot dump;
    Slot *low = std::lower_bound(slots, slots + count, dump, compare);
    Slot *start =
        (Slot *) (block + BLOCK_SIZE - sizeof(int) - count * sizeof(Slot));
    return (unsigned short) (low - start);
}

static void CharHtobe(void *) {}
static void CharBetoh(void *) {}

static void SmallIntHtobe(void *buf)
{
    unsigned short *p = reinterpret_cast<unsigned short *>(buf);
    *p = htobe16(*p);
}
static void SmallIntBetoh(void *buf)
{
    unsigned short *p = reinterpret_cast<unsigned short *>(buf);
    *p = be16toh(*p);
}

static void IntHtobe(void *buf)
{
    unsigned int *p = reinterpret_cast<unsigned int *>(buf);
    *p = htobe32(*p);
}
static void IntBetoh(void *buf)
{
    unsigned int *p = reinterpret_cast<unsigned int *>(buf);
    *p = be32toh(*p);
}

static void BigIntHtobe(void *buf)
{
    unsigned long long *p = reinterpret_cast<unsigned long long *>(buf);
    *p = htobe64(*p);
}
static void BigIntBetoh(void *buf)
{
    unsigned long long *p = reinterpret_cast<unsigned long long *>(buf);
    *p = be64toh(*p);
}

static bool charless(
    unsigned char *x,
    unsigned int xlen,
    unsigned char *y,
    unsigned int ylen)
{
    if (xlen < ylen) {
        int ret = memcmp(x, y, xlen);
        if (ret < 0)
            return true;
        else if (ret > 0)
            return false;
        else
            return true;
    } else {
        int ret = memcmp(x, y, ylen);
        if (ret < 0)
            return true;
        else if (ret > 0)
            return false;
        else
            return false;
    }
}
static bool tinyintless(
    unsigned char *x,
    unsigned int xlen,
    unsigned char *y,
    unsigned int ylen)
{
    return *x < *y;
}
static bool smallintless(
    unsigned char *x,
    unsigned int xlen,
    unsigned char *y,
    unsigned int ylen)
{
    unsigned short *sx = (unsigned short *) x;
    unsigned short *sy = (unsigned short *) y;
    return be16toh(*sx) < be16toh(*sy);
}
static bool intless(
    unsigned char *x,
    unsigned int xlen,
    unsigned char *y,
    unsigned int ylen)
{
    unsigned int *sx = (unsigned int *) x;
    unsigned int *sy = (unsigned int *) y;
    return be32toh(*sx) < be32toh(*sy);
}
static bool bigintless(
    unsigned char *x,
    unsigned int xlen,
    unsigned char *y,
    unsigned int ylen)
{
    unsigned long long *sx = (unsigned long long *) x;
    unsigned long long *sy = (unsigned long long *) y;
    return be64toh(*sx) < be64toh(*sy);
}

DataType *findDataType(const char *name)
{
    static DataType gdatatype[] = {
        {"CHAR", //
         65535,
         CharSort,
         CharSearch,
         charless,
         CharHtobe,
         CharBetoh}, // 0
        {"VARCHAR",
         -65535,
         VarCharSort,
         VarCharSearch,
         charless,
         CharHtobe,
         CharBetoh}, // 1
        {"TINYINT",  //
         1,
         TinyIntSort,
         TinyIntSearch,
         tinyintless,
         CharHtobe,
         CharBetoh}, // 2
        {"SMALLINT",
         2,
         SmallIntSort,
         SmallIntSearch,
         smallintless,
         SmallIntHtobe,
         SmallIntBetoh}, // 3
        {"INT",          //
         4,
         IntSort,
         IntSearch,
         intless,
         IntHtobe,
         IntBetoh}, // 4
        {"BIGINT",  //
         8,
         BigIntSort,
         BigIntSearch,
         bigintless,
         BigIntHtobe,
         BigIntBetoh}, // 5
        {},            // x
    };

    int index = 0;
    do {
        if (gdatatype[index].name == NULL)
            break;
        else if (strcmp(gdatatype[index].name, name) == 0)
            return &gdatatype[index];
        else
            ++index;
    } while (true);
    return NULL;
}

} // namespace db