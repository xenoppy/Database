////
// @file record.cc
// @brief
// 实现物理记录
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/record.h>

namespace db {

// TODO: 加上log

size_t Record::size(std::vector<struct iovec> &iov)
{
    size_t iovoff = 0; // 字段偏移量，第0个字段的偏移量为0
    size_t total = 0;  // 整个记录长度
    Integer it;

    // 累加各个字段的长度
    for (size_t i = 0; i < iov.size(); ++i) {
        it.set(iovoff);
        total += it.size() + iov[i].iov_len;
        iovoff += iov[i].iov_len;
    }

    // 再加上总长度
    it.set(total);
    total += it.size() + 1;

    return total;
}

size_t Record::startOfoffsets()
{
    Integer it;
    it.decode((char *) buffer_ + 1, length_ - 1);
    return it.size() + 1;
}

size_t Record::startOfFields()
{
    size_t offset = 1;

    // 总长度所占大小
    Integer it;
    bool ret = it.decode((char *) buffer_ + 1, length_ - 1);
    if (!ret)
        return 0;
    else
        offset += it.size();

    // 枚举所有字段长度
    while (true) {
        if (offset >= length_) return 0;
        ret = it.decode((char *) buffer_ + offset, length_ - offset);
        if (!ret)
            return 0;
        else
            offset += it.size();

        // 找到尾部
        if (it.value_ == 0) break;
    }

    return offset;
}

bool Record::set(std::vector<struct iovec> &iov, const unsigned char *header)
{
    // 偏移量
    unsigned int offset = 1;

    // 先计算所需空间大小
    size_t total = size(iov);
    if ((size_t) length_ < total) return false;

    // 输出头部
    memcpy(buffer_, header, HEADER_SIZE);

    // 输出记录长度
    Integer it;
    it.set(total);
    it.encode((char *) buffer_ + offset, length_);
    offset += (unsigned int) it.size();

    // 输出字段偏移量数组
    size_t len = 0;
    for (size_t i = 0; i < iov.size(); ++i)
        len += iov[i].iov_len; // 计算总长
    // 逆序输出
    for (size_t i = iov.size(); i > 0; --i) {
        len -= iov[i - 1].iov_len;
        it.set(len);
        it.encode((char *) buffer_ + offset, length_);
        offset += (unsigned int) it.size();
    }

    // 顺序输出各字段
    for (size_t i = 0; i < iov.size(); ++i) {
        memcpy(buffer_ + offset, iov[i].iov_base, iov[i].iov_len);
        offset += (unsigned int) iov[i].iov_len;
    }

    // 设置length
    length_ =
        (unsigned short) (total + ALIGN_SIZE - 1) / ALIGN_SIZE * ALIGN_SIZE;

    // 输出padding
    if (total < length_)
        for (size_t i = 0; i < length_ - total; ++i)
            this->buffer_[length_ - i] = 0;

    return true;
}

size_t Record::length()
{
    Integer it;
    return it.decode((char *) buffer_ + 1, length_) ? it.value_ : 0;
}

size_t Record::fields()
{
    unsigned short offset = 1; // 含头部字段

    // 计算总长度的字节数
    Integer it;
    bool ret = it.decode((char *) buffer_ + 1, length_);
    if (!ret)
        return 0;
    else
        offset += it.size();

    // 枚举所有字段长度
    size_t total = 0;
    while (true) {
        if (offset >= length_) return 0;
        ret = it.decode((char *) buffer_ + offset, length_ - offset);
        if (!ret)
            return 0;
        else
            offset += it.size();

        ++total;
        // 找到尾部
        if (it.value_ == 0) break;
    }

    return total;
}

bool Record::get(std::vector<struct iovec> &iov, unsigned char *header)
{
    size_t offset = 1; // 偏移量

    // 拷贝header
    if (header == NULL) return false;
    ::memcpy(header, buffer_, HEADER_SIZE);

    // 总长
    Integer it;
    bool ret = it.decode((char *) buffer_ + 1, length_);
    if (!ret) return false;
    size_t length = it.get(); // 记录总长度
    offset += it.size();

    // 枚举所有字段长度
    size_t index = 0;
    std::vector<size_t> vec(iov.size()); // 存放各字段长度
    while (true) {
        if (offset >= length_) return false;
        ret = it.decode((char *) buffer_ + offset, length_ - offset);
        if (!ret) return false;

        vec[index] = it.get(); // 临时存放偏移量，注意是逆序
        if (++index > iov.size()) return false;

        // 找到尾部
        offset += it.size();
        if (it.value_ == 0) break;
    }
    if (index != iov.size()) return false; // 字段数目不对

    // 逆序，先交换
    for (size_t i = 0; i < iov.size() / 2; ++i) {
        size_t tmp = vec[i];
        vec[i] = vec[iov.size() - i - 1];
        vec[iov.size() - i - 1] = tmp;
    }
    // check长度
    for (size_t i = 0; i < iov.size() - 1; ++i) {
        vec[i] = vec[i + 1] - vec[i];
        if (vec[i] > iov[i].iov_len)
            return false; // 要求iov长度足够
        else
            iov[i].iov_len = vec[i];
    }

    // 最后一个字段长度
    vec[iov.size() - 1] = length - vec[iov.size() - 1] - offset;
    if (vec[iov.size() - 1] > iov[iov.size() - 1].iov_len)
        return false;
    else
        iov[iov.size() - 1].iov_len = vec[iov.size() - 1];

    // 拷贝字段
    for (size_t i = 0; i < iov.size(); ++i) {
        ::memcpy(iov[i].iov_base, buffer_ + offset, iov[i].iov_len);
        offset += iov[i].iov_len;
    }

    return true;
}

bool Record::ref(std::vector<struct iovec> &iov, unsigned char *header)
{
    size_t offset = 1; // 偏移量

    // 拷贝header
    if (header == NULL) return false;
    ::memcpy(header, buffer_, HEADER_SIZE);

    // 总长
    Integer it;
    bool ret = it.decode((char *) buffer_ + 1, length_);
    if (!ret) return false;
    size_t length = it.get();                         // 记录总长度
    length_ = (unsigned short) ALIGN_TO_SIZE(length); // 调整总长
    offset += it.size();

    // 枚举所有字段长度
    size_t index = 0;
    std::vector<size_t> vec; // 存放各字段长度
    while (true) {
        if (offset >= length_) return false;
        ret = it.decode((char *) buffer_ + offset, length_ - offset);
        if (!ret) return false;

        vec.push_back(it.get()); // 临时存放偏移量，注意是逆序
        ++index;

        // 找到尾部
        offset += it.size();
        if (it.value_ == 0) break;
    }
    iov.resize(index); // 调整iov的大小

    // 逆序，先交换
    for (size_t i = 0; i < iov.size() / 2; ++i) {
        size_t tmp = vec[i];
        vec[i] = vec[iov.size() - i - 1];
        vec[iov.size() - i - 1] = tmp;
    }

    // check长度
    for (size_t i = 0; i < iov.size() - 1; ++i) {
        vec[i] = vec[i + 1] - vec[i];
        iov[i].iov_len = vec[i];
    }
    // 最后一个字段长度
    vec[iov.size() - 1] = length - vec[iov.size() - 1] - offset;
    iov[iov.size() - 1].iov_len = vec[iov.size() - 1];

    // 引用各字段
    for (size_t i = 0; i < iov.size(); ++i) {
        iov[i].iov_base = (void *) (buffer_ + offset);
        offset += iov[i].iov_len;
    }

    return true;
}

bool Record::getByIndex(char *buffer, unsigned int *len, unsigned int idx)
{
    size_t offset = 1; // 偏移量

    // 总长
    Integer it;
    bool ret = it.decode((char *) buffer_ + 1, length_);
    if (!ret) return false;
    size_t length = it.get(); // 记录总长度
    offset += it.size();

    // 枚举所有字段长度
    size_t index = 0;
    std::vector<size_t> vec; // 存放各字段长度
    while (true) {
        if (offset >= length_) return false;
        ret = it.decode((char *) buffer_ + offset, length_ - offset);
        if (!ret) return false;

        vec.push_back(it.get()); // 临时存放偏移量，注意是逆序
        ++index;

        // 找到尾部
        offset += it.size();
        if (it.value_ == 0) break;
    }
    if (idx >= index) return false;

    // 逆序，先交换
    for (int i = 0; i < index / 2; ++i) {
        size_t tmp = vec[i];
        vec[i] = vec[index - i - 1];
        vec[index - i - 1] = tmp;
    }

    // 计算长度
    std::vector<size_t> lvec; // 存放各字段长度
    for (int i = 0; i < index - 1; ++i) {
        lvec.push_back(vec[i + 1] - vec[i]);
        if (i == idx) {
            if (*len < lvec[idx]) return false;
            *len = (unsigned int) lvec[idx];
            memcpy(buffer, buffer_ + offset + vec[idx], lvec[idx]);
            return true;
        }
    }

    // 最后一个字段长度
    lvec.push_back(length - vec[index - 1] - offset);
    if (*len < lvec[idx]) return false;
    *len = (unsigned int) lvec[idx];
    memcpy(buffer, buffer_ + offset + vec[idx], lvec[idx]);
    return true;
}

bool Record::refByIndex(
    unsigned char **buffer,
    unsigned int *len,
    unsigned int idx)
{
    size_t offset = 1; // 偏移量

    // 总长
    Integer it;
    bool ret = it.decode((char *) buffer_ + 1, length_);
    if (!ret) return false;
    size_t length = it.get(); // 记录总长度
    offset += it.size();

    // 枚举所有字段长度
    size_t index = 0;
    std::vector<size_t> vec; // 存放各字段长度
    while (true) {
        if (offset >= length_) return false;
        ret = it.decode((char *) buffer_ + offset, length_ - offset);
        if (!ret) return false;

        vec.push_back(it.get()); // 临时存放偏移量，注意是逆序
        ++index;

        // 找到尾部
        offset += it.size();
        if (it.value_ == 0) break;
    }
    if (idx >= index) return false;

    // 逆序，先交换
    for (int i = 0; i < index / 2; ++i) {
        size_t tmp = vec[i];
        vec[i] = vec[index - i - 1];
        vec[index - i - 1] = tmp;
    }

    // 计算长度
    std::vector<size_t> lvec; // 存放各字段长度
    for (int i = 0; i < index - 1; ++i) {
        lvec.push_back(vec[i + 1] - vec[i]);
        if (i == idx) {
            *len = (unsigned int) lvec[idx];
            *buffer = buffer_ + offset + vec[idx];
            return true;
        }
    }

    // 最后一个字段长度
    lvec.push_back(length - vec[index - 1] - offset);
    *len = (unsigned int) lvec[idx];
    *buffer = buffer_ + offset + vec[idx];
    return true;
}

} // namespace db
