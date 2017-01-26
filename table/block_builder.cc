// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"

namespace leveldb {

/*
sstable格式:
    整个文件包括一系列的Block，Block是磁盘io以及内存cache的基本单位，通过Block读写可以均摊每次IO的开销，利用局部性原理。Block的大小是用户可配置的
    1) Data Blocks：存放kv对，即实际的数据。kv对会按照大小划分成Block，无法保证所有的Block大小一致，
        但基本接近于配置的Block大小，但是当kv对数据大于该值时，会作为一个Block。Block内部还包括其他的一些元数据，后文会深入介绍。
    2) Filter Block: Filter用于确定SSTable是否包含一个key，从而在查找该key时，避免磁盘IO。Filter Block用于存储Filter序列化后的结果。
    3) Meta Index Block: 用于存放元数据，目前只会kv格式存储Filter block的偏移量，key是filter.${FILTER_NAME}，value是filter block在文件的偏移量。
    4) Index Block: 对Data block的索引，保存了各个Block中最小key，从而确定了Block的key的范围，加速某个key的搜索。
    5) Footer: 元数据的元数据，其中包含Index Block和Meta Index Block的偏移量。Footer之所以在最后，是因为文件生成时是顺序追加的，
        而Footer的信息又依赖于之前的所有信息，所以只能在最后。由于包含了元数据，所以读取SSTable时首要的就是加载footer.

    格式详解：
        为了支持Filter机制，LevelDB为SSTable增加了一个新的Meta Block类型：”filter” Meta Block。如果数据库在打开时，指定了FilterPolicy，
        metaindex block内将会包含一条记录用来将"filter.<N>"映射到针对该filter meta block的block handle。
        此处”<N>”代表了由filter policy的Name()方法返回的字符串{!也就是说在metaindex block内将会有一个KeyValue对用来寻址filter summary数据
        组成的meta block，对于该KeyValue对来说，Key就是"filter.<N>"，value就是filter meta block的handle}。

        Filter Block存储了一系列的filters。对于这些filters来说，filter i包含了以offset落在[ i*base ... (i+1)*base-1 ]的那个block的所有key为输入
        的FilterPolicy::CreateFilter()函数的输出结果{!因此实际中一个filter可能对应不止一个block，但是肯定是整数个block，一个block不会跨越两个filter，
        这样可以快速地计算出一个block对应的filter，因为data index block中存储了data block的offset，
        直接根据offset和base取值就可以计算出该data block对应了第几个filter。只要知道了是第几个filter，根据Filter Block自身的结构，
        就可以直接访问该filter数据了}。

        目前，base的值是2KB。因此， 比如blocks X和Y的起始位置都是位于[0KB,2KB-1]，X和Y中的所有key将会通过调用FilterPolicy::CreateFilter()转变为一个filter，
        同时最终计算出的filter将会作为filter block的第一个filter进行存储。
    
        通过Filter Block底部的offset数组可以很容易地将一个data block的offset映射到它所对应的filter{!根据前面的解释，给定data block我们很容易得出
        它对应的是第几个filter。知道了第几个filter，再根据此处的offset数组很容易就计算出filter的offset了，知道了offset再知道size，
        就可以取出该filter对应的数据了，而size又可以根据下一个filter的offset-当前offset计算得出。这里的offset是指在filter block内的偏移，而不是文件内的。
        另注意lg(base)存储的是base对2取对数的值，也就是说比如base值为2KB，那么存储的就是lg(2*1024)=11}。

数据结构:
    BlockHandle: 指向文件中的一个Block，有两个属性Block的偏移量（offset_）和大小（size_）。
    Footer: 表示SSTable文件的Footer，大小固定
    这两个结构提供到string的序列化和反序列化的方法

    TableBuilder: 构造SSTable的入口。将一系列的kv对构造成SSTable。
    BlockBuilder: 构造Block，对添加的kv对进行序列化。
    FilterBlockBuilder: 构造Filter Block
    以上便是生成SSTable文件的主要数据结构


*/

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);       // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);       // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                        // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +   // Restart array
          sizeof(uint32_t));                      // Restart array length
}

/*
    当当前data_block中的内容足够多，预计大于预设的门限值的时候，就开始flush，
    所谓data block的Flush就是将所有的重启点指针记录下来，并且记录重启点的个数
*/
Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

/*
    注意，很多key可能有重复的字节，比如“hellokitty”和”helloworld“是两个相邻的key，由于key中有公共的部分“hello”，
        因此，如果将公共的部分提取，可以有效的节省存储空间。
    处于这种考虑，LevelDb采用了前缀压缩(prefix-compressed)，由于LevelDb中key是按序排列的，这可以显著的减少空间占用。
        另外，每间隔16个keys(目前版本中options_->block_restart_interval默认为16)，
        LevelDb就取消使用前缀压缩，而是存储整个key(我们把存储整个key的点叫做重启点)
    example:
        abcd123 1111111111
        abcd456 2222222222
        4 3 10 456 0123456789
        shared_length + non_shared_length + value_length + non_shared_value + value
*/
void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty() // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
      //新的重启点，记录下位置
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
