// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {

Comparator::~Comparator() { }

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  /*
        TableBuild中Add函数的这段逻辑就是用来计算分割key，并插入key-value pair到index block。关键函数在
        r->options.comparator->FindShortestSeparator(&r->last_key, key);
        FindShortestSeparator的声明如下： FindShortestSeparator(std::string *start, const Slice& limit)
        
        该函数的的作用是，如果start<limit,就把start修改为*start和limit的共同前缀后面多一个字符加1。很难理解用例子来说明下：
            *start:    helloleveldb        上一个data block的最后一个key
            limit:     helloworld          下一个data block的第一个key
            由于 *start < limit, 所以调用 FindShortSuccessor(start, limit)之后，start变成：
            hellom (保留前缀，第一个不相同的字符+1)
        
        这里面有一个特例，即上一个data block的最后一个key是下一个data block第一个可以 的子串，怎么处理?
            *start:    hello               上一个data block的最后一个key
            limit:     helloworld          下一个data block的第一个key
            由于 *start < limit, 所以调用 FindShortSuccessor(start, limit)之后，start变成：
            hello (保留前缀，第一个不相同的字符+1)
  */
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

static port::OnceType once = LEVELDB_ONCE_INIT;
static const Comparator* bytewise;

static void InitModule() {
  bytewise = new BytewiseComparatorImpl;
}

const Comparator* BytewiseComparator() {
  port::InitOnce(&once, InitModule);
  return bytewise;
}

}  // namespace leveldb
