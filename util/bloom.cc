// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

namespace {
static uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

/*
    leveldb搞个全局的Bloom Filter是不现实的，因为无法预知客户到底存放多少key，leveldb可能存放百万千万个key－value pair，
    也可能存放几百 几千个key－value，因为n不能确定范围，因此位图的大小m，也很难事先确定。
    如果设置的m过大，势必造成内存浪费，如果设置的m过小，就会造成很大的虚警。
    leveldb的设计，并不是全局的bloom filter，而是根据局部的bloom filter，每一部分的数据，设计出一个bloom filter，
    多个bloom filter来完成任务。
    leveldb中的bloom filter有第二个层面的改进，即采用了下面论文中的算法思想：
    Less Hashing, Same Performance: Building a Better Bloom Filter (https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf)
    这个论文有兴趣的筒子可以读一下，我简单summary一下论文的思想，为了尽可能的降低虚概率，最优的hash函数个数可能很高，
    比如需要10个hash函数，这势必带来很多的计算，而且要设计多个不同的hash函数，
    论文提供了一个思想，用1个hash函数，多次移位和加法，达到多个hash 的结果。
*/
class BloomFilterPolicy : public FilterPolicy {
 private:
  size_t bits_per_key_;
  size_t k_;

 public:
  explicit BloomFilterPolicy(int bits_per_key)
      : bits_per_key_(bits_per_key) {
    // We intentionally round down to reduce probing cost a little bit
    k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  virtual const char* Name() const {
    return "leveldb.BuiltinBloomFilter2";
  }

  /*
        n:key的个数；dst:存放过滤器处理的结果
        https://bean-li.github.io/leveldb-sstable-bloom-filter/

  */
  virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const {
    // Compute bloom filter size (in both bits and bytes)
    size_t bits = n * bits_per_key_;

    // For small n, we can see a very high false positive rate.  Fix it
    // by enforcing a minimum bloom filter length.
    //位列bits最小64位，8个字节
    if (bits < 64) bits = 64;

    // bits位占多少个字节
    size_t bytes = (bits + 7) / 8;

    // 得到真实的位列bits
    bits = bytes * 8;

    const size_t init_size = dst->size();
    dst->resize(init_size + bytes, 0);

    // 在过滤器集合最后记录需要k_次哈希
    dst->push_back(static_cast<char>(k_));  // Remember # of probes in filter
    char* array = &(*dst)[init_size];
    // 外层循环是每一个key值
    for (int i = 0; i < n; i++) {
      // Use double-hashing to generate a sequence of hash values.
      // See analysis in [Kirsch,Mitzenmacher 2006].
      uint32_t h = BloomHash(keys[i]);
      const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits

      // 使用k个哈希函数，计算出k位，每位都赋值为1。
      // 为了减少哈希冲突，减少误判。
      for (size_t j = 0; j < k_; j++) {
          //得到元素在位列bits中的位置
        const uint32_t bitpos = h % bits;

        /*
            bitpos/8计算元素在第几个字节；
            (1 << (bitpos % 8))计算元素在字节的第几位；
            例如：
            bitpos的值为3， 则元素在第一个字节的第三位上，那么这位上应该赋值为1。
            bitpos的值为11，则元素在第二个字节的第三位上，那么这位上应该赋值为1。
            为什么要用|=运算，因为字节位上的值可能为1，那么新值赋值，还需要保留原来的值。
        */
        array[bitpos/8] |= (1 << (bitpos % 8));
        h += delta;
      }
    }
  }

  virtual bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const {
    const size_t len = bloom_filter.size();
    if (len < 2) return false;

    const char* array = bloom_filter.data();
    const size_t bits = (len - 1) * 8;

    // Use the encoded k so that we can read filters generated by
    // bloom filters created using different parameters.
    const size_t k = array[len-1];
    if (k > 30) {
        //为短bloom filter保留，当前认为直接match 
      // Reserved for potentially new encodings for short bloom filters.
      // Consider it a match.
      return true;
    }

    uint32_t h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k; j++) {
      
      // 得到元素在位列bits中的位置
      const uint32_t bitpos = h % bits;

      // 只要有一位为0，说明元素肯定不在过滤器集合内
      if ((array[bitpos/8] & (1 << (bitpos % 8))) == 0) return false;
      h += delta;
    }
    return true;
  }
};
}

const FilterPolicy* NewBloomFilterPolicy(int bits_per_key) {
  return new BloomFilterPolicy(bits_per_key);
}

}  // namespace leveldb
