// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

/*
	FilterBlock内部不像Block有复杂的(key,value)格式，其只有一个字符串。该字符串的内部结构如下图。
	filterblock保存多个filter string，每个filter string针对一段block，间隔为2^base_lg_，保存在末尾，
	默认值为2KB，也就是说，filter block每隔2kb block offset的key生成一个filter string，offset_array_
	指出了这些filter string的偏移。过滤时，一般先通过data index block获取key的大致block offset，
	再通过filter string的offset_array_获取该block offset的filter string，再进行过滤。生成时，
	对同一个block offset范围的数据一起构建filter string。具体代码细节在filter_block.cc/.h中。
	读写分别用FilterBlockReader与FilterBlockBuilder来封装

*/

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  // 注意本轮keys产生的位图计算完毕后，会将keys_, start_, 还有tmp_keys_ 清空
  const FilterPolicy* policy_;
  std::string keys_;              // Flattened key contents // 暂时存放本轮所有keys，追加往后写入
  std::vector<size_t> start_;     // Starting index in keys_ of each key  // 记录本轮key与key之间的边界的位置，便于分割成多个key
  std::string result_;            // Filter data computed so far  // 计算出来的位图，多轮计算则往后追加写入
  std::vector<Slice> tmp_keys_;   // policy_->CreateFilter() argument  // 将本轮的所有key，存入该vector，其实并无存在的必要，用临时变量即可
  std::vector<uint32_t> filter_offsets_;  //计算出来的多个位图的边界位置，用于分隔多轮keys产生的位图

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

class FilterBlockReader {
 public:
 // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  const char* data_;    // Pointer to filter data (at block-start)
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
