// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <stdint.h>
#include "leveldb/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.

/*
	table目录下的主要改动有：
	新增了filter_block.h，filter_block.cc，内有FilterBlockReader,FilterBlockBuilder用于filter block的读写。
	FilterBlockBuilder有三个接口StartBlock,AddKey,Finish ,这三个接口调用必须按照如下形式(StartBlock AddKey*)* Finish。
	StartBlock函数会判断是否需要将在此block之前的那些block的key组成的集合求一次filter，如果之前的block所对应的filter与
	当前block对应的不是同一个，则GenerateFilter()。AddKey会把key连续存放到一个string中，在GenerateFilter()时会根据keys_
	和start_构建出一个key的list，然后计算它们对应的summary。最终result_里保存的就是最终的filter block数据。
	FilterBlockReader功能主要在于根据现有的filter block数据，判断给定的key及其所在block的offset，判断key是否可能命中。

	Table类中新增InternalGet,ReadMeta,ReadFilter函数，同时新增filter, filter_data两个成员变量。
	在Open()时，会调用ReadMeta函数，该函数会读取metaindex block，并根据” filter.<N>”找到filter meta block的handle，
	然后调用ReadFilter函数将filter meta block数据读出，并设置filter和filter_data。
	InternalGet函数，会首先找到key所对应的block handle，这样就得到了该block对应的offset，
	然后就可以通过filter->KeyMayMatch判断是否需要继续读取，如果不需要就可以直接返回，避免了对data block的读取。

	TableBuilder类中增加filter_block成员变量，在TableBuilder::Add()调用的同时会通过调用AddKey函数更新filter数据，
	在TableBuilder::Flush()调用时，调用StartBlock函数，在TableBuilder::Finish()调用时，会首先写入filter block数据，
	此时会调用Finish函数，然后写入metaindex block，然后是index block，最后是footer
*/

class Table {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to NULL and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options,
                     RandomAccessFile* file,
                     uint64_t file_size,
                     Table** table);

  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key) const;

 private:
  struct Rep;
  Rep* rep_;

  explicit Table(Rep* rep) { rep_ = rep; }
  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  friend class TableCache;
  Status InternalGet(
      const ReadOptions&, const Slice& key,
      void* arg,
      void (*handle_result)(void* arg, const Slice& k, const Slice& v));


  void ReadMeta(const Footer& footer);
  void ReadFilter(const Slice& filter_handle_value);

  // No copying allowed
  Table(const Table&);
  void operator=(const Table&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
