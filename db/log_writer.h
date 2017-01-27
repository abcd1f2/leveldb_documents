// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_WRITER_H_

#include <stdint.h>
#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class WritableFile;

namespace log {
/*
	log�ļ���ʽҲ�Ƿֿ�洢�ġ���sstable�ļ���ͬ���ǣ�һ����sstable������洢�ģ����Ϊ�˼��ٶ�ȡ��
	�������������log�ļ�ʼ����˳���д�ģ�����Ҫ��λĳ��key���������Ҫ������Ϣ����һ����sstable��
	data block����°�4KB�ֿ飬��ʵ���ϴ洢�Ŀ��Сͨ�����4KB������Ҫ����Ϊ����record�����block��
	��log�ļ��е�block���ϸ�֤Ϊһ����Ĭ��ֵΪ32KB����ˣ�log�ļ��е�record���ܻ��飬Ϊ������record�Ƿ������
	��ͬ��block�в�ͬ�����ͣ�kFullType/kFirstType/kMiddleType/kLastType��
*/

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(WritableFile* dest);

  // Create a writer that will append data to "*dest".
  // "*dest" must have initial length "dest_length".
  // "*dest" must remain live while this Writer is in use.
  Writer(WritableFile* dest, uint64_t dest_length);

  ~Writer();

  Status AddRecord(const Slice& slice);

 private:
  WritableFile* dest_;
  int block_offset_;       // Current offset in block

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  // No copying allowed
  Writer(const Writer&);
  void operator=(const Writer&);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_WRITER_H_
