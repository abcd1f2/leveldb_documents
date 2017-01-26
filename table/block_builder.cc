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
sstable��ʽ:
    �����ļ�����һϵ�е�Block��Block�Ǵ���io�Լ��ڴ�cache�Ļ�����λ��ͨ��Block��д���Ծ�̯ÿ��IO�Ŀ��������þֲ���ԭ��Block�Ĵ�С���û������õ�
    1) Data Blocks�����kv�ԣ���ʵ�ʵ����ݡ�kv�Իᰴ�մ�С���ֳ�Block���޷���֤���е�Block��Сһ�£�
        �������ӽ������õ�Block��С�����ǵ�kv�����ݴ��ڸ�ֵʱ������Ϊһ��Block��Block�ڲ�������������һЩԪ���ݣ����Ļ�������ܡ�
    2) Filter Block: Filter����ȷ��SSTable�Ƿ����һ��key���Ӷ��ڲ��Ҹ�keyʱ���������IO��Filter Block���ڴ洢Filter���л���Ľ����
    3) Meta Index Block: ���ڴ��Ԫ���ݣ�Ŀǰֻ��kv��ʽ�洢Filter block��ƫ������key��filter.${FILTER_NAME}��value��filter block���ļ���ƫ������
    4) Index Block: ��Data block�������������˸���Block����Сkey���Ӷ�ȷ����Block��key�ķ�Χ������ĳ��key��������
    5) Footer: Ԫ���ݵ�Ԫ���ݣ����а���Index Block��Meta Index Block��ƫ������Footer֮�������������Ϊ�ļ�����ʱ��˳��׷�ӵģ�
        ��Footer����Ϣ��������֮ǰ��������Ϣ������ֻ����������ڰ�����Ԫ���ݣ����Զ�ȡSSTableʱ��Ҫ�ľ��Ǽ���footer.

    ��ʽ��⣺
        Ϊ��֧��Filter���ƣ�LevelDBΪSSTable������һ���µ�Meta Block���ͣ���filter�� Meta Block��������ݿ��ڴ�ʱ��ָ����FilterPolicy��
        metaindex block�ڽ������һ����¼������"filter.<N>"ӳ�䵽��Ը�filter meta block��block handle��
        �˴���<N>����������filter policy��Name()�������ص��ַ���{!Ҳ����˵��metaindex block�ڽ�����һ��KeyValue������Ѱַfilter summary����
        ��ɵ�meta block�����ڸ�KeyValue����˵��Key����"filter.<N>"��value����filter meta block��handle}��

        Filter Block�洢��һϵ�е�filters��������Щfilters��˵��filter i��������offset����[ i*base ... (i+1)*base-1 ]���Ǹ�block������keyΪ����
        ��FilterPolicy::CreateFilter()������������{!���ʵ����һ��filter���ܶ�Ӧ��ֹһ��block�����ǿ϶���������block��һ��block�����Խ����filter��
        �������Կ��ٵؼ����һ��block��Ӧ��filter����Ϊdata index block�д洢��data block��offset��
        ֱ�Ӹ���offset��baseȡֵ�Ϳ��Լ������data block��Ӧ�˵ڼ���filter��ֻҪ֪�����ǵڼ���filter������Filter Block����Ľṹ��
        �Ϳ���ֱ�ӷ��ʸ�filter������}��

        Ŀǰ��base��ֵ��2KB����ˣ� ����blocks X��Y����ʼλ�ö���λ��[0KB,2KB-1]��X��Y�е�����key����ͨ������FilterPolicy::CreateFilter()ת��Ϊһ��filter��
        ͬʱ���ռ������filter������Ϊfilter block�ĵ�һ��filter���д洢��
    
        ͨ��Filter Block�ײ���offset������Ժ����׵ؽ�һ��data block��offsetӳ�䵽������Ӧ��filter{!����ǰ��Ľ��ͣ�����data block���Ǻ����׵ó�
        ����Ӧ���ǵڼ���filter��֪���˵ڼ���filter���ٸ��ݴ˴���offset��������׾ͼ����filter��offset�ˣ�֪����offset��֪��size��
        �Ϳ���ȡ����filter��Ӧ�������ˣ���size�ֿ��Ը�����һ��filter��offset-��ǰoffset����ó��������offset��ָ��filter block�ڵ�ƫ�ƣ��������ļ��ڵġ�
        ��ע��lg(base)�洢����base��2ȡ������ֵ��Ҳ����˵����baseֵΪ2KB����ô�洢�ľ���lg(2*1024)=11}��

���ݽṹ:
    BlockHandle: ָ���ļ��е�һ��Block������������Block��ƫ������offset_���ʹ�С��size_����
    Footer: ��ʾSSTable�ļ���Footer����С�̶�
    �������ṹ�ṩ��string�����л��ͷ����л��ķ���

    TableBuilder: ����SSTable����ڡ���һϵ�е�kv�Թ����SSTable��
    BlockBuilder: ����Block������ӵ�kv�Խ������л���
    FilterBlockBuilder: ����Filter Block
    ���ϱ�������SSTable�ļ�����Ҫ���ݽṹ


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
    ����ǰdata_block�е������㹻�࣬Ԥ�ƴ���Ԥ�������ֵ��ʱ�򣬾Ϳ�ʼflush��
    ��νdata block��Flush���ǽ����е�������ָ���¼���������Ҽ�¼������ĸ���
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
    ע�⣬�ܶ�key�������ظ����ֽڣ����硰hellokitty���͡�helloworld�����������ڵ�key������key���й����Ĳ��֡�hello����
        ��ˣ�����������Ĳ�����ȡ��������Ч�Ľ�ʡ�洢�ռ䡣
    �������ֿ��ǣ�LevelDb������ǰ׺ѹ��(prefix-compressed)������LevelDb��key�ǰ������еģ�����������ļ��ٿռ�ռ�á�
        ���⣬ÿ���16��keys(Ŀǰ�汾��options_->block_restart_intervalĬ��Ϊ16)��
        LevelDb��ȡ��ʹ��ǰ׺ѹ�������Ǵ洢����key(���ǰѴ洢����key�ĵ����������)
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
      //�µ������㣬��¼��λ��
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
