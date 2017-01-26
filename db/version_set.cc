// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

/*
    MANIFEST�ļ��𻵺���λָ�leveldb

    ΪʲôMANIFEST�𻵻��߶�ʧ֮����Ȼ���Իָ�������LevelDB���������
    ����LevelDB���ԣ��޸��������£�
        1.���ȴ���log����Щ��δ���ü�д��ļ�¼��д���µ�.sst�ļ�
        2.ɨ�����е�sst�ļ�������Ԫ������Ϣ������number filesize�� ��Сkey�����key
        3.������ЩԪ������Ϣ���������µ�MANIFEST�ļ���
    ��������������µ�MANIFEST�� ��Ϊsstable�ļ��Ƿ�level�ģ����Ǻܲ��ң������޷����������жϳ����ļ������ĸ�level��
        �����������ԭ���ǣ���Ȼ�ҷֲ��������Ҿ���Ϊ���е�sstale�ļ�������level 0����Ϊlevel 0�������ص��ģ���˲�û��Υ��������׼��

    ���޸�֮�󣬵�һ��Open LevelDB��ʱ�򣬺�����level 0 ���ļ�����ԶԶ����4���ļ�����˻�Compaction�� 
        ����Ϊ���е��ļ�����Level 0 ���Compaction�����Ƿǳ����صġ�����ɨ�����е��ļ����鲢���򣬲�����level 1�ļ�����������������level���ļ���

    ������Ĵ������̿������ֻ��MANIFEST�ļ���ʧ�������ļ�û���𻵣�LevelDB�ǲ��ᶪʧ���ݵģ�
        ԭ���ǣ�LevelDB��Ȼ�Ѿ��޷������е����ݷֵ���ͬ��Level���������ݱϾ�û�ж��������ļ���number����ȫ�����жϳ��ļ����¾ɣ�
        �Ӷ�ȷ����ͬsstable�ļ��е��ظ����ݣ�which�����µġ�����һ�αȽϺ�ʱ�Ĺ鲢���򣬾Ϳ����������µ�levelDB��
    
    �����ķ������ӹ��ܵĽǶȿ�������ȷ�ģ�����Ч���ϲ��ҹ�ά��Riak�������Թ�78000��sstable �ļ���490G�����ݣ���Ҷ�λ��Level 0��
        �鲢������Ҫ����6 weeks��6�ܰ��������ʱ���˷���ġ�
    
    Riak 1.3 �汾�����Ż����ı���Ŀ¼�ṹ������google ����汾��LevelDB�����е��ļ�����һ��Ŀ¼�£�����Riak 1.3�汾��������Ŀ¼��
        ����ͬlevel��sst �ļ����벻ͬ����Ŀ¼��
        sst_0
        sst_1
        ...
        sst_6
    �����������������MANIFEST��Ȼ�ͺܼ��ˣ�ͬ����78000 sstable�ļ���Repair���̺�ʱ�Ƿ��Ӽ����
*/

namespace leveldb {

static const int kTargetFileSize = 2 * 1048576;

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static const int64_t kMaxGrandParentOverlapBytes = 10 * kTargetFileSize;

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static const int64_t kExpandedCompactionByteSizeLimit = 25 * kTargetFileSize;

static double MaxBytesForLevel(int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  double result = 10 * 1048576.0;  // Result for both level-0 and level-1
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(int level) {
  return kTargetFileSize;  // We could vary per level to reduce number of files?
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  //��VersionSet��ע��
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  //��Version�����е��ļ������ü�����1
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

/*
��files�в���key�������Ƕ��ֲ��ң�˵��files�е�key������ģ����ҵ�����vector��index
upper_bound
*/
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
             const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

/*
user_key�Ƿ��f��largest��user_key��
*/
static bool AfterFile(const Comparator* ucmp,
                      const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs before all keys and is therefore never after *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs after all keys and is therefore never before *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

/*
�ص���
��Сkey��file����Сkey���󣬲��ң����key��file�����key��С
*/
bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != NULL) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small(*smallest_user_key, kMaxSequenceNumber,kValueTypeForSeek);
    index = FindFile(icmp, files, small.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp),
        flist_(flist), //ĳһ���ļ�����
        index_(flist->size()) {        // Marks as invalid ĳһ���ļ��ı�ţ������ļ�����ʱ����Ϊ��Ч 
  }
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  virtual void Seek(const Slice& target) {
      //�������Ҽ�ֵ���ڵ���target���ļ�����
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const {
      //���ش��ڵ���target�ļ�������ֵ
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const {
    assert(Valid()); //��������ļ����ļ���źʹ�С��װ�ɵ��ַ���
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_+8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

// ����ĺ���ָ�룬����Ϊ�˽��ļ�Ԫ������ӽ�Table_cache
static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options,
                              DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

/*
����������table��˫�������
��������Ϊlevel�ĵ�����
�ڲ������Ϊfile*
*/
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]),
      &GetFileIterator, vset_->table_cache_, options);
}

/*
level0��Ϊ���ص�������files��key��������ģ�����ÿ��file��key������ģ�������Ҫ��Ӷ��iter
0���ϵ�files���key��������ģ�����ֱ��ʹ��FindFile�����ң�ֻ��Ҫһ��iter
*/
void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(
        vset_->table_cache_->NewIterator(
            options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key,
                                 void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  // TODO(sanjay): Change Version::Get() to use this function.
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

/*
    �õ����version�е�key-valueֵ
*/
Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
                    GetStats* stats) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  Status s;

  stats->seek_file = NULL;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = NULL;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.
  std::vector<FileMetaData*> tmp;
  FileMetaData* tmp2;
  //����level-0��˳�����ÿ���ļ���������level�Ƕ��ֲ���
  for (int level = 0; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Get the list of files to search in this level
    FileMetaData* const* files = &files_[level][0];
    if (level == 0) {
      // Level-0 files may overlap each other.  Find all files that
      // overlap user_key and process them in order from newest to oldest.
      tmp.reserve(num_files);
      for (uint32_t i = 0; i < num_files; i++) {
        FileMetaData* f = files[i];
        if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
            ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
          tmp.push_back(f);
        }
      }
      if (tmp.empty()) continue;

      //��file�������ǵ�number�Ӵ�С���У���Ϊ���Խ���fileԽ�� 
      std::sort(tmp.begin(), tmp.end(), NewestFirst);
      files = &tmp[0];
      num_files = tmp.size();
    } else {
      // Binary search to find earliest index whose largest key >= ikey.
      uint32_t index = FindFile(vset_->icmp_, files_[level], ikey);
      if (index >= num_files) {
        files = NULL;
        num_files = 0;
      } else {
        tmp2 = files[index];
        if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
          // All of "tmp2" is past any data for user_key
          files = NULL;
          num_files = 0;
        } else {
          files = &tmp2;
          num_files = 1;
        }
      }
    }

    //�ӷ����������ļ��в��� 
    for (uint32_t i = 0; i < num_files; ++i) {
      if (last_file_read != NULL && stats->seek_file == NULL) {
        // We have had more than one seek for this read.  Charge the 1st file.
        stats->seek_file = last_file_read;
        stats->seek_file_level = last_file_read_level;
      }

      FileMetaData* f = files[i];
      last_file_read = f;
      last_file_read_level = level;

      Saver saver;
      saver.state = kNotFound;
      saver.ucmp = ucmp;
      saver.user_key = user_key;
      saver.value = value;
      s = vset_->table_cache_->Get(options, f->number, f->file_size,
                                   ikey, &saver, SaveValue);
      if (!s.ok()) {
        return s;
      }
      switch (saver.state) {
        case kNotFound:
          break;      // Keep searching in other files
        case kFound:
          return s;
        case kDeleted:
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
        case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
      }
    }
  }

  return Status::NotFound(Slice());  // Use an empty error message for speed
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != NULL) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == NULL) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() {
  ++refs_;
}

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

/*
    �ҵ����Դ�����memtable��level
    �����ԭ���ǣ������ҵ�û���ص������level�������ǵ�2��if����˼
    ��3��if����˼��˵�����parent��û���ص���levelӦ��ѡȡparent��
    �����������֮����ص�����̫��Ļ�����һ��compact�ĸ��ʾͻ�����
    ���������������֮���һ��������㡱��������compact�Ĺ���(�Ͼ�����Խ�ߣ��ļ�Խ��)
    ��Ҳ��lazy˼������֡�

    �²���������sstable ����һ�����Ǵ���level 0�� ���ܴ��������£�����level 0���´����ĳ�����sstable�ļ�Ӧ��λ����һ���أ� ��PickLevelForMemTableOutput ���������㣺
    �Ӳ�����Ҫ��������compact���ļ�������level���Ͼ���level 0 ��Ҫ�����ļ����࣬compaction IO�Ͳ��Ҷ��ȽϺķѣ���һ����Ҳ������������level��
    һ���̶��Ͽ��Ʋ��ҵĴ�����������ĳЩ��Χ��key���±Ƚ�Ƶ�����������߲�compaction IO����Ҳ�ܴ� ����PickLevelForMemTableOutput���Ǹ�Ȩ�����С�
    ��������ɵ�sstable��Level 0��sstable�н�������ô�²�����sstable��ֱ�Ӽ���level 0���������һ���Ĳ��ԣ������Ƶ�Level1 ������Level 2��
    ��������Ƶ�Level2��������һ�����Ʋ�����kMaxMemCompactLevel��
*/
int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
    const Slice& largest_user_key) {
  int level = 0;
  // ���1.level0����files��������Сkey������(��һ���ǰ������е�key)  
  // ����s = 0�� l = 10�� ��ôfile��key����Ϊ��-1�� 2�� ��4�� 9�� 11��  
  // Ҳ�п���Ϊ��0�� 1�� 4�� 9�� 10����ͬ��key����DoCompactWork��ʱ��drop��  
  // �����Level 0�е�SSTable�ļ����ص���ֱ�ӷ��� level �� 0 
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    // levelֻ����0-kMaxMemCompactLevel(2)֮����� while ѭ��Ѱ�Һ��ʵ�level�㼶�����levelΪ2�����ܸ���
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        // ����2.����parent�����ص�  
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        // ����3.����grandparent���ص�������һ����ֵ
        if (sum > kMaxGrandParentOverlapBytes) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

/*
�ҵ�[begin,end]��һ�׶ε�file
����[40,100]����ôfile1[0,30],file2[31,50],file3[60,80],file4[90,110]��
2��3��4�������
���ң�����level0��˵���������и��ǣ��������һ��fileֻ��һ���ֱ�������[begin,end]��
��Ҫ����[begin,end]�ķ�Χ�������ͻὫ�����ص���files�����뵽inputs�С�
���൱�ڸ�Ⱦ��������level0��������������ص���files�����뵽inputs�С�
��ʵ���Կ�����level0�����⴦��ֻ�Ƿ�����compact�׶Ρ�
��������Ⱦ�����Ժ󣬾ͻᾡ���ܶ�ذ����ص����ļ��ռ�������������һ��ֻ��һС���ֽ���compact
��Ϊlevel-0���ļ���С��compact��ʱ�����Կ�һЩ��ʹ�ö�level-0��compact��Ϊ���ס�
���ﲢû��ʹ��lazy��˼�룬�����������һ���˷ܼ�һ������level-0�Ĵ�����ǰ�ˡ�
*/
// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputs(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != NULL) {
    user_begin = begin->user_key();
  }
  if (end != NULL) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size(); ) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != NULL && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) {
        //Ҳ����˵���ҵ�Level0�����е�file����С��smallest������largest��ȡ���е�file����inputs  
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != NULL && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != NULL && user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.

/*
    ��Ҫ����������Apply��SaveTo��
    Apply: ��VersionEdit���޸�Ӧ�õ�VersionSet��
    SaveTo: ����Щ�޸ķ��뵽һ��Version�С�

    ��������ڽ�manifest�ļ�������ӽ���ǰ�汾��������ǰ�汾��ӽ��汾����

    ��Version ��������һ��Version�м俿����VersionEdit��VersionEdit����������Щ�ļ�����ɾ���ˣ���Щ�ļ��������ġ����������LogAndApply��Ϊ�˷���ʵ��
    Version(N) + VersionEdit(N) = Version(N+1)
    ������Build���ݽṹ,������ݽṹ��һ��helper�࣬��æʵ��Version��Ծ����
*/

class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  //����һ�����������ļ�Ԫ���ݵĺ����������ǰ���С��ֵ���������С��ֵ��ȣ��Ͱ��ļ���Ŵ�С��������
  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  
  //�����ļ��������ͣ����ϴ�С��������
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  //�����İ汾����
  VersionSet* vset_;
  
  //��ǰ�汾
  Version* base_;

  //ÿһ���ļ�״̬����ӻ�ɾ���ļ�
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base)
      : vset_(vset),
        base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin();
          it != added->end(); ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  /*
      ��VersionEdit���޸�Ӧ�õ�VersionSet��
      ��edit�е���ϢӦ�õ�builder�У�new_filesӦ������ǰ������sst�ļ�
      ����һ���Ѵ��ڵ����ݿ�ʱ����ʱ��Ҫ�����̵��ļ���Ϣ�ָ���һ���汾��Ҳ���ǽ�manifest�ڵ���Ϣ��װ��Version_edit��
      ��Ӧ�õ���ǰ�汾����ʱ����Ҫ����Builder->Apply�������ⷽ�����ǽ�edit�����Ϣ������Builder�У������ٵ���Builder->Saveto���浽��ǰ�汾��

      ��һ���ǽ��汾��汾�ı仯����VersionEdit ��¼��Builder
  */
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
      // ��ÿ��compaction�ڵ���ӽ�version_set
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    /*
        ��ɾ���ļ���ӽ�Builder
        ��ɾ�����ֵ��ļ��������ļ������Ĳ㼶����¼��Builder�����ݽṹ��
        ɾ�����ļ���ֻ��Ҫ��¼sstable�ļ������־Ϳ�����
    */
    const VersionEdit::DeletedFileSet& del = edit->deleted_files_;
    for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) {
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    // �����ӵ��ļ���ӽ�Builder
    // ���������ļ����֣�Ҫ��VersionEdit�е������ļ����ֵ�����FileMetaData����¼����
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      // һ���ļ���seek������Ϊ�˱�������ļ����������ڵͲ��������Ч���ϵ���ʧ�� 
      f->allowed_seeks = (f->file_size / 16384);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  /*
        ����Щ�޸ķ��뵽һ��Version�С�
        ��һ�����Ǹ���Builder�е�base_ָ��ĵ�ǰ�汾current_, �Լ���Apply���ּ�¼��ɾ���ļ����ϣ�
        �����ļ����Ϻ�CompactionPointer���ϣ������һ���µ�Verison��������v��
  */
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      /*
          ������ӵ��ļ����ϸ��汾���ļ��ϲ�
          ɾ����deleted�����е��ļ����ѽ���������°汾v��
      */
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added->size());
      /*
        ��ԭ�����ڵ�files(base_files)��������files(added)���մ�С��˳����뵽files_��
        ���Ʋ�������
        Merge,���Ч��Ӧ�ñȽϸ�һЩ(Ӧ��Ҳ������)
        OPTIMISZE ME:����ط��ܲ���д����ͨ��merge?
      */
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) {
        // Add all smaller files listed in base_
          // ���ѭ���ǽ�������ӵ��ļ���С�����ļ�����ӽ��°汾��
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos;
             ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        //��������ļ���ӽ��°汾��
        MaybeAddFile(v, level, *added_iter);
      }

      // Add remaining base files
      // ��������ļ�����ʵҲ���Ǳ�������ļ����󡰵��ļ�
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i-1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    prev_end.DebugString().c_str(),
                    this_begin.DebugString().c_str());
            abort();
          }
        }
      }
#endif
    }
  }

  /*
  ɾ��deleted�������ļ��Ĳ�������MaybeAddFile���⺯��������ĺܺã�������ӣ���Ϊ�����deleted�����У������
  */
  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
      //��Ϊapply��ʱ������delete��add�ģ���add��ʱ����ظ��Ķ�ɾȥ�ˣ��������if�������е�  
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      /*
          ��level0����κ�level ��1��6����Version v�ڵĶ�Ӧ�㼶���ļ��б����������ģ����ܽ���
          ���Դ˴����ж� level >0,������Ϊ����care level0 �Ƿ񽻲棬��ʵ�ϣ����������ǽ����
      */
      if (level > 0 && !files->empty()) {
        // Must not overlap
          // ����Ǵ���0����ļ�������ӵ��ļ����ܺͼ������Ѵ��ڵ��ļ��н���
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++; //�ļ����ü�1
      files->push_back(f); //����ļ�
    }
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      dummy_versions_(this),
      current_(NULL) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

/*
��LogAndApply��Recover�е��ã�
*/
void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != NULL) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

/*
    ��edit��������Ϊһ��record���뵽manifest�ļ��У�
    ������ǰversion��edit��������½�һ��version��Ȼ����뵽versions_��
    ���������Ҫ�ǽ�edit��Ϣд��manifest�ļ��У���Ӧ�õ��°汾�С��������ļ��ϲ�֮��,������ļ��ļ����ɾ�����,������Ҫ������־�Լ�������һ���°�
    1.��version_set�ڵ��ļ��ڵ��ļ���ű����edit;
    2.�½�һ��Version,Ȼ�����Builder->apply��Builder->SaveTo������editӦ�õ��°汾��.
    3.��editд��manifest�ļ���,������Current�ļ�,ָ������manifest.
    4.���°汾��ӵ��汾������,������Ϊ��ǰ����.


    ����LogAndApply��ʱ����4�������е�һ���Ǵ�DB��ʱ������3������Compaction�й�ϵ��
    1.Open DB ��ʱ����Щ��¼����һ�β����У�������һЩ��¼ֻ��log�У���δд��sstable�������Ҫreplay�� �е�����journal�ļ�ϵͳ�ϵ�֮���replay������
    2.Immutable MemTable dump��SStable֮�󣬵���LogAndApply
    3.����Ƿ�manual��ͬʱ������sstable�ļ��򵥵��ڲ�ͬlevel֮���ƶ�������ǣ��������ͬlevel��sstable֮��鲢���򣬾�ֱ�ӵ���LogAndApply
    4.Major Compaction����ͬlevel���ļ����ڽ��棬��Ҫ�鲢���������µĲ������ص���sstable�ļ���ͬʱ���ܽ��ϵ��ļ�������

    ����Compaction���ԣ����������� : ������ͨ��MemTable dump�������µ�sstable�ļ���һ����level0 ������level1������level2������һ����ʱ���£��������е�sstable��ͨ���鲢���򣬽�ĳЩ�ļ�������ߵ�level�������ݼ��ϱ�ø�������
    ����������ļ�����Ҫ�����µ�version�������LogAndApply����������
*/
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  Finalize(v); //���������Ҫ����������ÿһ���ļ��ϲ�����.

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  // descriptor_log_ == NULL ��Ӧ���ǲ������ϵ�MANIFEST�ļ�
  if (descriptor_log_ == NULL) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == NULL);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      //������������һ��edit����comparator������������Ϊfalse����Ϊ���½��� 
      // ��ǰ�İ汾���������գ���Ϊ��MANIFEST�������
      s = WriteSnapshot(descriptor_log_);
    }
  }

  /*
      ����������ϵ�MANIFEST�ļ���������һ���յ�MANIFEST�ļ���ͬʱ����WriteSnapShot����ǰ�汾�����Ϊ����¼��MANIFEST�ļ���
      ��������£�MANIFEST�ļ��Ĵ�С������٣��������ҽ��ܣ���ȫ�����Լ�������ʼ��������ȫ���ش��̹ſ���ٵؽ�����
      ��ϧ���ǣ�ֻҪDB���رգ�MANIFEST�ļ���û�л���������˶���ceph-mon����daemon���̣�MANIFEST�ļ��Ĵ�С��һֱ��������������ceph-mon���л�������
  */
  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      //�ȵ���VersionEdit�� EncodeTo���������л����ַ���
      //����ǰVersionEdit Encode����Ϊ��¼��д��MANIFEST
      edit->EncodeTo(&record);
      //�����������һ��edit��û��comparator������������Ϊtrue 
      //ע�⣬descriptor_log_��log�ļ�
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
        //��CURRENT�ļ��м�¼manifest�ļ�������
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
      /*
            �µİ汾�Ѿ�����(ͨ��current_��VersionEdit)�� VersionEditҲ�Ѿ�д��MANIFEST��
            ��ʱ���Խ�v���ó�current_, ͬʱ�����µ�version v����VersionSet��˫������

            current_�汾�ĸ���ʱ��һ��Ҫע�⵽��LogAndApply�����°汾֮��ͬʱ��VersionEdit��¼��MANIFEST�ļ�֮��
      */
    AppendVersion(v);

    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      env_->DeleteFile(new_manifest_file);
    }
  }

  return s;
}

/*
    ����ǰ��db��edit�洢��һ��v�У����Ǳ���db�ĵ�һ��version��Ȼ��append��versions_��
    1.�ȴ�Current�ļ���ȡĿǰ����ʹ�õ�manifest�ļ���
    2.��manifest�ļ���ȡ���ݣ��������л�ΪVersion_set�ࣻ
    3.����builder.Apply��������editcompaction�㣬�����ļ����ϣ�ɾ���ļ����ϷŽ�builder�У����ӽ�edit�ڸ����ļ���Ÿ�ֵ��Version_set��Ӧ�ı�����
    4.�½�һ���汾v����builder����ϢӦ�õ�����汾�У�Ȼ���ٽ�����汾��ӽ��汾�����У�������Ϊ��ǰ�汾��
    5.������Version_set�ڵ��ļ���š�

    �ط���ϣ�������һ�����հ��Verison v��Finalize֮�󣬵�����AppendVersion���������������˼��
    ��ʵ�ϣ�LogAndApply��VersionEditд��MANIFEST�ļ�֮��Ҳ������AppendVersion
*/
Status VersionSet::Recover(bool *save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  //ȥ��\n  
  current.resize(current.size() - 1);

  //CURRENT�ļ���¼��MANIFEST���ļ����֣�����ΪMANIFEST-number
  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    return s;
  }

  // ���µı�����Ϣ���Ǵ�manifest�л�õ� 
  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);

  {
    //��CURRENTָʾ��manifest�ļ��ж�ȡversionedit����Ϣ
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        // Apply�е�levels_[level].added_files��ʵ����sst�ļ� 
        //���մ��򣬽�Verison�ı仯�����طţ����ջ�õ����հ汾��Version
        builder.Apply(&edit);
      }

      // ���±�����Ϣ
      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = NULL;

  // ��next_file_number_���������  
  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  // versions_����Ӹ�version  
  if (s.ok()) {
    Version* v = new Version(this);
    // ͨ���ط����е�VersionEdit���õ����հ汾��Version������v
    builder.SaveTo(v);
    
    // Install recovered version
    Finalize(v);

    // AppendVersion���汾v����VersionSet���ϣ�ͬʱ����curret_����v
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  }

  return s;
}

/*
    
*/
bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;

  /*
    ����ϵ�MANIFEST�ļ�̫���ˣ��Ͳ������ã�return false
    ���û��ǲ����õĹؼ����������:

    ���dscriptor_log_ ΪNULL��������б䣬�����˰汾��Ծ������VersionEdit��Ҫд���MANIFEST��ʱ��
    �������ж�descriptor_log_�Ƿ�ΪNULL�����ΪNULL����ʾ��Ҫ�������ϵ�MANIFEST�ˣ�Ҫ����¯��
    ��ν����¯�����һ���յ�MANIFEST����Ҫ���°汾��Snapshot��Ȼ��VersionEdit׷��д��
  */
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= kTargetFileSize) {
    return false;
  }

  assert(descriptor_file_ == NULL);
  assert(descriptor_log_ == NULL);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == NULL);
    return false;
  }

  /*
      �ⲿ���߼�Ҫ��LogAndApply���տ��������ϵ�MANIFEST����ô�ͻ�ִ�����µ���䣺
      descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
      manifest_file_number_ = manifest_number;

      ������Ľ���ǣ���version�����仯�������µ�VersionEdit��ʱ�򣬲������´���MANIFEST�ļ������෴����׷��д��VersionEdit��
      �������MANIFEST�ļ��Ѿ�̫���ˣ�����û��Ҫ����ȫ������ʷVersionEdit��������ȫ�����Ե�ǰ�汾Ϊ��׼����һ��SnapShot��
      �����ı仯���Ը�SnapShotΪ��׼����ͣ׷���µ�VersionEdit
  */
  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

/*
    ��level-0ʹ���ļ��������ԣ���Ϊ����file-size��С����������ʹ���ļ���С���ԡ�
     Ѱ�����version��һ��compationʱ�����level��score
    OPTIMIZE��ME:
    С�ļ�����leveldb��˵������һ�����£���Ϊ��Ҫһ��һ����б������ܲ��ҵ�һ��key��
    �������ﲻ�ܵ�������size��nums���жϣ�����Ҫ��С�ļ���һЩ�ر�Ĵ���

    ���������������æѡ����һ��CompactionӦ�ô�which level ��ʼ�����㲿�ֱȽϼ򵥣��������ǿ���level���ļ���Ŀ���������ļ���size ֮���Ƿ񳬹�����
*/
void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels-1; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
        //��Ҫ������Ϊlevel-0��file_size̫С��  
        //����level0 ���ԣ�����ļ���Ŀ������config::kL0_CompactionTrigger�� �ͱ����ҪCompaction
      score = v->files_[level].size() /
          static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score = static_cast<double>(level_bytes) / MaxBytesForLevel(level);
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  assert(config::kNumLevels == 7);
  snprintf(scratch->buffer, sizeof(scratch->buffer),
           "files[ %d %d %d %d %d %d %d ]",
           int(current_->files_[0].size()),
           int(current_->files_[1].size()),
           int(current_->files_[2].size()),
           int(current_->files_[3].size()),
           int(current_->files_[4].size()),
           int(current_->files_[5].size()),
           int(current_->files_[6].size()));
  return scratch->buffer;
}

/*
ikey��ǰ�����е�file�Ĵ�С��level-0Ҫȫ������������level˳�������Ȼ��break��
*/
uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        //ikey�����ļ���smallest��largest֮�䣬����ikeyǰ�����ݵĴ�С
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != NULL) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

/*
������file��seqnumber���뵽live��
*/
void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

/*
��ǰversion��file�Ĵ�С
*/
int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

/*
����level������files�����ϲ�level��������ص��ֽ�����file��level+1�е��ص���
*/
int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
/*
�õ�inputs�е������Сkey
*/
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

/*
Ҫ����Compaction������level��ÿ��level��һ��iterator(level-0����concate����������twolevel)
��������iterator��ϳ�merger iterator
*/
Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(
              options, files[i]->number, files[i]->file_size);
        }
      } else {
        // Create concatenating iterator for the files from this level
          // ����˫�������
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

/*
    ��������ʹ��size_compaction������seek_compaction��
    ��level-0�����ص��Ĳ��ֶ�����input[0]��

    ����Compaction��
    1.������size����seek���ʹ���һ��compaction
    2.������һ�θ��µ�compact_pointer_[level]Ѱ�ҵ�һ�����ڸ�key��file
    3.����start��end��parent���ҵ�������η�Χ��files
    3.1 ��level-0����Դ�����Ϊlevel-0�е��ļ��������ص�����ô���ø�Ⱦ�ķ�ʽ���������file�ص���filesȫ�����뵽input_[0]�С�
    4.����input_[1]���Ż�input_[0](SetupOtherInputs).

    Compaction��ʱ�򣬵���Pick Compaction������ѡ��compaction�Ĳ㼶����ʱ����Ȱ���Finalize�������level����Compaction��
    ��Ȼ�ˣ�������ļ���С���ļ������ĽǶȿ���û���κ�level��ҪCompaction���Ͱ���seek����������
*/
Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  //size_compactionҪ����seek_compaction 
  //ע��ע�ͣ����Ȱ���size��ѡ��compaction�Ĳ㼶��ѡ������ݼ�����Finalize���������score
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels);
    c = new Compaction(level);

    // Pick the first file that comes after compact_pointer_[level]
    // �ҵ���һ��largest_key��compact_pointer_[level]���file 
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      // ���compact_pointer_[level]�����ǿյģ�˵���ǵ�һ��pick���level��compaction  
      // �����ҵ�����һ�ε�compact_poiner_[level]�����file���뵽inputs_[0]�� 
      if (compact_pointer_[level].empty() ||
          //NOTE ME:  
          //ע������Ƚϵ�������key����������С��key��  
          //��Ϊ�����level-0�Ļ���key�������ص�����һ���ļ�����Сkey���ܱ�keyС���������key��keyҪ��
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;//Ҳ����˵��input[0]������ֻ��һ���ļ�
      }
    }
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return NULL;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  //input[0]��ʱֻ����һ���ļ�������leveldb��Ǩ�Ʋ����ǽ����е�.sstǨ�Ƶ��߲㡣  
  //���ǲ���SetOtherInput�ķ���������Ǩ�Ƶķ�Χ���������ܴﵽlevelTree��ƽ�⡣  
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    // �ҵ�level0����������smallest��largest֮���files���뵽inputs_��
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  //��������SetupOtherInputs�������������Level n��1�Ĳ�ս�ļ�����������䶼����������level n�Ĳ�ս�ļ���
  SetupOtherInputs(c);

  return c;
}

/*
    ��level��level+1��filesѡȡ�ϣ�������������
    1.Ϊ��ʹlevel��level+1��ϵ�level+1��ʱ��level+1�������غϣ���Ҫ�õ�level��samllest��largest��level+1�и��ǵ�files��
    2.��level+1��filesȷ���Ժ������ܻ�������Щfiles(levle��level+1)��range����compact��size���������£�
    ���Է���������level��file�ķ�Χ������Ա������Ժ��compaction�У�level+1���γɵ��ļ����뵽��Щfile��compaction������
    3.���level��files��չ�ˣ���ô����key��range�϶�ҲҪ��չ�ģ�Ϊ�˱�֤1���������¼���level+1��files��
    Դ���е������������ʱֱ���˳��ˣ������Ҿ��ÿ�����2�м�һ��whileѭ����

    STAGE-GET&OPTIMISE:�Ż�inputs_[0]�͸�inputs_[1]��ʼ����
    1.�õ�input_[0]��key�ķ�Χsmallest��largest
    2.����������key�õ�parents��������key���ǵ�files
    (����Ϊ�˱�֤parents��filesû���ص���������Ҫ����������keysȫ����������)
    3.����parent�����ҵ�����files�ܿ����ǿ�����(Ҳ����parent��key��range��child����)��
    ����0��1��files���¶���smallest��largest��
    (����Ϊ�˼���һ��compactʱ�Ĺ�������Ϊ���parent��child���ص��Ļ����´�child����compact��ʱ��
    �Ὣ���compact���ɵ�file����������Ϊ�˱����Ժ��������compact�������ɴ�����������child��file�ķ�Χ)
    4.�ҵ�0��1�������Сkey���ҵ�level�и�����������files���뵽expand0
    5.���expand0�е��ļ�������input_[0]�е��ļ�����˵����չ�ˣ����¶���key�����䣬�õ�parent�е�files����expand1
    6.�ж�expand1�Ƿ�����չ��input_[1]�������������������չ��(�����������ɼ�ע��)

    STAGE-UPDATE:���¡�
    1.�������¶����key��Χ��Ѱ��grandparent�и��ǵ�files�������db_impl.cc�е�ShoulStopBefore�������õ�
    2.�������level���´�Compactʱ��ʼ��key:compact_pointer_[level]�����PickCompactionѰ�ҵ�һ��fileʱ�õ�

    OPTIMIZE ME:
    ���ܳ���input[0]ֻ��һ��file���������file��Ԫ�غ��٣���������Խ����һ��ĺܶ�files
    ����input[1]�л��кܶ��files��������compact��ʱ�򣬻����Ͼ��ǰ�input[1]��files������һ��
    ���ƶ��ѡ���û�п����Ż�һ�£�
    �Ҿ��ÿ��Խ�����ļ�ͬ����child����compact��Ҳ����˵���������������ļ�ʱ��������ʱ���ù���������
    ��child����compact��ʱ����Ȼ����ļ��ͷ����ˡ��������ļ�����level-0����ôһ��ʼ��ȥ������
    ���ϲ������֮�󣬿��Խ���merge��Ҳ����˵�������ڼ���expanded1��
    �ж�һ��expanded1�����input[1]�����˶��٣�������ӵö࣬��ʾ������Ҫ��һЩ���ù��������ļ�
    Ҳ���ǳ����ˡ�thin file���������������ļ�Ӧ�ü�¼���������´��������ʱ����
    ���ǣ���Compact::ShouldStopBefore�����У���һ���ļ���key range���ϲ㸲��̫��ʱ�����Զ�ֹͣ����
    ��������һ����п��ܳ���С�ļ���������Ҫ��¼���С�ļ������������д���
    ���ݵĴ洢��������һЩ���������ߵ���״��Ҳ����˵һЩ��Χ��key���ֵú��٣��������ǵ�overlapsȴ�ܴ�
    ����Щ�ļ���Ϊthin file����ômerge�Ͳ���Ҫ�Ӹ߶������֣�����ҲӦ���п���ϵ��Ż���
*/
void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;
  //��ȡlevel n���в�ս�ļ�����Сkey�����key
  GetRange(c->inputs_[0], &smallest, &largest);

  // �ҵ�����parents����smallest��largest��Χ��overlapps
  //������Сkey����� key������level n��1���ļ����ڸ÷�Χ���ص����ļ�������c->inputs_[1]��
  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

  // Get entire range covered by compaction
  // �ҵ�level��level+1��smallest��largest������all_�У������Χ��Ȼ��С��level��Ӧ�ķ�Χ  
  InternalKey all_start, all_limit;
  /*
    ������GetOverlappingInputs��level+1��files�Ѿ�ѡȡ�˳���������GetOverlappingInputs
    �Ĳ��������ʱ��input[1]��files����Сkey�����key�������Ѿ�������input[0]key�ķ�Χ
    ����ȷ�������Сkey��ֵ��
    ��ʵ�Ҿ���key����չֻ�ᷢ����parent����parent��level�϶�����0���������ص�������key����չ
    ֻ�ᷢ����input_[1]��[0]��[size-1]�ļ���
  */
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  /*
        ������ͼ�е� level n�еĲ�ս�ļ�A ������� level n��1 �е�B C D��Ҫ��ս������û���κ�����ģ����� ����B C D�ļ��룬key�ķ�Χ�����ˣ���һ�������� level n���E�費��Ҫ��ս��
  */
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    //�Ҿ������������һ��whileѭ�������ڶ�������������ʱ���break������һֱ����level�е�file��ֱ���ﵽlimit 
    if (expanded0.size() > c->inputs_[0].size() && //level����inputs[0]��û�а����� 
        inputs1_size + expanded0_size < kExpandedCompactionByteSizeLimit) {
        /*
            �������������̫�࣬�ڴ治����ʱ��̫��
            ���ʱ��̫���Ļ�����ε�compactionû���꣬�������level�ֿ��Խ���compaction�ˣ�
            ��ʱlevel����һ����Ҫcompact��file�ܿ�����˴�compact���ɵ�file(��ǰ��������)���ص������ܽ���compact
            ��������ط��п����Ż�һ�£����ǿ���������file������û���ص�������compact������level��
            ���߸����version��������һ��versionset��������compact���֮���ٽ���merge��
            ���������������岻��
            Ȼ���ҵ�level+1����expand0��Χ�ڵ�files
        */
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1);
      /*
        ֻ��input_[1]û����չ���Ż���չinput_[0]��
        ���inout_[1]��չ�ˣ���ô����������߼���input_[0]��Ҫ��չ��input_[0]����չ����Ҫ��չinput_[1]...
        ���ѭ��û��û�ˡ����Ըɴ�ֻ����input[1]�����ʱ�����չinput_[0]��
      */
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level,
            int(c->inputs_[0].size()),
            int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size),
            int(expanded0.size()),
            int(expanded1.size()),
            long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        //largestֻ����input_[1]��sizeû�иı��ʱ����ܸ��£������´�compactʱ��©���file 
        largest = new_limit;

        //��չinput_[0]��input_[1] 
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  /*
      STAGE-UPDATE:��ʼ����
      ΪCompact::ShouldStopBefore�ṩgrandparents_
      Compute the set of grandparent files that overlap this compaction
      (parent == level+1; grandparent == level+2)
      NOTE ME:
      ʹ��all_����Ϊ���Ǵ�parent�ĽǶȿ�grandparent
  */
  if (level + 2 < config::kNumLevels) { // ����grandparent��files
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  if (false) {
    Log(options_->info_log, "Compacting %d '%s' .. '%s'",
        level,
        smallest.DebugString().c_str(),
        largest.DebugString().c_str());
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  // ʹ��largest����Ϊ���Ǵ�level�ĽǶȿ��ģ�largest�Ǹ���level��range�õ��ġ�
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

/*
������PickCompaction������ֻ����manual_compact��ʱ�����
*/
Compaction* VersionSet::CompactRange(
    int level,
    const InternalKey* begin,
    const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return NULL;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(level)),
      input_version_(NULL),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != NULL) {
    input_version_->Unref();
  }
}

/*
���levelֻ��һ����level+1��0��������level+2��bytesС�����ƣ���ô��level�е�file���뵽level+1��
*/
bool Compaction::IsTrivialMove() const {
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 &&
          num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <= kMaxGrandParentOverlapBytes);
}

/*
Ҫɾ����sst�ļ�
������Compact���ļ�ɾ����input[0]��input[1]�е��ļ���Ҫɾ��
*/
void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->DeleteFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

/*
ֻ�Ǽ��ж�һ�����key�Ƿ������ڸ߲�overlap�ˡ�
��de_impl.cc�е�DoCompactionWork���ã�����ֹ���µ�(�Ͳ�)delete��Ϣ��ʧ�������
*/
bool Compaction::IsBaseLevelForKey(const Slice& user_key) { //���level�����key���ڵ����level��
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  /*
    ���user_key��lvl��file�У��ͷ���false
    ��������compact lvl��lvl+1�����Դ�grandparent��ʼ
    NOTE ME:
    level_ptrs_[lvl]һ��ʼ��0(�ڹ��캯���б���ֵ)���������ڲ�forѭ��������̭��user_key��Խ��file
    �ڲ�forѭ������ƣ���FindFile�Ķ��ֲ��һ�Ҫ��Ч��
  */
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    for (; level_ptrs_[lvl] < files.size(); ) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      // ����Ҫ�Ƚ�����key 
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      /*
        ������key����f�ķ�Χ֮�⣬��ô�������key(������Ҫ��)�����������������key֮�ڣ�ţ�ơ�
        ����������ǹ����ģ�������compact���ã�ÿ��key��Ҫ���еġ�
      */
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

/*
ͬVersion::PickLevelForMemTableOutputһ�����ⶼ��Ϊ�˱�����ϲ���̫����ص���
�ж�grandparent�и��Ǹ�key���ļ���size���������̫�࣬��ֹͣ��ε�compact
OPTIMIZE ME:
���ȣ�grandparent�����е�files��size��������Ҳ����ﵽkMaxGrandParentOverlapBytes
��Σ�����ܹ�������Ҳ����һ���ԷֺöΣ���compact��key���֣�����������������˷�ʱ�����
�������Ҳ�֪���ҵ��ƶ϶Բ��ԡ�
�п��ܵ���С�ļ��ĳ��֣��ο�VersionSet::SetupOtherInputs��ע��
*/
bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &input_version_->vset_->icmp_;
  /*
    ����grandparent���ļ��Ǵ�С�������еġ�����DoCompactionWork�ڵ����������ʱ
    Ҳ���Ѿ�������Iterator����ô���Լ�¼��һ��key���ǵĴ�С��ֻ�е�grandparent_index_
    ���Ӻ��+=��
    grandparents_��ȷ���´�compaction��start_keyʱ���¡����Ǹ����ϴ�PickCompactionLevel
    ʱ�õ��������Сkey�õ���level+2��overlap��files
  */
  while (grandparent_index_ < grandparents_.size() &&
      icmp->Compare(internal_key,
                    grandparents_[grandparent_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  /*
      ��Ϊÿ��.sst�ļ��Ĵ�С�ǹ̶��ġ����������һ����bytes��Ҳ����˵���Compaction�����γɵ�
      ���.sst�ļ�(��level-n+1��)��level��grandparent(level-n+2)��̫����ص�����һ��Compact���
      .sst�ļ�ʱ�ͻ��̫���.sst�ļ����в������Ӷ��Ὣ̫������ݷ����ڴ棬
      (��ҲӦ����û�и���.sst�ļ����������Ǹ����ص���size�������ԭ��)
      ���ҪֹͣCompaction��������Ϊ�˼����ڴ����ĸ�����
  */
  if (overlapped_bytes_ > kMaxGrandParentOverlapBytes) {
    // Too much overlap for current output; start new output
    // �ܸо�����Ӧ������һ��seen_key_ = false; 
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != NULL) {
    input_version_->Unref();
    input_version_ = NULL;
  }
}

}  // namespace leveldb
