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
    MANIFEST文件损坏后如何恢复leveldb

    为什么MANIFEST损坏或者丢失之后，依然可以恢复出来？LevelDB如何做到。
    对于LevelDB而言，修复过程如下：
        1.首先处理log，这些还未来得及写入的记录，写入新的.sst文件
        2.扫描所有的sst文件，生成元数据信息：包括number filesize， 最小key，最大key
        3.根据这些元数据信息，将生成新的MANIFEST文件。
    第三步如何生成新的MANIFEST？ 因为sstable文件是分level的，但是很不幸，我们无法从名字上判断出来文件属于哪个level。
        第三步处理的原则是，既然我分不出来，我就认为所有的sstale文件都属于level 0，因为level 0是允许重叠的，因此并没有违法基本的准则。

    当修复之后，第一次Open LevelDB的时候，很明显level 0 的文件可能远远超过4个文件，因此会Compaction。 
        又因为所有的文件都在Level 0 这次Compaction无疑是非常沉重的。它会扫描所有的文件，归并排序，产生出level 1文件，进而产生出其他level的文件。

    从上面的处理流程看，如果只有MANIFEST文件丢失，其他文件没有损坏，LevelDB是不会丢失数据的，
        原因是，LevelDB既然已经无法将所有的数据分到不同的Level，但是数据毕竟没有丢，根据文件的number，完全可以判断出文件的新旧，
        从而确定不同sstable文件中的重复数据，which是最新的。经过一次比较耗时的归并排序，就可以生成最新的levelDB。
    
    上述的方法，从功能的角度看，是正确的，但是效率上不敢恭维。Riak曾经测试过78000个sstable 文件，490G的数据，大家都位于Level 0，
        归并排序需要花费6 weeks，6周啊，这个耗时让人发疯的。
    
    Riak 1.3 版本做了优化，改变了目录结构，对于google 最初版本的LevelDB，所有的文件都在一个目录下，但是Riak 1.3版本引入了子目录，
        将不同level的sst 文件放入不同的子目录：
        sst_0
        sst_1
        ...
        sst_6
    有了这个，重新生成MANIFEST自然就很简单了，同样的78000 sstable文件，Repair过程耗时是分钟级别的
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
  //从VersionSet中注销
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  //本Version下所有的文件，引用计数减1
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
在files中查找key，由于是二分查找，说明files中的key是有序的，查找到的是vector的index
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
user_key是否比f的largest的user_key大
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
重叠：
最小key比file的最小key还大，并且，最大key比file的最大key还小
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
        flist_(flist), //某一层文件集合
        index_(flist->size()) {        // Marks as invalid 某一层文件的编号，等于文件个数时，即为无效 
  }
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  virtual void Seek(const Slice& target) {
      //在这层查找键值大于等于target的文件索引
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
      //返回大于等于target文件的最大键值
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const {
    assert(Valid()); //返回这个文件的文件编号和大小封装成的字符串
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

// 传入的函数指针，就是为了将文件元数据添加进Table_cache
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
连锁，类似table的双层迭代器
外层迭代器为level的迭代器
内层迭代器为file*
*/
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]),
      &GetFileIterator, vset_->table_cache_, options);
}

/*
level0因为有重叠，所以files的key不是有序的，但是每个file的key是有序的，所以需要添加多个iter
0以上的files里的key都是有序的，可以直接使用FindFile来查找，只需要一个iter
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
    得到这个version中的key-value值
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
  //对于level-0是顺序查找每个文件。对其它level是二分查找
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

      //将file按照它们的number从大到小排列，因为序号越大的file越新 
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

    //从符合条件的文件中查找 
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
    找到可以存放这个memtable的level
    这里的原则是，尽量找到没有重叠的最高level——这是第2个if的意思
    第3个if的意思是说，如果parent里没有重叠，level应该选取parent，
    但是如果两层之间的重叠部分太多的话，下一次compact的概率就会增加
    但是如果在这两层之间加一个“缓冲层”，则会减少compact的工作(毕竟层数越高，文件越大)
    这也是lazy思想的体现。

    新产生出来的sstable 并不一定总是处于level 0， 尽管大多数情况下，处于level 0。新创建的出来的sstable文件应该位于那一层呢？ 由PickLevelForMemTableOutput 函数来计算：
    从策略上要尽量将新compact的文件推至高level，毕竟在level 0 需要控制文件过多，compaction IO和查找都比较耗费，另一方面也不能推至过高level，
    一定程度上控制查找的次数，而且若某些范围的key更新比较频繁，后续往高层compaction IO消耗也很大。 所以PickLevelForMemTableOutput就是个权衡折中。
    如果新生成的sstable和Level 0的sstable有交叠，那么新产生的sstable就直接加入level 0，否则根据一定的策略，向上推到Level1 甚至是Level 2，
    但是最高推到Level2，这里有一个控制参数：kMaxMemCompactLevel。
*/
int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
    const Slice& largest_user_key) {
  int level = 0;
  // 如果1.level0里有files的最大和最小key包含它(不一定是包含所有的key)  
  // 例如s = 0， l = 10， 那么file的key可能为，-1， 2， ，4， 9， 11，  
  // 也有可能为：0， 1， 4， 9， 10，相同的key会在DoCompactWork的时候drop掉  
  // 如果和Level 0中的SSTable文件由重叠，直接返回 level ＝ 0 
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    // level只能是0-kMaxMemCompactLevel(2)之间的数 while 循环寻找合适的level层级，最大level为2，不能更大
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        // 或者2.它的parent里有重叠  
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        // 或者3.它的grandparent里重叠数大于一定的值
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
找到[begin,end]这一阶段的file
例如[40,100]。那么file1[0,30],file2[31,50],file3[60,80],file4[90,110]中
2，3，4都会加入
并且，对于level0来说，由于其有覆盖，这样如果一个file只有一部分被包含在[begin,end]中
就要扩大[begin,end]的范围，这样就会将所有重叠的files都加入到inputs中。
这相当于感染，将所有level0中与这段区间有重叠的files都加入到inputs中。
其实可以看到对level0的特殊处理只是发生在compact阶段。
这样做感染处理以后，就会尽可能多地把有重叠的文件收集起来，而不是一次只对一小部分进行compact
因为level-0的文件较小，compact的时候会相对快一些。使得对level-0的compact更为彻底。
这里并没有使用lazy的思想，反而是像打了一针兴奋剂一样，将level-0的处理提前了。
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
        //也就是说，找到Level0中所有的file的最小的smallest和最大的largest，取其中的file放入inputs  
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
    主要有两个函数Apply和SaveTo。
    Apply: 将VersionEdit的修改应用到VersionSet中
    SaveTo: 将这些修改放入到一个Version中。

    这个类用于将manifest文件内容添加进当前版本，并将当前版本添加进版本链表

    从Version 升级到另一个Version中间靠的是VersionEdit。VersionEdit告诉我们哪些文件可以删除了，哪些文件是新增的。这个过程是LogAndApply。为了方便实现
    Version(N) + VersionEdit(N) = Version(N+1)
    引入了Build数据结构,这个数据结构是一个helper类，帮忙实现Version的跃升。
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

  //定义一个用于排序文件元数据的函数对象，先是按最小键值排序，如果最小键值相等，就按文件编号从小到大排序
  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  
  //定义文件集合类型，集合从小到大排序
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  //所属的版本链表
  VersionSet* vset_;
  
  //当前版本
  Version* base_;

  //每一层文件状态，添加或删除文件
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
      将VersionEdit的修改应用到VersionSet中
      将edit中的信息应用到builder中，new_files应该是以前创建的sst文件
      当打开一个已存在的数据库时，此时需要将磁盘的文件信息恢复到一个版本，也就是将manifest内的信息包装成Version_edit，
      并应用到当前版本，这时就需要调用Builder->Apply方法，这方法先是将edit里的信息解析到Builder中，接着再调用Builder->Saveto保存到当前版本中

      这一步是将版本与版本的变化部分VersionEdit 记录在Builder
  */
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
      // 将每层compaction节点添加进version_set
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    /*
        将删除文件添加进Builder
        将删除部分的文件，包括文件所属的层级，记录在Builder的数据结构中
        删除的文件，只需要记录sstable文件的数字就可以了
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
    // 将增加的文件添加进Builder
    // 处理新增文件部分，要将VersionEdit中的增加文件部分的整个FileMetaData都记录下来
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
      // 一个文件的seek次数是为了避免这个文件长期滞留在低层带来查找效率上的损失。 
      f->allowed_seeks = (f->file_size / 16384);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  /*
        将这些修改放入到一个Version中。
        这一部分是根据Builder中的base_指向的当前版本current_, 以及在Apply部分记录的删除文件集合，
        新增文件集合和CompactionPointer集合，计算出一个新的Verison，保存在v中
  */
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      /*
          将新添加的文件和上个版本的文件合并
          删除掉deleted集合中的文件，把结果保存在新版本v中
      */
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added->size());
      /*
        将原来存在的files(base_files)和新增的files(added)按照大小的顺序加入到files_中
        类似插入排序
        Merge,这个效率应该比较高一些(应该也不见得)
        OPTIMISZE ME:这个地方能不能写成普通的merge?
      */
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) {
        // Add all smaller files listed in base_
          // 这个循环是将比新添加的文件“小“的文件先添加进新版本中
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos;
             ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        //将新添加文件添加进新版本中
        MaybeAddFile(v, level, *added_iter);
      }

      // Add remaining base files
      // 添加其他文件，其实也就是比新添加文件”大“的文件
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
  删除deleted集合中文件的操作就在MaybeAddFile，这函数名字起的很好，可能添加，因为如果在deleted集合中，则不添加
  */
  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
      //因为apply的时候是先delete再add的，在add的时候把重复的都删去了，所以这个if不会运行到  
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      /*
          除level0外的任何level （1～6），Version v内的对应层级的文件列表必须是有序的，不能交叉
          所以此处有判断 level >0,这是因为并不care level0 是否交叉，事实上，它几乎总是交叉的
      */
      if (level > 0 && !files->empty()) {
        // Must not overlap
          // 如果是大于0层的文件，新添加的文件不能和集合中已存在的文件有交集
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++; //文件引用加1
      files->push_back(f); //添加文件
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
在LogAndApply和Recover中调用，
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
    把edit的内容作为一个record加入到manifest文件中，
    并将当前version和edit结合起来新建一个version，然后加入到versions_中
    这个函数主要是将edit信息写进manifest文件中，并应用到新版本中。经常在文件合并之后,会出现文件文件添加删除情况,所以需要保存日志以及新生成一个新版
    1.将version_set内的文件内的文件编号保存进edit;
    2.新建一个Version,然后调用Builder->apply和Builder->SaveTo方法将edit应用到新版本中.
    3.将edit写进manifest文件中,并更新Current文件,指向最新manifest.
    4.将新版本添加到版本链表中,并设置为当前链表.


    调用LogAndApply的时机有4个，其中第一个是打开DB的时候，其余3个都与Compaction有关系。
    1.Open DB 的时候，有些记录在上一次操作中，可能有一些记录只在log中，并未写入sstable，因此需要replay， 有点类似journal文件系统断电之后的replay操作。
    2.Immutable MemTable dump成SStable之后，调用LogAndApply
    3.如果是非manual，同时仅仅是sstable文件简单地在不同level之间移动，并不牵扯两个不同level的sstable之间归并排序，就直接调用LogAndApply
    4.Major Compaction，不同level的文件存在交叉，需要归并排序，生成新的不交叉重叠的sstable文件，同时可能将老的文件废弃。

    对于Compaction而言，它的作用是 : 或者是通过MemTable dump，生成新的sstable文件（一般是level0 或者是level1或者是level2），在一定的时机下，整理已有的sstable，通过归并排序，将某些文件推向更高的level，让数据集合变得更加有序。
    生成完毕新文件后，需要产生新的version，这就是LogAndApply的做的事情
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
  Finalize(v); //这个函数主要是用来更新每一层文件合并分数.

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  // descriptor_log_ == NULL 对应的是不延用老的MANIFEST文件
  if (descriptor_log_ == NULL) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == NULL);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      //这个函数添加了一个edit，有comparator，但是其它的为false，因为是新建。 
      // 当前的版本情况打个快照，作为新MANIFEST的新起点
      s = WriteSnapshot(descriptor_log_);
    }
  }

  /*
      如果不延用老的MANIFEST文件，会生成一个空的MANIFEST文件，同时调用WriteSnapShot将当前版本情况作为起点记录到MANIFEST文件。
      这种情况下，MANIFEST文件的大小会大大减少，就像自我介绍，完全可以自己出生起开始介绍起，完全不必从盘古开天辟地介绍起。
      可惜的是，只要DB不关闭，MANIFEST文件就没有机会整理。因此对于ceph-mon这种daemon进程，MANIFEST文件的大小会一直增长，除非重启ceph-mon才有机会整理。
  */
  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      //先调用VersionEdit的 EncodeTo方法，序列化成字符串
      //将当前VersionEdit Encode，作为记录，写入MANIFEST
      edit->EncodeTo(&record);
      //现在又添加了一个edit，没有comparator，但是其它的为true 
      //注意，descriptor_log_是log文件
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
        //在CURRENT文件中记录manifest文件的名字
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
      /*
            新的版本已经生成(通过current_和VersionEdit)， VersionEdit也已经写入MANIFEST，
            此时可以将v设置成current_, 同时将最新的version v链入VersionSet的双向链表

            current_版本的更替时机一定要注意到，LogAndApply生成新版本之后，同时将VersionEdit记录到MANIFEST文件之后
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
    把以前的db的edit存储到一个v中，这是本次db的第一个version，然后append到versions_中
    1.先从Current文件读取目前正在使用的manifest文件；
    2.从manifest文件读取数据，并反序列化为Version_set类；
    3.调用builder.Apply方法，将editcompaction点，增加文件集合，删除文件集合放进builder中，并从将edit内各个文件编号赋值给Version_set相应的变量；
    4.新建一个版本v，将builder中信息应用到这个版本中，然后再将这个版本添加进版本链表中，并设置为当前版本。
    5.最后更新Version_set内的文件编号。

    回放完毕，生成了一个最终版的Verison v，Finalize之后，调用了AppendVersion，这个函数很有意思，
    事实上，LogAndApply讲VersionEdit写入MANIFEST文件之后，也调用了AppendVersion
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
  //去掉\n  
  current.resize(current.size() - 1);

  //CURRENT文件记录着MANIFEST的文件名字，名字为MANIFEST-number
  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    return s;
  }

  // 以下的变量信息都是从manifest中获得的 
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
    //从CURRENT指示的manifest文件中读取versionedit的信息
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
        // Apply中的levels_[level].added_files其实就是sst文件 
        //按照次序，将Verison的变化量层层回放，最终会得到最终版本的Version
        builder.Apply(&edit);
      }

      // 更新变量信息
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

  // 将next_file_number_更新至最高  
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

  // versions_中添加该version  
  if (s.ok()) {
    Version* v = new Version(this);
    // 通过回放所有的VersionEdit，得到最终版本的Version，存入v
    builder.SaveTo(v);
    
    // Install recovered version
    Finalize(v);

    // AppendVersion将版本v放入VersionSet集合，同时设置curret_等于v
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
    如果老的MANIFEST文件太大了，就不在延用，return false
    延用还是不延用的关键在如下语句:

    如果dscriptor_log_ 为NULL，当情况有变，发生了版本的跃升，有VersionEdit需要写入的MANIFEST的时候，
    会首先判断descriptor_log_是否为NULL，如果为NULL，表示不要在延用老的MANIFEST了，要另起炉灶
    所谓另起炉灶，即起一个空的MANIFEST，先要记下版本的Snapshot，然后将VersionEdit追加写入
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
      这部分逻辑要和LogAndApply对照看：延用老的MANIFEST，那么就会执行如下的语句：
      descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
      manifest_file_number_ = manifest_number;

      这个语句的结果是，当version发生变化，出现新的VersionEdit的时候，并不会新创建MANIFEST文件，正相反，会追加写入VersionEdit。
      但是如果MANIFEST文件已经太大了，我们没必要保留全部的历史VersionEdit，我们完全可以以当前版本为基准，打一个SnapShot，
      后续的变化，以该SnapShot为基准，不停追加新的VersionEdit
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
    对level-0使用文件个数策略，因为它的file-size较小。对其它层使用文件大小策略。
     寻找这个version下一次compation时的最佳level和score
    OPTIMIZE　ME:
    小文件对于leveldb来说并不是一件好事，因为需要一层一层进行遍历才能查找到一个key。
    所以这里不能单纯依靠size和nums来判断，还需要对小文件做一些特别的处理

    这个部分是用来帮忙选择下一次Compaction应该从which level 开始。计算部分比较简单，基本就是看该level的文件数目或者所有文件的size 之和是否超过上限
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
        //主要还是因为level-0的file_size太小了  
        //对于level0 而言，如果文件数目超过了config::kL0_CompactionTrigger， 就标记需要Compaction
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
ikey以前的所有的file的大小。level-0要全部遍历。其它level顺序遍历，然后break。
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
        //ikey处于文件的smallest和largest之间，计算ikey前面数据的大小
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
把所有file的seqnumber加入到live中
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
当前version的file的大小
*/
int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

/*
计算level里所有files中与上层level具有最高重叠字节数的file在level+1中的重叠数
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
得到inputs中的最大最小key
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
要进行Compaction的两个level，每个level建一个iterator(level-0建立concate，其它的是twolevel)
后将这两个iterator组合成merger iterator
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
          // 两个双层迭代器
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
    更倾向于使用size_compaction而不是seek_compaction。
    将level-0中有重叠的部分都放入input[0]中

    生成Compaction。
    1.根据是size还是seek类型创建一个compaction
    2.根据上一次更新的compact_pointer_[level]寻找第一个大于该key的file
    3.根据start和end在parent中找到覆盖这段范围的files
    3.1 对level-0特殊对待。因为level-0中的文件可能有重叠，那么采用感染的方式将所有与该file重叠的files全部加入到input_[0]中。
    4.设置input_[1]并优化input_[0](SetupOtherInputs).

    Compaction的时候，调用Pick Compaction函数来选择compaction的层级，那时候会先按照Finalize算出来的level进行Compaction。
    当然了，如果从文件大小和文件个数的角度看，没有任何level需要Compaction，就按照seek次数来决定
*/
Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  //size_compaction要优于seek_compaction 
  //注意注释，优先按照size来选择compaction的层级，选择的依据即按照Finalize函数计算的score
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels);
    c = new Compaction(level);

    // Pick the first file that comes after compact_pointer_[level]
    // 找到第一个largest_key比compact_pointer_[level]大的file 
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      // 如果compact_pointer_[level]本来是空的，说明是第一次pick这个level的compaction  
      // 或者找到比上一次的compact_poiner_[level]还大的file加入到inputs_[0]中 
      if (compact_pointer_[level].empty() ||
          //NOTE ME:  
          //注意这里比较的是最大的key，而不是最小的key。  
          //因为如果是level-0的话，key可能有重叠。第一个文件的最小key可能比key小，但是最大key比key要大。
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;//也就是说，input[0]里现在只有一个文件
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
  //input[0]这时只含有一个文件。所以leveldb的迁移并不是将所有的.sst迁移到高层。  
  //而是采用SetOtherInput的方法逐步扩大迁移的范围，这样就能达到levelTree的平衡。  
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    // 找到level0中其它的在smallest和largest之间的files加入到inputs_中
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  //出了最后的SetupOtherInputs函数，负责计算Level n＋1的参战文件，其他的语句都是用来计算level n的参战文件。
  SetupOtherInputs(c);

  return c;
}

/*
    在level和level+1的files选取上，有两个考量：
    1.为了使level跟level+1结合到level+1的时候level+1不能有重合，需要得到level的samllest和largest在level+1中覆盖的files。
    2.当level+1的files确定以后，它可能会扩大这些files(levle和level+1)的range，在compact的size允许的情况下，
    可以反过来扩大level的file的范围。这可以避免在以后的compaction中，level+1新形成的文件加入到这些file的compaction中来。
    3.如果level的files扩展了，那么它的key的range肯定也要扩展的，为了保证1，必须重新计算level+1的files，
    源码中当碰到这种情况时直接退出了，但是我觉得可以在2中加一个while循环。

    STAGE-GET&OPTIMISE:优化inputs_[0]和给inputs_[1]初始化。
    1.得到input_[0]中key的范围smallest和largest
    2.根据这两个key得到parents中这两个key覆盖的files
    (这是为了保证parents中files没有重叠。所以需要把这个区间的keys全部包含进来)
    3.由于parent的左右的两个files很可能是开区间(也就是parent的key的range比child还大)，
    根据0和1的files重新定义smallest和largest。
    (这是为了简化下一次compact时的工作。因为如果parent与child有重叠的话，下次child进行compact的时候
    会将这次compact生成的file包含进来，为了避免以后做更多的compact工作，干脆又重新扩大child的file的范围)
    4.找到0和1的最大最小key。找到level中覆盖这个区间的files加入到expand0
    5.如果expand0中的文件数大于input_[0]中的文件数，说明扩展了，重新定义key的区间，得到parent中的files加入expand1
    6.判断expand1是否又扩展了input_[1]。如果不是这样，才扩展。(这样做的理由见注释)

    STAGE-UPDATE:更新。
    1.根据重新定义的key范围，寻找grandparent中覆盖的files，这会在db_impl.cc中的ShoulStopBefore函数中用到
    2.更新这个level中下次Compact时开始的key:compact_pointer_[level]这会在PickCompaction寻找第一个file时用到

    OPTIMIZE ME:
    可能出现input[0]只有一个file，而且这个file的元素很少，但是它跨越了上一层的很多files
    这样input[1]中会有很多的files，而进行compact的时候，基本上就是把input[1]的files进行了一下
    复制而已。有没有可能优化一下？
    我觉得可以将这个文件同它的child进行compact，也就是说，当出现这样的文件时，可以暂时不用管它，等它
    的child进行compact的时候自然这个文件就丰满了。如果这个文件处在level-0，那么一开始不去管它，
    等上层丰满了之后，可以进行merge。也就是说，可以在计算expanded1后，
    判断一下expanded1相对于input[1]增加了多少，如果增加得多，表示我们需要做一些无用功来复制文件
    也就是出现了“thin file”。对于这样的文件应该记录并加以向下处理或者延时处理。
    但是，在Compact::ShouldStopBefore函数中，当一个文件的key range与上层覆盖太多时，会自动停止处理
    这样，这一层就有可能出现小文件，我们需要记录这个小文件，并对它进行处理。
    数据的存储难免会出现一些类似抛物线的形状，也就是说一些范围的key出现得很少，但是他们的overlaps却很大
    把这些文件成为thin file。那么merge就不仅要从高度上着手，而且也应该有宽度上的优化。
*/
void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;
  //获取level n所有参战文件的最小key和最大key
  GetRange(c->inputs_[0], &smallest, &largest);

  // 找到它的parents中在smallest和largest范围的overlapps
  //根据最小key和最大 key，计算level n＋1的文件中于该范围有重叠的文件，放入c->inputs_[1]中
  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

  // Get entire range covered by compaction
  // 找到level和level+1的smallest和largest，存入all_中，这个范围必然不小于level对应的范围  
  InternalKey all_start, all_limit;
  /*
    在上面GetOverlappingInputs后，level+1的files已经选取了出来。根据GetOverlappingInputs
    的操作，这个时候input[1]的files中最小key和最大key都可能已经扩大了input[0]key的范围
    重新确定最大最小key的值。
    其实我觉得key的扩展只会发生在parent，而parent的level肯定大于0，不会有重叠，所以key的扩展
    只会发生在input_[1]的[0]和[size-1]文件中
  */
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  /*
        根据上图中的 level n中的参战文件A 计算出了 level n＋1 中的B C D需要参战，这是没有任何问题的，但是 由于B C D的加入，key的范围扩大了，又一个问题是 level n层的E需不需要参战？
  */
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    //我觉得这里可以是一个while循环，当第二个条件成立的时候就break，否则一直加入level中的file，直到达到limit 
    if (expanded0.size() > c->inputs_[0].size() && //level里有inputs[0]中没有包含的 
        inputs1_size + expanded0_size < kExpandedCompactionByteSizeLimit) {
        /*
            避免操作的数据太多，内存不够，时间太长
            如果时间太长的话，这次的compaction没有完，但是这层level又可以进行compaction了，
            此时level层下一次需要compact的file很可能与此次compact生成的file(当前正在生成)有重叠而不能进行compact
            但是这个地方有可能优化一下，就是看看这两个file究竟有没有重叠，或者compact其它的level。
            或者给这个version赋予另外一个versionset，等两个compact完成之后再进行merge。
            但是这样做的意义不大。
            然后找到level+1中在expand0范围内的files
        */
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1);
      /*
        只有input_[1]没有扩展，才会扩展input_[0]。
        如果inout_[1]扩展了，那么按照上面的逻辑，input_[0]又要扩展。input_[0]更扩展了又要扩展input_[1]...
        如此循环没完没了。所以干脆只有在input[1]不变的时候才扩展input_[0]。
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
        //largest只有在input_[1]的size没有改变的时候才能更新，避免下次compact时遗漏这个file 
        largest = new_limit;

        //扩展input_[0]和input_[1] 
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  /*
      STAGE-UPDATE:开始更新
      为Compact::ShouldStopBefore提供grandparents_
      Compute the set of grandparent files that overlap this compaction
      (parent == level+1; grandparent == level+2)
      NOTE ME:
      使用all_，因为这是从parent的角度看grandparent
  */
  if (level + 2 < config::kNumLevels) { // 设置grandparent的files
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
  // 使用largest，因为这是从level的角度看的，largest是根据level的range得到的。
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

/*
类似于PickCompaction，但是只是在manual_compact的时候调用
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
如果level只有一个，level+1有0个，并且level+2的bytes小于限制，那么将level中的file加入到level+1中
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
要删除的sst文件
将所有Compact的文件删除。input[0]和input[1]中的文件都要删除
*/
void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->DeleteFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

/*
只是简单判断一下这个key是否有能在高层overlap了。
由de_impl.cc中的DoCompactionWork调用，来防止最新的(低层)delete信息丢失的情况。
*/
bool Compaction::IsBaseLevelForKey(const Slice& user_key) { //这个level是这个key存在的最高level？
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  /*
    如果user_key在lvl的file中，就返回false
    现在正在compact lvl和lvl+1，所以从grandparent开始
    NOTE ME:
    level_ptrs_[lvl]一开始是0(在构造函数中被赋值)，后来在内层for循环中逐步淘汰被user_key超越的file
    内层for循环的设计，比FindFile的二分查找还要高效。
  */
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    for (; level_ptrs_[lvl] < files.size(); ) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      // 是先要比较最大的key 
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      /*
        如果这个key落在f的范围之外，那么它后面的key(比它还要大)，更不可能落在这个key之内，牛逼。
        这个函数不是孤立的，它是由compact调用，每个key都要运行的。
      */
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

/*
同Version::PickLevelForMemTableOutput一样，这都是为了避免跟上层有太多的重叠。
判断grandparent中覆盖该key的文件的size。如果覆盖太多，就停止这次的compact
OPTIMIZE ME:
首先，grandparent中所有的files的size加起来，也许不会达到kMaxGrandParentOverlapBytes
其次，如果能够超过，也可以一次性分好段，将compact的key划分，这个函数可能是最浪费时间的了
不过，我不知道我的推断对不对。
有可能导致小文件的出现，参看VersionSet::SetupOtherInputs的注释
*/
bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &input_version_->vset_->icmp_;
  /*
    由于grandparent的文件是从小到大排列的。而且DoCompactionWork在调用这个函数时
    也是已经建立了Iterator，那么可以记录上一个key覆盖的大小。只有当grandparent_index_
    增加后才+=。
    grandparents_在确定下次compaction的start_key时更新。它是根据上次PickCompactionLevel
    时得到的最大最小key得到的level+2中overlap的files
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
      因为每层.sst文件的大小是固定的。如果超过了一定的bytes，也就是说这个Compaction即将形成的
      这个.sst文件(在level-n+1中)与level的grandparent(level-n+2)有太多的重叠，下一次Compact这个
      .sst文件时就会对太多的.sst文件进行操作。从而会将太多的数据放入内存，
      (这也应该是没有根据.sst文件的数量而是根据重叠的size来计算的原因)
      因此要停止Compaction，可能是为了减轻内存分配的负担。
  */
  if (overlapped_bytes_ > kMaxGrandParentOverlapBytes) {
    // Too much overlap for current output; start new output
    // 总感觉这里应该再添一个seen_key_ = false; 
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
