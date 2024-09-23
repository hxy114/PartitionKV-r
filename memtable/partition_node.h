#ifndef PARTITION_NODE
#define PARTITION_NODE
#include <cstdint>
#include <vector>
#include "db/memtable.h"
#include "memory/nvm_module.h"
//partition node节点
//主要用于在分区内索引

namespace rocksdb{
class VersionSet;
class DBImpl;
class PartitionNode{
 public:
  enum MyStatus{
    sucess,
    split,
    merge,
    noop,
  };
  char *base_;
  std::string start_key;
  std::string end_key;
  MemTable *pmtable;
  //bool set_high_queue_;//把immupmtable放到高优先级队列
  MemTable *immuPmtable;
  //std::atomic<bool> has_immuPmtable_;
  MemTable *other_immuPmtable;
  //std::atomic<bool> has_otherimmuPmtable_;
  MetaNode *metaNode;
  int refs_;
  CacheAlignedInstrumentedMutex &mutex_;
  VersionSet *const versions_;
  InternalKeyComparator internal_comparator_;

  std::vector<uint64_t>cover_;
  size_t index_=0;

  InstrumentedCondVar  &background_work_finished_signal_L0_;
  PmtableQueue &top_queue_;
  PmtableQueue &high_queue_;
  PmtableQueue &low_queue_;
  DBImpl *dbImpl_;
  size_t immu_number_;
  ColumnFamilyData *cfd_;
 public:
  void reset_cover();
  MyStatus needSplitOrMerge();
  MyStatus needSplitOrMerge(size_t all_size,size_t cover_size);
  void init(const std::string &startkey,const std::string &endkey);
  void FLush();
  PartitionNode(const std::string &start_key,
                const std::string &end_key,
                MetaNode *metaNode1,VersionSet *versions,
                CacheAlignedInstrumentedMutex &mutex,
                InstrumentedCondVar  &background_work_finished_signal,
                InternalKeyComparator &internal_comparator,
                PmtableQueue &top_queue,
                PmtableQueue &high_queue,
                PmtableQueue &low_queue,
                DBImpl *dbImpl,
                ColumnFamilyData * cfd);
  PartitionNode(MetaNode *metaNode1,VersionSet *versions,
                CacheAlignedInstrumentedMutex &mutex,
                InstrumentedCondVar &background_work_finished_signal,
                const InternalKeyComparator &internal_comparator,
                PmtableQueue &top_queue,
                PmtableQueue &high_queue,
                PmtableQueue &low_queue,
                DBImpl *dbImpl,
                ColumnFamilyData * cfd);
  ~PartitionNode();
  void FreePartitionNode();//更改metanode头，并且持久化
  //设置该partition_node的key range
  void set_range( std::string &startkey,std::string &endkey);
  //设置指针
  void set_other_immupmtable(MemTable *otherImmuPmtable);
  void add_immuPmtable(MemTable *immuPmtable);
  void add_immuPmtable_list(MemTable *immuPmtable);
  void set_immuPmtable(MemTable *immuPmtable1);
  void set_pmtable(MemTable *pmTable);
  void reset_other_immupmtable(int n,MemTable *pm);
  void reset_immuPmtable(int n,MemTable *pm);
  void reset_other_immupmtable();
  void reset_immuPmtable();
  void reset_pmtable();
  bool has_other_immupmtable();
  //将partition_node持久化到Meta node中
  void persistence_storage();
  //返回end key
  std::string &get_end_key();
  //返回start key
  std::string &get_start_key();

  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }
    MyStatus Add(SequenceNumber s, ValueType type, const Slice& key,
           const Slice& value,bool is_force,size_t  capacity,
          const ProtectionInfoKVOS64* kv_prot_info,
          bool allow_concurrent,
            MemTablePostProcessInfo* post_process_info, void** hint);//当is_force=true时候，不进行split和merge
};
}


#endif