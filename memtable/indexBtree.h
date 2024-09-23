// @Misc{TLX,
//   title = 	 {{TLX}: Collection of Sophisticated {C++} Data Structures, Algorithms, and Miscellaneous Helpers},
//   author = 	 {Timo Bingmann},
//   year = 	 2018,
//   note = 	 {\url{https://panthema.net/tlx}, retrieved {Oct.} 7, 2020},
// }

//在索引层使用基于B+树实现的map作为索引
#pragma once

#include<iostream>
#include<tlx/container/btree_map.hpp>
#include"partition_node.h"

namespace rocksdb{
class VersionSet;
class Version;
class DBImpl;
class PartitionIndexLayer{
 private:

  //存储分区 key range 的最大值，指针指向partitionnode
  tlx::btree_map<std::string,PartitionNode*> *bmap;
  CacheAlignedInstrumentedMutex &mutex_;
  VersionSet *const versions_;
  InternalKeyComparator internal_comparator_;
  InstrumentedCondVar  &background_work_finished_signal_L0_;
  PmtableQueue &top_queue_;
  PmtableQueue &high_queue_;
  PmtableQueue &low_queue_;
  uint64_t  capacity_;
  DBImpl *dbImpl_;

  PartitionNode::MyStatus merge(PartitionNode *partitionNode);
  PartitionNode::MyStatus split(PartitionNode *partitionNode,SequenceNumber s);
  //PartitionNode::MyStatus init_split(PartitionNode *partitionNode);
  PartitionNode * getAceeptNode(Version *current,PartitionNode *partitionNode);
 public:

  PartitionIndexLayer(VersionSet *const versions,
                      CacheAlignedInstrumentedMutex &mutex,
                      InstrumentedCondVar  &background_work_finished_signal_L0_,
                      const InternalKeyComparator &internal_comparator,
                      PmtableQueue &top_queue,
                      PmtableQueue &high_queue,
                      PmtableQueue &low_queue,
                      DBImpl *dbImpl,
                      ColumnFamilyData *cfd);
  ~PartitionIndexLayer();
  void Add(SequenceNumber s, ValueType type, const Slice& key,
                                  const Slice& value,
           const ProtectionInfoKVOS64* kv_prot_info,
           bool allow_concurrent,
           MemTablePostProcessInfo* post_process_info, void** hint);
  //根据key的值查找，当前KV应该写在哪个分区内
  PartitionNode* seek_partition(const std::string &key);
  //添加指向新的分区的索引
  bool add_new_partition(PartitionNode *partition_node);
  //移除指向该分区的索引
  bool remove_partition(PartitionNode *partition_node);
  bool remove_partition_by_key(std::string &key);
  void init();
  //const ImmutableMemTableOptions* GetImmutableMemTableOptions() const;
  void recover();

  ColumnFamilyData *cfd_;

  tlx::btree_map<std::string,PartitionNode*> * get_bmap();
};

}




