#include"partition_node.h"
#include <iostream>
#include <string.h>
#include <cassert>
#include "db/version_edit.h"
#include "db/version_set.h"
#include "util/mutexlock.h"
#include "db/db_impl/db_impl.h"
#include "logging/logging.h"
namespace  rocksdb{
PartitionNode::PartitionNode(const std::string &start_key1,
                             const std::string &end_key1,
                             MetaNode *metaNode1,
                             VersionSet * versions,
                             CacheAlignedInstrumentedMutex &mutex,
                             InstrumentedCondVar &background_work_finished_signal_L0,
                             InternalKeyComparator &internal_comparator,
                             PmtableQueue &top_queue,
                             PmtableQueue &high_queue,
                             PmtableQueue &low_queue,
                             DBImpl *dbImpl,
                             ColumnFamilyData* cfd)
              :base_(nvmManager->get_base()),
              pmtable(nullptr),
              immuPmtable(nullptr),
              other_immuPmtable(nullptr),
              metaNode(metaNode1),
              refs_(0),
              mutex_(mutex),
              versions_(versions),
              internal_comparator_(internal_comparator),
              background_work_finished_signal_L0_(background_work_finished_signal_L0),
              top_queue_(top_queue),
              high_queue_(high_queue),
              low_queue_(low_queue),
              dbImpl_(dbImpl),
              immu_number_(0),
              cfd_(cfd){
  init(start_key1,end_key1);
}

PartitionNode::PartitionNode(MetaNode *metaNode1,
                             VersionSet * versions,
                             CacheAlignedInstrumentedMutex &mutex,
                             InstrumentedCondVar  &background_work_finished_signal_L0,
                             const InternalKeyComparator &internal_comparator,
                             PmtableQueue &top_queue,
                             PmtableQueue &high_queue,
                             PmtableQueue &low_queue,
                             DBImpl *dbImpl,
                             ColumnFamilyData* cfd)
    :
      base_(nvmManager->get_base()),
      pmtable(nullptr),
      immuPmtable(nullptr),
      other_immuPmtable(nullptr),
      metaNode(metaNode1),
      refs_(0),
      mutex_(mutex),
      versions_(versions),
      internal_comparator_(internal_comparator),
      background_work_finished_signal_L0_(background_work_finished_signal_L0),
      top_queue_(top_queue),
      high_queue_(high_queue),
      low_queue_(low_queue),
      dbImpl_(dbImpl),
      immu_number_(0),
      cfd_(cfd){
  start_key.assign(metaNode->start_key,metaNode->start_key_size);
  end_key.assign(metaNode->end_key,metaNode->end_key_size);

}

PartitionNode::~PartitionNode(){
  pmem_persist(metaNode,sizeof(MetaNode));
  nvmManager->free_meta_node(metaNode);

}
void PartitionNode::FreePartitionNode() {
  assert(immuPmtable== nullptr&&pmtable== nullptr&&other_immuPmtable== nullptr);
  metaNode->magic_number=NVM_INVALID;
  //pmem_persist(metaNode,sizeof(MetaNode));
}

void PartitionNode::FLush(){
  if(IS_FLUSH){
    pmem_persist(metaNode,PM_META_NODE_SIZE);
  }

}
void PartitionNode::init(const std::string &startkey,const std::string &endkey){
  assert(endkey>=startkey);
  //小端存储
  start_key=startkey;
  end_key=endkey;
  metaNode->start_key_size=start_key.size();
  pmem_memcpy_nodrain(&metaNode->start_key,startkey.c_str(),startkey.size());
  metaNode->end_key_size=end_key.size();
  pmem_memcpy_nodrain(&metaNode->end_key,endkey.c_str(),endkey.size());
  metaNode->magic_number=META_NODE_MAGIC;
  //pmem_drain();
}
void PartitionNode::set_range(std::string &startkey,std::string &endkey){
  assert(endkey>=startkey);
  //小端存储
  start_key=startkey;
  end_key=endkey;
  metaNode->start_key_size=start_key.size();
  pmem_memcpy_nodrain(&metaNode->start_key,startkey.c_str(),startkey.size());
  metaNode->end_key_size=end_key.size();
  pmem_memcpy_nodrain(&metaNode->end_key,endkey.c_str(),endkey.size());
  //pmem_drain();
}
void PartitionNode::set_other_immupmtable(MemTable *otherImmuPmtable1){//增加一堆
  assert(other_immuPmtable== nullptr&&otherImmuPmtable1!= nullptr);
  auto tmp=otherImmuPmtable1;
  while(tmp){
    tmp->Ref();
    tmp=tmp->next_;
  }
  other_immuPmtable=otherImmuPmtable1;
  metaNode->other_immu_pm_log=(uint64_t)other_immuPmtable->pmLogHead_-(uint64_t)base_;
  //pmem_drain();

}
void PartitionNode::set_immuPmtable(MemTable *immuPmtable1){//增加一个
  assert(immuPmtable== nullptr&&immuPmtable1!= nullptr);
  immuPmtable1->Ref();
  immuPmtable=immuPmtable1;
  metaNode->immu_pm_log=(uint64_t)immuPmtable->pmLogHead_-(uint64_t)base_;
  //pmem_drain();

}
void PartitionNode::add_immuPmtable(MemTable *immuPmtable1){//增加一个
  //assert(immuPmtable== nullptr&&immuPmtable1!= nullptr);
  immuPmtable1->Ref();
  if(immuPmtable== nullptr){
    immuPmtable=immuPmtable1;
    immuPmtable1->status_=MemTable::IN_HEAD;
    metaNode->immu_pm_log=(uint64_t)immuPmtable->pmLogHead_-(uint64_t)base_;
  }else{
    //extra_pm_log--;
    //assert(extra_pm_log>=0);
    auto tmp=immuPmtable;
    while(tmp->next_!= nullptr){
      tmp=tmp->next_;
    }
    tmp->SetNext(immuPmtable1);
    immuPmtable1->status_=MemTable::IN_FOLLOW;
  }
  immu_number_++;
  //pmem_drain();

}
void PartitionNode::add_immuPmtable_list(MemTable *immuPmtable1){//增加一个
  //assert(immuPmtable== nullptr&&immuPmtable1!= nullptr);
  immuPmtable1->Ref();
  if(immuPmtable== nullptr){
    immuPmtable=immuPmtable1;
    immuPmtable1->status_=MemTable::IN_HEAD;
    metaNode->immu_pm_log=(uint64_t)immuPmtable->pmLogHead_-(uint64_t)base_;
  }else{
    //extra_pm_log--;
    //assert(extra_pm_log>=0);
    auto tmp=immuPmtable;
    while(tmp->next_!= nullptr){
      tmp=tmp->next_;
    }
    tmp->SetNext(immuPmtable1);
    immuPmtable1->status_=MemTable::IN_FOLLOW;
  }
  immu_number_++;
  immuPmtable1=immuPmtable1->next_;
  while(immuPmtable1!= nullptr){
    immuPmtable1->Ref();
    immuPmtable1->status_=MemTable::IN_FOLLOW;
    immuPmtable1=immuPmtable1->next_;
    immu_number_++;
  }
  //pmem_drain();

}
void PartitionNode::set_pmtable(MemTable *pmTable){
  pmTable->Ref();
  pmtable=pmTable;
  metaNode->pm_log=(uint64_t)pmtable->pmLogHead_-(uint64_t)base_;
  //pmem_drain();
}
void PartitionNode::reset_other_immupmtable(int n,MemTable *pm){
  MemTable* tmp=other_immuPmtable,*next;
  assert(n>0);
  while(n--){
    next=tmp->next_;
    delete tmp->Unref();
    tmp=next;
  }
  assert(tmp==pm);
  other_immuPmtable= pm;
  if(other_immuPmtable){
    other_immuPmtable->status_=MemTable::IN_HEAD;
    metaNode->other_immu_pm_log=(uint64_t)other_immuPmtable->pmLogHead_-(uint64_t)base_;
  }else{
    metaNode->other_immu_pm_log=0;
  }

  //pmem_drain();

}
void PartitionNode::reset_immuPmtable(int n,MemTable *pm){

  MemTable* tmp=immuPmtable,*next;
  assert(n>0);
  int i=n;
  while(i--){
    next=tmp->next_;
    delete tmp->Unref();
    tmp=next;
  }
  assert(tmp==pm);
  immuPmtable= pm;
  if(immuPmtable){
    immuPmtable->status_=MemTable::IN_HEAD;
    metaNode->immu_pm_log=(uint64_t)immuPmtable->pmLogHead_-(uint64_t)base_;
  }else{
    metaNode->immu_pm_log=0;
  }

  immu_number_=immu_number_-n;
  //pmem_drain();

}

void PartitionNode::reset_other_immupmtable(){
  MemTable* tmp=other_immuPmtable,*next;
  while(tmp){
    next=tmp->next_;
    delete tmp->Unref();
    tmp=next;
  }

  other_immuPmtable= nullptr;
  metaNode->other_immu_pm_log=0;
  //pmem_drain();

}
void PartitionNode::reset_immuPmtable(){
  MemTable* tmp=immuPmtable,*next;
  while(tmp){
    next=tmp->next_;
    delete tmp->Unref();
    tmp=next;
  }

  immuPmtable= nullptr;
  metaNode->immu_pm_log=0;


  immu_number_=0;
  //pmem_drain();

}
bool PartitionNode::has_other_immupmtable(){
  return !(other_immuPmtable == nullptr);
}
void PartitionNode::reset_pmtable(){
  if(pmtable!= nullptr){
    delete pmtable->Unref();
  }
  pmtable= nullptr;
  metaNode->pm_log=0;
  //pmem_drain();

}
//返回end key
std::string & PartitionNode::get_end_key(){
  return end_key;
}
//返回start key
std::string & PartitionNode::get_start_key(){
  return start_key;
}
PartitionNode::MyStatus PartitionNode::Add(SequenceNumber s, ValueType type, const Slice& key,
                                const Slice& value,bool is_force,size_t capacity,const ProtectionInfoKVOS64* kv_prot_info,
                                bool allow_concurrent,
                                MemTablePostProcessInfo* post_process_info, void** hint) {

  if(immu_number_>=3){
    ROCKS_LOG_INFO(dbImpl_->immutable_db_options().logger,"immu_number long :%zu,num_run_flush:%d",immu_number_, dbImpl_->num_running_flushes_);
    //Log(dbImpl_->options_.info_log,"immu_number long :%zu",immu_number_);
    dbImpl_->env_->SleepForMicroseconds(1000);
  }
  Status ret=pmtable->Add(s,type,key,value,kv_prot_info,allow_concurrent,post_process_info,hint);
  if(ret.IsNvmNoSpace()){
    mutex_.Lock();
    MemTable *immupmTable=pmtable;

    //const uint64_t start_micros = dbImpl_->env_->NowMicros();
    ROCKS_LOG_INFO(dbImpl_->immutable_db_options().logger,"top_queue:%zu,high_queue:%zu,low_queue:%zu,thread:%d",
                   top_queue_.capacity(),high_queue_.capacity(),low_queue_.capacity(),dbImpl_->bg_flush_scheduled_);
    /*Log(dbImpl_->options_.info_log,"top_queue:%zu,high_queue:%zu,low_queue:%zu,thread:%d",
        top_queue_.capacity(),high_queue_.capacity(),low_queue_.capacity(),dbImpl_->background_compaction_scheduled_L0_);*/
    /*if(capacity<MIN_PARTITION&&other_immuPmtable!= nullptr){
      while(immu_number_>=3){
        Log(dbImpl_->options_.info_log,"L0 immu wait reason:immu number>=3 and capacity small");
        background_work_finished_signal_L0_.Wait();
      }
    }
    while(immu_number_>=1&&extra_pm_log<=0){
      Log(dbImpl_->options_.info_log,"L0 immu wait reason:immu number>=1 and extra_pm_log<=0");
      background_work_finished_signal_L0_.Wait();
    }

    const uint64_t cost = dbImpl_->env_->NowMicros() - start_micros;
    if(cost>1000){
      Log(dbImpl_->options_.info_log,"top_queue:%zu,high_queue:%zu,low_queue:%zu,thread:%d",top_queue_.capacity(),high_queue_.capacity(),low_queue_.capacity(),dbImpl_->background_compaction_scheduled_L0_);
      Log(dbImpl_->options_.info_log,"L0 immu wait %ld",cost);
    }*/

    add_immuPmtable(immupmTable);
    reset_pmtable();
    immupmTable->role_=MemTable::immuPmtable;
    if(immupmTable->status_==MemTable::IN_HEAD){
      immupmTable->status_=MemTable::IN_LOW_QUQUE;
      low_queue_.InsertPmtable(immupmTable);
    }else if(immuPmtable->status_==MemTable::IN_LOW_QUQUE){
      low_queue_.RemovePmtable(immuPmtable);
      high_queue_.InsertPmtable(immuPmtable);
      immuPmtable->status_=MemTable::IN_HIGH_QUEUE;
    }/*else if(immuPmtable->status_==PmTable::IN_LOW_QUQUE){
      low_queue_.RemovePmtable(immuPmtable);
      top_queue_.InsertPmtable(immuPmtable);
      immuPmtable->status_=PmTable::IN_TOP_QUEUE;
    }*/
    dbImpl_->MaybeScheduleFlushOrCompaction();

    if(is_force){
      PmLogHead *pmlog= nullptr;
      while((pmlog=nvmManager->get_pm_log())== nullptr){
        ROCKS_LOG_INFO(dbImpl_->immutable_db_options().logger,"no pm log");
        //Log(dbImpl_->options_.info_log,"no pm log");
        background_work_finished_signal_L0_.Wait();
      }
      MemTable *newPmTable=cfd_->ConstructNewMemtable(*cfd_->GetLatestMutableCFOptions(),s,this,pmlog);
      //MemTable *newPmTable=new MemTable(internal_comparator_,this,pmlog);


      set_pmtable(newPmTable);
      mutex_.Unlock();
      FLush();
      pmtable->Add(s,type,key,value,kv_prot_info,allow_concurrent,post_process_info,hint);
      return sucess;
    }else{
      MyStatus status=sucess;
      auto current=versions_->column_family_set_->GetDefault()->current();
      bool has_other_immupmtable= other_immuPmtable != nullptr;
      current->Ref();
      mutex_.Unlock();

      SequenceNumber max_snapshot = versions_->LastSequence(),min_snapshot=0;
      InternalKey start=InternalKey(start_key, max_snapshot, ValueType::kTypeValue) ;
      InternalKey end=InternalKey(end_key, min_snapshot, ValueType::kTypeValue) ;
      auto all_size=current->storage_info()->NumLevelFiles(1);
      size_t cover_size = 0;
      if (1 < current->storage_info()->num_non_empty_levels()) {
         cover_size=current->storage_info()->GetOverlappingSize(1,&start,&end);
      }


      if(cover_.size()<K){
        cover_.push_back(cover_size);
      }else{
        cover_[index_]=cover_size;
        index_=(index_+1)%K;
      }

      if(!has_other_immupmtable){
        if(all_size<560&&capacity<AVG_PARTITION){
          if(capacity<MIN_PARTITION||cover_size>=SPLIT){
            status=split;
            mutex_.Lock();
            current->Unref();
            mutex_.Unlock();
            return status;
          }else if(cover_size<=MERGE){
            status=merge;
            mutex_.Lock();
            current->Unref();
            mutex_.Unlock();
            return status;
          }else{
            if((status=needSplitOrMerge())==sucess){
              mutex_.Lock();
              PmLogHead *pmlog= nullptr;
              while((pmlog=nvmManager->get_pm_log())== nullptr){
                ROCKS_LOG_INFO(dbImpl_->immutable_db_options().logger,"no pm log");
                //Log(dbImpl_->options_.info_log,"no pm log");
                background_work_finished_signal_L0_.Wait();
              }
              MemTable *newPmTable=cfd_->ConstructNewMemtable(*cfd_->GetLatestMutableCFOptions(),s,this,pmlog);
              //MemTable *newPmTable=new MemTable(internal_comparator_,this,pmlog);

              set_pmtable(newPmTable);
              current->Unref();
              mutex_.Unlock();
              FLush();
              pmtable->Add(s,type,key,value,kv_prot_info,allow_concurrent,post_process_info,hint);
              return sucess;
            }
          }
          mutex_.Lock();
          current->Unref();
          mutex_.Unlock();
          return status;
        }else{
          if((status=needSplitOrMerge(all_size,cover_size))==sucess){
            mutex_.Lock();
            PmLogHead *pmlog= nullptr;
            while((pmlog=nvmManager->get_pm_log())== nullptr){
              ROCKS_LOG_INFO(dbImpl_->immutable_db_options().logger,"no pm log");
              //Log(dbImpl_->options_.info_log,"no pm log");
              background_work_finished_signal_L0_.Wait();
            }
            MemTable *newPmTable=cfd_->ConstructNewMemtable(*cfd_->GetLatestMutableCFOptions(),s,this,pmlog);
            //MemTable *newPmTable=new MemTable(internal_comparator_,this,pmlog);

            set_pmtable(newPmTable);
            current->Unref();
            mutex_.Unlock();
            FLush();
            pmtable->Add(s,type,key,value,kv_prot_info,allow_concurrent,post_process_info,hint);
            return sucess;
          }

          mutex_.Lock();
          current->Unref();
          mutex_.Unlock();
          return status;

        }


      }else{
        mutex_.Lock();
        PmLogHead *pmlog= nullptr;
        while((pmlog=nvmManager->get_pm_log())== nullptr){
          ROCKS_LOG_INFO(dbImpl_->immutable_db_options().logger,"no pm log");
          //Log(dbImpl_->options_.info_log,"no pm log");
          background_work_finished_signal_L0_.Wait();
        }
        MemTable *newPmTable=cfd_->ConstructNewMemtable(*cfd_->GetLatestMutableCFOptions(),s,this,pmlog);
        //MemTable *newPmTable=new MemTable(internal_comparator_,this,pmlog);

        set_pmtable(newPmTable);
        current->Unref();
        mutex_.Unlock();
        FLush();
        pmtable->Add(s,type,key,value,kv_prot_info,allow_concurrent,post_process_info,hint);
        return sucess;

      }

    }
  }
  /*if(pmtable->ApproximateMemoryUsage()>PM_LOG_SIZE/2){
    mutex_.Lock();
    if(immuPmtable!= nullptr&&immuPmtable->GetPmTableStatus()==PmTable::PmTable_Status::IN_LOW_QUQUE){
      low_queue_.RemovePmtable(immuPmtable);
      high_queue_.InsertPmtable(immuPmtable);
      immuPmtable->SetPmTableStatus(PmTable::IN_HIGH_QUEUE);
    }
    mutex_.Unlock();
  }*/

  return PartitionNode::MyStatus::sucess;

}
void PartitionNode::reset_cover(){
  cover_.clear();
  index_=0;
}
PartitionNode::MyStatus PartitionNode::needSplitOrMerge(){
  MyStatus status=sucess;
  size_t split_number=0,merge_number=0;
  for(uint64_t i=0;i<cover_.size();i++){
    if(cover_[i]>=PRE_SPLIT){
      split_number++;
    }else if(cover_[i]<=PRE_MERGE){
      merge_number++;
    }
  }
  if(split_number>=PRE_SPLIT_NUMBER){
    status=split;
    return status;
  }
  if(merge_number>=PRE_MERGE_NUMBER){
    status=merge;
    return status;
  }
  return status;
}
PartitionNode::MyStatus PartitionNode::needSplitOrMerge(size_t  all_size,size_t cover_size){
 /* Status status=sucess;
  size_t split_number=0,merge_number=0;
  for(int i=0;i<cover_.size();i++){
    if(cover_[i]>=PRE_SPLIT){
      split_number++;
    }else if(cover_[i]<=PRE_MERGE){
      merge_number++;
    }
  }
  if(split_number>=PRE_SPLIT_NUMBER){
    status=split;
    return status;
  }
  if(merge_number>=PRE_MERGE_NUMBER){
    status=merge;
    return status;
  }
  return status;*/

  double  cover_rate=cover_size*1.0/all_size;
  if(cover_rate>=NEW_SPLIT){
    return split;
  }
  if(cover_rate<=NEW_MERGE){
    return merge;
  }
  return sucess;





}

}

