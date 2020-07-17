// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
#define CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "common/AsyncOpTracker.h"
#include "librbd/cache/ImageCache.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/Utils.h"
#include "librbd/BlockGuard.h"
#include "librbd/cache/Types.h"
#include "librbd/cache/rwl/LogOperation.h"
#include "librbd/cache/rwl/Request.h"
#include "librbd/cache/rwl/LogMap.h"
#include "ParentWriteLog.h"
#include <functional>
#include <list>

class Context;
class SafeTimer;

namespace librbd {

struct ImageCtx;

namespace cache {

template <typename ImageCtxT>
class ReplicatedWriteLog : public ParentWriteLog<ImageCtxT> {
public:
  typedef io::Extent Extent;
  typedef io::Extents Extents;

  ReplicatedWriteLog(ImageCtxT &image_ctx, librbd::cache::rwl::ImageCacheState<ImageCtxT>* cache_state);
  ~ReplicatedWriteLog();
  ReplicatedWriteLog(const ReplicatedWriteLog&) = delete;
  ReplicatedWriteLog &operator=(const ReplicatedWriteLog&) = delete;

private:
  using This = ParentWriteLog<ImageCtxT>;
  using C_WriteRequestT = rwl::C_WriteRequest<This>;
  using C_BlockIORequestT = rwl::C_BlockIORequest<This>;
  using C_FlushRequestT = rwl::C_FlushRequest<This>;
  using C_DiscardRequestT = rwl::C_DiscardRequest<This>;
  using C_WriteSameRequestT = rwl::C_WriteSameRequest<This>;
  using C_CompAndWriteRequestT = rwl::C_CompAndWriteRequest<This>;

  using ParentWriteLog<ImageCtxT>::m_lock;
  using ParentWriteLog<ImageCtxT>::m_log_pool;
  using ParentWriteLog<ImageCtxT>::m_log_entries;
  using ParentWriteLog<ImageCtxT>::m_image_ctx;
  using ParentWriteLog<ImageCtxT>::m_perfcounter;
  using ParentWriteLog<ImageCtxT>::m_ops_to_flush;

  void alloc_op_log_entries(rwl::GenericLogOperations &ops) override;
  int append_op_log_entries(rwl::GenericLogOperations &ops) override;
  bool retire_entries(const unsigned long int frees_per_tx) override;
  void persist_last_flushed_sync_gen() override;
  void schedule_flush_and_append(rwl::GenericLogOperationsVector &ops) override;
  Context *construct_flush_entry_ctx(
       const std::shared_ptr<rwl::GenericLogEntry> log_entry) override;

  void flush_then_append_scheduled_ops(void);
  void enlist_op_flusher();
  void flush_op_log_entries(rwl::GenericLogOperationsVector &ops);
  template <typename V>
  void flush_pmem_buffer(V& ops);
};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
