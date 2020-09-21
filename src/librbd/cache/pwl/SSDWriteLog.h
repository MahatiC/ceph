// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_SSD_WRITE_LOG
#define CEPH_LIBRBD_CACHE_SSD_WRITE_LOG

#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "common/AsyncOpTracker.h"
#include "librbd/cache/ImageCache.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/Utils.h"
#include "librbd/BlockGuard.h"
#include "librbd/cache/Types.h"
#include "librbd/cache/pwl/LogOperation.h"
#include "librbd/cache/pwl/Request.h"
#include "librbd/cache/pwl/LogMap.h"
#include "AbstractWriteLog.h"
#include "common/environment.h"
#include "common/Checksummer.h"
#include "blk/BlockDevice.h"
#include <functional>
#include <list>

class Context;
class SafeTimer;

namespace librbd {

struct ImageCtx;

namespace cache {

namespace pwl {

template <typename ImageCtxT>
class SSDWriteLog : public AbstractWriteLog<ImageCtxT> {
public:

  SSDWriteLog(ImageCtxT &image_ctx, librbd::cache::pwl::ImageCacheState<ImageCtxT>* cache_state);
  ~SSDWriteLog() {}
  SSDWriteLog(const SSDWriteLog&) = delete;
  SSDWriteLog &operator=(const SSDWriteLog&) = delete;

private:
  using This = AbstractWriteLog<ImageCtxT>;
  using C_WriteRequestT = pwl::C_WriteRequest<This>;
  using C_BlockIORequestT = pwl::C_BlockIORequest<This>;
  using C_FlushRequestT = pwl::C_FlushRequest<This>;
  using C_DiscardRequestT = pwl::C_DiscardRequest<This>;
  using C_WriteSameRequestT = pwl::C_WriteSameRequest<This>;
  using C_CompAndWriteRequestT = pwl::C_CompAndWriteRequest<This>;

  using AbstractWriteLog<ImageCtxT>::m_lock;
  using AbstractWriteLog<ImageCtxT>::m_log_entries;
  using AbstractWriteLog<ImageCtxT>::m_image_ctx;
  using AbstractWriteLog<ImageCtxT>::m_perfcounter;
  using AbstractWriteLog<ImageCtxT>::m_bytes_allocated;
  using AbstractWriteLog<ImageCtxT>::m_async_op_tracker;
  using AbstractWriteLog<ImageCtxT>::m_ops_to_append;
  using AbstractWriteLog<ImageCtxT>::m_cache_state;
  using AbstractWriteLog<ImageCtxT>::m_first_free_entry;
  using AbstractWriteLog<ImageCtxT>::m_first_valid_entry;

  uint64_t m_log_pool_ring_buffer_size; /* Size of ring buffer */
  std::atomic<int> m_async_update_superblock = {0};

  void load_existing_entries(pwl::DeferredContexts &later);
  void alloc_op_log_entries(pwl::GenericLogOperations &ops);
  int append_op_log_entries(pwl::GenericLogOperations &ops);
  bool retire_entries(const unsigned long int frees_per_tx);
  bool has_sync_point_logs(pwl::GenericLogOperations &ops);

  void release_ram(const std::shared_ptr<pwl::GenericLogEntry> log_entry) override;
  void process_work() override;
  void schedule_append_ops(pwl::GenericLogOperations &ops) override;
  void append_scheduled_ops(void) override;
  void initialize_pool(Context *on_finish, pwl::DeferredContexts &later) override;
  void remove_pool_file() override;
  void setup_schedule_append(pwl::GenericLogOperationsVector &ops,
                             bool do_early_flush) override;
  Context *construct_flush_entry_ctx(
       const std::shared_ptr<pwl::GenericLogEntry> log_entry) override;
  bool alloc_resources(C_BlockIORequestT *req) override;
  void update_resources(C_WriteRequestT *req,
      uint64_t &bytes_cached, uint64_t &bytes_dirtied, uint64_t &bytes_allocated,
      uint64_t &number_lanes, uint64_t &number_log_entries) override;
  void update_resources(C_WriteSameRequestT *req,
      uint64_t &bytes_cached, uint64_t &bytes_dirtied, uint64_t &bytes_allocated,
      uint64_t &number_lanes, uint64_t &number_log_entries) override;

//classes and methods to faciliate block device operations
private:
  struct WriteLogPoolRootUpdate {
    std::shared_ptr<pwl::WriteLogPoolRoot> root;
    Context *ctx;
    WriteLogPoolRootUpdate(std::shared_ptr<pwl::WriteLogPoolRoot> r, Context* c): root(r), ctx(c) {}
  };

  using WriteLogPoolRootUpdateList = std::list<std::shared_ptr<WriteLogPoolRootUpdate>>;
  WriteLogPoolRootUpdateList m_poolroot_to_update; /* pool root list to update to SSD */
  bool m_updating_pool_root = false;

  class AioTransContext {
  public:
    Context *on_finish;
    ::IOContext ioc;
    explicit AioTransContext(CephContext* cct, Context *cb)
      :on_finish(cb), ioc(cct, this) {
    }
    ~AioTransContext(){}

    void aio_finish() {
      on_finish->complete(ioc.get_return_value());
      delete this;
    }
  }; //class AioTransContext

  BlockDevice *bdev = nullptr;
  uint64_t pool_size;
  pwl::WriteLogPoolRoot pool_root;

  void aio_read_data_block(pwl::WriteLogPmemEntry *log_entry,
                           bufferlist *bl, Context *ctx);
  void aio_read_data_block(std::vector<pwl::WriteLogPmemEntry*> &log_entries,
                           std::vector<bufferlist *> &bls, Context *ctx);
  void read_with_pos(uint64_t off, uint64_t len, bufferlist *bl, ::IOContext *ioctx);
  void append_ops(pwl::GenericLogOperations &ops, Context *ctx,
                  uint64_t *new_first_free_entry, uint64_t &bytes_allocated);
  void pre_io_check(pwl::WriteLogPmemEntry *log_entry, uint64_t &length);
  void write_log_entries(pwl::GenericLogEntriesVector log_entries,
                         AioTransContext *aio);
  void schedule_update_root(std::shared_ptr<pwl::WriteLogPoolRoot> root, Context *ctx);
  int update_pool_root_sync(std::shared_ptr<pwl::WriteLogPoolRoot> root);
  void update_pool_root(std::shared_ptr<pwl::WriteLogPoolRoot> root,
                        AioTransContext *aio);
  void update_root_scheduled_ops();
  void enlist_op_update_root();

  static void aio_cache_cb(void *priv, void *priv2) {
    AioTransContext *c = static_cast<AioTransContext*>(priv2);
    c->aio_finish();
  }
};//class SSDWriteLog

} // namespace pwl
} // namespace cache
} // namespace librbd

extern template class librbd::cache::pwl::SSDWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_SSD_WRITE_LOG
