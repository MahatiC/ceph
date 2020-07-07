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
#include "librbd/cache/rwl/LogOperation.h"
#include "librbd/cache/rwl/Request.h"
#include "librbd/cache/rwl/LogMap.h"
#include "ParentWriteLog.h"
#include "common/environment.h"
#include "common/Checksummer.h"
#include "os/bluestore/BlockDevice.h"
#include <functional>
#include <list>

class Context;
class SafeTimer;

namespace librbd {

struct ImageCtx;

namespace cache {

template <typename ImageCtxT>
class SSDWriteLog : public ParentWriteLog<ImageCtxT> {
public:

  SSDWriteLog(ImageCtxT &image_ctx, librbd::cache::rwl::ImageCacheState<ImageCtxT>* cache_state);
  ~SSDWriteLog();
  SSDWriteLog(const SSDWriteLog&) = delete;
  SSDWriteLog &operator=(const SSDWriteLog&) = delete;

private:
  using This = ParentWriteLog<ImageCtxT>;
  using C_WriteRequestT = rwl::C_WriteRequest<This>;
  using C_BlockIORequestT = rwl::C_BlockIORequest<This>;
  using C_FlushRequestT = rwl::C_FlushRequest<This>;
  using C_DiscardRequestT = rwl::C_DiscardRequest<This>;
  using C_WriteSameRequestT = rwl::C_WriteSameRequest<This>;
  using C_CompAndWriteRequestT = rwl::C_CompAndWriteRequest<This>;

  using ParentWriteLog<ImageCtxT>::m_image_ctx;

  //classes and methods to faciliate block device operations
  struct WriteLogPoolRootUpdate {
    std::shared_ptr<rwl::WriteLogPoolRoot> root;
    Context *ctx;
    WriteLogPoolRootUpdate(std::shared_ptr<rwl::WriteLogPoolRoot> r, Context* c): root(r), ctx(c) {}
  };

  using WriteLogPoolRootUpdateList = std::list<std::shared_ptr<WriteLogPoolRootUpdate>>;
  WriteLogPoolRootUpdateList m_poolroot_to_update; /* pool root list to update to SSD */
  bool m_updating_pool_root = false;

  class AioTransContext {
  public:
    Context *on_finish;
    IOContext ioc;
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
  rwl::WriteLogPoolRoot pool_root;

  void aio_read_data_block(rwl::WriteLogPmemEntry *log_entry,
                           bufferlist *bl, Context *ctx);
  void aio_read_data_block(std::vector<rwl::WriteLogPmemEntry*> &log_entries,
                           std::vector<bufferlist *> &bls, Context *ctx);
  void read_with_pos(uint64_t off, uint64_t len, bufferlist *bl, IOContext *ioctx);
  void append_ops(rwl::GenericLogOperations &ops,
                  Context *ctx, uint64_t *new_first_free_entry);
  void pre_io_check(rwl::WriteLogPmemEntry *log_entry, uint64_t &length);
  void write_log_entries(rwl::GenericLogEntriesVector log_entries,
                         AioTransContext *aio);
  void schedule_update_root(std::shared_ptr<rwl::WriteLogPoolRoot> root, Context *ctx);
  int update_pool_root_sync(std::shared_ptr<rwl::WriteLogPoolRoot> root);
  void update_pool_root(std::shared_ptr<rwl::WriteLogPoolRoot> root,
                        AioTransContext *aio);
  void update_root_scheduled_ops();
  void enlist_op_update_root();

  void aio_cache_cb(void *priv, void *priv2) {
    AioTransContext *c = static_cast<AioTransContext*>(priv2);
    c->aio_finish();
  }
};//class SSDWriteLog

} // namespace cache
} // namespace librbd

extern template class librbd::cache::SSDWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_SSD_WRITE_LOG
