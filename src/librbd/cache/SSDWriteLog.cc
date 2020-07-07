// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SSDWriteLog.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/ceph_assert.h"
#include "common/deleter.h"
#include "common/dout.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "common/Timer.h"
#include "common/perf_counters.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/rwl/ImageCacheState.h"
#include "librbd/cache/rwl/LogEntry.h"
#include <map>
#include <vector>

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_rwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::SSDWriteLog: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

using namespace librbd::cache::rwl;

template <typename I>
SSDWriteLog<I>::SSDWriteLog(I &image_ctx, librbd::cache::rwl::ImageCacheState<I>* cache_state)
  : ParentWriteLog<I>(image_ctx, cache_state)
{
}

template <typename I>
void SSDWriteLog<I>::update_pool_root(std::shared_ptr<WriteLogPoolRoot> root,
                                      AioTransContext *aio) {
  bufferlist bl;
  super_block_t superblock;
  superblock.root = *root;
  superblock.checksum = 0;
  encode(superblock, bl);
  bl.append_zero(MIN_WRITE_ALLOC_SIZE - bl.length());
  ceph_assert(bl.length() % MIN_WRITE_ALLOC_SIZE == 0);
  bdev->aio_write(0, bl, &aio->ioc, false, WRITE_LIFE_NOT_SET);
  bdev->aio_submit(&aio->ioc);
}

template <typename I>
int SSDWriteLog<I>::update_pool_root_sync(std::shared_ptr<WriteLogPoolRoot> root) {
  bufferlist bl;
  super_block_t superblock;
  superblock.root = *root;
  superblock.checksum = 0;
  encode(superblock, bl);
  bl.append_zero(MIN_WRITE_ALLOC_SIZE - bl.length());
  ceph_assert(bl.length() % MIN_WRITE_ALLOC_SIZE == 0);
  return bdev->write(0, bl, false);
}

template <typename I>
void SSDWriteLog<I>::pre_io_check(WriteLogPmemEntry *log_entry, uint64_t &length) {
  assert(log_entry->is_write() || log_entry->is_writesame());
  ceph_assert(log_entry->write_data_pos <= pool_size);

  length = log_entry->is_write() ? log_entry->write_bytes : log_entry->ws_datalen;
  length = round_up_to(length, MIN_WRITE_ALLOC_SIZE);
  ceph_assert(length != 0 && log_entry->write_data_pos + length <= pool_size);
}

template <typename I>
void SSDWriteLog<I>::aio_read_data_block(
    WriteLogPmemEntry *log_entry, bufferlist *bl, Context *ctx) {
  std::vector<WriteLogPmemEntry*> log_entries {log_entry};
  std::vector<bufferlist *> bls {bl};
  aio_read_data_block(log_entries, bls, ctx);
}

template <typename I>
void SSDWriteLog<I>::aio_read_data_block(
    std::vector<WriteLogPmemEntry*> &log_entries,
    std::vector<bufferlist *> &bls, Context *ctx) {
  ceph_assert(log_entries.size() == bls.size());

  //get the valid part
  Context *read_ctx = new LambdaContext(
    [this, log_entries, bls, ctx](int r) {
      for (unsigned int i = 0; i < log_entries.size(); i++) {
        bufferlist valid_data_bl;
        auto length = log_entries[i]->is_write() ? log_entries[i]->write_bytes : log_entries[i]->ws_datalen;
        valid_data_bl.substr_of(*bls[i], 0, length);
        bls[i]->clear();
        bls[i]->append(valid_data_bl);
      }
     ctx->complete(r);
    });

  CephContext *cct = m_image_ctx.cct;
  AioTransContext *aio = new AioTransContext(cct, read_ctx);
  for (unsigned int i = 0; i < log_entries.size(); i++) {
    auto log_entry = log_entries[i];

    uint64_t length;
    pre_io_check(log_entry, length);
    ldout(cct, 20) << "Read at " << log_entry->write_data_pos
                   << ", length " << length << dendl;

    bdev->aio_read(log_entry->write_data_pos, length, bls[i], &aio->ioc);
  }
  bdev->aio_submit(&aio->ioc);
}

template <typename I>
void SSDWriteLog<I>::read_with_pos(uint64_t off, uint64_t len,
    bufferlist *bl, IOContext *ioctx) {
   // align read to block size
  uint64_t length = len;
  auto align_size = round_up_to(length, MIN_WRITE_ALLOC_SSD_SIZE);
  if (length < align_size) {
    length += (align_size - length);
  }
  bdev->read(off, length, bl, ioctx, false);
}

} // namespace cache
} // namespace librbd

template class librbd::cache::SSDWriteLog<librbd::ImageCtx>;
template class librbd::cache::ImageCache<librbd::ImageCtx>;
