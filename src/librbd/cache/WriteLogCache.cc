// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "WriteLogCache.h"
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
#include "ReplicatedWriteLog.h"
#include "SSDWriteLog.h"
#include "librbd/cache/rwl/ImageCacheState.h"
#include "librbd/cache/rwl/LogEntry.h"
#include <map>
#include <vector>

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_rwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::WriteLogCache: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

using namespace librbd::cache::rwl;

typedef WriteLogCache<ImageCtx>::Extent Extent;
typedef WriteLogCache<ImageCtx>::Extents Extents;

template <typename I>
WriteLogCache<I>::WriteLogCache(I &image_ctx, librbd::cache::rwl::ImageCacheState<I>* cache_state,
                               bool isRWL) {
  if (isRWL) {
    m_write_log = new ReplicatedWriteLog<I>(image_ctx, cache_state);
  } else {
    m_write_log = new SSDWriteLog<I>(image_ctx, cache_state);
  }
}

template <typename I>
WriteLogCache<I>::~WriteLogCache() {
  delete m_write_log;
}

template <typename I>
void WriteLogCache<I>::aio_read(Extents&& image_extents,
                                     ceph::bufferlist* bl,
                                     int fadvise_flags, Context *on_finish) {
  m_write_log->read(std::move(image_extents), std::move(bl), fadvise_flags,
                    on_finish);
}

template <typename I>
void WriteLogCache<I>::aio_write(Extents &&image_extents,
                                      bufferlist&& bl,
                                      int fadvise_flags,
                                      Context *on_finish) {
  m_write_log->write(std::move(image_extents), std::move(bl),
                     fadvise_flags, on_finish);
}

template <typename I>
void WriteLogCache<I>::aio_discard(uint64_t offset, uint64_t length,
                                        uint32_t discard_granularity_bytes,
                                        Context *on_finish) {
  m_write_log->discard(offset, length, discard_granularity_bytes, on_finish);
}

template <typename I>
void WriteLogCache<I>::aio_writesame(uint64_t offset, uint64_t length,
                                          bufferlist&& bl, int fadvise_flags,
                                          Context *on_finish) {
  m_write_log->writesame(offset, length, std::move(bl), fadvise_flags,
                         on_finish);
}

template <typename I>
void WriteLogCache<I>::aio_compare_and_write(Extents &&image_extents,
                                                  bufferlist&& cmp_bl,
                                                  bufferlist&& bl,
                                                  uint64_t *mismatch_offset,
                                                  int fadvise_flags,
                                                  Context *on_finish) {
  m_write_log->compare_and_write(std::move(image_extents), std::move(cmp_bl),
                                 std::move(bl), mismatch_offset, fadvise_flags,
                                 on_finish);
}

template <typename I>
void WriteLogCache<I>::init(Context *on_finish) {
  m_write_log->init(on_finish);
}

template <typename I>
void WriteLogCache<I>::shut_down(Context *on_finish) {
  m_write_log->shut_down(on_finish);
}

template <typename I>
void WriteLogCache<I>::invalidate(Context *on_finish) {
  m_write_log->invalidate(on_finish);
}

template <typename I>
void WriteLogCache<I>::flush(Context *on_finish) {
  m_write_log->flush(on_finish);
}

} // namespace cache
} // namespace librbd

template class librbd::cache::WriteLogCache<librbd::ImageCtx>;
template class librbd::cache::ImageCache<librbd::ImageCtx>;
