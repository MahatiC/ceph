// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <libpmemobj.h>
#include "ReplicatedWriteLog.h"
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
#include "librbd/asio/ContextWQ.h"
#include "librbd/cache/rwl/ImageCacheState.h"
#include "librbd/cache/rwl/LogEntry.h"
#include "librbd/cache/rwl/Types.h"
#include <map>
#include <vector>

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_rwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::ReplicatedWriteLog: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

using namespace librbd::cache::rwl;

const unsigned long int OPS_APPENDED_TOGETHER = MAX_ALLOC_PER_TRANSACTION;

template <typename I>
ReplicatedWriteLog<I>::ReplicatedWriteLog(I &image_ctx, librbd::cache::rwl::ImageCacheState<I>* cache_state)
  : ParentWriteLog<I>(image_ctx, cache_state)
{
}

/*
 * Allocate the (already reserved) write log entries for a set of operations.
 *
 * Locking:
 * Acquires lock
 */
template <typename I>
void ReplicatedWriteLog<I>::alloc_op_log_entries(GenericLogOperations &ops)
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  struct WriteLogPmemEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);

  ceph_assert(ceph_mutex_is_locked_by_me(this->m_log_append_lock));

  /* Allocate the (already reserved) log entries */
  std::lock_guard locker(m_lock);

  for (auto &operation : ops) {
    uint32_t entry_index = this->m_first_free_entry;
    this->m_first_free_entry = (this->m_first_free_entry + 1) % this->m_total_log_entries;
    auto &log_entry = operation->get_log_entry();
    log_entry->log_entry_index = entry_index;
    log_entry->ram_entry.entry_index = entry_index;
    log_entry->pmem_entry = &pmem_log_entries[entry_index];
    log_entry->ram_entry.entry_valid = 1;
    m_log_entries.push_back(log_entry);
    ldout(m_image_ctx.cct, 20) << "operation=[" << *operation << "]" << dendl;
  }
}

/*
 * Write and persist the (already allocated) write log entries and
 * data buffer allocations for a set of ops. The data buffer for each
 * of these must already have been persisted to its reserved area.
 */
template <typename I>
int ReplicatedWriteLog<I>::append_op_log_entries(GenericLogOperations &ops)
{
  CephContext *cct = m_image_ctx.cct;
  GenericLogOperationsVector entries_to_flush;
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  int ret = 0;

  ceph_assert(ceph_mutex_is_locked_by_me(this->m_log_append_lock));

  if (ops.empty()) {
    return 0;
  }
  entries_to_flush.reserve(OPS_APPENDED_TOGETHER);

  /* Write log entries to ring and persist */
  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (!entries_to_flush.empty()) {
      /* Flush these and reset the list if the current entry wraps to the
       * tail of the ring */
      if (entries_to_flush.back()->get_log_entry()->log_entry_index >
          operation->get_log_entry()->log_entry_index) {
        ldout(m_image_ctx.cct, 20) << "entries to flush wrap around the end of the ring at "
                                   << "operation=[" << *operation << "]" << dendl;
        this->flush_op_log_entries(entries_to_flush);//TODO
        entries_to_flush.clear();
        now = ceph_clock_now();
      }
    }
    ldout(m_image_ctx.cct, 20) << "Copying entry for operation at index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "from " << &operation->get_log_entry()->ram_entry << " "
                               << "to " << operation->get_log_entry()->pmem_entry << " "
                               << "operation=[" << *operation << "]" << dendl;
    ldout(m_image_ctx.cct, 05) << "APPENDING: index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "operation=[" << *operation << "]" << dendl;
    operation->log_append_time = now;
    *operation->get_log_entry()->pmem_entry = operation->get_log_entry()->ram_entry;
    ldout(m_image_ctx.cct, 20) << "APPENDING: index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "pmem_entry=[" << *operation->get_log_entry()->pmem_entry
                               << "]" << dendl;
    entries_to_flush.push_back(operation);
  }
  this->flush_op_log_entries(entries_to_flush);//TODO

  /* Drain once for all */
  pmemobj_drain(m_log_pool);

  /*
   * Atomically advance the log head pointer and publish the
   * allocations for all the data buffers they refer to.
   */
  utime_t tx_start = ceph_clock_now();
  TX_BEGIN(m_log_pool) {
    D_RW(pool_root)->first_free_entry = this->m_first_free_entry;
    for (auto &operation : ops) {
      if (operation->reserved_allocated()) {
        auto write_op = (std::shared_ptr<WriteLogOperation>&) operation;
        pmemobj_tx_publish(&write_op->buffer_alloc->buffer_alloc_action, 1);
      } else {
        ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
      }
    }
  } TX_ONCOMMIT {
  } TX_ONABORT {
    lderr(cct) << "failed to commit " << ops.size()
               << " log entries (" << this->m_log_pool_name << ")" << dendl;
    ceph_assert(false);
    ret = -EIO;
  } TX_FINALLY {
  } TX_END;

  utime_t tx_end = ceph_clock_now();
  m_perfcounter->tinc(l_librbd_rwl_append_tx_t, tx_end - tx_start);
  m_perfcounter->hinc(
    l_librbd_rwl_append_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(), ops.size());
  for (auto &operation : ops) {
    operation->log_append_comp_time = tx_end;
  }

  return ret;
}

/*
 * Flush the persistent write log entries set of ops. The entries must
 * be contiguous in persistent memory.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_op_log_entries(GenericLogOperationsVector &ops)
{
  if (ops.empty()) {
    return;
  }

  if (ops.size() > 1) {
    ceph_assert(ops.front()->get_log_entry()->pmem_entry < ops.back()->get_log_entry()->pmem_entry);
  }

  ldout(m_image_ctx.cct, 20) << "entry count=" << ops.size() << " "
                             << "start address="
                             << ops.front()->get_log_entry()->pmem_entry << " "
                             << "bytes="
                             << ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry))
                             << dendl;
  pmemobj_flush(m_log_pool,
                ops.front()->get_log_entry()->pmem_entry,
                ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry)));
}

/**
 * Retire up to MAX_ALLOC_PER_TRANSACTION of the oldest log entries
 * that are eligible to be retired. Returns true if anything was
 * retired.
 */
template <typename I>
bool ReplicatedWriteLog<I>::retire_entries(const unsigned long int frees_per_tx) {
  CephContext *cct = m_image_ctx.cct;
  GenericLogEntriesVector retiring_entries;
  uint32_t initial_first_valid_entry;
  uint32_t first_valid_entry;

  std::lock_guard retire_locker(this->m_log_retire_lock);
  ldout(cct, 20) << "Look for entries to retire" << dendl;
  {
    /* Entry readers can't be added while we hold m_entry_reader_lock */
    RWLock::WLocker entry_reader_locker(this->m_entry_reader_lock);
    std::lock_guard locker(m_lock);
    initial_first_valid_entry = this->m_first_valid_entry;
    first_valid_entry = this->m_first_valid_entry;
    auto entry = m_log_entries.front();
    while (!m_log_entries.empty() &&
           retiring_entries.size() < frees_per_tx &&
           this->can_retire_entry(entry)) {
      if (entry->log_entry_index != first_valid_entry) {
        lderr(cct) << "Retiring entry index (" << entry->log_entry_index
                   << ") and first valid log entry index (" << first_valid_entry
                   << ") must be ==." << dendl;
      }
      ceph_assert(entry->log_entry_index == first_valid_entry);
      first_valid_entry = (first_valid_entry + 1) % this->m_total_log_entries;
      m_log_entries.pop_front();
      retiring_entries.push_back(entry);
      /* Remove entry from map so there will be no more readers */
      if ((entry->write_bytes() > 0) || (entry->bytes_dirty() > 0)) {
        auto gen_write_entry = static_pointer_cast<GenericWriteLogEntry>(entry);
        if (gen_write_entry) {
          this->m_blocks_to_log_entries.remove_log_entry(gen_write_entry);
        }
      }
      entry = m_log_entries.front();
    }
  }

  if (retiring_entries.size()) {
    ldout(cct, 20) << "Retiring " << retiring_entries.size() << " entries" << dendl;
    TOID(struct WriteLogPoolRoot) pool_root;
    pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);

    utime_t tx_start;
    utime_t tx_end;
    /* Advance first valid entry and release buffers */
    {
      uint64_t flushed_sync_gen;
      std::lock_guard append_locker(this->m_log_append_lock);
      {
        std::lock_guard locker(m_lock);
        flushed_sync_gen = this->m_flushed_sync_gen;
      }

      tx_start = ceph_clock_now();
      TX_BEGIN(m_log_pool) {
        if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) {
          ldout(m_image_ctx.cct, 20) << "flushed_sync_gen in log updated from "
                                     << D_RO(pool_root)->flushed_sync_gen << " to "
                                     << flushed_sync_gen << dendl;
          D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen;
        }
        D_RW(pool_root)->first_valid_entry = first_valid_entry;
        for (auto &entry: retiring_entries) {
          if (entry->write_bytes()) {
            ldout(cct, 20) << "Freeing " << entry->ram_entry.write_data.oid.pool_uuid_lo
                           << "." << entry->ram_entry.write_data.oid.off << dendl;
            TX_FREE(entry->ram_entry.write_data);
          } else {
            ldout(cct, 20) << "Retiring non-write: " << *entry << dendl;
          }
        }
      } TX_ONCOMMIT {
      } TX_ONABORT {
        lderr(cct) << "failed to commit free of" << retiring_entries.size() << " log entries (" << this->m_log_pool_name << ")" << dendl;
        ceph_assert(false);
      } TX_FINALLY {
      } TX_END;
      tx_end = ceph_clock_now();
    }
    m_perfcounter->tinc(l_librbd_rwl_retire_tx_t, tx_end - tx_start);
    m_perfcounter->hinc(l_librbd_rwl_retire_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(), retiring_entries.size());

    /* Update runtime copy of first_valid, and free entries counts */
    {
      std::lock_guard locker(m_lock);

      ceph_assert(this->m_first_valid_entry == initial_first_valid_entry);
      this->m_first_valid_entry = first_valid_entry;
      this->m_free_log_entries += retiring_entries.size();
      for (auto &entry: retiring_entries) {
        if (entry->write_bytes()) {
          ceph_assert(this->m_bytes_cached >= entry->write_bytes());
          this->m_bytes_cached -= entry->write_bytes();
          uint64_t entry_allocation_size = entry->write_bytes();
          if (entry_allocation_size < MIN_WRITE_ALLOC_SIZE) {
            entry_allocation_size = MIN_WRITE_ALLOC_SIZE;
          }
          ceph_assert(this->m_bytes_allocated >= entry_allocation_size);
          this->m_bytes_allocated -= entry_allocation_size;
        }
      }
      this->m_alloc_failed_since_retire = false;
      this->wake_up();
    }
  } else {
    ldout(cct, 20) << "Nothing to retire" << dendl;
    return false;
  }
  return true;
}

template <typename I>
Context* ReplicatedWriteLog<I>::construct_flush_entry_ctx(std::shared_ptr<GenericLogEntry> log_entry) {
  bool invalidating = this->m_invalidating; // snapshot so we behave consistently
  Context *ctx = this->construct_flush_entry(log_entry, invalidating);

  if (invalidating) {
    return ctx;
  }
  return new LambdaContext(
    [this, log_entry, ctx](int r) {
      m_image_ctx.op_work_queue->queue(new LambdaContext(
        [this, log_entry, ctx](int r) {
          ldout(m_image_ctx.cct, 15) << "flushing:" << log_entry
                                     << " " << *log_entry << dendl;
          log_entry->writeback(this->m_image_writeback, ctx);
        }), 0);
    });
}

const unsigned long int ops_flushed_together = 4;
/*
 * Performs the pmem buffer flush on all scheduled ops, then schedules
 * the log event append operation for all of them.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_then_append_scheduled_ops(void)
{
  GenericLogOperations ops;
  bool ops_remain = false;
  ldout(m_image_ctx.cct, 20) << dendl;
  do {
    {
      ops.clear();
      std::lock_guard locker(m_lock);
      if (m_ops_to_flush.size()) {
        auto last_in_batch = m_ops_to_flush.begin();
        unsigned int ops_to_flush = m_ops_to_flush.size();
        if (ops_to_flush > ops_flushed_together) {
          ops_to_flush = ops_flushed_together;
        }
        ldout(m_image_ctx.cct, 20) << "should flush " << ops_to_flush << dendl;
        std::advance(last_in_batch, ops_to_flush);
        ops.splice(ops.end(), m_ops_to_flush, m_ops_to_flush.begin(), last_in_batch);
        ops_remain = !m_ops_to_flush.empty();
        ldout(m_image_ctx.cct, 20) << "flushing " << ops.size() << ", "
                                   << m_ops_to_flush.size() << " remain" << dendl;
      } else {
        ops_remain = false;
      }
    }
    if (ops_remain) {
      enlist_op_flusher();
    }

    /* Ops subsequently scheduled for flush may finish before these,
     * which is fine. We're unconcerned with completion order until we
     * get to the log message append step. */
    if (ops.size()) {
      flush_pmem_buffer(ops);
      this->schedule_append(ops);
    }
  } while (ops_remain);
  this->append_scheduled_ops();
}

template <typename I>
void ReplicatedWriteLog<I>::enlist_op_flusher()
{
  this->m_async_flush_ops++;
  this->m_async_op_tracker.start_op();
  Context *flush_ctx = new LambdaContext([this](int r) {
      flush_then_append_scheduled_ops();
      this->m_async_flush_ops--;
      this->m_async_op_tracker.finish_op();
    });
  this->m_work_queue.queue(flush_ctx);
}

/*
 * Takes custody of ops. They'll all get their pmem blocks flushed,
 * then get their log entries appended.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_flush_and_append(GenericLogOperationsVector &ops)
{
  GenericLogOperations to_flush(ops.begin(), ops.end());
  bool need_finisher;
  ldout(m_image_ctx.cct, 20) << dendl;
  {
    std::lock_guard locker(m_lock);

    need_finisher = m_ops_to_flush.empty();
    m_ops_to_flush.splice(m_ops_to_flush.end(), to_flush);
  }

  if (need_finisher) {
    enlist_op_flusher();
  }
}

/*
 * Flush the pmem regions for the data blocks of a set of operations
 *
 * V is expected to be GenericLogOperations<I>, or GenericLogOperationsVector<I>
 */
template <typename I>
template <typename V>
void ReplicatedWriteLog<I>::flush_pmem_buffer(V& ops)
{
  for (auto &operation : ops) {
    operation->flush_pmem_buf_to_cache(m_log_pool);
  }

  /* Drain once for all */
  pmemobj_drain(m_log_pool);

  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (operation->reserved_allocated()) {
      operation->buf_persist_comp_time = now;
    } else {
      ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
    }
  }
}

/**
 * Update/persist the last flushed sync point in the log
 */
template <typename I>
void ReplicatedWriteLog<I>::persist_last_flushed_sync_gen()
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  uint64_t flushed_sync_gen;

  std::lock_guard append_locker(this->m_log_append_lock);
  {
    std::lock_guard locker(m_lock);
    flushed_sync_gen = this->m_flushed_sync_gen;
  }

  if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) {
    ldout(m_image_ctx.cct, 15) << "flushed_sync_gen in log updated from "
                               << D_RO(pool_root)->flushed_sync_gen << " to "
                               << flushed_sync_gen << dendl;
    TX_BEGIN(m_log_pool) {
      D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen;
    } TX_ONCOMMIT {
    } TX_ONABORT {
      lderr(m_image_ctx.cct) << "failed to commit update of flushed sync point" << dendl;
      ceph_assert(false);
    } TX_FINALLY {
    } TX_END;
  }
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;
