// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Intel <mahati.chamarthy@intel.com>
 *
 * Author: Mahati Chamarthy <mahati.chamarthy@intel.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_LIBRADOS_EPOLLEVENT_H
#define CEPH_LIBRADOS_EPOLLEVENT_H

#include <unistd.h>
#include <sys/epoll.h>
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "common/errno.h"
#include "common/Mutex.h"
#include "common/dout.h" 

class CephContext;

class EpollEvent{
  int ep_fd;
  struct epoll_event ee;
  int size;
  struct epoll_event *events;
  CephContext *cct;

 public:
 int init(){
   int nevent = 5000;
   events = (struct epoll_event*)malloc(sizeof(struct epoll_event)*nevent);
   if (!events) {
     return -ENOMEM;
   }
   memset(events, 0, sizeof(struct epoll_event)*nevent);

   ep_fd = epoll_create(1024);
   if (ep_fd == -1) {
      return -errno;
   }

   size = nevent;

   return 0;
 }

 int add_event(int efd){
  ee.events = EPOLLIN;
  ee.data.fd = efd;
  if (epoll_ctl(ep_fd, EPOLL_CTL_ADD, efd, &ee) == -1) {
    return -errno;
  }

  return 0;
 }

 int process_fevents(){
   int retval, numevents = 0;
   const unsigned timeout_microseconds = 30000000;
   struct timeval tv;
   tv.tv_sec = timeout_microseconds / 1000000;
   tv.tv_usec = timeout_microseconds % 1000000;

   retval = epoll_wait(ep_fd, events, size,
                      (tv.tv_sec*1000 + tv.tv_usec/1000));
   if (retval > 0) {
     numevents = retval;
   }

   return numevents;
 }
 
};

#endif
