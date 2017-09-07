// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <boost/scoped_ptr.hpp>

#include <iostream>
#include <string>
using namespace std;

#include "osd/OSD.h"
#include "os/ObjectStore.h"
#include "mon/MonClient.h"
#include "include/ceph_features.h"

#include "common/config.h"

#include "mon/MonMap.h"

#include "msg/Messenger.h"
#include "msg/async/AsyncMessenger.h"
#include "msg/async/PosixStack.h"
#include "msg/async/net_handler.h"

#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#include <algorithm>
#include "include/buffer.h"
#include "include/str_list.h"
#include "include/sock_compat.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "common/dout.h"
#include "include/assert.h"
#include "common/simple_spin.h"

#include "common/Timer.h"
#include "common/TracepointProvider.h"
#include "common/ceph_argparse.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "include/color.h"
#include "common/errno.h"

#include "common/pick_address.h"

#include "perfglue/heap_profiler.h"

#include "include/assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd

namespace {

TracepointProvider::Traits osd_tracepoint_traits("libosd_tp.so",
                                                 "osd_tracing");
TracepointProvider::Traits os_tracepoint_traits("libos_tp.so",
                                                "osd_objectstore_tracing");

} // anonymous namespace

OSD *osd = NULL;

static void usage()
{
  cout << "usage: ceph-osd-s -i <osdid>\n"
       << "  --osd-data PATH data directory\n"
       << "  --osd-journal PATH\n"
       << "                    journal file or block device\n"
       << "  --mkfs            create a [new] data directory\n"
       << "  --convert-filestore\n"
       << "                    run any pending upgrade operations\n"
       << "  --flush-journal   flush all data out of journal\n"
       << "  --mkjournal       initialize a new journal\n"
       << "  --check-wants-journal\n"
       << "                    check whether a journal is desired\n"
       << "  --check-allows-journal\n"
       << "                    check whether a journal is allowed\n"
       << "  --check-needs-journal\n"
       << "                    check whether a journal is required\n"
       << "  --debug_osd <N>   set debug level (e.g. 10)\n"
       << "  --get-device-fsid PATH\n"
       << "                    get OSD fsid for the given block device\n"
       << std::endl;
  generic_server_usage();
}

#ifdef BUILDING_FOR_EMBEDDED
void cephd_preload_embedded_plugins();
void cephd_preload_rados_classes(OSD *osd);
extern "C" int cephd_osd(int argc, const char **argv)
#else
int main(int argc, const char **argv)
#endif
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  vector<const char*> def_args;
  def_args.push_back("--leveldb-log=");

  auto cct = global_init(&def_args, args, CEPH_ENTITY_TYPE_OSD,
                         CODE_ENVIRONMENT_DAEMON,
                         0, "osd_data");
  ceph_heap_profiler_init();

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
        usage();
    }
  }

  std::string public_msgr_type = g_conf->ms_public_type.empty() ? g_conf->get_val<std::string>("ms_type") : g_conf->ms_public_type;
  char *end;
  const char *id = g_conf->name.get_id().c_str();
  int whoami = strtol(id, &end, 10);

  Messenger *ms_public = Messenger::create(g_ceph_context, public_msgr_type,
                                           entity_name_t::OSD(whoami), "client",
                                           getpid(),
                                           Messenger::HAS_HEAVY_TRAFFIC |
                                           Messenger::HAS_MANY_CONNECTIONS);

  boost::scoped_ptr<Throttle> client_byte_throttler(
    new Throttle(g_ceph_context, "osd_client_bytes",
                 g_conf->osd_client_message_size_cap));
  boost::scoped_ptr<Throttle> client_msg_throttler(
    new Throttle(g_ceph_context, "osd_client_messages",
                 g_conf->osd_client_message_cap));

  ms_public->set_default_policy(Messenger::Policy::stateless_server(0));
  ms_public->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
                                   client_byte_throttler.get(),
                                   client_msg_throttler.get());
  ms_public->set_policy(entity_name_t::TYPE_MON,
                               Messenger::Policy::lossy_client(CEPH_FEATURE_UID |
                                                               CEPH_FEATURE_PGID64 |
                                                               CEPH_FEATURE_OSDENC));
  ms_public->set_policy(entity_name_t::TYPE_MGR,
                               Messenger::Policy::lossy_client(CEPH_FEATURE_UID |
                                                               CEPH_FEATURE_PGID64 |
                                                               CEPH_FEATURE_OSDENC));

  //try to poison pill any OSD connections on the wrong address
  ms_public->set_policy(entity_name_t::TYPE_OSD,
                        Messenger::Policy::stateless_server(0));

  //r = ms_public->bind(g_conf->public_addr);
  //if (r < 0)
  //  exit(1);

  // custom socket, bind, listen implementation:: this is implemented in processor::listen
  // in src/msg/async/PosixStack.cc
  AsyncMessenger *msgr;
  SocketOptions opts;
  opts.nodelay = msgr->cct->_conf->ms_tcp_nodelay;
  opts.rcbuf_size = msgr->cct->_conf->ms_tcp_rcvbuf;

  //const md_config_t *conf = msgr->cct->_conf;

  int family = AF_INET;
  ServerSocket listen_socket;

  entity_addr_t listen_addr = g_conf->public_addr;
  if (listen_addr.get_type() == entity_addr_t::TYPE_NONE) {
    listen_addr.set_type(entity_addr_t::TYPE_LEGACY);
  }
  listen_addr.set_family(family);

  int listen_sd;
  int r = 0;

  if ((listen_sd = ::socket(AF_INET, SOCK_STREAM, 0)) == -1) {
    r = errno;
    return -r;
  }

  r = ::bind(listen_sd, listen_addr.get_sockaddr(), listen_addr.get_sockaddr_len());
  if (r < 0) {
    r = -errno;
    ::close(listen_sd);
    return r;
  }

  r = ::listen(listen_sd, 128);
  if (r < 0) {
    r = -errno;
    ::close(listen_sd);
    return r;
  }

  /**sock = ServerSocket(
          std::unique_ptr<PosixServerSocketImpl>(
              new PosixServerSocketImpl(net, listen_sd)));

  assert(sock); */
  sockaddr_storage ss;
  socklen_t slen = sizeof(ss);
  int sd = ::accept(listen_sd, (sockaddr*)&ss, &slen);
  if (sd < 0) {
    return -errno;
  }

  /*ssize_t read(char *buf, size_t len) override {
    ssize_t r = ::read(sd, buf, len);
    if (r < 0)
      r = -errno;
    return r;
  }*/


}

