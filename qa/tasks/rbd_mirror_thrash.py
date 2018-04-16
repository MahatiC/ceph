"""
Task for thrashing rbd-mirror daemons
"""

import contextlib
import logging
import random
import signal
import time

from gevent import sleep
from gevent.greenlet import Greenlet
from gevent.event import Event

from teuthology import misc
from teuthology.task import Task

log = logging.getLogger(__name__)


class RBDMirrorThrasher(Greenlet):
    """
    RBDMirrorThrasher::

    The RBDMirrorThrasher thrashes rbd-mirror daemons during execution of other
    tasks (workunits, etc).

    The config is optional.  Many of the config parameters are a maximum value
    to use when selecting a random value from a range.  The config is a dict
    containing some or all of:

    cluster: [default: ceph] cluster to thrash

    max_thrash: [default: 1] the maximum number of active rbd-mirror daemons per
      cluster will be thrashed at any given time.

    max_thrash_delay: [default: 30] maximum number of seconds to delay before
      thrashing again.

    max_revive_delay: [default: 10] maximum number of seconds to delay before
      bringing back a thrashed rbd-mirror daemon.

    randomize: [default: true] enables randomization and use the max/min values

    seed: [no default] seed the random number generator

    Examples::

      The following example disables randomization, and uses the max delay
      values:

      tasks:
      - ceph:
      - rbd_mirror_thrash:
          randomize: False
          max_thrash_delay: 10
    """

    def __init__(self, ctx, config, cluster, daemons):
        Greenlet.__init__(self)

        self.ctx = ctx
        self.config = config
        self.cluster = cluster
        self.daemons = daemons

        self.e = None
        self.logger = log
        self.name = 'thrasher.rbd_mirror.[{cluster}]'.format(cluster = cluster)
        self.stopping = Event()

        self.randomize = bool(self.config.get('randomize', True))
        self.max_thrash = int(self.config.get('max_thrash', 1))
        self.max_thrash_delay = float(self.config.get('thrash_delay', 60.0))
        self.max_revive_delay = float(self.config.get('max_revive_delay', 10.0))

    def _run(self):
        try:
            self.do_thrash()
        except Exception as e:
            self.e = e
            self.logger.exception("exception:")

    def log(self, x):
        """Write data to logger assigned to this RBDMirrorThrasher"""
        self.logger.info(x)

    def stop(self):
        self.stopping.set()

    def do_thrash(self):
        """
        Perform the random thrashing action
        """

        self.log('starting thrash for cluster {cluster}'.format(cluster=self.cluster))
        stats = {
            "kill": 0,
        }

        while not self.stopping.is_set():
            delay = self.max_thrash_delay
            if self.randomize:
                delay = random.randrange(0.0, self.max_thrash_delay)

            if delay > 0.0:
                self.log('waiting for {delay} secs before thrashing'.format(delay=delay))
                self.stopping.wait(delay)
                if self.stopping.is_set():
                    continue

            killed_daemons = []

            weight = 1.0 / len(self.daemons)
            count = 0
            for daemon in self.daemons:
                # if we've reached max_thrash, we're done
                count = count + 1
                if count > self.max_thrash:
                    break

                skip = random.randrange(0.0, 1.0)
                if weight <= skip:
                    self.log('skipping thrash iteration with skip ({skip}) > weight ({weight})'.format(skip=skip, weight=weight))
                    continue

                self.log('kill {label}'.format(label=daemon.id_))
                daemon.signal(signal.SIGTERM)
                killed_daemons.append(daemon)
                stats['kill'] += 1

            if killed_daemons:
                # wait for a while before restarting

                delay = self.max_revive_delay
                if self.randomize:
                    delay = random.randrange(0.0, self.max_revive_delay)

                self.log('waiting for {delay} secs before reviving daemons'.format(delay=delay))
                sleep(delay)

                for daemon in killed_daemons:
                    self.log('reviving {label}'.format(label=daemon.id_))
                    daemon.start()

        for stat in stats:
            self.log("stat['{key}'] = {value}".format(key = stat, value = stats[stat]))

@contextlib.contextmanager
def task(ctx, config):
    """
    Stress test the rbd-mirror by thrashing while another task/workunit
    is running.

    Please refer to RBDMirrorThrasher class for further information on the
    available options.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'rbd_mirror_thrash task only accepts a dict for configuration'

    cluster = config.get('cluster', 'ceph')
    daemons = list(ctx.daemons.iter_daemons_of_role('rbd-mirror', cluster))
    assert len(daemons) > 0, \
        'rbd_mirror_thrash task requires at least 1 rbd-mirror daemon'

    # choose random seed
    if 'seed' in config:
        seed = int(config['seed'])
    else:
        seed = int(time.time())
    log.info('rbd_mirror_thrash using random seed: {seed}'.format(seed=seed))
    random.seed(seed)

    thrasher = RBDMirrorThrasher(ctx, config, cluster, daemons)
    thrasher.start()

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining rbd_mirror_thrash')
        thrasher.stop()
        if thrasher.e:
            raise RuntimeError('error during thrashing')
        thrasher.join()
        log.info('done joining')
