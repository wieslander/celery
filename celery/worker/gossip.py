import os
import paramiko
import subprocess

from socket import gethostname
from threading import Thread
from time import sleep

from celery.app import app_or_default
from celery.events import EventReceiver
from celery.events.state import State
from celery.utils.compat import defaultdict
from celery.utils.timer2 import Timer


class RestartStrategy(object):

    def __init__(self, argv):
        self.argv = argv

    def __call__(self):
        return subprocess.call(self.argv) == 0


class SSHRestartStrategy(object):

    def __init__(self, username, host, argv, password=None):
        self.username = username
        self.password = password
        self.host = host
        self.argv = argv

    def __call__(self):
        client = self.connect()
        stdin, stdout, stderr = client.exec_command(self.argv)

    def connect(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.host,
                       username=self.username,
                       password=self.password)









class Gossip(object):

    def __init__(self, connection, hostname=None, logger=None,
            routing_key="worker.#", app=None):
        self.app = app_or_default(app)
        self.ev = EventReceiver(connection, {"*": self.handle},
                                routing_key, app)
        self.hostname = hostname or gethostname()
        self.logger = logger or self.app.log.get_default_logger()
        self.state = State()
        self.timer = Timer()
        self.timers = defaultdict(lambda: None)

    def Consumer(self):
        return self.ev.consumer()

    def _update_timer(node):
        old_timer = self.timers[node.hostname]
        if old_timer:
            old_timer.cancel()
        self.timers[node.hostname] = self.timer.apply_interval(40,
                self._verify_node, (node, ))

    def node_joins(self, node):
        self.logger.info(
                "Gossip: %s just moved into the neighboorhood. stats: %r" % (
                    node.hostname, node.stats))

    def node_heartbeat(self, node):
        self.timers[node.hostname] = self.timer.apply_interval(40,
                self._verify_node, (node, ))
        stats = node.stats
        self.logger.info(
                "Gossip: %s heartbeat: stats:%r" % (node.hostname,
                                                    node.stats))

    def node_leaves(self, node):
        self.logger.info("Gossip: %s left" % (node.hostname, ))

    def _to_node(self, event):
        return self.state.workers[event["hostname"]]

    def _verify_node(self, node):
        print("VERIFYING NODE: %r" % (node, ))
        if not node.alive:
            self.node_leaves(node)
            self.timers[node.hostname].cancel()

    def handle(self, event):
        if event["hostname"] == self.hostname:
            return
        joins = False
        if event["type"] == "worker-heartbeat" and \
                event["hostname"] not in self.state.workers:
                    joins = True
        self.state.event(event)
        if joins or event["type"] == "worker-online":
            node = self._to_node(event)
            if node.alive:  # event may be old, so verify timestamp.
                self.node_joins(node)
        elif event["type"] == "worker-offline":
            node = self._to_node(event)
            if not node.alive:
                self.node_leaves(node)
        elif event["type"] == "worker-heartbeat":
            node = self._to_node(event)
            if node.alive:
                self.node_heartbeat(node)
