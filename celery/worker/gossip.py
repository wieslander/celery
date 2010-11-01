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
            routing_key="worker.#", dispatcher=None,
            known_nodes=None, app=None):
        self.app = app_or_default(app)
        self.ev = EventReceiver(connection, {"*": self.handle},
                                routing_key, app)
        self.known_nodes = known_nodes or self.app.conf.CELERYD_NODES
        self.hostname = hostname or gethostname()
        self.logger = logger or self.app.log.get_default_logger()
        self.dispatcher = None
        self.state = State()
        self.timer = Timer()
        self.timers = defaultdict(lambda: None)
        self.rx_replies = {}

    def Consumer(self):
        return self.ev.consumer()

    def try_restart(self, node):
        info = self.known_nodes.get(node.hostname) or {}
        restart_strategy = info.get("restart")
        if restart_strategy:
            self.dispatcher.send("worker-rx", patient=node.hostname)
            self.rx_replies[node.hostname] = []

    def _update_timer(self, node):
        old_timer = self.timers[node.hostname]
        if old_timer:
            old_timer.cancel()
        self.timers[node.hostname] = self.timer.apply_interval(40000,
                self._verify_node, (node, ))

    def node_joins(self, node):
        self._update_timer(node)
        self.logger.info(
                "Gossip: %s just moved into the neighboorhood. stats: %r" % (
                    node.hostname, node.stats))

    def node_heartbeat(self, node):
        self._update_timer(node)
        stats = node.stats
        self.logger.info(
                "Gossip: %s heartbeat: stats:%r" % (node.hostname,
                                                    node.stats))

    def node_leaves(self, node):
        self.logger.info("Gossip: %s left" % (node.hostname, ))
        self.try_restart(node)

    def _to_node(self, event):
        return self.state.workers[event["hostname"]]

    def _verify_node(self, node):
        print("VERIFYING NODE: %r" % (node, ))
        if not node.alive:
            self.node_leaves(node)
            self.timers[node.hostname].cancel()
            self.state.workers.pop(node.hostname, None)
        print("RETURNED")

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
        elif event["type"] == "worker-rx":
            patient = event["patient"]
            self.dispatcher.send("worker-rx-ack", patient=event["patient"])
            if patient in self.rx_requests:
                heapq.heappush(self.rx_requests[patient], (event["timestamp"],
                                                           event["hostname"]))
            else:
                self.rx_requests[patient] = [(event["timestamp"],
                                              event["hostname"])]
        elif event["type"] == "worker-rx-ack":
            patient = event["patient"]
            alive_nodes = self.state.alive_workers()
            if patient in alive_workers:
                return self.rx_replies.pop(patient, None)
            try:
                rx_replies = self.rx_replies[patient]
            except KeyError:
                return

            rx_replies.append(event["hostname"])
            if len(rx_replies) == len(alive_nodes):
                print("GOT ENOUGH REPLIES TO RESTART")
                self.timer.apply_after(10000, self._restart_node, (node, ))

        def _restart_node(self, node):
            print("ACTUALLY RESTART NODE: %r" % (node, ))
