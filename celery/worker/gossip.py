import heapq
import os

from socket import gethostname
from threading import Thread
from time import sleep

from celery.app import app_or_default
from celery.events import EventReceiver
from celery.events.state import State
from celery.utils.compat import defaultdict
from celery.utils.timer2 import Timer


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
        self.dispatcher = dispatcher
        self.state = State()
        self.timer = Timer()
        self.timers = defaultdict(lambda: None)
        self.rx_replies = {}
        self.rx_requests = {}
        self.event_handlers = {
            "worker-online": self.on_worker_online,
            "worker-offline": self.on_worker_offline,
            "worker-heartbeat": self.on_worker_heartbeat,
            "worker-rx": self.on_worker_rx,
            "worker-rx-ack": self.on_worker_rx_ack,
        }


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
        self.timers[node.hostname] = self.timer.apply_interval(5000,
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

    def handle(self, event):
        print("EVENT: %r" % (event, ))
        self.state.event(event)
        handler = self.event_handlers.get(event["type"])
        if handler:
            handler(event)

    def on_worker_online(self, event):
        if event["hostname"] != self.hostname:
            node = self._to_node(event)
            if node.alive:  # event may be old, so verify timestamp.
                self.node_joins(node)

    def on_worker_offline(self, event):
        if event["hostname"] != self.hostname:
            node = self._to_node(event)
            if not node.alive:
                self.node_leaves(node)

    def on_worker_heartbeat(self, event):
        if event["hostname"] != self.hostname:
            node = self._to_node(event)
            if node.alive:
                if node.hostname not in self.state.workers:
                    self.node_joins(node)
                else:
                    self.node_heartbeat(node)

    def on_worker_rx(self, event):
        patient = event["patient"]
        self.dispatcher.send("worker-rx-ack", patient=event["patient"])
        pos = event["clock"], event["hostname"]
        if patient in self.rx_requests:
            heapq.heappush(self.rx_requests[patient], pos)
        else:
            self.rx_requests[patient] = [pos]

    def on_worker_rx_ack(self, event):
        patient = event["patient"]
        alive_workers = self.state.alive_workers()

        if patient in alive_workers:
            return self.rx_replies.pop(patient, None)

        try:
            rx_replies = self.rx_replies[patient]
        except KeyError:
            return
        rx_replies.append(event["hostname"])

        if len(rx_replies) == len(alive_workers):
            leader = self.rx_requests[patient][0][1]
            if leader == self.hostname:
                print("Won the race to restart node %s" % (patient, ))
                self.timer.apply_after(2000,
                                       self._restart_node, (patient, ))
            else:
                print("Node %s elected to restart %s" % (leader, patient))
            self.rx_replies.pop(patient, None)

    def _restart_node(self, hostname):
        try:
            strategy = self.known_nodes[hostname]["restart"]
        except KeyError:
            self.logger.error(
                    "Restart strategy for %r suddenly missing" % (hostname, ))
        print("RESTARTING NODE: %r using restart strategy %r" % (hostname,
                                                                 strategy))
        strategy()
