from socket import gethostname
from threading import Thread

from celery.app import app_or_default
from celery.events import EventReceiver
from celery.events.state import State



class Gossip(object):

    def __init__(self, connection, hostname=None, logger=None,
            routing_key="worker.#", app=None):
        self.app = app_or_default(app)
        self.ev = EventReceiver(connection, {"*": self.handle},
                                routing_key, app)
        self.hostname = hostname or gethostname()
        self.logger = logger or self.app.log.get_default_logger()
        self.state = State()

    def Consumer(self):
        return self.ev.consumer()

    def node_joins(self, node, event):
        self.logger.info(
                "Gossip: %s just moved into the neighboorhood. "
                "concurrency=%s tasks[active:%s reserved:%s] prefetch_count:%s" % (
            node.hostname, event["concurrency"],
            event["active_requests"], event["reserved_requests"],
            event["prefetch_count"]))

    def node_heartbeat(self, node, event):
        self.logger.info(
                "Gossip: %s heartbeat. "
                "concurrency=%s tasks[active:%s reserved:%s] prefetch_count:%s" % (
            node.hostname, event["concurrency"],
            event["active_requests"], event["reserved_requests"],
            event["prefetch_count"]))


    def node_leaves(self, node):
        self.logger.info("Gossip: %s left" % (node.hostname, ))

    def _to_node(self, event):
        return self.state.workers[event["hostname"]]

    def handle(self, event):
        if event["hostname"] == self.hostname:
            return
        self.state.event(event)
        if event["type"] == "worker-online":
            node = self._to_node(event)
            if node.alive:  # event may be old, so verify timestamp.
                self.node_joins(node, event)
        elif event["type"] == "worker-offline":
            node = self._to_node(event)
            if not node.alive:
                self.node_leaves(node)
        elif event["type"] == "worker-heartbeat":
            node = self._to_node(event)
            if not node.alive:
                self.node_heartbeat(node, event)

