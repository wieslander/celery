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

    def handle(self, event):
        if event["hostname"] == self.hostname:
            return
        self.state.event(event)
        if event["type"] == "worker-online":
            node = self.state.workers[event["hostname"]]
            if node.alive:  # event may be old, so verify heartbeat.
                self.logger.info("%s just moved into the neighboorhood." % (
                    node.hostname, ))
        if event["type"] == "worker-offline":
            node = self.state.workers[event["hostname"]]
            if not node.alive:
                self.logger.warning("%s is offline." % (node.hostname, ))
        print("STATE: %r" % (self.state.workers, ))

