from __future__ import absolute_import
from __future__ import with_statement

import socket
import threading

from .. import abstract
from ..datastructures import AttributeDict

from .control import Panel


class Component(abstract.StartStopComponent):
    name = "consumer.pidbox"
    requires = ("heartbeat", )

    def __init__(self, c, pool=None, **kwargs):
        self.pool = pool

    def create(self, c):
        return self

    def start(self, c):
        self.pidbox = Pidbox(connection=c.connection,
                             app=self.app, logger=self.logger,
                             hostname=c.hostname,
                             state=AttributeDict(app=self.app,
                                                 logger=self.logger,
                                                 hostname=c.hostname,
                                                 listener=c, consumer=c))
        self.pidbox.start()

    def stop(self, c):
        self.pidbox.stop()


class Pidbox(object):
    consumer = None

    def __init__(self, connection, app=None, hostname=None, state=None,
            logger=None, handlers=None):
        self.app = app
        self.connection = connection
        self.logger = logger
        self.errors = connection.connection_errors + connection.channel_errors
        self.node = self.app.control.mailbox.Node(hostname, state=state,
                                                  handlers=Panel.data)

    def reset(self):
        self.stop()
        self.start()

    def start(self):
        # close previously opened channel if any.
        if self.node.channel:
            try:
                self.node.channel.close()
            except self.errors:
                pass

        self.node.channel = self.connection.channel()
        self.consumer = self.node.listen(callback=self.on_control)
        self.consumer.consume()

    def stop(self):
        consumer, self.consumer = self.consumer, None
        if consumer:
            self.logger.debug("Closing broadcast channel...")
            consumer.cancel()
            consumer.channel.close()

    def on_control(self, body, message):
        """Process remote control command message."""
        try:
            self.node.handle_message(body, message)
        except KeyError, exc:
            self.logger.error("No such control command: %s", exc)
        except Exception, exc:
            self.logger.error(
                "Error occurred while handling control command: %r",
                    exc, exc_info=True)
            self.reset()


class GreenPidbox(Pidbox):
    _stopped = _shutdown = None

    def stop(self):
        self._shutdown.set()
        self._debug("Waiting for broadcast thread to shutdown...")
        self._stopped.wait()
        self._stopped = self._shutdown = None

    def start(self):
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        try:
            with self.connection.clone() as conn:
                self.node.channel = conn.default_channel
                self.consumer = self.node.listen(callback=self.on_control)
                with self.consumer:
                    while not self._shutdown.isSet():
                        try:
                            conn.drain_events(timeout=1.0)
                        except socket.timeout:
                            pass
        finally:
            self._stopped.set()
