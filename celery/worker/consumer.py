# -*- coding: utf-8 -*-
"""
celery.worker.consumer
~~~~~~~~~~~~~~~~~~~~~~

This module contains the component responsible for consuming messages
from the broker, processing the messages and keeping the broker connections
up and running.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import socket

from .. import abstract
from ..app import app_or_default
from ..utils import timer2
from ..utils.functional import noop

RUN = 0x1
CLOSE = 0x2
STOP = 0x3


class Namespace(abstract.Namespace):
    name = "consumer"
    builtin_boot_steps = ("celery.worker.tasks",
                          "celery.worker.pidbox",
                          "celery.worker.heartbeat")

    def modules(self):
        return self.builtin_boot_steps


class Component(abstract.StartStopComponent):
    name = "worker.consumer"
    last = True

    def create(self, w):
        prefetch_count = w.concurrency * w.prefetch_multiplier
        c = w.consumer = self.instantiate(
                w.consumer_cls, w.ready_queue, w.scheduler,
                logger=w.logger, hostname=w.hostname,
                send_events=w.send_events,
                init_callback=w.ready_callback,
                initial_prefetch_count=prefetch_count,
                pool=w.pool,
                priority_timer=w.priority_timer,
                app=w.app,
                controller=w)
        return c


class Events(abstract.StartStopComponent):
    name = "consumer.events"
    #requires = ("connection", )

    def __init__(self, c):
        c.event_dispatcher = None

    def create(self, c):
        return self

    def start(self, c):
        prev = c.event_dispatcher
        c.event_dispatcher = c.app.events.Dispatcher(c.connection,
                                                     hostname=c.hostname,
                                                     enabled=c.send_events)
        if prev:
            # Flush events sent while connection was down.
            c.event_dispatcher.copy_buffer(prev)
            c.event_dispatcher.flush()

    def stop(self, c):
        if c.event_dispatcher:
            self.logger.debug("Shutting down event dispatcher...")
            ev, c.event_dispatcher = c.event_dispatcher, None
            ev.close()


class Consumer(object):

    #: The queue that holds tasks ready for immediate processing.
    ready_queue = None

    #: Timer for tasks with an ETA/countdown.
    eta_schedule = None

    #: Enable/disable events.
    send_events = False

    #: Optional callback to be called when the connection is established.
    #: Will only be called once, even if the connection is lost and
    #: re-established.
    init_callback = None

    #: The current hostname.  Defaults to the system hostname.
    hostname = None

    #: The logger instance to use.  Defaults to the default Celery logger.
    logger = None

    #: The broker connection.
    connection = None

    #: The current worker pool instance.
    pool = None

    #: A timer used for high-priority internal tasks, such
    #: as sending heartbeats.
    priority_timer = None

    # Consumer state, can be RUN or CLOSE.
    _state = None

    def __init__(self, ready_queue, eta_schedule, logger,
            init_callback=noop, send_events=False, hostname=None,
            initial_prefetch_count=2, pool=None, app=None,
            priority_timer=None, controller=None):
        self.app = app_or_default(app)
        self.connection = None
        self.controller = controller
        self.ready_queue = ready_queue
        self.eta_schedule = eta_schedule
        self.send_events = send_events
        self.init_callback = init_callback
        self.logger = logger
        self.hostname = hostname or socket.gethostname()
        self.pool = pool
        self.priority_timer = priority_timer or timer2.default_timer
        conninfo = self.app.broker_connection()
        self.connection_errors = conninfo.connection_errors
        self.channel_errors = conninfo.channel_errors

        self.components = []
        self.namespace = None

    def start(self):
        """Start the consumer.

        Automatically survives intermittent connection failure,
        and will retry establishing the connection and restart
        consuming messages.

        """
        reset = self.reset_connection

        self.init_callback(self)

        while self._state != CLOSE:
            try:
                reset()
                drain_events = self.connection.drain_events

                while self._state != CLOSE and self.connection:
                    #if qos.prev != qos.value:
                    #    qos.update()
                    try:
                        drain_events(timeout=1)
                    except socket.timeout:
                        pass
                    except socket.error:
                        if self._state != CLOSE:
                            raise
            except self.connection_errors + self.channel_errors:
                self.logger.error("Consumer: Connection to broker lost."
                                + " Trying to re-establish the connection...",
                                exc_info=True)

    def maybe_conn_error(self, fun, *args, **kwargs):
        """Applies function but ignores any connection or channel
        errors raised."""
        try:
            fun(*args, **kwargs)
        except (AttributeError, ) + \
                self.connection_errors + \
                self.channel_errors:
            pass

    def close_connection(self):
        """Closes the current broker connection and all open channels."""
        # We must set self.connection to None here, so
        # that the green pidbox thread exits.
        connection, self.connection = self.connection, None

        if connection:
            self._debug("Closing broker connection...")
            self.maybe_conn_error(connection.close)

    def stop_consumers(self):
        if self._state != STOP:
            print("STOP CONSUMERS: %r" % (self._state == RUN, ))
            for component in reversed(self.components):
                print("STOPPING COMPONENT: %r" % (component, ))
                self.maybe_conn_error(component.stop, self)

    def reset_connection(self):
        """Re-establish the broker connection and set up consumers,
        heartbeat and the event dispatcher."""
        self._debug("Re-establishing connection to the broker...")

        # Clear internal queues to get rid of old messages.
        # They can't be acked anyway, as a delivery tag is specific
        # to the current channel.
        self.ready_queue.clear()
        self.eta_schedule.clear()

        # Re-establish the broker connection and setup the task consumer.
        self.connection = self._open_connection()
        self._debug("Connection established.")

        self.components = []
        self.namespace = Namespace(app=self.app,
                                   logger=self.logger).apply(self)
        print("STARTING COMP: %r" % (self.components, ))
        for component in self.components:
            print("STARTING: %r" % (component, ))
            component.start(self)

        self._state = RUN

    def _open_connection(self):
        """Establish the broker connection.

        Will retry establishing the connection if the
        :setting:`BROKER_CONNECTION_RETRY` setting is enabled

        """

        # Callback called for each retry while the connection
        # can't be established.
        def _error_handler(exc, interval):
            self.logger.error("Consumer: Connection Error: %s. "
                              "Trying again in %d seconds...", exc, interval)

        # remember that the connection is lazy, it won't establish
        # until it's needed.
        conn = self.app.broker_connection()
        if not self.app.conf.BROKER_CONNECTION_RETRY:
            # retry disabled, just call connect directly.
            conn.connect()
            return conn

        return conn.ensure_connection(_error_handler,
                    self.app.conf.BROKER_CONNECTION_MAX_RETRIES)

    def stop(self):
        """Stop consuming.

        Does not close the broker connection, so be sure to call
        :meth:`close_connection` when you are finished with it.

        """
        # Notifies other threads that this instance can't be used
        # anymore.
        self._state = CLOSE
        self._debug("Stopping consumers...")
        self.stop_consumers()
        self._state = STOP

    @property
    def info(self):
        """Returns information about this consumer instance
        as a dict.

        This is also the consumer related info returned by
        ``celeryctl stats``.

        """
        conninfo = {}
        if self.connection:
            conninfo = self.connection.info()
            conninfo.pop("password", None)  # don't send password.
        return {"broker": conninfo, "prefetch_count": self.qos.value}

    def _debug(self, msg, **kwargs):
        self.logger.debug("Consumer: %s", msg, **kwargs)
