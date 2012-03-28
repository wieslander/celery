from __future__ import absolute_import

import logging
import warnings

from kombu.utils.encoding import safe_repr

from .. import abstract
from ..exceptions import InvalidTaskError
from ..utils import timer2

from . import state
from .qos import QoS

#: Error message for when an unregistered task is received.
UNKNOWN_TASK_ERROR = """\
Received unregistered task of type %s.
The message has been ignored and discarded.

Did you remember to import the module containing this task?
Or maybe you are using relative imports?
Please see http://bit.ly/gLye1c for more information.

The full contents of the message body was:
%s
"""

#: Error message for when an invalid task message is received.
INVALID_TASK_ERROR = """\
Received invalid task message: %s
The message has been ignored and discarded.

Please ensure your message conforms to the task
message protocol as described here: http://bit.ly/hYj41y

The full contents of the message body was:
%s
"""

MESSAGE_REPORT_FMT = """\
body: %s {content_type:%s content_encoding:%s delivery_info:%s}\
"""


class ConsumerComponent(abstract.StartStopComponent):
    name = "consumer.tasks"
    requires = ("heartbeat", )

    def __init__(self, c, initial_prefetch_count=2, **kwargs):
        self.initial_prefetch_count = initial_prefetch_count

    def create(self, c):
        return self

    def start(self, c):
        c.task_consumer = TaskConsumer(c.connection, hostname=c.hostname,
                            eventer=c.event_dispatcher,
                            prefetch_count=self.initial_prefetch_count,
                            logger=self.logger, app=self.app,
                            ready_queue=c.ready_queue,
                            eta_schedule=c.eta_schedule)
        c.task_consumer.start()

    def stop(self, c):
        c.task_consumer.stop()


class TaskConsumer(object):

    def __init__(self, connection, hostname=None, eventer=None,
                 prefetch_count=None, logger=None, app=None,
            ready_queue=None, eta_schedule=None):
        self.app = app
        self.connection = connection
        self.hostname = hostname
        self.eventer = eventer
        self.prefetch_count = prefetch_count
        self.logger = logger
        self.ready_queue = ready_queue
        self.eta_schedule = eta_schedule
        self.strategies = {}
        self.connection_errors = self.connection.connection_errors
        self._does_info = self.logger.isEnabledFor(logging.INFO)

        self._consumer = None
        self.qos = None

    def start(self, *args):
        print("STARTING CONSUMER")
        self.update_strategies()
        self._consumer = self.app.amqp.get_task_consumer(self.connection,
                                    on_decode_error=self.on_decode_error)
        # QoS: Reset prefetch window.
        self.qos = QoS(self._consumer, self.prefetch_count, self.logger)
        self.qos.update()

        # receive_message handles incoming messages.
        self._consumer.register_callback(self.receive_message)
        self._consumer.consume()

    def stop(self, *args):
        if self._consumer:
            self.logger.debug("Cancelling task consumer...")
            try:
                self._consumer.cancel()

                print("STOPPING CONSUMER")
                self.logger.debug("Closing consumer channel...")
                self._consumer.close()
            finally:
                self._consumer = None

    def update_strategies(self):
        self.strategies.update(dict((task.name,
                                     task.start_strategy(self.app, self))
                                for task in self.app.tasks.itervalues()))

    def on_decode_error(self, message, exc):
        """Callback called if an error occurs while decoding
        a message received.

        Simply logs the error and acknowledges the message so it
        doesn't enter a loop.

        :param message: The message with errors.
        :param exc: The original exception instance.

        """
        self.logger.critical(
            "Can't decode message body: %r (type:%r encoding:%r raw:%r')",
                    exc, message.content_type, message.content_encoding,
                    safe_repr(message.body))
        message.ack()

    def receive_message(self, body, message):
        """Handles incoming messages.

        :param body: The message body.
        :param message: The kombu message object.

        """
        try:
            name = body["task"]
        except (KeyError, TypeError):
            warnings.warn(RuntimeWarning(
                "Received and deleted unknown message. Wrong destination?!? \
                the full contents of the message body was: %s" % (
                 self._message_report(body, message), )))
            message.reject_log_error(self.logger, self.connection_errors)
            return

        try:
            self.strategies[name](message, body, message.ack_log_error)
        except KeyError, exc:
            self.logger.error(UNKNOWN_TASK_ERROR, exc, safe_repr(body),
                              exc_info=True)
            message.reject_log_error(self.logger, self.connection_errors)
        except InvalidTaskError, exc:
            self.logger.error(INVALID_TASK_ERROR, str(exc), safe_repr(body),
                              exc_info=True)
            message.reject_log_error(self.logger, self.connection_errors)

    def _message_report(self, body, message):
        return MESSAGE_REPORT_FMT % (safe_repr(body),
                                     safe_repr(message.content_type),
                                     safe_repr(message.content_encoding),
                                     safe_repr(message.delivery_info))

    def on_task(self, task):
        """Handle received task.

        If the task has an `eta` we enter it into the ETA schedule,
        otherwise we move it the ready queue for immediate processing.

        """

        if task.revoked():
            return

        if self._does_info:
            self.logger.info("Got task from broker: %s", task.shortinfo())

        if self.eventer.enabled:
            self.eventer.send("task-received", uuid=task.id,
                    name=task.name, args=safe_repr(task.args),
                    kwargs=safe_repr(task.kwargs),
                    retries=task.request_dict.get("retries", 0),
                    eta=task.eta and task.eta.isoformat(),
                    expires=task.expires and task.expires.isoformat())

        if task.eta:
            try:
                eta = timer2.to_timestamp(task.eta)
            except OverflowError, exc:
                self.logger.error(
                    "Couldn't convert eta %s to timestamp: %r. Task: %r",
                    task.eta, exc, task.info(safe=True),
                    exc_info=True)
                task.acknowledge()
            else:
                self.qos.increment()
                self.eta_schedule.apply_at(eta,
                                           self.apply_eta_task, (task, ))
        else:
            state.task_reserved(task)
            self.ready_queue.put(task)

    def apply_eta_task(self, task):
        """Method called by the timer to apply a task with an
        ETA/countdown."""
        state.task_reserved(task)
        self.ready_queue.put(task)
        self.qos.decrement_eventually()
