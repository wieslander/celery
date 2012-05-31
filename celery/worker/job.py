# -*- coding: utf-8 -*-
"""
    celery.worker.job
    ~~~~~~~~~~~~~~~~~

    This module defines the :class:`Request` class,
    which specifies how tasks are executed.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import logging
import time
import sys

from datetime import datetime

from kombu.utils import kwdict, reprcall
from kombu.utils.encoding import safe_repr, safe_str

from celery import current_app
from celery import exceptions
from celery.app import app_or_default
from celery.app.task import Context
from celery.datastructures import AttributeDict, ExceptionInfo
from celery.task.trace import (
    build_tracer,
    trace_task,
    trace_task_ret,
    report_internal_error,
    execute_bare,
)
from celery.platforms import set_mp_process_title as setps
from celery.utils import fun_takes_kwargs
from celery.utils.functional import noop
from celery.utils.log import get_logger
from celery.utils.text import truncate
from celery.utils.timeutils import maybe_iso8601, timezone

from . import state

logger = get_logger(__name__)
debug, info, warn, error = (logger.debug, logger.info,
                            logger.warn, logger.error)
_does_debug = logger.isEnabledFor(logging.DEBUG)
_does_info = logger.isEnabledFor(logging.INFO)

#: Format string used to log task failure.
error_msg = """\
    Task %(name)s[%(id)s] raised exception: %(exc)s
""".strip()

#: Format string used to log internal error.
internal_error_msg = """\
    Task %(name)s[%(id)s] INTERNAL ERROR: %(exc)s
""".strip()

#: Format string used to log task retry.
retry_msg = """Task %(name)s[%(id)s] retry: %(exc)s"""


# Localize
tz_to_local = timezone.to_local
tz_or_local = timezone.tz_or_local
tz_utc = timezone.utc

NEEDS_KWDICT = sys.version_info <= (2, 6)

_current_app_for_proc = None

task_accepted = state.task_accepted
task_ready = state.task_ready
revoked_tasks = state.revoked


def execute_and_trace(name, uuid, args, kwargs, request=None, **opts):
    """This is a pickleable method used as a target when applying to pools.

    It's the same as::

        >>> trace_task(name, *args, **kwargs)[0]

    """
    global _current_app_for_proc
    if _current_app_for_proc is None:
        _current_app_for_proc = current_app._get_current_object()
    task = _current_app_for_proc.tasks[name]
    try:
        hostname = opts.get("hostname")
        setps("celeryd", name, hostname, rate_limit=True)
        try:
            if task.__tracer__ is None:
                task.__tracer__ = build_tracer(name, task, **opts)
            return task.__tracer__(uuid, args, kwargs, request)[0]
        finally:
            setps("celeryd", "-idle-", hostname, rate_limit=True)
    except Exception, exc:
        return report_internal_error(task, exc)


class Request(object):
    """A request for task execution."""
    eta = None
    started = False
    acknowledged = _already_revoked = False
    worker_pid = _terminate_on_ack = None
    _tzlocal = None
    expires = None
    delivery_info = {}
    flags = 0
    args = ()

    def __init__(self, body, on_ack=noop,
            hostname=None, eventer=None, app=None,
            connection_errors=None, request_dict=None,
            delivery_info=None, task=None, Context=Context, **opts):
        self.app = app
        self.name = body["task"]
        self.id = body["id"]
        self.args = body["args"]
        try:
            self.kwargs = body["kwargs"]
            if NEEDS_KWDICT:
                self.kwargs = kwdict(self.kwargs)
        except KeyError:
            self.kwargs = {}
        try:
            self.flags = body["flags"]
        except KeyError:
            pass
        self.on_ack = on_ack
        self.hostname = hostname
        self.eventer = eventer
        self.connection_errors = connection_errors or ()
        self.task = task or self.app._tasks[self.name]
        utc = body.get("utc")
        if "eta" in body:
            eta = body["eta"]
            if eta:
                tz = tz_utc if utc else self.tzlocal
                self.eta = tz_to_local(maybe_iso8601(eta), self.tzlocal, tz)
        if "expires" in body:
            expires = body["expires"]
            if expires:
                tz = tz_utc if utc else self.tzlocal
                self.expires = tz_to_local(maybe_iso8601(expires),
                                       self.tzlocal, tz)
        if delivery_info:
            self.delivery_info = {
                "exchange": delivery_info.get("exchange"),
                "routing_key": delivery_info.get("routing_key"),
            }

        self.request_dict = AttributeDict(
                {"called_directly": False,
                 "callbacks": [],
                 "errbacks": [],
                 "chord": None}, **body)

    @classmethod
    def from_message(cls, message, body, **kwargs):
        # should be deprecated
        return Request(body,
            delivery_info=getattr(message, "delivery_info", None), **kwargs)

    def extend_with_default_kwargs(self, loglevel, logfile):
        """Extend the tasks keyword arguments with standard task arguments.

        Currently these are `logfile`, `loglevel`, `task_id`,
        `task_name`, `task_retries`, and `delivery_info`.

        See :meth:`celery.task.base.Task.run` for more information.

        Magic keyword arguments are deprecated and will be removed
        in version 3.0.

        """
        kwargs = dict(self.kwargs)
        default_kwargs = {"logfile": logfile,
                          "loglevel": loglevel,
                          "task_id": self.id,
                          "task_name": self.name,
                          "task_retries": self.request_dict.get("retries", 0),
                          "task_is_eager": False,
                          "delivery_info": self.delivery_info}
        fun = self.task.run
        supported_keys = fun_takes_kwargs(fun, default_kwargs)
        extend_with = dict((key, val) for key, val in default_kwargs.items()
                                if key in supported_keys)
        kwargs.update(extend_with)
        return kwargs

    def execute_using_pool(self, pool, **kwargs):
        """Like :meth:`execute`, but using a worker pool.

        :param pool: A :class:`celery.concurrency.base.TaskPool` instance.
        """
        task = self.task
        if self.flags & 0x004:
            return pool.apply_async(execute_bare,
                    args=(self.task, self.id, self.args, self.kwargs),
                    accept_callback=self.on_accepted,
                    timeout_callback=self.on_timeout,
                    callback=self.on_success,
                    error_callback=self.on_failure,
                    soft_timeout=task.soft_time_limit,
                    timeout=task.time_limit)
        if (revoked_tasks or self.expires) and self.revoked():
            return

        hostname = self.hostname
        kwargs = self.kwargs
        if task.accept_magic_kwargs:
            kwargs = self.extend_with_default_kwargs(loglevel, logfile)
        request = self.request_dict
        request.update({"hostname": hostname, "is_eager": False,
                        "delivery_info": self.delivery_info})
        result = pool.apply_async(trace_task_ret,
                                  (task, self.id, self.args, kwargs, request),
                                  accept_callback=self.on_accepted,
                                  timeout_callback=self.on_timeout,
                                  callback=self.on_success,
                                  error_callback=self.on_failure,
                                  soft_timeout=task.soft_time_limit,
                                  timeout=task.time_limit)
        return result

    def execute(self, loglevel=None, logfile=None):
        """Execute the task in a :func:`~celery.task.trace.trace_task`.

        :keyword loglevel: The loglevel used by the task.
        :keyword logfile: The logfile used by the task.

        """
        if (revoked_tasks or self.expires) and self.revoked():
            return

        # acknowledge task as being processed.
        if not self.task.acks_late:
            self.acknowledge()

        kwargs = self.kwargs
        if self.task.accept_magic_kwargs:
            kwargs = self.extend_with_default_kwargs(loglevel, logfile)
        request = self.request_dict
        request.update({"loglevel": loglevel, "logfile": logfile,
                        "hostname": self.hostname, "is_eager": False,
                        "delivery_info": self.delivery_info})
        retval, _ = trace_task(self.task, self.id, self.args, kwargs,
                               **{"hostname": self.hostname,
                                  "loader": self.app.loader,
                                  "request": request})
        self.acknowledge()
        return retval

    def maybe_expire(self):
        """If expired, mark the task as revoked."""
        if self.expires and datetime.now(self.tzlocal) > self.expires:
            revoked_tasks.add(self.id)
            if self.store_errors:
                self.task.backend.mark_as_revoked(self.id)

    def terminate(self, pool, signal=None):
        if self.started:
            return pool.terminate_job(self.worker_pid, signal)
        else:
            self._terminate_on_ack = (True, pool, signal)

    def revoked(self):
        """If revoked, skip task and mark state."""
        if self._already_revoked:
            return True
        if self.expires:
            self.maybe_expire()
        if self.id in revoked_tasks:
            warn("Skipping revoked task: %s[%s]", self.name, self.id)
            if self.eventer and self.eventer.enabled:
                self.eventer.send("task-revoked", uuid=self.id)
            self.acknowledge()
            self._already_revoked = True
            return True
        return False

    def on_accepted(self, pid, *args):
        """Handler called when task is accepted by worker pool."""
        self.started = True
        self.worker_pid = pid
        task_accepted(self)
        if not self.task.acks_late:
            self.acknowledge()
        if self.eventer and self.eventer.enabled:
            self.eventer.send("task-started", uuid=self.id, pid=pid)
        if _does_debug:
            debug("Task accepted: %s[%s] pid:%r", self.name, self.id, pid)
        if self._terminate_on_ack is not None:
            _, pool, signal = self._terminate_on_ack
            self.terminate(pool, signal)

    def on_timeout(self, soft, timeout):
        """Handler called if the task times out."""
        task_ready(self)
        if soft:
            warn("Soft time limit (%ss) exceeded for %s[%s]",
                 timeout, self.name, self.id)
            exc = exceptions.SoftTimeLimitExceeded(timeout)
        else:
            error("Hard time limit (%ss) exceeded for %s[%s]",
                  timeout, self.name, self.id)
            exc = exceptions.TimeLimitExceeded(timeout)

        if self.store_errors:
            self.task.backend.mark_as_failure(self.id, exc)

    def on_success(self, ret_value):
        """Handler called if the task was successfully processed."""
        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, (
                    SystemExit, KeyboardInterrupt)):
                raise ret_value.exception
            return self.on_failure(ret_value)
        task_ready(self)
        if self.task.acks_late:
            self.acknowledge()

        if self.eventer and self.eventer.enabled:
            now = time.time()
            runtime = 0 #self.time_start and (time.time() - self.time_start) or 0
            self.eventer.send("task-succeeded", uuid=self.id,
                              result=safe_repr(ret_value), runtime=runtime)

    def on_retry(self, exc_info):
        """Handler called if the task should be retried."""
        if self.eventer and self.eventer.enabled:
            self.eventer.send("task-retried", uuid=self.id,
                              exception=safe_repr(exc_info.exception.exc),
                              traceback=safe_str(exc_info.traceback))

        if _does_info:
            info(self.retry_msg.strip(), {
                "id": self.id, "name": self.name,
                "exc": safe_repr(exc_info.exception.exc)}, exc_info=exc_info)

    def on_failure(self, exc_info):
        """Handler called if the task raised an exception."""
        task_ready(self)

        if not exc_info.internal:

            if isinstance(exc_info.exception, exceptions.RetryTaskError):
                return self.on_retry(exc_info)

            # This is a special case as the process would not have had
            # time to write the result.
            if isinstance(exc_info.exception, exceptions.WorkerLostError) and \
                    self.store_errors:
                self.task.backend.mark_as_failure(self.id, exc_info.exception)
            # (acks_late) acknowledge after result stored.
            if self.task.acks_late:
                self.acknowledge()

        self._log_error(exc_info)

    def _log_error(self, einfo):
        exception, traceback, exc_info, internal, sargs, skwargs = (
            safe_repr(einfo.exception),
            safe_str(einfo.traceback),
            einfo.exc_info,
            einfo.internal,
            safe_repr(self.args),
            safe_repr(self.kwargs),
        )
        format = error_msg
        description = "raised exception"
        severity = logging.ERROR
        if self.eventer and self.eventer.enabled:
            self.eventer.send("task-failed", uuid=self.id,
                              exception=exception,
                              traceback=traceback)

        if internal:
            format = internal_error_msg
            description = "INTERNAL ERROR"
            severity = logging.CRITICAL

        context = {
            "hostname": self.hostname,
            "id": self.id,
            "name": self.name,
            "exc": exception,
            "traceback": traceback,
            "args": sargs,
            "kwargs": skwargs,
            "description": description,
        }

        logger.log(severity, format.strip(), context,
                   exc_info=exc_info,
                   extra={"data": {"id": self.id,
                                   "name": self.name,
                                   "args": sargs,
                                   "kwargs": skwargs,
                                   "hostname": self.hostname,
                                   "internal": internal}})

        self.task.send_error_email(context, einfo.exception)

    def acknowledge(self):
        """Acknowledge task."""
        if not self.acknowledged:
            self.on_ack(logger, self.connection_errors)
            self.acknowledged = True

    def info(self, safe=False):
        return {"id": self.id,
                "name": self.name,
                "args": self.args if safe else safe_repr(self.args),
                "kwargs": self.kwargs if safe else safe_repr(self.kwargs),
                "hostname": self.hostname,
                "acknowledged": self.acknowledged,
                "delivery_info": self.delivery_info,
                "worker_pid": self.worker_pid}

    def shortinfo(self):
        return "%s[%s]%s%s" % (
                    self.name, self.id,
                    " eta:[%s]" % (self.eta, ) if self.eta else "",
                    " expires:[%s]" % (self.expires, ) if self.expires else "")
    __str__ = shortinfo

    def __repr__(self):
        return '<%s %s: %s>' % (type(self).__name__, self.id,
            reprcall(self.name, self.args, self.kwargs))

    @property
    def tzlocal(self):
        if self._tzlocal is None:
            self._tzlocal = tz_or_local(self.app.conf.CELERY_TIMEZONE)
        return self._tzlocal

    @property
    def store_errors(self):
        return (not self.task.ignore_result
                or self.task.store_errors_even_if_ignored)

    def _compat_get_task_id(self):
        return self.id

    def _compat_set_task_id(self, value):
        self.id = value

    def _compat_get_task_name(self):
        return self.name

    def _compat_set_task_name(self, value):
        self.name = value

    task_id = property(_compat_get_task_id, _compat_set_task_id)
    task_name = property(_compat_get_task_name, _compat_set_task_name)


class TaskRequest(Request):

    def __init__(self, name, id, args=(), kwargs={},
            eta=None, expires=None, **options):
        """Compatibility class."""

        super(TaskRequest, self).__init__({
            "task": name, "id": id, "args": args,
            "kwargs": kwargs, "eta": eta,
            "expires": expires}, **options)
