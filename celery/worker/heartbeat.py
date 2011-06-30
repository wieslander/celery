from celery.worker.state import SOFTWARE_INFO


class Heart(object):
    """Timer sending heartbeats at regular intervals.

    :param timer: Timer instance.
    :param eventer: Event dispatcher used to send the event.
    :keyword interval: Time in seconds between heartbeats.
                       Default is 30 seconds.
    :keyword stats: Function providing current worker statistics.

    """
    def __init__(self, timer, eventer, interval=None, stats=None):
        self.timer = timer
        self.eventer = eventer
        self.interval = interval or 20
        self.stats = stats or (lambda: {})
        self.tref = None

    def _send(self, event):
        return self.eventer.send(event, stats=self.stats(), **SOFTWARE_INFO)

    def start(self):
        self._send("worker-online")
        self.tref = self.timer.apply_interval(self.interval * 1000.0,
                self._send, ("worker-heartbeat", ))

    def stop(self):
        if self.tref is not None:
            self.timer.cancel(self.tref)
            self.tref = None
        self._send("worker-offline")
