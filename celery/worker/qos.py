import threading

#: Prefetch count can't exceed short.
PREFETCH_COUNT_MAX = 0xFFFF


class QoS(object):
    """Quality of Service for Channel.

    For thread-safe increment/decrement of a channels prefetch count value.

    :param consumer: A :class:`kombu.messaging.Consumer` instance.
    :param initial_value: Initial prefetch count value.
    :param logger: Logger used to log debug messages.

    """
    prev = None

    def __init__(self, consumer, initial_value, logger):
        self.consumer = consumer
        self.logger = logger
        self._mutex = threading.RLock()
        self.value = initial_value

    def increment(self, n=1):
        """Increment the current prefetch count value by n."""
        with self._mutex:
            if self.value:
                new_value = self.value + max(n, 0)
                self.value = self.set(new_value)
        return self.value

    def _sub(self, n=1):
        assert self.value - n > 1
        self.value -= n

    def decrement(self, n=1):
        """Decrement the current prefetch count value by n."""
        with self._mutex:
            if self.value:
                self._sub(n)
                self.set(self.value)
        return self.value

    def decrement_eventually(self, n=1):
        """Decrement the value, but do not update the qos.

        The MainThread will be responsible for calling :meth:`update`
        when necessary.

        """
        with self._mutex:
            if self.value:
                self._sub(n)

    def set(self, pcount):
        """Set channel prefetch_count setting."""
        if pcount != self.prev:
            new_value = pcount
            if pcount > PREFETCH_COUNT_MAX:
                self.logger.warning("QoS: Disabled: prefetch_count exceeds %r",
                                    PREFETCH_COUNT_MAX)
                new_value = 0
            self.logger.debug("basic.qos: prefetch_count->%s", new_value)
            self.consumer.qos(prefetch_count=new_value)
            self.prev = pcount
        return pcount

    def update(self):
        """Update prefetch count with current value."""
        with self._mutex:
            return self.set(self.value)
