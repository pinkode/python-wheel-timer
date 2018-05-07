import time
import threading
import logging
import collections
import math

from Queue import Queue


class SystemClock(object):
    def microseconds(self):
        return int(time.time() * 1000000)


class WheelTimer(object):
    __STATE_INIT = 2
    __STATE_RUNNING = 3
    __STATE_STOPPED = 4

    def __init__(self,
                 tick_duration_millis=100,
                 ticks_in_wheel=2048,
                 clock=SystemClock()):
        self.__validate_parameters(tick_duration_millis, ticks_in_wheel, clock)
        self.__lock = threading.Lock()
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__clock = clock
        self.__lifecycle = self.__STATE_INIT
        self.__tick_duration = self.millis_to_micro(tick_duration_millis)
        self.__executor_q = Queue()
        self.__canceled_q = Queue()
        self.__remaining_after_stop = []
        self.__wheel = self.__create_wheel(ticks_in_wheel, self.__executor_q)
        self.__start_time = None
        self.__unassigned_timer_tasks = collections.deque()
        self.__worker = threading.Thread(name='wheel-timer-worker',
                                         target=self.__start)
        self.__executor = threading.Thread(name='wheel-timer-exec',
                                           target=self.__execute_tasks)

    def start(self):
        with self.__lock:
            if self.__lifecycle == self.__STATE_INIT:
                self.__lifecycle = self.__STATE_RUNNING
            elif self.__lifecycle == self.__STATE_RUNNING:
                return
            elif self.__lifecycle == self.__STATE_STOPPED:
                raise ValueError('timer was already stopped')
            else:
                raise ValueError('unknown state: %s' % str(self.__lifecycle))

        self.__executor.daemon = True
        self.__executor.start()
        self.__worker.daemon = True
        self.__worker.start()

    def schedule(self, delay_millis, task):
        if delay_millis is None:
            raise ValueError('delay millis: None not permitted')
        if not isinstance(delay_millis, int):
            raise ValueError('delay millis: must be int')
        if delay_millis <= 0 or delay_millis > 1000 * 60 * 60 * 24:
            raise ValueError('delay must be in range (1 ms, 24 hours)')
        if not callable(task):
            raise ValueError('task: must be callable')
        if task is None:
            raise ValueError('task: None not permitted')
        if self.__lifecycle == self.__STATE_STOPPED:
            raise ValueError('timer has already stopped')

        self.start()

        while self.__start_time is None:
            # in case called before worker started
            pass

        task_delay = self.millis_to_micro(delay_millis)
        runtime = self.__clock.microseconds() - self.__start_time
        expiry = runtime + task_delay
        timertask = self.TimerTask(expiry, task, self.__canceled_q)

        with self.__lock:
            if self.__lifecycle == self.__STATE_STOPPED:
                self.__remaining_after_stop.append(timertask)
            else:
                self.__unassigned_timer_tasks.append(timertask)

        return timertask

    def __execute_tasks(self):
        while self.__start_time is None:
            # wait for worker to start
            pass

        q = self.__executor_q
        timeout = 0.2
        logger = self.__logger
        max_lag = 100000
        while self.__lifecycle != self.__STATE_STOPPED:
            qsize = q.qsize()
            if qsize > max_lag:
                logger.error('timer executor cant keep up, dropping task')
                while q.qsize() > max_lag:
                    q.get(block=False)

            if qsize == 0:
                time.sleep(timeout)
            else:
                callable_ = q.get(block=True, timeout=timeout)
                try:
                    callable_()
                except Exception:
                    name = callable_.__name__
                    logger.exception('uncaught exception for %s', name)

    def __start(self):
        self.__ensure_worker_context()
        wheel = self.__wheel
        wheel_len = len(wheel)
        start_time = self.__start_time = self.__clock.microseconds()
        ticks = 0

        while self.__lifecycle != self.__STATE_STOPPED:
            self.__ensure_start_time_not_changed(start_time)
            runtime = self.__sleep_until_next_tick(ticks)
            self.__assign_timer_tasks(ticks)
            self.__remove_canceled_tasks()
            bucket_index = int(ticks % wheel_len)
            bucket = wheel[bucket_index]
            bucket._fire_expiry(runtime)

            ticks += 1

        for bucket in self.__wheel:
            bucket._clear_all(self.__remaining_after_stop)

        with self.__lock:
            for task in self.__unassigned_timer_tasks:
                self.__remaining_after_stop.append(task)

        self.__remove_canceled_tasks()

    def stop(self):
        self.__lifecycle = self.__STATE_STOPPED

        while self.__worker.isAlive():
            try:
                self.__worker.join(0.1)
            except:
                pass

        return self.__remaining_after_stop

    def __sleep_until_next_tick(self, tick):
        next_tick_time = self.__tick_duration * (tick + 1)
        while True:
            runtime = self.__clock.microseconds() - self.__start_time
            # if diff is very small, lets sleep at least one millisecond
            time_to_sleep_millis = (next_tick_time - runtime + 999) / 1000

            if time_to_sleep_millis <= 0:
                return runtime

            secs_to_sleep = float(time_to_sleep_millis) / 1000
            time.sleep(secs_to_sleep)

    def __assign_timer_tasks(self, ticks):
        wheel_len = len(self.__wheel)
        tasks_to_assign = []
        unassigned = self.__unassigned_timer_tasks
        with self.__lock:
            # avoid starvation of other worker activities
            max_assignments = 100000
            while len(unassigned) > 0:
                tasks_to_assign.append(unassigned.popleft())
                if len(tasks_to_assign) >= max_assignments:
                    break

        tick_dur = self.__tick_duration
        wheel = self.__wheel
        for task in tasks_to_assign:
            if task._canceled:
                # could have been canceledby the time we assign
                continue

            expiry = task._expiry
            ticks_to_expiry = int(math.ceil(expiry / float(tick_dur)))
            # if tick advanced before we had chance to assign this
            ticks_to_expiry = max(ticks_to_expiry, ticks)
            # how many passes will have to be performed until expiry
            remaining_passes = (ticks_to_expiry - ticks) / wheel_len
            bucket_index = int(ticks_to_expiry % wheel_len)
            bucket = wheel[bucket_index]
            task._bucket = bucket
            task._remaining_passes = remaining_passes
            bucket._add_task(task)

    def __remove_canceled_tasks(self):
        cancels = 10000
        while self.__canceled_q.qsize() > 0 and cancels > 0:
            task = self.__canceled_q.get(block=False)
            task._remove()
            cancels -= 1

    class TimerTask(object):

        def __init__(self, expiry, callable_, cancel_q):
            self._expiry = expiry
            self._bucket = None
            self._next = None
            self._prev = None
            self._canceled = False
            self._expired = False
            self._sys_aborted = False
            self._lock = threading.Lock()
            self._remaining_passes = 0
            self._callable = callable_
            self._cancel_q = cancel_q

        def cancel(self):
            self._canceled = True
            self._cancel_q.put_nowait(self)

        def _remove(self):
            if self._bucket is not None:
                self._bucket._remove_task(self)

        def __repr__(self):
            s = '['
            s += self.__class__.__name__
            s += ' expiry=%d' % self._expiry
            s += ' canceled=%s' % str(self._canceled)
            s += ' expired=%s' % str(self._expired)
            s += ' callable=%s' % str(self._callable)
            s += ']'

            return s

        def __str__(self):
            return repr(self)

        def __unicode__(self):
            return str(self).decode('utf8')

    class TickBucket(object):

        def __init__(self, execution_q):
            self._head = None
            self._tail = None
            self._execution_q = execution_q

        def _fire_expiry(self, runtime):
            task = self._head
            while task is not None:
                nxt = task._next
                if task._remaining_passes <= 0:
                    nxt = self._remove_task(task)
                    if task._expiry <= runtime:
                        with task._lock:
                            if not task._sys_aborted:
                                task._expired = True
                                self._execution_q.put_nowait(task._callable)
                    else:
                        raise ValueError('placed in wrong bucket or passes')
                elif task._canceled:
                    nxt = self._remove_task(task)
                else:
                    task._remaining_passes -= 1

                task = nxt

        def _remove_task(self, task):
            nxt = task._next
            if task._prev is not None:
                task._prev._next = nxt
            if task._next is not None:
                task._next._prev = task._prev

            if task is self._head:
                if task is self._tail:
                    self._head = None
                    self._tail = None
                else:
                    self.head = nxt
            elif task is self._tail:
                self._tail = task._prev

            self.__clear_task_references(task)
            return nxt

        def _add_task(self, task):
            if task is None:
                raise ValueError('timeout task is None')

            if self._head is None:
                self._head = task
                self._tail = task
            else:
                self._tail._next = task
                task._prev = self._tail
                self._tail = task

        def _clear_all(self, collector):
            while True:
                task = self.__remove_head()
                if task is None:
                    return
                with task._lock:
                    if not task._canceled and not task._expired:
                        task._sys_aborted = True
                        collector.append(task)

        def __remove_head(self):
            head_ = self._head
            if head_ is None:
                return None

            nxt = head_._next
            if nxt is None:
                self._head = None
                self._tail = None
            else:
                self._head = nxt
                nxt._prev = None

            self.__clear_task_references(head_)
            return head_

        def __clear_task_references(self, task):
            task._next = None
            task._prev = None
            task._bucket = None
            task._cancel_q = None

    def __ensure_worker_context(self):
        if threading.currentThread() != self.__worker:
            raise ValueError('only the timer worker thread is permitted')

    def __ensure_start_time_not_changed(self, cached_start_time):
        if cached_start_time != self.__start_time:
            raise ValueError('start time changed mid run')

    @classmethod
    def __create_wheel(cls, size, execution_q):
        return [cls.TickBucket(execution_q) for _ in xrange(size)]

    @staticmethod
    def millis_to_micro(n):
        return int(n * 1000)

    @staticmethod
    def __validate_parameters(tick_duration, ticks_in_wheel, clock):
        if tick_duration is None:
            raise ValueError('tick duration: None not permitted')

        if ticks_in_wheel is None:
            raise ValueError('ticks in wheel: None not permitted')

        if clock is None:
            raise ValueError('clock: None not permitted')

        if not isinstance(tick_duration, int):
            t = type(tick_duration)
            raise ValueError('tick duration: must be int: %s' % t)

        if tick_duration <= 0 or tick_duration > 1000 * 60 * 60:
            raise ValueError('tick duration: must be in range (1, 3600000)')

        if ticks_in_wheel <= 0 or ticks_in_wheel > 2 ** 30:
            raise ValueError('ticks in wheel: must be in range (1, 2^30)')

        if not hasattr(clock, 'microseconds'):
            raise ValueError('clock: must support microseconds')
