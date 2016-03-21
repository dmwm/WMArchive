#!/usr/bin/python
#pylint: disable=W0141
"""
File       : TaskManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive TaskManager

spawn(func, \*args) to spawn execution of given func(args)
is_alive(pid) return status of executing job
joinall() to join all tasks in a queue and exiting existing workers
join(jobs) to join all tasks without stopping workers
"""
from __future__ import print_function

# system modules
from threading import Thread, Event
from Queue import Queue

# DAS modules
from WMArchive.Utils.Utils import wmaHash

class UidSet(object):
    "UID holder keeps track of uid frequency"
    def __init__(self):
        self._set = {}

    def add(self, uid):
        "Add given uid or increment uid occurence in a set"
        if  not uid:
            return
        if  uid in self._set.keys():
            self._set[uid] += 1
        else:
            self._set[uid]  = 1

    def discard(self, uid):
        "Either discard or downgrade uid occurence in a set"
        if  uid in self._set:
            self._set[uid] -= 1
        if  uid in self._set and not self._set[uid]:
            del self._set[uid]

    def __contains__(self, uid):
        "Check if uid present in a set"
        if  uid in self._set:
            return True
        return False

    def get(self, uid):
        "Get value for given uid"
        return self._set.get(uid, 0)

class Worker(Thread):
    """Thread executing worker from a given tasks queue"""
    def __init__(self, name, taskq, pidq, uidq):
        Thread.__init__(self, name=name)
        self.exit   = 0
        self._tasks = taskq
        self._pids  = pidq
        self._uids  = uidq
        self.daemon = True
        self.start()

    def force_exit(self):
        """Force run loop to exit in a hard way"""
        self.exit   = 1

    def run(self):
        """Run thread loop."""
        while True:
            if  self.exit:
                return
            task = self._tasks.get()
            if  task == None:
                return
            evt, pid, func, args, kwargs = task
            try:
                func(*args, **kwargs)
                self._pids.discard(pid)
            except Exception as err:
                self._pids.discard(pid)
                print("ERROR, TaskManager::Worker", func, args, kwargs, str(err))
            evt.set()

class TaskManager(object):
    """
    Task manager class based on thread module which
    executes assigned tasks concurently. It uses a
    pool of thread workers, queue of tasks and pid
    set to monitor jobs execution.

    .. doctest::

        Use case:
        mgr  = TaskManager()
        jobs = []
        jobs.append(mgr.spaw(func, args))
        mgr.joinall(jobs)

    """
    def __init__(self, nworkers=4, name='TaskManager', debug=0):
        self.name   = name
        self.debug  = debug
        self._pids  = set()
        self._uids  = UidSet()
        self._tasks = Queue()
        self._workers = [Worker(name, self._tasks, self._pids, self._uids) \
                        for _ in xrange(0, nworkers)]

    def status(self):
        "Return status of task manager queue"
        info = {'qsize':self._tasks.qsize(), 'full':self._tasks.full(),
                'unfinished':self._tasks.unfinished_tasks,
                'nworkers':len(self._workers)}
        return {self.name:info}

    def nworkers(self):
        """Return number of workers associated with this manager"""
        return len(self._workers)

    def spawn(self, func, pid, spec, fields):
        """Spawn new process for given function"""
        evt = Event()
        if  not pid in self._pids:
            self._pids.add(pid)
            task  = (evt, pid, func, spec, fields)
            self._tasks.put(task)
        else:
            # the event was not added to task list, invoke set()
            # to pass it in wait() call, see joinall
            evt.set()
        return evt, pid

    def remove(self, pid):
        """Remove pid and associative process from the queue"""
        self._pids.discard(pid)

    def is_alive(self, pid):
        """Check worker queue if given pid of the process is still running"""
        return pid in self._pids

    def clear(self, tasks):
        """
        Clear all tasks in a queue. It allows current jobs to run, but will
        block all new requests till workers event flag is set again
        """
        map(lambda evt_pid: (evt_pid[0].clear(), evt_pid[1]), tasks)

    def joinall(self, tasks):
        """Join all tasks in a queue and quite"""
        map(lambda evt_pid1: (evt_pid1[0].wait(), evt_pid1[1]), tasks)

    def quit(self):
        """Put None task to all workers and let them quit"""
        map(lambda w: self._tasks.put(None), self._workers)
        map(lambda w: w.join(), self._workers)

    def force_exit(self):
        """Force all workers to exit"""
        map(lambda w: w.force_exit(), self._workers)

