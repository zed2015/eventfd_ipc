#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import time
import os
import logging
import threading
import multiprocessing
import gevent

from linuxfd import eventfd
from multiprocessing import context
from multiprocessing import connection


class ProcessSemaphore(object):
    """ semaphore for ipc which is devlop by linux eventfd
    compatial with gevent, select, poll, epoll

    Args:
        init_value(int): init value for semaphore
    """
    def __init__(self, init_value=1):
        self.init_value = init_value
        self.eventfd = eventfd(initval=init_value, semaphore=True, nonBlocking=True)
    
    def acquire(self, block=True, timeout=None):
        """acquire semaphore

        Args:
            block (bool, optional): [description]. Defaults to True.
            timeout ([type], optional): [description]. Defaults to None.

        Returns:
            [type]: [description]
        """
        # initial rc equal False
        rc = False
        if not block:
            try:
                ret = self.eventfd.read()
                # read success
                rc = True
            except BlockingIOError:
                # read fail
                pass
        
        else:
            endtime = None
            while 1:
                if timeout is not None:
                    if endtime is None:
                        endtime  = time.time() + timeout
                    else:
                        timeout = endtime - time.time()
                        if timeout <= 0:
                            break
                # use mulitiprocessing.connection.wait which used selectors module
                event_list = connection.wait([self.eventfd], timeout)
                if event_list:
                    try:
                        ret = self.eventfd.read()
                        # get sem, break return
                        rc = True
                        break
                    except BlockingIOError:
                        # fail get sem
                        continue
        return rc

    def release(self):
        """release"""
        self.eventfd.write()

    def __enter__(self):
        self.acquire()
    
    def __exit__(self, *args):
        self.release()


class ProcessLock(ProcessSemaphore):
    """process lock by use linux eventfd semaphore
    support threading , multiprocessing
    compatilable with gevent coroutine
    """
    def __init__(self):
        super(ProcessLock, self).__init__(init_value=1)


class BoundedEventFDSemaphore(ProcessSemaphore):
    """BoundedEventFD Semaphore
    compatialable with gevent completely
    with attrs:
        _semlock which can get qsize from it
        
    Args:
    """
    def __init__(self, value):
        super(BoundedEventFDSemaphore, self).__init__(init_value=value)
        self.count_sem = multiprocessing.Semaphore(value)
        self._semlock = self.count_sem._semlock
    
    def get_value(self):
        """get value"""
        return self._semlock._get_value()
        
    def acquire(self, block=True, timeout=None):
        """acquire sem

        Args:
            block (bool, optional): [description]. Defaults to True.
            timeout ([type], optional): [description]. Defaults to None.

        Returns:
            bool: 
        """
        rc = super(BoundedEventFDSemaphore, self).acquire(block=block, timeout=timeout)
        if rc:
            # sem -= 1, will not block forever after super.acquire
            assert self.count_sem.acquire(block=False), 'evenfd BoundedEventFD acquire error!!!'
        return rc
    
    def release(self):
        """release semaphore"""
        # sem += 1
        self.count_sem.release()
        return super(BoundedEventFDSemaphore, self).release()


class GeventConnection(connection.Connection):
    """ unblock Connection"""
    def __init__(self, *args, **kwargs):
        super(GeventConnection, self).__init__(*args, **kwargs)
        gevent.os.make_nonblocking(self._handle)
        
    _write = gevent.os.nb_write
    _read = gevent.os.nb_read

    def _send(self, buf, write=_write): 
        """send"""
        return super(GeventConnection, self)._send(buf, write)

    def _recv(self, size, read=_read):
        """recv"""
        return super(GeventConnection, self)._recv(size, read)

raw_Lock = context.BaseContext.Lock
raw_BoundedSemaphore = context.BaseContext.BoundedSemaphore
raw_Connection = connection.Connection


def patch_multiprocessing():
    """patch multiprocessing.Lock, BoundedSemaphore, Connection, Queue
    unblockable, can compatialable with gevent
    """
    logging.warning('eventfd_ipc patch multirprocessing')
    context.BaseContext.Lock = ProcessLock
    context.BaseContext.BoundedSemaphore = BoundedEventFDSemaphore
    connection.Connection = GeventConnection


def unpatch_multiprocessing():
    """
    description: unpatch ,recover raw
    param {*}
    return {*}
    """    
    logging.warning('eventfd_ipc unpatch multirprocessing')
    context.BaseContext.Lock = raw_Lock
    context.BaseContext.BoundedSemaphore = raw_BoundedSemaphore
    connection.Connection = raw_Connection
    







