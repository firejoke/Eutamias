# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/7/26 下午4:46
import fcntl
import hashlib
import struct
from threading import Condition, RLock
from typing import Union

import msgpack


class RWLock:
    _read_lock = Condition(RLock())
    _write_lock = Condition(RLock())
    readers = 0
    waiting_writers = 0
    waiting_readers = 0
    writing = False

    def acquire_read(self):
        with self._read_lock:
            self.waiting_readers += 1
            self._read_lock.wait_for(
                lambda : not self.waiting_writers and not self.writing
            )
            self.waiting_readers -= 1
            self.readers += 1

    def release_read(self):
        with self._read_lock:
            self.readers -= 1
        if not self.readers and self.waiting_writers:
            with self._write_lock:
                self._write_lock.notify()

    def acquire_write(self):
        self._write_lock.acquire()
        self.waiting_writers += 1
        self._write_lock.wait_for(lambda : not self.readers)
        self.waiting_writers -= 1
        self.writing = True

    def release_write(self):
        self.writing = False
        notify_reader = True
        if self.waiting_writers:
            notify_reader = False
        self._write_lock.notify()
        self._write_lock.release()
        if notify_reader and self.waiting_readers:
            with self._read_lock:
                self._read_lock.notify_all()

    class ReadLock:
        def __init__(self, rw_lock: "RWLock"):
            self.rw_lock = rw_lock

        def __enter__(self):
            self.rw_lock.acquire_read()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.rw_lock.release_read()

    def read_lock(self):
        return self.ReadLock(self)

    class WriteLock:
        def __init__(self, rw_lock: "RWLock"):
            self.rw_lock = rw_lock

        def __enter__(self):
            self.rw_lock.acquire_write()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.rw_lock.release_write()

    def write_lock(self):
        return self.WriteLock(self)


class FNLock:

    class _Lock:
        def __init__(self, fd: int, cmd, length: int=0, start: int=0, whence=0):
            self.fd = fd
            self.cmd = cmd
            self.length = length
            self.start = start
            self.whence = whence

        def __enter__(self):
            fcntl.lockf(self.fd, self.cmd, self.length, self.start, self.whence)
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            fcntl.lockf(self.fd, fcntl.LOCK_UN)

    @staticmethod
    def query_lock(fd: int, start=0, length=0, whence=0):
        arg = struct.pack('hhllh', fcntl.F_WRLCK, whence, start, length, 0)
        return fcntl.fcntl(fd, fcntl.F_OFD_GETLK, arg)

    def read_lock(self, fd: int, start=0, length=0, whence=0):
        return self._Lock(fd, fcntl.LOCK_SH, length, start, whence)

    def write_lock(self, fd: int, start=0, length=0, whence=0):
        return self._Lock(fd, fcntl.LOCK_EX, length, start, whence)


class KeyDigest:
    SIZE = 8

    def to_digest(self, key: Union[str, bytes]) -> bytes:
        if not isinstance(key, bytes):
            key = key.encode("utf-8")
        h = hashlib.blake2b(key, digest_size=self.SIZE)
        return h.digest()

    def to_int(self, key: Union[str, bytes]) -> int:
        if not isinstance(key, bytes):
            key = key.encode("utf-8")
        h = hashlib.blake2b(key, digest_size=self.SIZE)
        return int.from_bytes(h.digest(), "big")


key_digest = KeyDigest()


def b(a, x):
    lo, hi = 0, len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if a[mid] < x: lo = mid + 1
        elif a[mid] == x: return mid
        else: hi = mid
    return lo


def b_1(a, x):
    lo, hi = 0, len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if a[mid] < x: lo = mid + 1
        elif a[mid] == x: return mid
        else: hi = mid
    return hi - 1 if hi else hi


def common_prefix(s: Union[str, bytes], s1: Union[str, bytes]):
    if type(s) != type(s1) or not isinstance(s, (str, bytes)):
        raise TypeError("common_prefix only support str or bytes")
    p = ""
    if isinstance(s, bytes):
        p = b""
    while s and s1:
        e, s = s[0], s[1:]
        e1, s1 = s1[0], s1[1:]
        if e != e1:
            break
        p += e
    return p


def chestnut_dumps(chestnut) -> bytes:
    return msgpack.packb(chestnut)


def chestnut_loads(bdata) -> dict:
    return msgpack.unpackb(bdata)
