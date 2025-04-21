# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/7/26 下午4:46
import fcntl
import hashlib
from threading import Condition
from typing import Union

import msgpack


class RWLock:
    _read_lock = Condition()
    _write_lock = Condition()
    readers = 0
    waiting_writers = 0
    waiting_readers = 0
    writing = False

    def acquire_read(self):
        self._read_lock.acquire()
        self.waiting_readers += 1
        self._read_lock.wait_for(
            lambda : not self.waiting_writers or not self.writing
        )
        self.waiting_readers -= 1
        self.readers += 1

    def release_read(self):
        self.readers -= 1
        if not self.readers:
            with self._write_lock:
                self._write_lock.notify()
        self._read_lock.release()

    def acquire_write(self):
        self._write_lock.acquire()
        self.waiting_writers += 1
        self._write_lock.wait_for(
            lambda : not self.readers or not self.writing
        )
        self.waiting_writers -= 1
        self.writing = True

    def release_write(self):
        self.writing = False
        if self.waiting_writers:
            self._write_lock.notify()
        else:
            with self._read_lock:
                self._read_lock.notify_all()
        self._write_lock.release()

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
        def __init__(self, fd, cmd, length: int=0, start: int=0, whence=0):
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

    def read_lock(self, fd, start=0, length=0, whence=0):
        return self._Lock(fd, fcntl.LOCK_SH, length, start, whence)

    def write_lock(self, fd, start=0, length=0, whence=0):
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


def chestnut_dumps(chestnut) -> bytes:
    return msgpack.packb(chestnut)


def chestnut_loads(bdata) -> dict:
    return msgpack.unpackb(bdata)
