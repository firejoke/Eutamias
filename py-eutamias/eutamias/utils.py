# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/7/26 下午4:46
import hashlib

import msgpack


def generate_digest(b: bytes) -> bytes:
    h = hashlib.sha1(b)
    return h.digest()


def generate_int_digest(b: bytes) -> int:
    h = hashlib.sha1(b)
    return int.from_bytes(h.digest(), "big")


def chestnut_dumps(chestnut) -> bytes:
    return msgpack.packb(chestnut)


def chestnut_loads(bdata) -> dict:
    return msgpack.unpackb(bdata)
