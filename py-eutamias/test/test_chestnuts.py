# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2025/2/13 09:37
import argparse
import json
import secrets
import string
import sys
import time
from logging import getLogger
from pathlib import Path
from queue import Queue
from threading import Thread


base_path = Path(__file__).parent

sys.path.append(base_path.parent.as_posix())

from eutamias import Settings
from eutamias.burrow import Chestnuts
import test_settings


Settings.configure("USER_SETTINGS", test_settings)
logger = getLogger("eutamias")

Settings.setup()
_n = time.time()
c = Chestnuts()
print(f"load chestnuts: {time.time() - _n}")


class ExceptionHandlingThread(Thread):
    def __init__(
        self, group=None, target=None, name=None, args=(), kwargs={}, *,
        daemon=None
    ):
        super().__init__(
            group=group, target=target, name=name, args=args, kwargs=kwargs,
            daemon=daemon
        )
        self.exception = None

    def run(self):
        try:
            super().run()
        except Exception as e:
            self.exception = e
            raise e


def _get(q, name):
    print(f"get({name}) start")
    i = 0
    while 1:
        k = q.get()
        if k == "end":
            print(f"get({name}) end, processed {i}")
            q.put("end")
            break
        i += 1
        try:
            v = c[k]
        except Exception as e:
            print(f"get({k}) error: {e}")
            raise e
        q.task_done()


def _create(q, name):
    print(f"create({name}) start")
    i = 0
    while 1:
        k, v = q.get()
        if k == "end":
            print(f"create({name}) end, processed {i}")
            q.put(("end", ""))
            break
        i += 1
        c.create(k, v)
        q.task_done()


def _delete(q, name, iq=None):
    print(f"remove({name}) start")
    i = 0
    while 1:
        k = q.get()
        if k == "end":
            print(f"remove({name}) end, processed {i}")
            q.put("end")
            break
        i += 1
        v = c.delete(k, True)
        q.task_done()
        if iq is not None:
            iq.put((k, v))


def test_concurrent():
    keys = Settings.CHESTNUTS.parent.joinpath("test_chestnuts_keys")
    q = Queue()
    iq = Queue()
    b = 1000
    t1 = ExceptionHandlingThread(target=_get, args=(q, "t1"))
    t2 = ExceptionHandlingThread(
        target=_delete, args=(q, "t2"), kwargs={"iq": iq}
    )
    t3 = ExceptionHandlingThread(target=_get, args=(q, "t3"))
    t4 = ExceptionHandlingThread(
        target=_delete, args=(q, "t4"), kwargs={"iq": iq}
    )
    t5 = ExceptionHandlingThread(target=_create, args=(iq, "t5"))
    t6 = ExceptionHandlingThread(target=_create, args=(iq, "t6"))
    for t in (t1, t2, t3, t4, t5, t6):
    # for t in (t1, t3):
        t.start()
    with open(keys) as f:
        for key in f:
            key = key.strip()
            q.put(key)
    q.put("end")
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    iq.put(("end", ""))
    t5.join()
    t6.join()
    for t in (t1, t2, t3, t4, t5, t6):
    # for t in (t1, t3):
        if t.exception:
            raise t.exception


def test(key_num: int):
    keys = Settings.CHESTNUTS.parent.joinpath("test_chestnuts_keys")
    if keys.exists():
        get_avg = 0
        _kn = 0

        with open(keys) as f:
            for key in f:
                key = key[:-1]
                get_t = time.time()
                _ = c[key]
                get_t = time.time() - get_t
                get_avg, _kn = (get_avg * _kn + get_t) / (_kn + 1), _kn + 1
        print(f"get avg({_kn}): {get_avg}")
    else:
        i = key_num
        set_avg = 0
        _kn = 0
        with open(keys, "a") as f:
            while i:
                i -= 1
                key = secrets.token_hex()
                value = {
                    secrets.token_hex(): secrets.token_hex(),
                    secrets.token_hex(): [secrets.token_hex() for _ in range(30)],
                    secrets.token_hex(): {secrets.token_hex(): secrets.token_hex() for _ in range(30)},
                }
                set_t = time.time()
                c.create(key, value)
                set_t = time.time() - set_t
                set_avg, _kn = (set_avg * _kn + set_t) / (_kn + 1), _kn + 1
                f.write(key + "\n")
        print(f"set avg({_kn}): {set_avg}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        # usage='alter network interfaces config',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Chestnuts",
        epilog="""
e.g.:
get key
alter key value
set key value
del key
            """
    )
    sub_parser = parser.add_subparsers(
        title="Chestnut-client"
    )

    create_parser = sub_parser.add_parser("set")
    create_parser.add_argument("key")
    create_parser.add_argument("value", type=json.loads, help="一个json字符串。")
    create_parser.set_defaults(func=c.create)
    update_parser = sub_parser.add_parser("alter")
    update_parser.add_argument("key")
    update_parser.add_argument("value", type=json.loads, help="一个json字符串。")
    update_parser.set_defaults(func=c.update)
    get_parser = sub_parser.add_parser("get")
    get_parser.add_argument("key")
    # get_parser.add_argument("--prefix", action="store_true")
    get_parser.set_defaults(func=lambda key: print(c[key]))
    delete_parser = sub_parser.add_parser("del")
    delete_parser.add_argument("key")
    delete_parser.set_defaults(func=c.delete)
    once_parser = sub_parser.add_parser("once")
    once_parser.add_argument("key_num", type=int)
    once_parser.set_defaults(func=test)
    concurrent_parser = sub_parser.add_parser("concurrent")
    concurrent_parser.set_defaults(func=test_concurrent)
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
    else:
        # try:
        func = getattr(args, "func")
        delattr(args, "func")
        func(**vars(args))
        # finally:
        #     c.close()
