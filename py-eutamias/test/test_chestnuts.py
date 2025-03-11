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


base_path = Path(__file__).parent

sys.path.append(base_path.parent.as_posix())

from eutamias.global_settings import Settings
from eutamias.burrow import Chestnuts
import test_settings


Settings.configure("USER_SETTINGS", test_settings)
logger = getLogger("eutamias")

Settings.setup()
_n = time.time()
c = Chestnuts()
print(f"load chestnuts: {time.time() - _n}")


def batch():
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
        print(f"get avg: {get_avg}")
    else:
        i = 100000
        set_avg = 0
        _kn = 0
        with open(keys, "a") as f:
            while i:
                i -= 1
                alphabet = string.ascii_letters + string.digits
                key = "".join(secrets.choice(alphabet) for i in range(8))
                _1 = "".join(secrets.choice(alphabet) for i in range(10))
                _2 = "".join(secrets.choice(alphabet) for i in range(20))
                _3 = "".join(secrets.choice(alphabet) for i in range(30))
                _4 = "".join(secrets.choice(alphabet) for i in range(40))
                _5 = "".join(secrets.choice(alphabet) for i in range(50))
                _6 = "".join(secrets.choice(alphabet) for i in range(60))
                _7 = "".join(secrets.choice(alphabet) for i in range(70))
                _8 = "".join(secrets.choice(alphabet) for i in range(80))
                _9 = "".join(secrets.choice(alphabet) for i in range(90))
                _10 = "".join(secrets.choice(alphabet) for i in range(100))
                value = {
                    _1: _10,
                    _2: {
                        _9: [_3, _8]
                    },
                    _4: {
                        _7: {
                            _6: _5
                        }
                    }
                }
                set_t = time.time()
                c.create(key, value)
                set_t = time.time() - set_t
                set_avg, _kn = (set_avg * _kn + set_t) / (_kn + 1), _kn + 1
                f.write(key + "\n")
        print(f"set avg: {set_avg}")


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
    batch_parser = sub_parser.add_parser("batch")
    batch_parser.set_defaults(func=batch)
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
