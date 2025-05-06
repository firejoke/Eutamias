# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/2/15 11:01
import math
from pathlib import Path
import zmq.constants as z_const

from gaterpc.utils import TypeValidator, LazyAttribute, empty, ensure_mkdir
from gaterpc.global_settings import (
    debug_format, verbose_format, simple_format, Settings
)


class _Settings:

    BASE_PATH = LazyAttribute(
        Path("/tmp/eutamias/"),
        render=lambda instance, p: ensure_mkdir(p),
        process=lambda instance, p: TypeValidator(Path)(p)
    )
    WORKER_ADDR = f"ipc:///tmp/eutamias.wal"

    ZMQ_SOCK = {
        z_const.SocketOption.IPV6: 1,
        z_const.HWM: 7000,
    }

    MESSAGE_MAX: int = 10000

    ZAP_PLAIN_DEFAULT_USER = "Eutamias"
    ZAP_PLAIN_DEFAULT_PASSWORD = "ZHIZHI"

    BURROW_HANDLERS = LazyAttribute(
        raw=list(),
        process=lambda instance, p: TypeValidator(list)(p)
    )
    BURROW_SERVICE_NAME = "Burrow"
    BURROW_SERVICE_MAX_RETRIES = 3
    BURROW_PATH = LazyAttribute(
        render=lambda instance, p: ensure_mkdir(
            instance.BASE_PATH.joinpath("burrow/")
        ) if p is empty else ensure_mkdir(p),
        process=lambda instance, p: TypeValidator(Path)(p)
    )
    BPT_FACTOR =  math.ceil(100000 ** (1/3))

    CHESTNUTS: Path = None
    CHESTNUTS_MEDATA = (
        "Eutamias {VERSION} {latest_addr} BlockHead{BLOCK_HEAD_LIMIT}"
    )
    CHESTNUTS_DELETE_PREFIX = "-DEL-"
    CHESTNUTS_BACKUP_COUNT = LazyAttribute(
        raw=10,
        process=lambda instance, p: TypeValidator(int)(p)
    )

    WAL_BUFFER_SIZE = LazyAttribute(
        raw=1000,
        process=lambda instance, p: TypeValidator(int)(p)
    )
    WAL_MAX_SIZE = LazyAttribute(
        raw=16 * 1024 * 1024,
        process=lambda instance, p: TypeValidator(int)(p)
    )
    WAL_BACKUP_COUNT = LazyAttribute(
        raw=16,
        process=lambda instance, p: TypeValidator(int)(p)
    )
    WAL_DIR = LazyAttribute(
        render=lambda instance, p: ensure_mkdir(
            instance.BURROW_PATH.joinpath("wal")
        ),
        process=lambda instance, p: (_ for _ in ()).throw(AttributeError)
    )

    DEFAULT_LOGGING = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "debug": {
                "()": "gaterpc.utils.ColorFormatter",
                "format": debug_format,
            },
            "verbose": {
                "format": verbose_format,
            },
            "simple": {
                "format": simple_format,
            },
            "console": {
                "()": "gaterpc.utils.ColorFormatter",
                "format": verbose_format
            },
        },
        "handlers": {
            "asyncio": {
                "level": "DEBUG",
                "class": "gaterpc.utils.AQueueHandler",
                "handler_class": "logging.handlers.TimedRotatingFileHandler",
                "listener": "gaterpc.utils.singleton_aqueue_listener",
                "filename": empty,
                "formatter": "debug",
                "when": "midnight",
                "backupCount": 10
            },
            "gaterpc": {
                "level": "DEBUG",
                "class": "gaterpc.utils.AQueueHandler",
                "handler_class": "logging.handlers.TimedRotatingFileHandler",
                "listener": "gaterpc.utils.singleton_aqueue_listener",
                "filename": empty,
                "formatter": "verbose",
                "when": "midnight",
                "backupCount": 10
            },
            "console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "verbose",
            },
            "eutamias": {
                "level": "DEBUG",
                "class": "gaterpc.utils.AQueueHandler",
                "handler_class": "logging.handlers.TimedRotatingFileHandler",
                "listener": "gaterpc.utils.singleton_aqueue_listener",
                "filename": empty,
                "formatter": "verbose",
                "when": "midnight",
                "backupCount": 10
            },
        },
        "loggers": {
            "multiprocessing": {
                "handlers": ["console"],
                "propagate": False,
                "level": "INFO"
            },
            "asyncio": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
            "gaterpc": {
                "level": "INFO",
                "handlers": ["gaterpc"],
                "propagate": True,
            },
            "gaterpc.zap": {
                "level": "INFO",
                "handlers": ["gaterpc"],
                "propagate": True,
            },
            "eutamias": {
                "level": "INFO",
                "handlers": ["eutamias"],
            }
        }
    }

for name, value in _Settings.__dict__.items():
    if name.isupper():
        Settings.configure(name, value)
