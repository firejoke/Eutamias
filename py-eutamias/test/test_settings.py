# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/7/30 下午1:40
from pathlib import Path

from gaterpc.utils import LazyAttribute


BASE_PATH = Path(__file__).parent.joinpath("workdir")
# CHESTNUTS = Path("/dev/loop1")
CHESTNUTS = BASE_PATH.joinpath("burrow/chestnuts")
WORKER_ADDR = LazyAttribute(
    render=lambda instance, p:
    f"ipc://{instance.RUN_PATH.joinpath('eutamias.wal').as_posix()}"
)
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "debug": {
            "format": "%(asctime)s %(levelname)s %(name)s "
                      "[%(processName)s(%(process)d):"
                      "%(threadName)s(%(thread)d)]\n"
                      "%(pathname)s[%(funcName)s:%(lineno)d] "
                      "-\n%(message)s",
        },
        "verbose": {
            "format": "%(asctime)s %(levelname)s %(name)s "
                      "%(module)s.[%(funcName)s:%(lineno)d] "
                      "-\n%(message)s",
        },
        "simple": {
            "format": "%(asctime)s %(levelname)s  %(name)s %(module)s "
                      "- %(message)s"
        },
    },
    "handlers": {
        # "asyncio": {
        #     "level": "DEBUG",
        #     "class": "gaterpc.utils.AQueueHandler",
        #     "handler_class": "logging.handlers.TimedRotatingFileHandler",
        #     "listener": "gaterpc.utils.singleton_aqueue_listener",
        #     "filename": empty,
        #     "formatter": "debug",
        #     "when": "midnight",
        #     "backupCount": 10
        # },
        # "gaterpc": {
        #     "level": "DEBUG",
        #     "class": "gaterpc.utils.AQueueHandler",
        #     "handler_class": "logging.handlers.TimedRotatingFileHandler",
        #     "listener": "gaterpc.utils.singleton_aqueue_listener",
        #     "filename": empty,
        #     "formatter": "verbose",
        #     "when": "midnight",
        #     "backupCount": 10
        # },
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        # "eutamias": {
        #     "level": "DEBUG",
        #     "class": "gaterpc.utils.AQueueHandler",
        #     "handler_class": "logging.handlers.TimedRotatingFileHandler",
        #     "listener": "gaterpc.utils.singleton_aqueue_listener",
        #     "filename": empty,
        #     "formatter": "verbose",
        #     "when": "midnight",
        #     "backupCount": 10
        # },
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
            "handlers": ["console"],
            "propagate": True,
        },
        "gaterpc.zap": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": True,
        },
        "eutamias": {
            "level": "INFO",
            "handlers": ["console"],
        }
    }
}
