# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/8/1 下午3:20
import asyncio
import sys
from lib2to3.fixes.fix_input import context
from logging import getLogger
from multiprocessing import Process
from statistics import multimode
from traceback import format_exception
from pathlib import Path
import zmq.auth
import zmq.constants as z_const
from zmq.backend import curve_keypair


base_path = Path(__file__).parent

sys.path.append(base_path.parent.as_posix())

from gaterpc.core import AMajordomo, AsyncZAPService, Client, Context
from gaterpc.utils import LazyAttribute, check_socket_addr
from gaterpc.global_settings import Settings
from eutamias.burrow import Chestnut, WAL, BurrowWorker, BurrowService
from eutamias.core import Eutamias
import test_settings


curve_dir = Path(__file__).parent.joinpath("curvekey/")
if curve_dir.exists():
    g_public, g_secret = zmq.auth.load_certificate(
        curve_dir.joinpath("gate.key_secret")
    )
    print(f"g_public: {g_public}, g_secret: {g_secret}")
    cw_public, cw_secret = curve_keypair()
else:
    g_public = g_secret = b""

Settings.configure("USER_SETTINGS", test_settings)
logger = getLogger("eutamias")


async def zap_server(ctx):
    zap = AsyncZAPService(context=ctx)
    zap.configure_plain(
        Settings.ZAP_DEFAULT_DOMAIN,
        {
            Settings.ZAP_PLAIN_DEFAULT_USER: Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        }
    )
    try:
        zap.start()
        await zap._recv_task
    finally:
        zap.stop()


def start_zap(ctx):
    loop = asyncio.new_event_loop()
    loop.slow_callback_duration = 0.01
    loop.run_until_complete(zap_server(ctx))


async def run_burrow(backend):
    Settings.setup()
    if g_public:
        Settings.ZMQ_SOCK.update({
            z_const.CURVE_SECRETKEY: cw_secret,
            z_const.CURVE_PUBLICKEY: cw_public,
            z_const.CURVE_SERVERKEY: g_public,
        })
    Settings.WORKER_ADDR = backend
    # loop = asyncio.get_event_loop()
    # loop.slow_callback_duration = 0.01
    ctx = Context()
    burrow = BurrowService()
    burrow_worker = burrow.create_worker(
        BurrowWorker,
        context=ctx,
    )
    try:
        burrow_worker._reconnect = True
        burrow_worker.run()
        while 1:
            if burrow_worker._recv_task:
                if burrow_worker._recv_task.done():
                    break
                await burrow_worker._recv_task
    finally:
        burrow_worker.stop()
        await burrow_worker._writer


def burrow_process(backend):
    asyncio.run(run_burrow(backend))


async def client(frontend_addr):
    logger.info("start client")
    loop = asyncio.get_running_loop()
    gr_cli = Client()

    await gr_cli.connect(check_socket_addr(frontend_addr))
    await asyncio.sleep(5)
    logger.info(gr_cli._remote_services)
    i = 100
    try:
        st = loop.time()
        for i in range(100000):
            value = {
                "name": "hostname",
                "ip": "1.1.1.1",
                "stat": "running" if i % 2 else "stop",
                "remote_hosts": ["hostname1", "hostname2", "hostname3"]
            }
            # logger.info("=====================================================")
            result = await gr_cli.WAL.emit("update", "tttxxxhhh", value)
            # logger.info("=====================================================")
            # logger.info(f"result: {result}")
        et = loop.time()
        logger.info(f"use time: {et - st}")
    except Exception as e:
        logger.info("*****************************************************")
        for line in format_exception(*sys.exc_info()):
            logger.error(line)
        logger.info("*****************************************************")
        raise e
    finally:
        logger.info(
            f"the length of client's replies: {len(gr_cli.replies)}"
        )
        logger.info(
            f"the length of client's replies: {len(gr_cli.replies)}"
        )
        gr_cli.close()


async def test_client(frontend):
    Settings.setup()
    loop = asyncio.get_event_loop()
    loop.slow_callback_duration = 0.01
    await client(frontend)


def client_process(frontend):
    asyncio.run(test_client(frontend))


class TEutamias(Eutamias):
    wait_for_wal_rep = True

    async def test_burrow(self):
        while 1:
            if Settings.BURROW_SERVICE_NAME not in self.services:
                await asyncio.sleep(5)
            else:
                break
        logger.info(f"start test burrow: {self._loop.time()}")
        try:
            st = self._loop.time()
            i = 0
            retries = 0
            while i < 100000:
                log = {
                    "gtid": i,
                    "action": "update",
                    "key": "tttxxxhhh",
                    "value": {
                        "name": "hostname",
                        "ip": "1.1.1.1",
                        "stat": "running" if i % 2 else "stop",
                        "remote_hosts": ["hostname1", "hostname2", "hostname3"]
                    }
                }
                # wal_st = loop.time()
                # logger.info("=====================================================")
                request_id = await self.request_burrow(log)
                if self.wait_for_wal_rep:
                    try:
                        await asyncio.wait_for(
                            self.internal_requests[request_id],
                            Settings.TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        del self.internal_requests[request_id]
                        if retries == Settings.BURROW_SERVICE_MAX_RETRIES:
                            logger.error(f"retries: {retries}")
                            break
                        retries += 1
                        continue
                # logger.debug(f"backend tasks number: {len(self.backend_tasks)}"
                # await asyncio.sleep(0)
                # logger.debug(f"wal result: {result}")
                # wal_et = loop.time()
                # logger.debug(f"use time from wal: {wal_et - wal_st}")
                # logger.info("=====================================================")
                # logger.info(f"result: {result}")
                i += 1
            et = self._loop.time()
            logger.info(f"put use time: {et - st}")
        except Exception as e:
            logger.info("*****************************************************")
            for line in format_exception(*sys.exc_info()):
                logger.error(line)
            logger.info("*****************************************************")
            raise e


async def test_eutamias(gate_port, backend, frontend=None):
    if g_secret:
        Settings.GATE_CURVE_KEY = g_secret
        Settings.GATE_CURVE_PUBKEY = g_public
    Settings.setup()
    if backend:
        Settings.WORKER_ADDR = backend
    ctx = Context()
    eutamias = TEutamias(
        gate_port, context=ctx
    )
    if g_secret:
        eutamias.bind_backend(
            sock_opt={
                z_const.CURVE_SECRETKEY: g_secret,
                z_const.CURVE_SERVER: True,
            }
        )
    else:
        eutamias.bind_backend()
    if frontend:
        if g_secret:
            eutamias.bind_frontend(
                frontend,
                sock_opt={
                    z_const.CURVE_SECRETKEY: g_secret,
                    # z_const.CURVE_PUBLICKEY: g_public,
                    z_const.CURVE_SERVER: True,
                }
            )
        else:
            eutamias.bind_frontend(frontend)
    await asyncio.sleep(1)
    logger.info("start test")
    try:
        eutamias.run()
        await asyncio.sleep(5)
        # gr_worker._recv_task.cancel()
        await eutamias.test_burrow()
        await eutamias._broker_task
    finally:
        eutamias.stop()


def eutamias_process(gate_port, backend, frontend=None):
    # asyncio.set_event_loop_policy(UnixEPollEventLoopPolicy())
    gate_port = int(gate_port)
    asyncio.run(test_eutamias(gate_port, backend, frontend))


def test_read_wal():
    Settings.setup()
    wal = WAL()
    for m in wal.read():
        # pass
        print(m)


if __name__ == '__main__':
    argv = sys.argv[1:]
    if argv[0] == "burrow":
        burrow_process(argv[1])
    elif argv[0] == "client":
        client_process(argv[1])
    elif argv[0] == "eutamias":
        eutamias_process(*argv[1:])
    elif argv[0] == "read_wal":
        test_read_wal()
