# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/8/1 下午3:20
import asyncio
import sys
from lib2to3.fixes.fix_input import context
from logging import getLogger
from multiprocessing import Process
from traceback import format_exception
from pathlib import Path
import zmq.auth
import zmq.constants as z_const


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
else:
    g_public = g_secret = b""

Settings.configure("USER_SETTINGS", test_settings)
logger = getLogger("eutamias")


# for name, value in EutamiasSettings.__dict__.items():
#     if name.isupper():
#         print(f"{name}, {value}")
#         Settings.configure(name, value)
frontend_addr = "ipc:///tmp/eutamias/run/c1"


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


async def async_majordomo():
    Settings.ZAP_REPLY_TIMEOUT = 10.0
    Settings.setup()
    # loop = asyncio.get_event_loop()
    # loop.slow_callback_duration = 0.01
    # Settings.EVENT_LOOP_POLICY = uvloop.EventLoopPolicy()
    ctx = Context()
    # zap_thread = Thread(target=start_zap, args=(ctx,))
    zipc = Settings.ZAP_ADDR
    # zipc = f"ipc://{zipc.as_posix()}"
    logger.info(f"zap ipc addr: {zipc}")
    zap = AsyncZAPService(addr=zipc)
    zap.configure_plain(
        Settings.ZAP_DEFAULT_DOMAIN,
        {
            Settings.ZAP_PLAIN_DEFAULT_USER: Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        }
    )

    gr_majordomo = AMajordomo(
        context=ctx,
        gate_zap_mechanism=Settings.ZAP_MECHANISM_PLAIN,
        gate_zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )
    gr_majordomo.bind_backend()
    gr_majordomo.bind_frontend(frontend_addr)
    await asyncio.sleep(1)
    logger.info("start test")
    try:
        zap.start()
        gr_majordomo.connect_zap(zap_addr=zipc)
        gr_majordomo.run()
        await asyncio.sleep(5)
        # gr_worker._recv_task.cancel()
        await gr_majordomo._broker_task
    finally:
        await asyncio.sleep(1)
        gr_majordomo.stop()
        logger.info(f"request_zap.cache_info: {gr_majordomo.zap_cache}")
        zap.stop()


async def run_wal():
    Settings.setup()
    # loop = asyncio.get_event_loop()
    # loop.slow_callback_duration = 0.01
    ctx = Context()
    burrow = BurrowService()
    burrow_worker = burrow.create_worker(
        BurrowWorker,
        context=ctx,
        zap_mechanism=Settings.ZAP_MECHANISM_PLAIN,
        zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        ),
        max_allowed_request=100000
        # max_allowed_request=50000
    )
    try:
        burrow_worker._reconnect = True
        burrow_worker.run()
        while 1:
            await asyncio.sleep(5)
    finally:
        burrow_worker.stop()
        await burrow_worker._writer


def wal_process():
    asyncio.run(run_wal())


def majordomo_process():
    asyncio.run(async_majordomo())


def test_wal():
    wal_p = Process(target=wal_process)
    try:
        wal_p.start()
        majordomo_process()
    except Exception:
        wal_p.close()
        wal_p.terminate()
        wal_p.join()
        raise
    # wal_p.join()


async def client():
    logger.info("start client")
    loop = asyncio.get_running_loop()
    gr_cli = Client(
        zap_mechanism=Settings.ZAP_MECHANISM_PLAIN,
        zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )

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


async def test_client():
    Settings.setup()
    loop = asyncio.get_event_loop()
    loop.slow_callback_duration = 0.01
    await client()


def client_process():
    asyncio.run(test_client())


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


async def test_eutamias(gate_port, mcast_port, frontend, backend):
    if g_secret:
        Settings.GATE_CURVE_KEY = g_secret
        Settings.GATE_CURVE_PUBKEY = g_public
    Settings.setup()
    ctx = Context()
    eutamias = TEutamias(
        gate_port, context=ctx
    )
    eutamias.bind_backend()
    await asyncio.sleep(1)
    logger.info("start test")
    try:
        zap.start()
        eutamias.connect_zap(zap_addr=zipc)
        eutamias.run()
        await asyncio.sleep(5)
        # gr_worker._recv_task.cancel()
        await eutamias.test_wal()
        await eutamias._broker_task
    finally:
        await asyncio.sleep(1)
        eutamias.stop()
        logger.info(f"request_zap.cache_info: {eutamias.zap_cache}")
        zap.stop()


def eutamias_process():
    asyncio.run(test_eutamias())


def test_read_wal():
    Settings.setup()
    wal = WAL()
    for m in wal.read():
        # pass
        print(m)


if __name__ == '__main__':
    argv = sys.argv[1:]
    if argv[0] == "service":
        test_wal()
    elif argv[0] == "majordomo":
        majordomo_process()
    elif argv[0] == "client":
        client_process()
    elif argv[0] == "wal":
        wal_process()
    elif argv[0] == "eutamias":
        eutamias_process()
    elif argv[0] == "read_wal":
        test_read_wal()
