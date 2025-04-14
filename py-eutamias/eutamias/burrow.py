# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/7/18 下午4:02
import asyncio
import bisect
import fcntl
import io
from collections import deque
from collections.abc import (
    Collection, Generator,
)
from logging import getLogger
from pathlib import Path
from threading import Lock as TLock, _CRLock, _PyRLock
from typing import Any, NamedTuple, Optional, Union

if _CRLock is None:
    RLock = _PyRLock
else:
    RLock = _CRLock

from gaterpc.core import Context, Service, Worker
from gaterpc.utils import (
    LazyAttribute, interface, msg_pack, msg_unpack,
    resolve_module_attr,
)
from .global_settings import Settings
from eutamias.bp_tree import BPTree

from .utils import chestnut_dumps, chestnut_loads, generate_int_digest
from eutamias.exceptions import (
    ChestnutExistsError, ChestnutNotFoundError, KeyExistsError,
)


logger = getLogger("eutamias.burrow")

VERSION_LENGTH = 16
ACTIONS = {
    0: "create",
    1: "delete",
    2: "update",
}



class Chestnut(NamedTuple):
    key: str
    value: Any
    version: int
    index: list

    def __repr__(self) -> str:
        return (
            f'<Chestnut '
            f'key={self.key} value={self.value} version={self.version} '
            f'index={self.index}>'
        )


class Chestnuts:
    """
    保存Chestnut的文件或块设备
    使用固定块大小（4096）的文件或块设备来读写持久化
    使用预读，读取某个数据时，将相邻或横跨的块也读出，可开关
    元数据保存在开头一个块：
    Eutamias 1.0
    """
    VERSION = "1.0"
    FILL_BYTE = (0).to_bytes(1, byteorder="big")
    BLOCK_SIZE = 4096
    DELETE_FLAG = (1).to_bytes(1, byteorder="big")
    ADDR_BIT = 64
    _ADDR_BYTE_LENGTH = ((2**ADDR_BIT).bit_length() + 7) // 8
    BLOCK_HEAD_FORMAT = (
        _ADDR_BYTE_LENGTH,
        _ADDR_BYTE_LENGTH,
        ((BLOCK_SIZE - 2 * _ADDR_BYTE_LENGTH).bit_length() + 7) // 8
    )
    BLOCK_HEAD_SIZE = sum(BLOCK_HEAD_FORMAT)
    # TODO: 缓存多少个？
    _CACHE: dict[str, Chestnut] = dict()
    medata = LazyAttribute(
        render=lambda instance, raw:
        f"Eutamias {instance.VERSION} {instance.latest_addr}"
    )

    def __init__(self, index: BPTree=None):
        if not Settings.CHESTNUTS or not Settings.CHESTNUTS.exists():
            raise ChestnutNotFoundError(Settings.CHESTNUTS.as_posix())
        self._index_file = None
        self.reuse_block_index: deque[int] = deque()
        self.ready = asyncio.Future()
        self.io_lock = TLock()
        self.file = io.open(
            Settings.CHESTNUTS, mode="rb+", buffering=0
        )
        try:
            fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            self.file.close()
            self.file = None
            logger.error(f"open {Settings.CHESTNUTS}: {e}")
            raise
        if index:
            self._index = index
        else:
            self._index_file = Settings.CHESTNUTS.parent.joinpath("bpt")
            if self._index_file.exists():
                self._index = BPTree.load(self._index_file, msg_unpack)
            else:
                self._index = BPTree(Settings.BPT_FACTOR)
        self.file.seek(0, io.SEEK_END)
        self._FILE_END_INDEX = self.file.tell()
        self.file.seek(0)
        medata = self._read(0).strip(self.FILL_BYTE)
        medata = medata.decode("utf-8")
        try:
            _, self.VERSION, self.latest_addr, *param = medata.split()
            self.latest_addr = int(self.latest_addr)
            self.load()
            self.ready.set_result(True)
        except ValueError:
            self.latest_addr = self.BLOCK_SIZE
            medata = self.medata.encode("utf-8").ljust(
                self.BLOCK_SIZE, self.FILL_BYTE
            )
            self._write(0, medata)
            self.ready.set_result(True)

    def _close_file(self):
        if self.ready.done():
            self.ready = asyncio.Future()
        if self.file:
            self.file.flush()
            try:
                fcntl.flock(self.file, fcntl.LOCK_UN)
            except ValueError:
                pass
            finally:
                self.file.close()
                self.file = None
        if self._index_file and self._index.root:
            BPTree.dump(self._index, self._index_file, msg_pack)

    def _read(self, addr):
        self.file.seek(addr)
        data = self.file.read(self.BLOCK_SIZE)
        return data

    def _write(self, addr, data):
        if not isinstance(data, bytes):
            raise TypeError(data)

        self.file.seek(addr)
        self.file.write(data)
        self.file.flush()

    def _update_metadata(self):
        with self.io_lock:
            self.file.seek(0)
            self.file.write(
                self.medata.encode("utf-8").center(self.BLOCK_SIZE, self.FILL_BYTE)
            )
            self.file.flush()

    def _get_block(self, index) -> tuple[int, int, bytes]:
        data = self._read(index)
        head, data = data[:self.BLOCK_HEAD_SIZE], data[self.BLOCK_HEAD_SIZE:]
        pre_index = head[0: self.BLOCK_HEAD_FORMAT[0]]
        post_index = head[
            self.BLOCK_HEAD_FORMAT[0]:
            self.BLOCK_HEAD_FORMAT[0] +
            self.BLOCK_HEAD_FORMAT[1]
        ]
        data_size = head[
            self.BLOCK_HEAD_FORMAT[0] +
            self.BLOCK_HEAD_FORMAT[1]:
            self.BLOCK_HEAD_FORMAT[0] +
            self.BLOCK_HEAD_FORMAT[1] +
            self.BLOCK_HEAD_FORMAT[2]
        ]
        pre_index = int.from_bytes(pre_index, byteorder="big")
        post_index = int.from_bytes(post_index, byteorder="big")
        data_size = int.from_bytes(data_size, byteorder="big")
        data = data[:data_size]
        return pre_index, post_index, data

    def _save_block(
        self, block: bytes, index, pre_index: int =0, post_index: int =0
    ):
        data_size = len(block).to_bytes(4, "big")
        data = block.ljust(
            self.BLOCK_SIZE - self.BLOCK_HEAD_SIZE, self.FILL_BYTE
        )
        if len(data) > self.BLOCK_SIZE:
            raise ValueError(f"len of data: {len(data)}")
        pre_index = pre_index.to_bytes(8, "big")
        post_index = post_index.to_bytes(8, "big")
        self._write(
            index,
            pre_index + post_index + data_size + data
        )

    def _recycle_addr(self, addr):
        bisect.insort_left(self.reuse_block_index, addr)
        # self.reuse_block_index.append(addr)
        # self.reuse_block_index.sort(reverse=True)

    def _clear_block(self, addr):
        self.file.seek(addr)
        self.file.write(self.FILL_BYTE * self.BLOCK_SIZE)
        self.file.flush()

    def _soft_delete(self, addr):
        self.file.seek(addr)
        self.file.write(self.DELETE_FLAG)

    def _search_chestnut_block(
        self, index: int
    ) -> Union[None, tuple[dict, list[int]]]:
        """
        从Chestnut任意块的索引找到该Chestnut的所有块
        """
        pre_index, post_index, data = self._get_block(index)
        # logger.debug(f"{pre_index} {post_index} {data}")
        if len(data) == 0:
            self._recycle_addr(index)
            return None
        data_indices = [index,]
        while pre_index or post_index:
            if pre_index:
                data_indices = [pre_index, *data_indices]
                pre_index, _, pre_data = self._get_block(pre_index)
                data = pre_data + data
            if post_index:
                data_indices = [*data_indices, post_index]
                _, post_index, post_data = self._get_block(post_index)
                data += post_data
        data = chestnut_loads(data)
        return data, data_indices

    def _read_chestnut_blocks(self, indices):
        data = b""
        for index in indices:
            if isinstance(index, str) and index.isdigit():
                index = int(index)
            elif not isinstance(index, int):
                raise TypeError("index must be integer")
            _pr, _po, _data = self._get_block(index)
            data += _data
        data = chestnut_loads(data)
        return data

    def _write_chestnut_block(self, chestnut_block) -> list[int]:
        blocks = list()
        with self.io_lock:
            start = 0
            while len(
                    block := chestnut_block[
                             start:self.BLOCK_SIZE - self.BLOCK_HEAD_SIZE
                             ]
            ):
                if self.reuse_block_index:
                    index = self.reuse_block_index.popleft()
                else:
                    index = self.latest_addr
                    self.latest_addr += self.BLOCK_SIZE
                if blocks:
                    pre_index = blocks[-1][1]
                    blocks[-1][3] = index
                else:
                    pre_index = 0
                blocks.append([block, index, pre_index, 0])
                start += self.BLOCK_SIZE - self.BLOCK_HEAD_SIZE
            for block in blocks:
                # logger.debug(block)
                self._save_block(*block)
        block_indices = [i[1] for i in blocks]
        return block_indices

    def _clear_chestnut_block(self, indices: list[int]):
        with self.io_lock:
            for index in indices:
                self._clear_block(index)

    def load(self):
        with self.io_lock:
            index = self.BLOCK_SIZE
            read = deque()
            while 1:
                if index not in read:
                    # logger.debug(f"read chestnut block in {index}")
                    if _ := self._search_chestnut_block(index):
                        data, indices = _
                        # use B+ Tree
                        key = generate_int_digest(data["key"].encode("utf-8"))
                        # logger.debug(f"key: {key}")
                        try:
                            self._index.insert(
                                key,
                                ",".join(str(i) for i in indices)
                            )
                        except KeyExistsError:
                            pass
                        for i in indices:
                            if i > index:
                                read.append(i)
                else:
                    read.remove(index)
                if index >= self._FILE_END_INDEX:
                    break
                index += self.BLOCK_SIZE

    def create(self, key, value):
        ki = generate_int_digest(key.encode("utf-8"))
        data = {
            "key": key,
            "value": value,
            "version": 0
        }
        chestnut_block = chestnut_dumps(data)
        block_indices = self._write_chestnut_block(chestnut_block)
        chestnut = Chestnut(index=block_indices, **data)
        # use B+Tree
        # logger.debug(f"key int digest: {key}")
        self._index.insert(
            ki, ",".join(str(i) for i in block_indices)
        )
        return chestnut

    def update(self, key, value):
        ki = generate_int_digest(key.encode("utf-8"))
        block_indices = self._index.get(ki).split(",")
        data = self._read_chestnut_blocks(block_indices)
        chestnut = Chestnut(index=block_indices, **data)
        data.update(
            {
                "key": key,
                "value": value,
                "version": chestnut.version + 1
            }
        )
        new_chestnut_blocks = chestnut_dumps(data)
        self._clear_chestnut_block(chestnut.index)
        block_indices = self._write_chestnut_block(new_chestnut_blocks)
        chestnut = Chestnut(index=block_indices, **data)
        self._index.update(
            ki, ",".join(str(i) for i in block_indices)
        )
        return chestnut

    def get(self, key):
        ki = generate_int_digest(key.encode("utf-8"))
        block_indices = self._index.get(ki).split(",")
        data = self._read_chestnut_blocks(block_indices)
        return Chestnut(index=block_indices, **data)

    def delete(self, key):
        ki = generate_int_digest(key.encode("utf-8"))
        block_indices = self._index.get(ki).split(",")
        self._clear_chestnut_block(block_indices)
        self._index.remove(ki)

    # def __len__(self):
    #     return len(self.CACHE)

    def __del__(self):
        self._close_file()

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        try:
            self.create(key, value)
        except KeyExistsError:
            self.update(key, value)

    def __delitem__(self, key):
        self.delete(key)


class TransactionLog:

    def __init__(self, *, gtid: int, action: str, key: str, value: Any):
        self.id = gtid
        self.action = action
        self.key = key
        self.value = value
        log = {
            "id": gtid,
            "action": action,
            "key": key,
            "value": value
        }
        log = msg_pack(log)
        ll = len(log)
        log = ll.to_bytes(8, byteorder="big") + log
        self.log = log

    def __hash__(self):
        return hash(self.log)

    @staticmethod
    def loads(s: bytes):
        log = msg_unpack(s)
        return TransactionLog(**log)


class WAL:
    """
    wal读写和管理
    """

    def __init__(self):
        self._wal_dir: Path = Settings.WAL_DIR
        self._file = self._wal_dir.joinpath("wal.0")
        self._stream: Optional[io.FileIO] = None
        self._lock: TLock = TLock()
        self._max_size: int = Settings.WAL_MAX_SIZE
        self._last_tid = -1

    @property
    def tid(self):
        return self._last_tid

    def _open_file(self, mode="rb"):
        if self._stream:
            return
            # raise OSError("File already opened")
        self._stream = open(self._file, mode=mode, buffering=0)
        try:
            fcntl.flock(self._stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            self._stream.close()
            self._stream = None
            logger.error(f"open {self._file.as_posix()}:\n{e}")
            raise

    def _close_file(self):
        if self._stream:
            self._stream.flush()
            logger.debug("close file")
            try:
                fcntl.flock(self._stream, fcntl.LOCK_UN)
            except ValueError:
                pass
            finally:
                self._stream.close()
                self._stream = None

    def __del__(self):
        self._close_file()

    def _rollover(self):
        if self._stream:
            self._close_file()
        for i in range(Settings.WAL_BACKUP_COUNT - 1, 0, -1):
            sfn, dfn = f"wal.{i}", f"wal.{i + 1}"
            if (sf := self._wal_dir.joinpath(sfn)).exists():
                sf.replace(self._wal_dir.joinpath(dfn))
        self._file.replace(self._wal_dir.joinpath("wal.1"))
        self._open_file("ab")

    def emit(self, tlog: TransactionLog):
        if tlog.id - self._last_tid != 1:
            raise RuntimeError(
                f"tid not continuous, current tid is {tlog.id},"
                f" last recorded tid is {self._last_tid}"
            )
        self._lock.acquire()
        try:
            if not self._stream:
                self._open_file("ab")
            if self._stream.tell() + len(tlog.log) >= self._max_size:
                self._rollover()
            self._stream.write(tlog.log)
            self._last_tid = tlog.id
        except Exception as e:
            logger.error(e)
            self._lock.release()
            raise e

    def read_once(self) -> Generator[TransactionLog]:
        """
        用于读取一个 WAL 文件
        """
        self._open_file()
        try:
            while 1:
                ll = int.from_bytes(self._stream.read(8), byteorder="big")
                if not ll:
                    raise StopIteration
                tlog = TransactionLog.loads(self._stream.read(ll))
                yield tlog
        finally:
            self._close_file()

    def write_once(self, tlogs: Collection[dict]):
        """
        用于批量写入
        """
        self._open_file(mode="ab")
        try:
            for tlog in tlogs:
                if isinstance(tlog, dict):
                    raise TypeError("log")
                tlog = TransactionLog(**tlog)
                self.emit(tlog)
        finally:
            self._close_file()


class Handler:

    def process(self, name, value) -> tuple[..., ...]:
        """
        校验或转换
        """

    def render(self, name, value) -> tuple[..., ...]:
        """
        去除或增加
        """


def handlers_validator(handlers: list) -> list:
    for handler in handlers:
        if isinstance(handler, str):
            handler = resolve_module_attr(handler)
        if not isinstance(handler, Handler):
            raise TypeError(
                f"Expected type of '{handler}' is an instance of 'Handler'"
            )
    return handlers


class Burrow:
    """
    使用 WAL 的键值对存储
    为了避免操作单文件出错，WAL 只能在单进程里运行，但可以操作于多线程
    TODO: 持久化时使用fcntl文件锁
    """

    def __init__(self):
        self.handlers: list[Handler] = handlers_validator(
            Settings.BURROW_HANDLERS
        )
        self.chestnuts = Chestnuts()
        self.wal = WAL()
        self.usable = True

    def add_handler(self, handler: Handler):
        """
        添加处理器
        """
        self.handlers.append(handler)

    def remove_handler(self, handler: Handler):
        self.handlers.remove(handler)

    def create_chestnut(self, name, value):
        for handler in self.handlers:
            name, value = handler.process(name, value)
        if name in self.chestnuts:
            raise ChestnutExistsError(name)
        tlog = TransactionLog(
            gtid=self.wal.tid + 1, action=ACTIONS[0],
            key=name, value=None
        )
        self.wal.emit(tlog)
        try:
            self.chestnuts.create(name, value)
        except (IOError, OSError):
            tlog = TransactionLog(
                gtid=self.wal.tid + 1, action=ACTIONS[1],
                key=name, value=None
            )
            self.wal.emit(tlog)
            self.usable = False
            raise

    def get_chestnut(self, name):
        raw_name = name
        for handler in self.handlers:
            name, _ = handler.process(name, None)
        try:
            value = self.chestnuts[name]
        except KeyError:
            raise KeyError(raw_name)
        for handler in self.handlers[::-1]:
            name, value = handler.render(name, value)
        return value

    def update_chestnut(self, name, value):
        raw_name = name
        for handler in self.handlers:
            name, value = handler.process(name, value)
        if name not in self.chestnuts:
            raise ChestnutNotFoundError(raw_name)
        tlog = TransactionLog(
            gtid=self.wal.tid + 1, action=ACTIONS[2],
            key=name, value=value
        )
        self.wal.emit(tlog)
        self.chestnuts[name] = value

    def remove_chestnut(self, name):
        for handler in self.handlers:
            name, _ = handler.process(name, None)
        tlog = TransactionLog(
            gtid=self.wal.tid + 1, action=ACTIONS[1],
            key=name, value=None
        )
        self.wal.emit(tlog)
        try:
            del self.chestnuts[name]
        except KeyError:
            pass

    def __iter__(self):
        return iter(self.chestnuts)


class BurrowWorker(Worker, Burrow):
    """
    预写事务日志，使用队列做 WAL 缓冲
    定期持久化键值对
    TODO: 缓冲多少条事务？
    TODO: 定期从缓冲取出事务提交 WAL
    TODO: 关闭时刷新全部缓冲
    """
    _sentinel = b"0"
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        broker_addr: str,
        service: "BurrowService",
        identity: str = "wal",
        heartbeat: int = None,
        context: Context = None,
        zap_mechanism: Union[str, bytes] = None,
        zap_credentials: tuple = None,
        max_allowed_request: int = None,
        thread_executor=None,
        process_executor=None
    ):
        Worker.__init__(
            self,
            broker_addr,
            service=service,
            identity=identity,
            heartbeat=heartbeat,
            context=context,
            zap_mechanism=zap_mechanism,
            zap_credentials=zap_credentials,
            max_allowed_request=max_allowed_request,
            thread_executor=thread_executor,
            process_executor=process_executor
        )
        Burrow.__init__(self)
        self._wal_task: Optional[asyncio.Task] = None
        self._stream_write_tasks = set()
        self.Q = asyncio.Queue()
        # 初始化本地的 Transaction ID
        self.last_tid: int = 0

    async def emit_wal(self):
        logger.info("WAL start.")
        st = 0
        while 1:
            try:
                log = await self.Q.get()
            except asyncio.CancelledError:
                log = await asyncio.wait_for(self.Q.get(), 1)
                if log != self._sentinel:
                    await self.Q.put(self._sentinel)
                logger.debug(f"Q.size: {self.Q.qsize()}")
            if not st:
                st = self._loop.time()
            self.Q.task_done()
            if log == self._sentinel:
                logger.info("get sentinel from Q.")
                break
            if log == self._sentinel:
                break
            tlog = TransactionLog(**log)
            self.wal.emit(tlog)
            self.last_tid = tlog.id

    @interface
    async def sync_wal(self, log: dict):
        try:
            if (gtid := log["gitd"]) - self.last_tid != 1:
                raise RuntimeError(
                    f"gtid error, new({log['gtid']}), last of local "
                    f"({self.last_tid})"
                )
            await self.Q.put(log)
            return gtid
        except Exception as e:
            logger.error(e)
            raise

    @interface("thread")
    def create(self, name, value):
        return self.create_chestnut(name, value)

    @interface("thread")
    def remove(self, name):
        return self.remove_chestnut(name)

    @interface("thread")
    def get(self, name):
        return self.get_chestnut(name)

    @interface("thread")
    def update(self, name, value):
        return self.update_chestnut(name, value)

    def close(self):
        super().close()

    def stop(self):
        super().stop()
        if self._wal_task:
            logger.info("WAL task stop.")
            self.Q.put_nowait(self._sentinel)
            if not self._wal_task.cancelled():
                self._wal_task.cancel()

    def run(self):
        super().run()
        if not self._wal_task:
            logger.debug("start writer")
            self._wal_task = self._loop.create_task(self.emit_wal())


class BurrowService(Service):

    def __init__(self):
        super().__init__("WAL", "Eutamias of WAL")
