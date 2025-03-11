# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/7/18 下午4:02
import asyncio
import bisect
import fcntl
import inspect
import io
import math
import sys
from collections import deque
from collections.abc import (
    Collection, Hashable, Iterable, MutableMapping, Generator, Callable,
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
    LazyAttribute, MsgPackError, from_bytes, interface, msg_pack, msg_unpack,
    Empty, empty,
    resolve_module_attr, to_bytes,
)
from .global_settings import Settings

from .utils import chestnut_dumps, chestnut_loads, generate_int_digest
from .exceptions import ChestnutExistsError, ChestnutNotFoundError


logger = getLogger("eutamias.burrow")

VERSION_LENGTH = 16
ACTIONS = {
    0: "create",
    1: "delete",
    2: "update",
}


def version_to_name(version: int):
    return f"{version:08x}"


def name_to_version(name: str):
    return int(name, 16)


class BPTDebug:

    def __enter__(self):
        if Settings.DEBUG > 1:
            stack = inspect.stack()
            snap_frame = stack[1]
            func_name = snap_frame.function
            f_locals = snap_frame.frame.f_locals
            obj = f_locals.pop("self")
            kwargs = ", ".join(f"{k}: {v}" for k,v in f_locals.items())
            snap_stack = f"{func_name}({kwargs}):\n"
            snap_stack += "=" * 80
            snap_stack += f"\n{obj}"
            del stack
            print(snap_stack)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if Settings.DEBUG > 1:
            stack = inspect.stack()
            frame = stack[1]
            func_name = frame.function
            f_locals = frame.frame.f_locals
            obj = f_locals.pop("self")
            kwargs = ", ".join(f"{k}: {v}" for k,v in f_locals.items())
            snap_stack = f"now for {func_name}({kwargs}):\n"
            snap_stack += "=" * 80
            snap_stack += f"\n{obj}"
            del stack
            print(snap_stack)


# BPTRLock = BPTDebug()
BPTRLock = RLock()


class KeyExistsError(Exception):
    def __init__(self, key):
        super().__init__(f"{key} is exists!")


class InternalNode:
    """
    使用线程递归锁
    """
    NodeType = "Internal"

    def __init__(
        self, tree: "BPTree",
        children: list[Union["InternalNode", "LeafageNode"]],
        parent: "InternalNode"=None
    ):
        self.tree = tree
        self.keys: list[int] = list()
        if children:
            self.children = children
            left = self.children[0]
            if isinstance(left, InternalNode):
                left.parent = self
            else:
                left.internal = self
            for children in self.children[1:]:
                if isinstance(children, InternalNode):
                    children.parent = self
                    self.keys.append(children.left.keys[0])
                elif isinstance(children, LeafageNode):
                    children.internal = self
                    self.keys.append(children.keys[0])
        else:
            self.children: list[Union["InternalNode", "LeafageNode"]] = list()
        if parent:
            self.parent = parent
        else:
            self.parent: Optional["InternalNode"] = None

    @property
    def left(self):
        with BPTRLock:
            node = self
            while 1:
                try:
                    node = node.children[0]
                    if isinstance(node, LeafageNode):
                        return node
                except IndexError:
                    return None

    def balanced(self):
        """
        子节点数量超出限制，触发分裂
          要满足分裂后，每个子节点都满足子节点数比键数多一，且键数不小于半满
        子节点数量不够半满，触发合并
        """
        parent = self.parent
        if not self.children:
            parent.remove_children(self)
        if not parent and len(self.keys) == 0:
            _child = self.children[-1]
            self.tree.root = _child
            if isinstance(_child, InternalNode):
                _child.parent = None
            else:
                _child.internal = None
            return
        if len(self.keys) < self.tree.half_full and parent:
            self.merge()
        elif len(self.keys) > self.tree.branching_factor + 1:
            self.split()
        if parent:
            return parent.balanced()

    def add_pointer(self, pointer: int) -> Optional[int]:
        with BPTRLock:
            left_children = self.children[0]
            if pointer < left_children.keys[0]:
                child_index = 0
            elif self.keys and pointer < self.keys[0]:
                child_index = 1
            else:
                child_index = bisect.bisect(self.keys, pointer) + 1
            if child_index:
                self.keys.insert(child_index - 1, pointer)
            else:
                if isinstance(left_children, InternalNode):
                    self.keys.insert(0, left_children.left.keys[0])
                else:
                    self.keys.insert(0, left_children.keys[0])
                if self.parent:
                    self.parent.update_pointer(
                        self.keys[1], pointer
                    )
            return child_index

    def remove_pointer(self, pointer: int):
        with BPTRLock:
            if pointer in self.keys:
                old_index = self.keys.index(pointer)
                self.keys.remove(pointer)
                return old_index
            if self.parent:
                self.parent.remove_pointer(pointer)
                return -1

    def update_pointer(self, old_pointer, new_pointer):
        with BPTRLock:
            if old_pointer in self.keys:
                index = self.keys.index(old_pointer)
                self.keys[index] = new_pointer
            if self.parent:
                self.parent.update_pointer(old_pointer, new_pointer)

    def _add_children(self,  child: Union["InternalNode", "LeafageNode"]):
        with BPTRLock:
            if isinstance(child, LeafageNode):
                child_index = self.add_pointer(child.keys[0])
            else:
                child_index = self.add_pointer(child.left.keys[0])
            self.children.insert(child_index, child)
            if isinstance(child, LeafageNode):
                child.internal = self
            elif isinstance(child, InternalNode):
                child.parent = self

    def add_children(self, child: Union["InternalNode", "LeafageNode"]):
        self._add_children(child)

    def _remove_children(self, child: Union["InternalNode", "LeafageNode"]):
        with BPTRLock:
            child_index = self.children.index(child)
            pointer = None
            # next_child = self.children[child_index + 1]
            self.children.remove(child)
            if isinstance(child, LeafageNode):
                child.internal = None
                pointer = child.keys[0]
            else:
                child.parent = None
                if child_left := child.left:
                    pointer = child_left.keys[0]
            if child_index == 0:
                new_pointer = self.keys.pop(0)
                if self.parent:
                    self.parent.update_pointer(pointer, new_pointer)
            elif pointer is not None:
                self.remove_pointer(pointer)
            # if not self.keys:
            #     self.keys.append(next_child.left.keys[0])

    def remove_children(self, child: Union["InternalNode", "LeafageNode"]):
        """
        """
        self._remove_children(child)

    def split(self):
        """
        分裂自身，从右边拆分出新节点，避免需要更新索引指针
        """
        with BPTRLock:
            right_children = self.children[self.tree.half_full + 1:]
            for child in right_children:
                self._remove_children(child)
            right_node = InternalNode(
                self.tree,
                parent=self.parent, children=right_children
            )
            if not self.parent:
                new_parent = InternalNode(
                    self.tree,
                    children=[self, right_node]
                )
                self.tree.root = self.parent = new_parent
                right_node.parent = new_parent
            else:
                self.parent.add_children(right_node)

    def merge(self):
        """
        键数量不到半满
        避免更新边界，尽可能从右边往左边合并
        """
        with BPTRLock:
            if not self.parent or len(self.parent.children) == 1:
                return
            index = self.parent.children.index(self)
            if index > 0:
                left = self.parent.children[index - 1]
                try:
                    right = self.parent.children[index + 1]
                except IndexError:
                    right = None
            else:
                left = None
                right = self.parent.children[1]
            if left and (
                    len(left.children) + len(self.children) <=
                    self.tree.branching_factor + 1
            ):
                dest = "left"
            elif right and (
                    len(right.children) + len(self.children) <=
                    self.tree.branching_factor + 1
            ):
                dest = "right"
            else:
                dest = None
            if dest == "left":
                self.parent._remove_children(self)
                for child in self.children:
                    # self._remove_children(child)
                    left._add_children(child)
            elif dest == "right":
                self.parent._remove_children(right)
                for child in right.children:
                    # right._remove_children(child)
                    self._add_children(child)

    def __repr__(self):
        debug = (
            f"<{self.NodeType}: "
            f"[{', '.join(str(k) for k in self.keys)}]({len(self.keys)}) "
            f"children({len(self.children)})>"
        )
        return debug


class LeafageNode:
    """
    使用线程递归锁
    """
    NodeType = "Leafage"

    def __init__(
        self, tree: "BPTree",
        keys: Iterable,
        datas: list[Any],
        internal: "InternalNode"=None,
        previous_leafage: "LeafageNode" = None,
        next_leafage: "LeafageNode" = None
    ):
        """
        :param tree: BPlusTree
        :param keys: 外部数据的索引
        :param datas: 已索引的外部数据，可以看作缓存
        :param internal: 关联的上级内部节点
        :param next_leafage: 链接的下一个叶子节点
        """
        self.tree = tree
        self.max_num = self.tree.branching_factor
        self.keys: list[int] = list()
        if keys:
            self.keys.extend(keys)
        if internal:
            self.internal = internal
        else:
            self.internal: Optional[InternalNode] = None
        self.datas: list[Any] = list()
        if datas:
            self.datas.extend(datas)
        if previous_leafage:
            self.previous_leafage = previous_leafage
            previous_leafage.next_leafage = self
        else:
            self.previous_leafage: Optional["LeafageNode"] = None
        if next_leafage:
            self.next_leafage = next_leafage
            next_leafage.previous_leafage = self
        else:
            self.next_leafage: Optional["LeafageNode"] = None

    def balanced(self):
        internal = self.internal
        if not self.datas:
            if self.previous_leafage:
                self.previous_leafage.next_leafage = self.next_leafage
            if self.next_leafage:
                self.next_leafage.previous_leafage = self.previous_leafage
            if not internal:
                self.keys.pop()
                return
            internal.remove_children(self)
        if len(self.keys) < self.tree.half_full and internal:
            self.merge()
        elif len(self.keys) > self.tree.branching_factor + 1:
            self.split()
        if internal:
            internal.balanced()

    def add_data(self, key, data):
        with BPTRLock:
            if key in self.keys:
                raise KeyExistsError(key)
            if not self.keys:
                index = 0
                old_key = None
            elif key > self.keys[-1]:
                index = len(self.keys)
                old_key = None
            else:
                index = bisect.bisect(self.keys, key)
                old_key = self.keys[index]
            self.keys.insert(index, key)
            self.datas.insert(index, data)
            if (
                    old_key is not None
                    and index == 0
                    and self.internal
            ):
                self.internal.update_pointer(old_key, key)

    def remove_data(self, key):
        with BPTRLock:
            index = self.keys.index(key)
            self.datas.pop(index)
            if self.datas:
                self.keys.pop(index)
                if index == 0 and self.internal:
                    self.internal.update_pointer(key, self.keys[0])

    def update_data(self, key, data):
        with BPTRLock:
            index = self.keys.index(key)
            self.datas[index] = data

    def split(self):
        """
        往右边拆分自身，调用上级内部节点的 add_children 方法添加新拆分出来的叶子节点
        """
        with BPTRLock:
            self.keys, right_keys = (
                self.keys[:self.tree.half_full],
                self.keys[self.tree.half_full:]
            )
            self.datas, right_datas = (
                self.datas[:self.tree.half_full],
                self.datas[self.tree.half_full:]
            )
            self.next_leafage = LeafageNode(
                self.tree, right_keys, right_datas,
                previous_leafage=self,
                next_leafage=self.next_leafage
            )
            if not self.internal:
                internal = InternalNode(
                    self.tree,
                    [self, self.next_leafage]
                )
                self.tree.root = self.internal = internal
                self.next_leafage.internal = internal
            else:
                self.internal.add_children(self.next_leafage)

    def merge(self):
        """
        找到距离最近的邻近叶子节点，合并过去，从上级内部节点删除合并前位于右边的叶子节点
        """
        with BPTRLock:
            inf = float("inf")
            key_num = len(self.keys)
            if (
                    previous_l := self.previous_leafage
            ) and (
                    previous_l.internal is self.internal
            ):
                distance = self.keys[0] - previous_l.keys[-1]
                previous_key_num = len(previous_l.keys)
            else:
                distance = previous_key_num = inf
            if (
                    next_l := self.next_leafage
            ) and (
                    next_l.internal is self.internal
            ):
                right_distance = next_l.keys[0] - self.keys[-1]
                next_key_num = len(next_l.keys)
            else:
                right_distance = next_key_num = inf

            def left_merge():
                if self.internal:
                    self.internal.remove_children(self)
                previous_l.keys.extend(self.keys)
                previous_l.datas.extend(self.datas)
                previous_l.next_leafage = next_l
                if previous_l.next_leafage:
                    previous_l.next_leafage.previous_leafage = previous_l
                self.previous_leafage = self.next_leafage = None

            def right_merge():
                if self.internal:
                    self.internal.remove_children(next_l)
                self.keys.extend(next_l.keys)
                self.datas.extend(next_l.datas)
                self.next_leafage = next_l.next_leafage
                if self.next_leafage:
                    self.next_leafage.previous_leafage = self
                next_l.previous_leafage = None
                next_l.next_leafage = None

            if (distance == right_distance == inf) and self.internal:
                if self.tree.root is self.internal:
                    self.tree.root = self
                return
            elif (
                    previous_key_num + key_num <= self.tree.branching_factor
                    and next_key_num + key_num <= self.tree.branching_factor
            ):
                if distance < right_distance:
                    left_merge()
                else:
                    right_merge()
            elif previous_key_num + key_num <= self.tree.branching_factor:
                left_merge()
            elif next_key_num + key_num <= self.tree.branching_factor:
                right_merge()

    def __repr__(self):
        debug = (
            f"<{self.NodeType}: "
            f"[{', '.join(str(k) for k in self.keys)}] "
            f"datas({len(self.datas)})>"
        )
        return debug


class BPTree:
    """
    当前线程使用递归锁

    branching factor = 3
    half full = ceil((3 + 1) / 2) = floor(3 / 2) + 1 = 2
    min num keys = (half full)
    max num keys = (half full) * 2 + 1
    min num children = (min num keys) + 1
    max num children (max num keys) + 1
    children = (keys) + 1
    """

    def __init__(
        self,
        branching_factor: int,
        root_node: Union[InternalNode, LeafageNode, None] = None,
    ):
        self.branching_factor = branching_factor
        self.half_full = math.floor(self.branching_factor / 2) + 1
        self.root = root_node

    def nearest_search(self, key: int) -> LeafageNode:
        """
        返回索引键范围包含该键的叶子节点
        """
        with BPTRLock:
            if not self.root:
                raise RuntimeError("Empty Tree")
            if isinstance(self.root, LeafageNode):
                return self.root
            node = self.root
            while 1:
                for index, pointer in enumerate(node.keys):
                    if key < pointer:
                        node = node.children[index]
                        break
                else:
                    node = node.children[-1]
                if isinstance(node, LeafageNode):
                    break
            return node

    def range_query(self, start: int, end: int) -> deque[tuple]:
        with BPTRLock:
            datas = deque()
            leafage = self.nearest_search(start)
            while 1:
                if start > leafage.keys[-1]:
                    if leafage.next_leafage:
                        leafage = leafage.next_leafage
                        continue
                    break
                elif end < leafage.keys[0]:
                    break
                elif start < leafage.keys[-1]:
                    for index, key in enumerate(leafage.keys):
                        if start <= key <= end:
                            datas.append((key, leafage.datas[index]))
            return datas

    def get(self, key: int) -> Any:
        with BPTRLock:
            leafage = self.nearest_search(key)
            if key in leafage.keys:
                index = leafage.keys.index(key)
                return leafage.datas[index]
            raise KeyError(f"not found {key} from {leafage}")

    def insert(self, key: int, data):
        with BPTRLock:
            if self.root:
                leafage = self.nearest_search(key)
                if (
                        key > leafage.keys[-1]
                        and len(leafage.keys) == self.branching_factor
                        and (next_leafage := leafage.next_leafage)
                        and len(next_leafage.keys) < self.branching_factor
                ):
                    leafage = next_leafage
                if key in leafage.keys:
                    raise KeyExistsError(key)
                try:
                    leafage.add_data(key, data)
                    leafage.balanced()
                except Exception as e:
                    raise RuntimeError(
                        f"add {key} to {leafage}({leafage.internal}) failed."
                        f"\n{self}"
                    ) from e
            else:
                leafage = LeafageNode(self, [key], [data])
                self.root = leafage

    def remove(self, key: int):
        with BPTRLock:
            leafage = self.nearest_search(key)
            leafage.remove_data(key)
            leafage.balanced()

    def update(self, key: int, data):
        with BPTRLock:
            leafage = self.nearest_search(key)
            leafage.update_data(key, data)

    def __bool__(self):
        if self.root:
            return True
        return False

    def __repr__(self):
        g = "<BPTree "
        if not self.root:
            g += "root: None>"
            return g
        g += f"root: {self.root} >"
        # nodes = [self.root]
        # while nodes:
        #     info = []
        #     new_nodes = []
        #     for node in nodes:
        #         info.append(str(node))
        #         if isinstance(node, InternalNode):
        #             new_nodes.extend(node.children)
        #     info = "    ".join(info)
        #     g += f"{info}\n"
        #     nodes = new_nodes
        return g

    @staticmethod
    def dump(
        bpt, file_path: Union[Path, str], data_to_bytes: Callable[Any, bytes]
    ):
        """
        持久化
        :bpt BPTree instance
        :param file_path: 要保持的文件路径
        :param data_to_bytes: 将数据处理成字节的回调函数
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)
        s = "BPTBranchingFactor:".encode("utf-8")
        s += bpt.branching_factor.to_bytes(8, "big")
        with BPTRLock:
            if not bpt.root:
                raise RuntimeError("Empty Tree")
            node = bpt.root
            while 1:
                if isinstance(node, LeafageNode):
                    break
                node = node.children[0]
            while node.next_leafage:
                for i, k in enumerate(node.keys):
                    d = data_to_bytes(node.datas[i])
                    k = k.to_bytes(32, "big")
                    d = len(d).to_bytes(32, "big") + k + d
                    s += d
                node = node.next_leafage
            file_path.write_bytes(s)

    @staticmethod
    def load(
        file_path: Union[Path, str],
        bytes_to_data: Callable[bytes, Any]
    ) -> "BPTree":
        """
        从文件加载
        :param file_path: 要保持的文件路径
        :param bytes_to_data: 将字节处理成数据的回调函数
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)
        s = file_path.read_bytes()
        head, s = s[:27], s[27:]
        head, branching_factor = head[:19], head[19:]
        if head.decode("utf-8") != "BPTBranchingFactor:":
            raise TypeError("Not a BPTree")
        bpt = BPTree(int.from_bytes(branching_factor, "big"))
        while s:
            l, k = s[:32], s[32:64]
            l = int.from_bytes(l, "big")
            b, s = s[64:64 + l], s[64+l:]
            k = int.from_bytes(k, "big")
            data = bytes_to_data(b)
            bpt.insert(k, data)
        return bpt


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
                # TODO: 计算最优阶数？
                self._index = BPTree(Settings.BURROW_BPT_INDEX_ORDER)
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
        if self._index:
            index_node = self._index.nearest_search(ki)
            if ki in index_node.keys:
                raise KeyExistsError(key)
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
