# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2025/3/13 15:47
import itertools
import logging
import math
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Optional, Union, overload

from .exceptions import KeyExistsError
from .utils import RWLock, b, b_1, common_prefix


logger = logging.getLogger(__name__)


BPTLock = RWLock()


class _KeyDataPair:
    def __init__(
        self,
        leafage: "LeafageNode",
        key: Union[int, str],
        data: Any,
    ):
        self.leafage = leafage
        self.key = key
        self.data = data

    def __lt__(self, other: Union["_KeyDataPair", int, str]):
        if isinstance(other, (int, str)):
            return self.key < other
        return self.key < other.key

    def __gt__(self, other: Union["_KeyDataPair", int, str]):
        if isinstance(other, (int, str)):
            return self.key > other
        return self.key > other.key

    def __eq__(self, other: Union["_KeyDataPair", int, str]):
        if isinstance(other, (int, str)):
            return self.key == other
        return self.key == other.key

    def __ne__(self, other: Union["_KeyDataPair", int, str]):
        if isinstance(other, (int, str)):
            return self.key != other
        return self.key != other.key

    def __le__(self, other: Union["_KeyDataPair", int, str]):
        if isinstance(other, (int, str)):
            return self.key <= other
        return self.key <= other.key

    def __ge__(self, other: Union["_KeyDataPair", int, str]):
        if isinstance(other, (int, str)):
            return self.key >= other
        return self.key >= other.key

    def __sub__(self, other: Union["_KeyDataPair", int, str]):
        if isinstance(other, _KeyDataPair):
            other = other.key
        if isinstance(other, int):
            return self.key - other
        cp = common_prefix(self.key, other)
        return max(len(cp), len(self.key)) - len(cp)

    def __rsub__(self, other: Union["_KeyDataPair", int, str]):
        if isinstance(other, _KeyDataPair):
            other = other.key
        if isinstance(other, int):
            return other - self.key
        cp = common_prefix(other, self.key)
        return max(len(cp), len(self.key)) - len(cp)

    def __repr__(self):
        return f"<_KeyDataPair {self.key}: {self.data}>"


class _Node(ABC):

    @property
    @abstractmethod
    def index(self) -> Union[int, str]:
        pass

    @index.setter
    @abstractmethod
    def index(self, value):
        pass

    def __lt__(self, other: Union["_Node", int, str]):
        if isinstance(other, (int, str)):
            return self.index < other
        return self.index < other.index

    def __gt__(self, other: Union["_Node", int, str]):
        if isinstance(other, (int, str)):
            return self.index > other
        return self.index > other.index

    def __eq__(self, other: Union["_Node", int, str]):
        if isinstance(other, (int, str)):
            return self.index == other
        return self.index == other.index

    def __le__(self, other: Union["_Node", int, str]):
        if isinstance(other, (int, str)):
            return self.index <= other
        return self.index <= other.index

    def __ge__(self, other: Union["_Node", int, str]):
        if isinstance(other, (int, str)):
            return self.index >= other
        return self.index >= other.index

    def __iter__(self):
        index = 0
        while 1:
            element = getattr(self, f"_key_{index}", None)
            if element is None:
                break
            yield element
            index += 1

    def indices(self):
        index = 1
        while 1:
            element = getattr(self, f"_key_{index}", None)
            if element is None:
                break
            yield index, element
            index += 1

    @abstractmethod
    def __len__(self):
        pass

    @abstractmethod
    def __getitem__(self, key_index: int):
        pass


class InternalNode(_Node):
    """
    B+Tree 内部节点
    """
    NodeType = "Internal"

    def __init__(
        self,
        tree: "BPTree",
        *children: Union["InternalNode", "LeafageNode"],
        parent: Optional["InternalNode"] = None,
        previous_node: "InternalNode" = None,
        next_node: "InternalNode" = None
    ):
        self.tree = tree
        self._key_num = 0
        self.parent = parent
        self.previous_node = previous_node
        self.next_node = next_node
        self._key_on_parent: int = -1
        if tree.key_type == "int":
            self._index: int = -1
        else:
            self._index: str = ""
        if children:
            if len(children) > self.tree.key_max_num:
                raise RuntimeError(
                    "The number of child nodes exceeds "
                    "the maximum number of keys"
                )
            self._index = children[0].index
            pre_c = None
            for i, child in enumerate(children):
                setattr(
                    self,
                    f"_key_{i}",
                    child
                )
                if pre_c is not None:
                    child.previous_node = pre_c
                    pre_c.next_node = child
                pre_c = child
                if isinstance(child, InternalNode):
                    child.parent = self
                    child._key_on_parent = i
                elif isinstance(child, LeafageNode):
                    child.internal = self
                    child._key_on_internal = i
                self._key_num += 1
            self._index = getattr(self, "_key_0").index
        if self.tree.root is None:
            self.tree.root = self

    @property
    def index(self) -> Union[int, str]:
        return self._index

    @index.setter
    def index(self, index: Union[int, str]):
        """
        更新索引
        """
        self._index = index
        if self.parent is not None and self.parent[0] is self:
            self.parent.index = index

    def balanced(self):
        """
        子节点数量超出限制，触发分裂
          要满足分裂后，每个子节点都满足子节点数比键数多一，且键数不小于半满
        子节点数量不够半满，触发合并
        """
        parent = self.parent
        if self._key_num > self.tree.key_max_num:
            self.split()
        elif self._key_num == 0:
            if self.tree.root is self:
                self.tree.root = None
            elif parent is not None:
                parent.remove_children(self)
        elif self._key_num == 1 and self.tree.root is self:
            _child = getattr(self, "_key_0")
            self.tree.root = _child
            if isinstance(_child, InternalNode):
                _child.parent = None
            else:
                _child.internal = None
        elif self._key_num < self.tree.half_full and parent is not None:
            self.merge()
        if parent is not None:
            return parent.balanced()
        return None

    def _add_children(self, child: Union["InternalNode", "LeafageNode"]):
        if isinstance(child, InternalNode):
            c_p = child.parent
        else:
            c_p = child.internal
        if c_p is self:
            raise RuntimeError(f"node is already in {self}")
        if self._key_num == 0:
            key_index = 0
            previous_node = next_node = None
        elif child.index > (
                previous_node := getattr(self, f"_key" f"_{self._key_num -1}")
        ):
            next_node = previous_node.next_node
            key_index = self._key_num
        elif child.index < self.index:
            next_node = getattr(self, "_key_0")
            previous_node = next_node.previous_node
            key_index = 0
        else:
            key_index = b(self, child)
            previous_node = getattr(self, f"_key_{key_index - 1}")
            next_node = getattr(self, f"_key_{key_index}")
        if (
                previous_node is not None
                and previous_node is not child
                and previous_node is not child.previous_node
        ):
            child.previous_node = previous_node
            previous_node.next_node = child
        if (
                next_node is not None
                and next_node is not child
                and next_node is not child.next_node
        ):
            child.next_node = next_node
            next_node.previous_node = child
        ki = key_index
        _node = child
        while 1:
            if ki > self._key_num:
                break
            _n  = getattr(self, f"_key_{ki}", None)
            setattr(self, f"_key_{ki}", _node)
            if isinstance(_node, LeafageNode):
                _node._key_on_internal = ki
            else:
                _node._key_on_parent = ki
            _node = _n
            ki += 1
        self._key_num += 1
        return key_index

    def add_children(self, child: Union["InternalNode", "LeafageNode"]):
        key_index = self._add_children(child)
        if isinstance(child, LeafageNode):
            child.internal = self
        else:
            child.parent = self
        if key_index == 0:
            self.index = child.index

    def _remove_children(self, child: Union["InternalNode", "LeafageNode"]):
        if isinstance(child, InternalNode):
            key_index = child._key_on_parent
        else:
            key_index = child._key_on_internal
        if getattr(self, f"_key_{key_index}", None) is not child:
            raise RuntimeError(f"{child} is not a child of {self}")
        if isinstance(child, LeafageNode):
            child._key_on_internal = -1
        else:
            child._key_on_parent = -1
        if child.previous_node is not None:
            child.previous_node.next_node = child.next_node
        if child.next_node is not None:
            child.next_node.previous_node = child.previous_node
        child.previous_node = child.next_node = None
        end_index = self._key_num - 1
        ki = key_index
        while 1:
            if ki == end_index:
                delattr(self, f"_key_{ki}")
                break
            _node = getattr(self, f"_key_{ki + 1}")
            setattr(self, f"_key_{ki}", _node)
            if isinstance(_node, LeafageNode):
                _node._key_on_internal = ki
            else:
                _node._key_on_parent = ki
            ki += 1
        self._key_num -= 1
        return key_index

    def remove_children(self, child: Union["InternalNode", "LeafageNode"]):
        key_index = self._remove_children(child)
        if isinstance(child, LeafageNode):
            child.internal = None
        else:
            child.parent = None
        del child
        if key_index == 0:
            if self._key_num:
                self.index = getattr(self, "_key_0").index
            else:
                self.index = -1

    def split(self):
        """
        分裂自身，从右边拆分出新节点，避免需要更新索引指针
        """
        next_node = InternalNode(
            self.tree,
        )
        key_index = self.tree.half_full + 1
        while 1:
            try:
                child = getattr(self, f"_key_{key_index}")
            except AttributeError:
                break
            delattr(self, f"_key_{key_index}")
            self._key_num -= 1
            next_node.add_children(child)
            key_index += 1
        if self.tree.root is self:
            new_parent = InternalNode(
                self.tree,
                self, next_node
            )
            self.tree.root = self.parent = new_parent
        else:
            self.parent.add_children(next_node)

    def merge(self):
        """
        键数量不到半满
        避免更新边界，尽可能从右边往左边合并
        """
        if not self.parent or self.parent._key_num == 1:
            return
        previous_node = None
        if (
                self.previous_node is not None
                and self.previous_node.parent is self.parent
        ):
            previous_node = self.previous_node
        next_node = None
        if (
                self.next_node is not None
                and self.next_node.parent is self.parent
        ):
            next_node = self.next_node
        if previous_node is not None and (
            previous_node._key_num + self._key_num
            <= self.tree.key_max_num
        ):
            source_node = self
            dest_node = previous_node
        elif next_node is not None and (
            next_node._key_num + self._key_num
            <= self.tree.key_max_num
        ):
            source_node = next_node
            dest_node = self
        else:
            return

        for child in source_node:
            dest_node.add_children(child)
        if source_node.parent is not None:
            source_node.parent.remove_children(source_node)

    def __len__(self):
        return self._key_num

    def __getitem__(self, key_index: int) -> Union[Iterable, "InternalNode", "LeafageNode"]:
        if isinstance(key_index, slice):
            return itertools.islice(
                self, key_index.start, key_index.stop, key_index.step
            )
        if key_index < 0:
            key_index = self._key_num + key_index
        try:
            return getattr(self, f"_key_{key_index}")
        except AttributeError:
            raise IndexError("InternalNode index out of range")

    def __bool__(self):
        return self._key_num > 0

    def __repr__(self):
        s = (
            f"<{self.NodeType}: "
            f"index({self.index}) "
            f"children({self._key_num})>"
        )
        return s


class LeafageNode(_Node):
    """
    B+Tree的叶子节点
    """
    NodeType = "Leafage"

    def __init__(
        self, tree: "BPTree",
        *datas: Union[dict, tuple, _KeyDataPair],
        internal: Optional["InternalNode"] = None,
        previous_node: "LeafageNode" = None,
        next_node: "LeafageNode" = None
    ):
        """
        :param tree: BPlusTree
        :param datas: 键数据对
        :param internal: 关联的上级内部节点
        :param previous_leafage: 链接的上一个叶子节点
        :param next_leafage: 链接的下一个叶子节点
        """
        self.tree = tree
        self._key_num = 0
        self.internal = internal
        self.previous_node = previous_node
        self.next_node = next_node
        self._key_on_internal: int = -1
        if tree.key_type == "int":
            self._index: int = -1
        else:
            self._index: str = ""
        if datas:
            if len(datas) > self.tree.key_max_num:
                raise RuntimeError(
                    "The number of child nodes exceeds "
                    "the maximum number of keys"
                )
            for kdp in datas:
                if not isinstance(kdp, _KeyDataPair):
                    if isinstance(kdp, dict):
                        kdp = tuple(kdp.items())[0]
                    kdp = _KeyDataPair(
                        leafage=self, key=kdp[0], data=kdp[1],
                    )
                else:
                    kdp.leafage = self
                setattr(
                    self,
                    f"_key_{self._key_num}",
                    kdp
                )
                self._key_num += 1
            self._index = getattr(self, "_key_0").key
        if self.tree.root is None:
            self.tree.root = self

    @property
    def index(self) -> Union[int, str]:
        return self._index

    @index.setter
    def index(self, index: Union[int, str]):
        """
        更新索引
        """
        self._index = index
        if self.internal is not None and self.internal[0] is self:
            self.internal.index = index

    def balanced(self):
        internal = self.internal
        if self._key_num > self.tree.key_max_num:
            self.split()
        elif self._key_num == 0:
            if internal is not None:
                internal.remove_children(self)
            elif self.tree.root is self:
                self.tree.root = None
        elif self._key_num < self.tree.half_full and internal is not None:
            self.merge()
        if internal is not None:
            return internal.balanced()
        return None

    @overload
    def add_data(self, key: Union[int, str], data) -> int:
        ...

    @overload
    def add_data(self, *, kdp: _KeyDataPair) -> int:
        ...

    def add_data(
        self, key: Union[None, int, str] = None, data: Optional[Any] = None,
        *, kdp: Optional[_KeyDataPair] = None,
    ) -> int:
        if kdp is not None:
            key = kdp.key
        # elif key < 0:
        #     raise IndexError(key)

        if not self._key_num:
            key_index = 0
        elif key > getattr(self, f"_key_{self._key_num - 1}"):
            key_index = self._key_num
        elif key < getattr(self, "_key_0"):
            key_index = 0
        else:
            key_index = b(self, key)
            if key == getattr(self, f"_key_{key_index}"):
                raise KeyExistsError(key)
        if kdp is None:
            kdp = _KeyDataPair(self, key, data)
        kdp.leafage = self
        ki = key_index
        while 1:
            if ki > self._key_num:
                break
            current_kdp = getattr(self, f"_key_{ki}", None)
            setattr(self, f"_key_{ki}", kdp)
            kdp = current_kdp
            ki += 1
        self._key_num += 1
        if key_index == 0:
            self.index = key
        return key_index

    def remove_data(self, k: Union[int, str, _KeyDataPair]):
        if isinstance(k, _KeyDataPair):
            key = k.key
        else:
            key = k
        if key == (kdp := getattr(self, f"_key_{self._key_num - 1}")):
            key_index = self._key_num - 1
        elif key == (kdp := getattr(self, "_key_0")):
                key_index = 0
        else:
            key_index = b(self, key)
            if key != (kdp := getattr(self, f"_key_{key_index}")):
                raise KeyError(f"{key} != {kdp}({self}[{key_index}])")
        kdp.leafage = None
        end_index = self._key_num - 1
        ki = key_index
        while 1:
            if ki == end_index:
                delattr(self, f"_key_{ki}")
                break
            kdp = getattr(self, f"_key_{ki + 1}")
            setattr(self, f"_key_{ki}", kdp)
            ki += 1
        self._key_num -= 1
        if key_index == 0:
            if self._key_num:
                self.index = getattr(self, "_key_0").key
            else:
                self.index = -1

    def update_data(self, key: Union[int, str], data):
        if key == getattr(self, f"_key_{self._key_num - 1}"):
            key_index = self._key_num - 1
        elif key == getattr(self, "_key_0"):
            key_index = 0
        else:
            key_index = b(self, key)
        kdp = getattr(self, f"_key_{key_index}")
        kdp.data = data
        if key_index == 0:
            self.index = getattr(self, "_key_0").key
        return key_index

    def split(self):
        """
        往右边拆分自身，调用上级内部节点的 add_children 方法添加新拆分出来的叶子节点
        """
        next_node = LeafageNode(self.tree)
        key_index = self.tree.half_full + 1
        while 1:
            try:
                kdp = getattr(self, f"_key_{key_index}")
            except AttributeError:
                break
            delattr(self, f"_key_{key_index}")
            self._key_num -= 1
            next_node.add_data(kdp=kdp)
            key_index += 1
        next_node.index = next_node[0].key
        if self.internal is None:
            internal = InternalNode(
                self.tree,
                self, next_node
            )
            self.tree.root = self.internal = internal
        else:
            self.internal.add_children(next_node)

    def merge(self):
        """
        找到距离最近的邻近叶子节点，合并过去，从上级内部节点删除合并前位于右边的叶子节点
        """
        key_num = self._key_num
        previous_leafage = None
        if (
                self.previous_node is not None
                and self.previous_node.internal is self.internal
        ):
            previous_leafage = self.previous_node
        next_leafage = None
        if (
            self.next_node is not None
            and self.next_node.internal is self.internal
        ):
            next_leafage = self.next_node
        first_kdp = getattr(self, "_key_0")
        latest_kdp = getattr(self, f"_key_{self._key_num - 1}")
        if previous_leafage is None:
            if self.tree.key_type == "int":
                previous_leafage_latest_kdp = 0
            else:
                previous_leafage_latest_kdp = ""
        else:
            previous_leafage_latest_kdp = previous_leafage[-1]
        if next_leafage is None:
            if self.tree.key_type == "int":
                next_leafage_first_kdp = 0
            else:
                next_leafage_first_kdp = ""
        else:
            next_leafage_first_kdp = next_leafage[0]
        previous_distance = first_kdp - previous_leafage_latest_kdp
        next_distance = latest_kdp - next_leafage_first_kdp

        if (
                previous_leafage
                and next_leafage
                and len(previous_leafage) + 2 * key_num + len(next_leafage)
                <= 2 * self.tree.key_max_num
        ):
            if previous_distance < next_distance:
                source_node = self
                dest_node = previous_leafage
            else:
                source_node = next_leafage
                dest_node = self
        elif (
                previous_leafage
                and len(previous_leafage) + key_num <= self.tree.key_max_num
        ):
            source_node = self
            dest_node = previous_leafage
        elif (
                next_leafage
                and len(next_leafage) + key_num <= self.tree.key_max_num
        ):
            source_node = next_leafage
            dest_node = self
        else:
            return

        for data_pair in source_node:
            dest_node.add_data(kdp=data_pair)
        if source_node.internal is not None:
            source_node.internal.remove_children(source_node)

    def __len__(self):
        return self._key_num

    def __getitem__(self, key_index: int) -> Union[Iterable, _KeyDataPair]:
        if isinstance(key_index, slice):
            return itertools.islice(
                self, key_index.start, key_index.stop, key_index.step
            )
        if key_index < 0:
            key_index = self._key_num + key_index
        try:
            return getattr(self, f"_key_{key_index}")
        except AttributeError:
            raise IndexError("LeafageNode index out of range")

    def __bool__(self):
        return self._key_num > 0

    def __repr__(self):
        start = getattr(self, "_key_0", None)
        if start is not None:
            start = start.key
        end = getattr(self, f"_key_{self._key_num - 1}", None)
        if end is not None:
            end = end.key
        s = (
            f"<{self.NodeType}({self.index}): "
            f"[{start} ... {end}] "
            f"datas({self._key_num})>"
        )
        return s


class BPTree:
    """
    branching factor = 3
    half full = ceil((3 + 1) / 2) = floor(3 / 2) + 1 = 2
    min num keys = (half full)
    max num keys = (half full) * 2
    min num children = (min num keys) + 1
    max num children (max num keys) + 1
    children = (keys) + 1
    """

    def __init__(
        self,
        branching_factor: int,
        key_type,
        root_node: Union[InternalNode, LeafageNode, None] = None,
    ):
        self.branching_factor = branching_factor
        self.half_full = math.floor(self.branching_factor / 2) + 1
        self.key_type = key_type
        self.key_max_num = self.half_full * 2 + 1
        self.root = root_node
        self._key_num = 0

    def _nearest_search(self, key: Union[int, str]) -> LeafageNode:
        """
        返回索引键范围包含该键的叶子节点
        """
        if not self.root:
            raise RuntimeError("Empty Tree")
        if isinstance(self.root, LeafageNode):
            return self.root
        node = self.root
        while 1:
            node = node[b_1(node, key)]
            if isinstance(node, LeafageNode):
                break
        return node

    def nearest_search(self, key: Union[int, str]) -> LeafageNode:
        with BPTLock.read_lock():
            return self._nearest_search(key)

    def range_query(
        self, start: Union[int, str], end: Union[int, str]
    ) -> deque[tuple]:
        with BPTLock.read_lock():
            datas = deque()
            leafage = self._nearest_search(start)
            while 1:
                for kdp in leafage:
                    if start <= kdp <= end:
                        datas.append((kdp.key, kdp.data))
                    if kdp > end:
                        return datas
                if leafage.next_node is not None:
                    leafage = leafage.next_node
                else:
                    break
            return datas

    def startswith(self, start: str) -> deque[tuple]:
        with BPTLock.read_lock():
            datas = deque()
            leafage = self._nearest_search(start)
            key_index = b(leafage, start)
            while 1:
                for kdp in leafage[key_index:]:
                    if kdp.key.startswith(start):
                        datas.append((kdp.key, kdp.data))
                    else:
                        return datas
                if leafage.next_node is not None:
                    leafage = leafage.next_node
                else:
                    break
            return datas

    def get(self, key: Union[int, str]) -> Any:
        with BPTLock.read_lock():
            leafage = self._nearest_search(key)
            key_index = b(leafage, key)
            if (_kdp := leafage[key_index]) != key:
                raise KeyError(
                    f"key_index: {key_index} key: {key} "
                    f"leafage: {', '.join(str(kd.key) for kd in leafage)} "
                    f"'{key}' in leafage: {key in leafage}"
                )
            return _kdp.data

    def insert(self, key: Union[int, str], data):
        with BPTLock.write_lock():
            if self.root:
                leafage = self._nearest_search(key)
                leafage.add_data(key, data)
                leafage.balanced()
            else:
                leafage = LeafageNode(self, (key, data))
                self.root = leafage
            self._key_num += 1

            return leafage

    def remove(self, key: Union[int, str]):
        with BPTLock.write_lock():
            leafage = self._nearest_search(key)
            leafage.remove_data(key)
            leafage.balanced()
            self._key_num -= 1

    def update(self, key: Union[int], data):
        with BPTLock.write_lock():
            leafage = self._nearest_search(key)
            leafage.update_data(key, data)

    def __len__(self):
        return self._key_num

    def __contains__(self, key: Union[int, str]):
        with BPTLock.read_lock():
            if not self.root:
                return False
            leafage = self._nearest_search(key)
            key_index = b(leafage, key)
            try:
                return key == leafage[key_index]
            except IndexError:
                return False

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
        return g

    @staticmethod
    def dump(
        bpt, file_path: Union[Path, str], data_to_bytes: Callable[..., bytes]
    ):
        """
        持久化
        :param bpt: BPTree instance
        :param file_path: 要保持的文件路径
        :param data_to_bytes: 将数据处理成字节的回调函数
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)
        file_path.unlink(missing_ok=True)
        s = "BPTBranchingFactor:".encode("utf-8")
        s += bpt.branching_factor.to_bytes(8, "big")
        s += bpt.key_type.encode("utf-8")
        if not bpt.root:
            raise RuntimeError("Empty Tree")
        node = bpt.root
        while 1:
            if isinstance(node, LeafageNode):
                break
            node = node[0]
        with file_path.open("ab+") as f:
            f.write(s)
            while node is not None:
                for kdp in node:
                    d = data_to_bytes((kdp.key, kdp.data))
                    d = len(d).to_bytes(32, "big") + d
                    f.write(d)
                node = node.next_node

    @staticmethod
    def load(
        file_path: Union[Path, str],
        bytes_to_data: Callable[..., Any]
    ) -> "BPTree":
        """
        从文件加载
        :param file_path: 要保持的文件路径
        :param bytes_to_data: 将字节处理成数据的回调函数
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)
        with file_path.open("rb") as f:
            end = f.seek(0, 2)
            f.seek(0)
            head = f.read(30)
            head, branching_factor, key_type = head[:19], head[19:27], head[27:]
            head = head.decode("utf-8")
            key_type = key_type.decode("utf-8")
            if head != "BPTBranchingFactor:":
                raise TypeError("Not is BPTree")
            branching_factor = int.from_bytes(branching_factor, "big")
            logger.debug(
                f"BPT head: {head} branching_factor: {branching_factor} "
                f"key_type: {key_type}"
            )
            bpt = BPTree(branching_factor, key_type)
            while f.tell() != end:
                l = f.read(32)
                l = int.from_bytes(l, "big")
                d = f.read(l)
                d = bytes_to_data(d)
                k, data = d[0], d[1]
                if bpt.key_type == "int":
                    k = int(k)
                bpt.insert(k, data)
        return bpt
