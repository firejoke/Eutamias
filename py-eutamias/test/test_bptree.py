# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/11/27 13:15
import itertools
import logging
import math
import random
import secrets
import sys
import time
from pathlib import Path

from gaterpc.utils import msg_pack, msg_unpack


base_path = Path(__file__).parent
sys.path.append(base_path.parent.as_posix())


from eutamias.global_settings import Settings
import test_settings


Settings.configure("USER_SETTINGS", test_settings)
Settings.setup()


from eutamias.bp_tree import BPTree, InternalNode, LeafageNode


class Seq:
    def __init__(self, *keys):
        self._key_num = 0
        for i, key in enumerate(keys):
            setattr(self, f"_key_{i}", key)
            self._key_num += 1

    def __iter__(self):
        i = 0
        while 1:
            try:
                key = getattr(self, f"_key_{i}")
                yield key
            except AttributeError:
                return
            i += 1

    def __getitem__(self, item):
        if isinstance(item, slice):
            return itertools.islice(self, item.start, item.stop, item.step)
        if item < 0:
            item = len(self) + item
        return getattr(self, f"_key_{item}")

    def __len__(self):
        return self._key_num


def sum_leafage(bpt):
    node = bpt.root
    while 1:
        if isinstance(node, InternalNode):
            node = node[0]
        else:
            break
    key_num = 0
    real_key_num = 0
    _okdp = -1
    while 1:
        if node is None:
            return key_num, real_key_num
        key_num += len(node)
        for kdp in node:
            if kdp.key - _okdp != 1:
                logger.error(f"node: {node}, kdp: {kdp}")
                sys.exit(1)
            _okdp = kdp.key
            real_key_num += 1
        node = node.next_node


def test_load():
    bpt_dump = base_path.joinpath("bpt.dump")
    bpt_key = base_path.joinpath("bpt_key.txt")
    bpt = BPTree.load(bpt_dump, msg_unpack)
    gtime = [1, 0, 0, 0]
    with open(bpt_key) as f:
        for l in f:
            k = int(l.strip())
            st = time.time()
            _ = bpt.get(k)
            ut = time.time() - st
            if ut < gtime[0]:
                gtime[0] = ut
            if ut > gtime[2]:
                gtime[2] = ut
            gtime[1] = (gtime[1] * gtime[3] + ut) / (gtime[3] + 1)
            gtime[3] += 1
    logger.info(f"bptgtime: {gtime}")


def test(key_num: int, dump=True):
    factor = math.ceil(math.pow(key_num, 1/3))
    d = dict()
    ks = list()
    bpt_dump = base_path.joinpath("bpt.dump")
    if dump:
        bpt_key = base_path.joinpath("bpt_key.txt")
        bpt_key.unlink()
        bpt_key = bpt_key.open("a+")
    else:
        bpt_key = None

    dctime = [1, 0, 0, 0]
    dgtime = [1, 0, 0, 0]
    drtime = [1, 0, 0, 0]
    bptctime = [1, 0, 0, 0]
    bptgtime = [1, 0, 0, 0]
    bptrtime = [1, 0, 0, 0]
    bpt = BPTree(factor)
    rl = list(range(key_num))
    for key in random.sample(rl, key_num):
        data = secrets.token_hex()
        st = time.time()
        d[key] = data
        ut = time.time() - st
        if ut < dctime[0]:
            dctime[0] = ut
        if ut > dctime[2]:
            dctime[2] = ut
        dctime[1] = (dctime[1] * dctime[3] + ut) / (dctime[3] + 1)
        dctime[3] += 1

        st = time.time()
        bpt.insert(key, data)
        ut = time.time() - st
        if ut < bptctime[0]:
            bptctime[0] = ut
        if ut > bptctime[2]:
            bptctime[2] = ut
        bptctime[1] = (bptctime[1] * bptctime[3] + ut) / (bptctime[3] + 1)
        bptctime[3] += 1
        # logger.info(f"{bptctime[3]}: bpt insert {key}")

        if bpt_key:
            bpt_key.write(f"{key}\n")
        ks.append(key)

    if dump:
        BPTree.dump(
            bpt, bpt_dump,
            msg_pack
        )
        bpt_key.close()

    logger.info(f"dctime: {dctime}")
    logger.info(f"bptctime: {bptctime}")

    logger.info(f"length of dict: {len(d)}")
    key_num, real_key_num = sum_leafage(bpt)
    bpt_key_num = len(bpt)
    assert bpt_key_num == real_key_num
    logger.info(f"length of bpt: {bpt_key_num}({real_key_num})")
    logger.info(f"length of key-set: {len(ks)}")
    i = key_num * 2
    while i:
        try:
            if not ks:
                break
            tkey = random.choice(ks)
            if i % 2 == 0:
                st = time.time()
                _ = d[tkey]
                ut = time.time() - st
                if ut < dgtime[0]:
                    dgtime[0] = ut
                if ut > dgtime[2]:
                    dgtime[2] = ut
                dgtime[1] = (dgtime[1] * dgtime[3] + ut) / (dgtime[3] + 1)
                dgtime[3] += 1

                st = time.time()
                _ = bpt.get(tkey)
                ut = time.time() - st
                if ut < bptgtime[0]:
                    bptgtime[0] = ut
                if ut > bptgtime[2]:
                    bptgtime[2] = ut
                bptgtime[1] = (bptgtime[1] * bptgtime[3] + ut) / (bptgtime[3] + 1)
                bptgtime[3] += 1
            else:
                st = time.time()
                d.pop(tkey)
                ut = time.time() - st
                if ut < drtime[0]:
                    drtime[0] = ut
                if ut > drtime[2]:
                    drtime[2] = ut
                drtime[1] = (drtime[1] * drtime[3] + ut) / (drtime[3] + 1)
                drtime[3] += 1

                st = time.time()
                bpt.remove(tkey)
                ut = time.time() - st
                if ut < bptrtime[0]:
                    bptrtime[0] = ut
                if ut > bptrtime[2]:
                    bptrtime[2] = ut
                bptrtime[1] = (bptrtime[1] * bptrtime[3] + ut) / (
                            bptrtime[3] + 1)
                bptrtime[3] += 1

                ks.remove(tkey)
            i -= 1
        except Exception as e:
            logger.info(bpt)
            raise e
    logger.info(f"dgtime: {dgtime}")
    logger.info(f"drtime: {drtime}")
    logger.info(f"bptgtime: {bptgtime}")
    logger.info(f"bptrmtime: {bptrtime}")


def batch_test(key_num:int, number: int):
    i = number
    while i:
        test(key_num, False)
        i -= 1


if __name__ == '__main__':
    logger = logging.getLogger("eutamias")
    argv = sys.argv[1:]
    if argv[0] == "batch":
        batch_test(int(argv[1]), int(argv[2]))
    elif argv[0] == "load":
        test_load()
    else:
        test(int(argv[0]))
    # s = Seq(111,35,4645,7567568,42343254,546456)
    # for k in enumerate(s):
    #     print(k)
    # print(len(s))
    # print(*s[0:3])
    # print(s[-1])
    # print(1111 in s)
