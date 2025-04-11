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
from queue import  Queue
from threading import Thread

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


def _get(bpt, q, name):
    i = 0
    while 1:
        k = q.get()
        if k == name:
            print(f"{name} get {i}")
            return
        elif not isinstance(k, int):
            q.put(k)
            continue
        i += 1
        _ = bpt.get(k)


def _rm(bpt, q, name):
    i = 0
    while 1:
        k = q.get()
        if k == name:
            print(f"{name} remove {i}")
            return
        elif not isinstance(k, int):
            q.put(k)
            continue
        i += 1
        _ = bpt.remove(k)


def test_concurrent():
    bpt_dump = base_path.joinpath("bpt.dump")
    bpt_key = base_path.joinpath("bpt_key.txt")
    bpt = BPTree.load(bpt_dump, msg_unpack)
    q = Queue()
    t1 = Thread(target=_get, args=(bpt, q, "t1"))
    t2 = Thread(target=_rm, args=(bpt, q, "t2"))
    t3 = Thread(target=_get, args=(bpt, q, "t3"))
    t4 = Thread(target=_rm, args=(bpt, q, "t4"))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    with open(bpt_key) as f:
        for l in f:
            q.put(int(l.strip()))
    q.put("t1")
    q.put("t2")
    q.put("t3")
    q.put("t4")
    t1.join()
    t2.join()
    t3.join()
    t4.join()


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
    ks = set()
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
        ks.add(key)

    if dump:
        BPTree.dump(
            bpt, bpt_dump,
            msg_pack
        )
        bpt_key.close()


    while 1:
        try:
            if not ks:
                break
            tkey = ks.pop()

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

        except Exception as e:
            logger.info(bpt)
            raise e
    return dctime, dgtime, drtime, bptctime, bptgtime, bptrtime


def batch_test(key_num:int, number: int):
    i = number
    dctime = [1, 1, 0]
    dgtime = [1, 1, 0]
    drtime = [1, 1, 0]
    bptctime = [1, 1, 0]
    bptgtime = [1, 1, 0]
    bptrtime = [1, 1, 0]
    while i:
        dc, dg, dr, bptc, bptg, bptr = test(key_num, False)
        i -= 1
        dctime[0] = min(dctime[0], dc[0])
        dctime[1] = min(dctime[1], dc[1])
        dctime[2] = max(dctime[2], dc[2])
        dgtime[0] = min(dgtime[0], dg[0])
        dgtime[1] = min(dgtime[1], dg[1])
        dgtime[2] = max(dgtime[2], dg[2])
        drtime[0] = min(drtime[0], dr[0])
        drtime[1] = min(drtime[1], dr[1])
        drtime[2] = max(drtime[2], dr[2])
        bptctime[0] = min(bptctime[0], bptc[0])
        bptctime[1] = min(bptctime[1], bptc[1])
        bptctime[2] = max(bptctime[2], bptc[2])
        bptgtime[0] = min(bptgtime[0], bptg[0])
        bptgtime[1] = min(bptgtime[1], bptg[1])
        bptgtime[2] = max(bptgtime[2], bptg[2])
        bptrtime[0] = min(bptrtime[0], bptr[0])
        bptrtime[1] = min(bptrtime[1], bptr[1])
        bptrtime[2] = max(bptrtime[2], bptr[2])

    logger.info(f"dctime: {dctime}")
    logger.info(f"bptctime: {bptctime}")
    logger.info(f"dgtime: {dgtime}")
    logger.info(f"bptgtime: {bptgtime}")
    logger.info(f"drtime: {drtime}")
    logger.info(f"bptrmtime: {bptrtime}")


if __name__ == '__main__':
    logger = logging.getLogger("eutamias")
    argv = sys.argv[1:]
    if argv[0] == "batch":
        batch_test(int(argv[1]), int(argv[2]))
    elif argv[0] == "load":
        test_load()
    elif argv[0] == "concurrent":
        test_concurrent()
    else:
        dct, dgt, drt, bptct, bptgt, bptrt = test(int(argv[0]))
        logger.info(f"dctime: {dct}")
        logger.info(f"bptctime: {bptct}")
        logger.info(f"dgtime: {dgt}")
        logger.info(f"bptgtime: {bptgt}")
        logger.info(f"drtime: {drt}")
        logger.info(f"bptrmtime: {bptrt}")
