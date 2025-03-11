# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/11/27 13:15
import random
import secrets
import sys
import time
from pathlib import Path

from gaterpc.utils import msg_pack, msg_unpack


base_path = Path(__file__).parent
sys.path.append(base_path.parent.as_posix())


from eutamias.global_settings import Settings
from eutamias.burrow import BPTree, LeafageNode
import test_settings


Settings.configure("USER_SETTINGS", test_settings)


def use_time(key_set, td, tl, tbpt):
    dtime = [float("inf"), 0, float("-inf")]
    ltime = [float("inf"), 0, float("-inf")]
    bpttime = [float("inf"), 0, float("-inf")]
    bptrmtime = [float("inf"), 0, float("-inf")]
    i = 20000
    while i:
        try:
            if not key_set:
                break
            tkey = random.choice(key_set)
            st = time.time()
            data = td[tkey]
            ut = time.time() - st
            if ut < dtime[0]:
                dtime[0] = ut
            dtime[1] = (ut + dtime[1]) / 2
            if ut > dtime[2]:
                dtime[2] = ut
            st = time.time()
            data = tl[tkey]
            ut = time.time() - st
            if ut < ltime[0]:
                ltime[0] = ut
            ltime[1] = (ut + ltime[1]) / 2
            if ut > ltime[2]:
                ltime[2] = ut
            if i % 2 == 0:
                st = time.time()
                data = tbpt.get(tkey)
                ut = time.time() - st
                if ut < bpttime[0]:
                    bpttime[0] = ut
                bpttime[1] = (ut + bpttime[1]) / 2
                if ut > bpttime[2]:
                    bpttime[2] = ut
            else:
                # Settings.DEBUG = 2
                st = time.time()
                tbpt.remove(tkey)
                ut = time.time() - st
                if ut < bptrmtime[0]:
                    bptrmtime[0] = ut
                bptrmtime[1] = (ut + bptrmtime[1]) / 2
                if ut > bptrmtime[2]:
                    bptrmtime[2] = ut
                key_set.remove(tkey)
                # Settings.DEBUG = 0
            i -= 1
        except Exception as e:
            print(tbpt)
            raise e
    print(f"dtime: {dtime}")
    print(f"ltime: {ltime}")
    print(f"bpttime: {bpttime}")
    print(f"bptrmtime: {bptrmtime}")


def test():
    Settings.setup()
    l = [0] * 100000
    d = dict()
    ks = list()
    if (bpt_dump := base_path.joinpath("bpt.dump")).exists():
        bpt = BPTree.load(bpt_dump, msg_unpack)
        node = bpt.root
        while 1:
            if isinstance(node, LeafageNode):
                break
            node = node.children[0]
        while node.next_leafage:
            for i, k in enumerate(node.keys):
                _d = node.datas[i]
                d[k] = _d
                l[k] = _d
                ks.append(k)
            node = node.next_leafage
    else:
        bpt = BPTree(100)
        rl = list(range(100000))
        for key in random.sample(rl, 100000):
            data = secrets.token_hex()
            d[key] = data
            l[key] = data
            ks.append(key)
            if not bpt_dump.exists():
                bpt.insert(key, data)

    if not bpt_dump.exists():
        BPTree.dump(
            bpt, bpt_dump,
            msg_pack
        )
    # print(bpt)
    use_time(ks, d, l, bpt)


if __name__ == '__main__':
    test()
