# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/9/12 13:56
"""The Module Has Been Build for..."""
import threading
import asyncio
from random import randint


# 创建一个全局锁对象
lock = threading.Lock()
shared_resource = 0


def thread_safe_increment():
    def increment():
        global shared_resource
        temp = shared_resource
        temp += 1
        shared_resource = temp

    with lock:
        increment()


async def coroutine_safe_increment():

    async def increment():
        global shared_resource

        await asyncio.sleep(t := randint(1, 3))  # 这里假设存在一些异步操作
        print(f"sleep {t} end.")
        temp = shared_resource
        temp += 1
        shared_resource = temp

    await asyncio.to_thread(lock.acquire)
    await increment()
    lock.release()


async def amain():
    await asyncio.gather(*[coroutine_safe_increment() for _ in range(10)])


def run_asyncio_coroutines():
    asyncio.run(amain())


# 建立多个线程来测试
threads = []
print("test thread")
for i in range(5):
    thread = threading.Thread(target=thread_safe_increment)
    threads.append(thread)
    thread.start()

print("test asyncio")
# 多个线程中去运行协程
for i in range(2):
    thread = threading.Thread(target=run_asyncio_coroutines)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print(shared_resource)
