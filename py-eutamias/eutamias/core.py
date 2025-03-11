# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/2/14 22:47
import asyncio
import logging
import secrets
from traceback import format_exception
from typing import Optional, Union
from uuid import uuid4

import zmq.asyncio as z_aio

from gaterpc.core import (
    Context, Gate, Service, GateClusterService, RemoteGate,
)
from gaterpc.utils import interface, msg_pack, to_bytes, run_in_executor
from gaterpc.exceptions import BusyWorkerError, ServiceUnAvailableError
from .global_settings import Settings


logger = logging.getLogger(__name__)


class EutamiasClusterService(GateClusterService):
    """
    “花栗鼠”集群服务
    """

    def __init__(self, eutamias_name, local_eutamias):
        super().__init__(eutamias_name, local_eutamias)
        self.description = Settings.CLUSTER_DESCRIPTION

    async def election(self):
        """
        TODO: 选举引导节点
        """

    async def vote(self):
        """
        TODO: 投票
        """


class Eutamias(Gate):
    """
    多个“花栗鼠”（Gate 节点）组成多主集群

    每一条键值对都有单独的版本控制，每次修改都生成一个版本id，版本id由上一个版本id和当前值来生成，
        只要初始版本id一样，无论在哪个节点生成，所有节点的版本id都会一致；

    因为侧重点是让其他机器能获取当前机器的信息，所以创建键值时会绑定一个元数据用于每个节点做权限判断

        metadata:
        {
            "creator": b"",
            "permission": 1,
            "version": 0,
        }
        creator: 创建该键值对的节点的节点id，创建者有绝对权限
        permission: 其他节点的权限，由创建者定义，当创建者已经被踢出该集群，权限将被置为2
            0: 不可查不可改
            1: 可查不可改
            2: 可查可改
        version: 键值对的版本，每次修改加1

    解决脑裂时，根据权限和版本号来解决每个键值对，可手动设置指定键值对的元数据用于调整；

    集群运行步骤
        1. 运行本地“Eutamias”节点
        2. 运行本地“Burrow”（NoSQL存储）服务
        3. 让本地“Burrow”服务连接“Eutamias”后台
        4. 本地“Eutamias”发现并连接其他“Eutamias”
        5. 同步所有“Eutamias”节点的所有键值对
    集群内部事务同步
        1. 每次连上一个“Eutamias”都要比对每一条键值对的最新版本和元数据，同步不一致的数据
        2. “Eutamias”收到查询请求直接返回本地结果
        3. “Eutamias”收到创建请求，转发给本节点的“Burrow”服务处理
        4. “Eutamias”收到删除和修改的请求，检查该键值对的元数据里的权限，
            等于2则本地处理，小于2则转发给有权限的节点处理，
        5. “Burrow”服务处理完毕后，“Eutamias”原路返回处理结果
        6. 键值对事务成功，就会同步事务和版本id给其他“Eutamias”，并假设其他节点一定会成功
        7. 键值对事务失败，“Eutamias”本地回滚，不同步未处理事务
        8. “花栗鼠”将从其他“花栗鼠”接收的事务交给后台的“洞穴”服务处理
    """

    def __init__(
        self,
        port: int,
        *,
        identity: str = None,
        context: Context = None,
        heartbeat: int = None,
        multicast: bool = True,
        cluster_name: str = None,
    ):
        if not identity:
            identity = f"Eutamias-{secrets.token_hex(8)}"
        super().__init__(
            port,
            identity=identity,
            context=context,
            heartbeat=heartbeat,
            multicast=multicast,
            cluster_name=cluster_name
        )
        self.wal_queue = asyncio.Queue()
        self.register_event_handler("SyncWAL", self.sync_wal)

    async def sync_wal(self, log: dict) -> bytes:
        request_id = to_bytes(uuid4().hex)
        body = (
            msg_pack("sync_wal"),
            msg_pack((log,))
        )
        await self.request_backend(
            Settings.BURROW_SERVICE_NAME,
            self.identity,
            request_id,
            body
        )
        return request_id

    def run(self):
        super().run()

    def stop(self):
        super().stop()
