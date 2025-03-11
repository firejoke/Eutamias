# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/11/14 17:07
class ChestnutExistsError(Exception):
    def __init__(self, name):
        super().__init__(f"Chestnut({name}) exists!")


class ChestnutNotFoundError(Exception):
    def __init__(self, path):
        super().__init__(f"Chestnut({path}) not found!")
