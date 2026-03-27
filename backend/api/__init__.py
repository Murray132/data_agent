# -*- coding: utf-8 -*-
"""
API模块
提供RESTful API服务
"""

from .server import app, start_server

__all__ = ["app", "start_server"]
