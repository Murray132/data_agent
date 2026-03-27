# -*- coding: utf-8 -*-
"""Skills Package - 可复用的Agent技能模块"""

import importlib.util
from pathlib import Path

# 动态导入database-schema-analysis skill的工具（目录名含连字符无法直接import）
_skill_path = Path(__file__).parent / "database-schema-analysis" / "tools" / "db_tools.py"
_spec = importlib.util.spec_from_file_location("db_tools", _skill_path)
db_tools = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(db_tools)

__all__ = ['db_tools']
