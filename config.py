# -*- coding: utf-8 -*-
"""
DATA AGENT 配置文件
存储API密钥和其他配置信息
"""

import os

# ============ API密钥配置 ============
# 阿里云百炼 DashScope API密钥
DASHSCOPE_API_KEY = os.environ.get(
    "DASHSCOPE_API_KEY", 
    "sk-ebe590f60fb24ccfb1a943c0cca057c6"
)

# OpenAI API密钥（备用）
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# ============ 模型配置 ============
DEFAULT_MODEL_TYPE = "dashscope"  # 或 "openai"
DEFAULT_MODEL_NAME = "qwen3-max"  # 通义千问

# ============ 服务配置 ============
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000

# ============ 数据库配置 ============
DATABASE_PATH = "data/finance.db"


def get_api_key(model_type: str = None) -> str:
    """
    获取API密钥
    
    Args:
        model_type: 模型类型，"dashscope" 或 "openai"
    
    Returns:
        str: API密钥
    """
    if model_type == "openai":
        return OPENAI_API_KEY
    return DASHSCOPE_API_KEY
