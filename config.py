# -*- coding: utf-8 -*-
"""
DATA AGENT 配置文件
存储API密钥和其他配置信息
"""

import os
import json
from pathlib import Path
from typing import Optional, Dict, Any

# ============ 服务配置 ============
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000

# ============ 数据库配置 ============
DATABASE_PATH = "data/finance.db"

# ============ 模型配置文件路径 ============
MODEL_CONFIG_FILE = Path(__file__).parent / "data" / "model_config.json"


class ModelConfig:
    """模型配置管理类"""

    # 默认配置（OpenAI兼容）
    DEFAULT_CONFIG = {
        "base_url": "https://api.openai.com/v1",
        "api_key": "YOUR_OPENAI_API_KEY",
        "model_name": "gpt-4o-mini",
        "enable_thinking": True,
        "temperature": 0.7,
    }

    _cached_config: Optional[Dict[str, Any]] = None

    @classmethod
    def _ensure_config_dir(cls):
        """确保配置目录存在"""
        MODEL_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def load(cls) -> Dict[str, Any]:
        """
        加载模型配置

        Returns:
            Dict: 配置字典，包含 base_url, api_key, model_name
        """
        if cls._cached_config is not None:
            return cls._cached_config

        cls._ensure_config_dir()

        if MODEL_CONFIG_FILE.exists():
            try:
                with open(MODEL_CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                # 验证配置完整性
                required_keys = ['base_url', 'api_key', 'model_name']
                for key in required_keys:
                    if key not in config:
                        config[key] = cls.DEFAULT_CONFIG[key]
                if 'enable_thinking' not in config:
                    config['enable_thinking'] = cls.DEFAULT_CONFIG['enable_thinking']
                if 'temperature' not in config:
                    config['temperature'] = cls.DEFAULT_CONFIG['temperature']
                cls._cached_config = config
                return config
            except Exception as e:
                print(f"[Config] 读取配置文件失败，使用默认配置: {e}")

        # 返回默认配置
        cls._cached_config = cls.DEFAULT_CONFIG.copy()
        return cls._cached_config

    @classmethod
    def save(cls, config: Dict[str, Any]) -> bool:
        """
        保存模型配置

        Args:
            config: 配置字典

        Returns:
            bool: 是否保存成功
        """
        try:
            cls._ensure_config_dir()

            # 只保存需要的字段
            save_config = {
                "base_url": config.get("base_url", cls.DEFAULT_CONFIG["base_url"]),
                "api_key": config.get("api_key", cls.DEFAULT_CONFIG["api_key"]),
                "model_name": config.get("model_name", cls.DEFAULT_CONFIG["model_name"]),
                "enable_thinking": bool(config.get("enable_thinking", cls.DEFAULT_CONFIG["enable_thinking"])),
                "temperature": float(config.get("temperature", cls.DEFAULT_CONFIG["temperature"])),
            }

            with open(MODEL_CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(save_config, f, ensure_ascii=False, indent=2)

            cls._cached_config = save_config
            return True
        except Exception as e:
            print(f"[Config] 保存配置文件失败: {e}")
            return False

    @classmethod
    def get_base_url(cls) -> str:
        """获取API Base URL"""
        return cls.load()["base_url"]

    @classmethod
    def get_api_key(cls) -> str:
        """获取API Key"""
        return cls.load()["api_key"]

    @classmethod
    def get_model_name(cls) -> str:
        """获取模型名称"""
        return cls.load()["model_name"]

    @classmethod
    def clear_cache(cls):
        """清除缓存"""
        cls._cached_config = None

    @classmethod
    def get_generate_kwargs(
        cls,
        base_url: Optional[str] = None,
        model_name: Optional[str] = None,
        stream: bool = True,
        enable_thinking: Optional[bool] = None,
        temperature: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        根据供应商/模型返回需要附加的生成参数。

        兼容规则：部分 Qwen 兼容端点在非流式调用时要求 enable_thinking=false。
        """
        kwargs: Dict[str, Any] = {}
        temperature_value = cls.load().get("temperature", 0.7) if temperature is None else temperature
        if temperature_value is not None:
            kwargs["temperature"] = float(temperature_value)

        if stream:
            return kwargs

        cfg = cls.load()
        url = (base_url or cfg.get("base_url", "") or "").lower()
        model = (model_name or cfg.get("model_name", "") or "").lower()

        enable_thinking_value = cls.load().get("enable_thinking", True) if enable_thinking is None else enable_thinking

        is_qwen_compatible_endpoint = ("aliyuncs.com" in url)
        is_qwen = model.startswith("qwen")

        if is_qwen_compatible_endpoint and is_qwen:
            # OpenAI SDK 的 chat.completions.create 不接受顶层 enable_thinking，
            # 需要通过 extra_body 透传给兼容端点。
            kwargs["extra_body"] = {"enable_thinking": bool(enable_thinking_value)}
            return kwargs

        return kwargs


# 为了向后兼容，保留原有的变量和函数
def get_api_key(model_type: Optional[str] = None) -> str:
    """
    获取API密钥（向后兼容）

    Args:
        model_type: 模型类型（已废弃，忽略）

    Returns:
        str: API密钥
    """
    return ModelConfig.get_api_key()
