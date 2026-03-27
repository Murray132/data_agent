#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
DATA AGENT 启动脚本

使用方法:
    python run.py                    # 使用默认配置启动
    python run.py --port 8080        # 指定端口
    python run.py --host 127.0.0.1   # 指定主机
    python run.py --init-db          # 初始化数据库
    
环境变量:
    OPENAI_API_KEY     - OpenAI 兼容 API 密钥
"""

import argparse
import os
import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root.parent / "src"))


def init_database():
    """初始化数据库"""
    from data.init_database import init_database as do_init
    do_init()


def start_server(host: str, port: int, reload: bool = False):
    """启动服务器"""
    import uvicorn
    from backend.api.server import app
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   DATA AGENT - 数据治理智能体平台                            ║
║                                                              ║
║   基于 AgentScope 框架开发                                   ║
║                                                              ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║   服务地址: http://{host}:{port}                              
║                                                              ║
║   API文档:  http://{host}:{port}/docs                         
║                                                              ║
║   功能模块:                                                  ║
║     - 数据表管理: 查看表结构、数据预览、编辑元数据           ║
║     - 元数据补全: AI自动生成表和字段的描述                   ║
║     - SQL生成:    自然语言转SQL查询                          ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    # 检查API密钥配置
    if not os.environ.get("OPENAI_API_KEY"):
        print("\n[警告] 未检测到API密钥配置，AI智能体功能可能无法使用。")
        print("请设置环境变量 OPENAI_API_KEY\n")
    
    uvicorn.run(app, host=host, port=port, reload=reload)


def main():
    parser = argparse.ArgumentParser(description="DATA AGENT 启动脚本")
    parser.add_argument("--host", default="0.0.0.0", help="服务器主机地址")
    parser.add_argument("--port", type=int, default=8000, help="服务器端口")
    parser.add_argument("--init-db", action="store_true", help="初始化数据库")
    parser.add_argument("--reload", action="store_true", help="开发模式热更新")
    
    args = parser.parse_args()
    
    # 初始化数据库
    if args.init_db:
        print("正在初始化数据库...")
        init_database()
        print("数据库初始化完成！\n")
    
    # 检查数据库文件是否存在
    db_file = project_root / "data" / "finance.db"
    if not db_file.exists():
        print("数据库文件不存在，正在初始化...")
        init_database()
        print("数据库初始化完成！\n")
    
    # 启动服务器
    start_server(args.host, args.port, reload=args.reload)


if __name__ == "__main__":
    main()
