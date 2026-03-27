# -*- coding: utf-8 -*-
"""
Database Schema Analysis Skill - 共享数据库工具模块

该模块封装了数据库结构分析的通用工具函数，
供元数据补全Agent和SQL生成Agent共同使用。
"""

import sys
from pathlib import Path
from typing import Optional

# 添加项目路径以导入db_service
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "backend"))

from agentscope.tool import ToolResponse
from agentscope.message import TextBlock

# 延迟导入db_service，避免循环依赖
_db_service = None

def _get_db_service():
    """延迟获取数据库服务实例"""
    global _db_service
    if _db_service is None:
        from database import db_service
        _db_service = db_service
    return _db_service


def list_all_tables() -> ToolResponse:
    """
    列出数据库中所有表及其基本信息
    
    Returns:
        ToolResponse: 包含所有表信息的响应
    """
    db = _get_db_service()
    tables = db.get_all_tables()
    
    result_lines = ["数据库表列表:\n"]
    for table in tables:
        name = table.get('table_name', '')  # db_service返回的是'table_name'
        desc = table.get('description', '')
        row_count = table.get('row_count', 0)
        column_count = table.get('column_count', 0)
        
        if desc:
            result_lines.append(f"- {name}: {desc} ({row_count}条数据, {column_count}个字段)")
        else:
            result_lines.append(f"- {name} ({row_count}条数据, {column_count}个字段)")
    
    return ToolResponse(
        content=[TextBlock(type="text", text="\n".join(result_lines))],
        is_last=True
    )


def get_table_schema(table_name: str) -> ToolResponse:
    """
    获取指定表的详细结构信息
    
    Args:
        table_name: 表名
        
    Returns:
        ToolResponse: 包含表结构信息的响应
    """
    db = _get_db_service()
    schema = db.get_table_schema(table_name)
    
    if not schema:
        return ToolResponse(
            content=[TextBlock(type="text", text=f"表 {table_name} 不存在")],
            is_last=True
        )
    
    result_lines = [f"表 {table_name} 的结构:"]
    
    # 表描述
    if schema.get('description'):
        result_lines.append(f"描述: {schema['description']}")
    
    result_lines.append("\n字段列表:")
    
    for col in schema.get('columns', []):
        col_name = col.get('name', '')
        col_type = col.get('type', '')
        constraints = []
        
        if col.get('primary_key'):
            constraints.append('主键')
        if col.get('not_null'):
            constraints.append('非空')
        if col.get('default') is not None:
            constraints.append(f"默认:{col['default']}")
        
        constraint_str = f" [{', '.join(constraints)}]" if constraints else ""
        description = f" -- {col.get('description')}" if col.get('description') else ""
        
        result_lines.append(f"  - {col_name} {col_type}{constraint_str}{description}")
    
    # 外键信息
    foreign_keys = schema.get('foreign_keys', [])
    if foreign_keys:
        result_lines.append("\n外键关系:")
        for fk in foreign_keys:
            result_lines.append(f"  - {fk.get('column')} -> {fk.get('references_table')}.{fk.get('references_column')}")
    
    return ToolResponse(
        content=[TextBlock(type="text", text="\n".join(result_lines))],
        is_last=True
    )


def get_sample_data(table_name: str, limit: int = 5) -> ToolResponse:
    """
    获取表的样本数据
    
    Args:
        table_name: 表名
        limit: 返回条数，默认5条
        
    Returns:
        ToolResponse: 包含样本数据的响应
    """
    db = _get_db_service()
    data = db.get_table_data(table_name, limit=limit)
    
    if not data or not data.get('data'):
        return ToolResponse(
            content=[TextBlock(type="text", text=f"表 {table_name} 无数据或不存在")],
            is_last=True
        )
    
    columns = data.get('columns', [])
    rows = data.get('data', [])  # db_service返回的是'data'不是'rows'
    
    # 限制显示的列数，避免内容过长
    max_cols = 10
    display_cols = columns[:max_cols]
    
    result_lines = [f"表 {table_name} 的样本数据:"]
    if len(columns) > max_cols:
        result_lines.append(f"(仅显示前{max_cols}个字段，共{len(columns)}个字段)")
    
    for i, row in enumerate(rows[:limit], 1):
        result_lines.append(f"\n记录 {i}:")
        for col in display_cols:
            # row是字典格式，使用get方法获取值
            val = row.get(col, '')
            if val is not None and len(str(val)) > 50:
                val = str(val)[:50] + "..."
            result_lines.append(f"  {col}: {val}")
    
    return ToolResponse(
        content=[TextBlock(type="text", text="\n".join(result_lines))],
        is_last=True
    )


def get_related_tables(table_name: str) -> ToolResponse:
    """
    获取与指定表有关联关系的表
    
    Args:
        table_name: 表名
        
    Returns:
        ToolResponse: 包含关联关系的响应
    """
    db = _get_db_service()
    relations = db.get_related_tables(table_name)
    
    result_lines = [f"表 {table_name} 的关联关系:"]
    
    # 引用的表 - db_service返回: column, referenced_table, referenced_column
    references = relations.get('references', [])
    if references:
        result_lines.append("\n该表引用的表:")
        for ref in references:
            result_lines.append(f"  - {table_name}.{ref.get('column')} -> {ref.get('referenced_table')}.{ref.get('referenced_column')}")
    
    # 被引用的表 - db_service返回: table, column, referenced_column
    referenced_by = relations.get('referenced_by', [])
    if referenced_by:
        result_lines.append("\n引用该表的表:")
        for ref in referenced_by:
            result_lines.append(f"  - {ref.get('table')}.{ref.get('column')} -> {table_name}.{ref.get('referenced_column')}")
    
    if not references and not referenced_by:
        result_lines.append("\n该表没有外键关联关系")
    
    return ToolResponse(
        content=[TextBlock(type="text", text="\n".join(result_lines))],
        is_last=True
    )


def get_sample_values(table_name: str, column_name: str, limit: int = 10) -> ToolResponse:
    """
    获取指定字段的样本值
    
    Args:
        table_name: 表名
        column_name: 字段名
        limit: 返回条数，默认10条
        
    Returns:
        ToolResponse: 包含样本值的响应
    """
    db = _get_db_service()
    values = db.get_sample_values(table_name, column_name, limit=limit)
    
    if not values:
        return ToolResponse(
            content=[TextBlock(type="text", text=f"字段 {table_name}.{column_name} 无数据或不存在")],
            is_last=True
        )
    
    result_lines = [f"字段 {table_name}.{column_name} 的样本值:"]
    for val in values:
        result_lines.append(f"  - {val}")
    
    return ToolResponse(
        content=[TextBlock(type="text", text="\n".join(result_lines))],
        is_last=True
    )


# Skill元信息
SKILL_NAME = "Database Schema Analysis"
SKILL_DESCRIPTION = "分析数据库表结构、字段信息和表间关系"

# 导出的工具函数列表
SKILL_TOOLS = [
    list_all_tables,
    get_table_schema,
    get_sample_data,
    get_related_tables,
    get_sample_values,
]


# ============ 命令行接口 ============
# 支持通过shell命令调用skill中的工具

def _extract_text(response: ToolResponse) -> str:
    """从ToolResponse中提取文本内容"""
    if response.content and len(response.content) > 0:
        block = response.content[0]
        if hasattr(block, 'text'):
            return block.text
        if isinstance(block, dict):
            return block.get('text', '')
    return ""


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Database Schema Analysis Skill - 数据库结构分析工具"
    )
    parser.add_argument(
        "--action",
        type=str,
        required=True,
        choices=["list_all_tables", "get_table_schema", "get_sample_data", 
                 "get_related_tables", "get_sample_values"],
        help="要执行的操作"
    )
    parser.add_argument(
        "--table_name",
        type=str,
        help="表名（get_table_schema, get_sample_data, get_related_tables, get_sample_values需要）"
    )
    parser.add_argument(
        "--column_name",
        type=str,
        help="字段名（get_sample_values需要）"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="返回条数限制（默认5）"
    )
    
    args = parser.parse_args()
    
    # 执行对应的操作
    result = None
    
    if args.action == "list_all_tables":
        result = list_all_tables()
        
    elif args.action == "get_table_schema":
        if not args.table_name:
            print("错误: get_table_schema 需要 --table_name 参数")
            sys.exit(1)
        result = get_table_schema(args.table_name)
        
    elif args.action == "get_sample_data":
        if not args.table_name:
            print("错误: get_sample_data 需要 --table_name 参数")
            sys.exit(1)
        result = get_sample_data(args.table_name, limit=args.limit)
        
    elif args.action == "get_related_tables":
        if not args.table_name:
            print("错误: get_related_tables 需要 --table_name 参数")
            sys.exit(1)
        result = get_related_tables(args.table_name)
        
    elif args.action == "get_sample_values":
        if not args.table_name or not args.column_name:
            print("错误: get_sample_values 需要 --table_name 和 --column_name 参数")
            sys.exit(1)
        result = get_sample_values(args.table_name, args.column_name, limit=args.limit)
    
    # 输出结果
    if result:
        print(_extract_text(result))
