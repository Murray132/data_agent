# -*- coding: utf-8 -*-
"""
SQL纠错校验Agent
基于AgentScope框架开发的智能体，用于检查SQL的语法正确性和性能问题

功能说明：
- 检查SQL语法错误（关键字拼写、括号匹配等）
- 检测性能问题（笛卡尔积、缺WHERE条件、SELECT *等）
- 生成测试用SQL（三种类型：正常/语法错误/性能问题）
- 提供修复建议和差异高亮
"""

import os
import sys
import json
import re
import random
from typing import Optional, List, Dict, Any
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from agentscope.agent import ReActAgent
from agentscope.model import OpenAIChatModel, DashScopeChatModel
from agentscope.formatter import OpenAIChatFormatter, DashScopeChatFormatter
from agentscope.memory import InMemoryMemory
from agentscope.tool import Toolkit, ToolResponse
from agentscope.message import Msg, TextBlock

# 导入数据库服务
sys.path.insert(0, str(Path(__file__).parent.parent))
from database import db_service


def extract_text_from_content(content_item):
    """
    从content块中安全提取文本内容
    """
    if content_item is None:
        return ""
    if hasattr(content_item, 'text'):
        return content_item.text
    if isinstance(content_item, dict):
        return content_item.get('text', '')
    return str(content_item)


class SQLValidationAgent:
    """
    SQL纠错校验智能体
    
    该智能体能够：
    1. 检查SQL语法错误
    2. 检测性能问题
    3. 生成测试用SQL
    4. 提供修复建议
    
    使用方式：
    ```python
    agent = SQLValidationAgent(api_key="your_api_key")
    result = await agent.validate_sql("SELECT * FROM users")
    ```
    """
    
    # 验证用系统提示词
    VALIDATION_PROMPT = """你是一个专业的SQL审核专家。你的任务是检查SQL语句的正确性和性能问题。

检查项目：
1. 语法错误：
   - 关键字拼写错误（如 SELCET, FORM, WEHRE）
   - 括号不匹配
   - 引号使用错误
   - 缺少逗号或多余逗号
   - 字段名或表名错误

2. 性能问题：
   - 缺少WHERE条件的全表扫描
   - 缺少JOIN ON条件的笛卡尔积
   - SELECT * 未指定具体字段
   - 缺少LIMIT的大表查询
   - 不必要的子查询
   - 不合理的ORDER BY

3. 最佳实践：
   - 建议使用表别名
   - 建议指定具体字段

你可以使用的工具：
- get_database_schema: 获取数据库结构（验证表名和字段名）
- validate_syntax: 使用EXPLAIN验证SQL语法
- check_performance: 检查性能问题

完成检查后，必须以JSON格式返回结果：
```json
{
    "is_valid": true/false,
    "sql_type": "normal/syntax_error/performance_issue",
    "errors": [
        {"position": "位置描述", "message": "错误描述", "suggestion": "修复建议"}
    ],
    "warnings": [
        {"type": "警告类型", "message": "警告描述", "suggestion": "优化建议"}
    ],
    "fixed_sql": "修复后的SQL（如果有错误）"
}
```
"""
    
    # 生成SQL用系统提示词
    GENERATION_PROMPT = """你是一个SQL专家。你的任务是根据数据库结构生成测试用SQL语句。

你需要生成的SQL类型由系统随机决定：
1. 正常SQL: 语法正确、逻辑合理的查询
2. 语法错误SQL: 包含明显语法错误的SQL
3. 性能问题SQL: 语法正确但有性能隐患的SQL

语法错误示例：
- 关键字拼写错误: SELCET, FORM, WEHRE, GRUOP BY
- 括号不匹配: SELECT (a, b FROM table
- 缺少逗号: SELECT a b c FROM table
- 多余逗号: SELECT a, b, FROM table

性能问题示例：
- 缺少WHERE: SELECT * FROM large_table
- 笛卡尔积: SELECT * FROM a, b（无ON条件）
- SELECT *: SELECT * FROM table（应指定具体字段）
- 无LIMIT: SELECT name FROM users ORDER BY id

你可以使用的工具：
- get_database_schema: 获取数据库结构

根据指定的SQL类型生成对应的SQL语句。
"""
    
    def __init__(
        self,
        api_key: str = None,
        model_name: str = "qwen3-max",
        model_type: str = "dashscope",
        base_url: str = None,
    ):
        """
        初始化SQL纠错校验智能体
        """
        self.api_key = api_key or os.environ.get("DASHSCOPE_API_KEY") or os.environ.get("OPENAI_API_KEY")
        self.model_name = model_name
        self.model_type = model_type
        self.base_url = base_url
        
        # 存储验证结果
        self._validation_result = None
        
        # 创建工具集
        self.toolkit = self._create_toolkit()
        
        # 智能体延迟初始化
        self._validation_agent = None
        self._generation_agent = None
    
    def _create_toolkit(self) -> Toolkit:
        """创建工具集"""
        toolkit = Toolkit()
        
        toolkit.register_tool_function(self.get_database_schema)
        toolkit.register_tool_function(self.validate_syntax)
        toolkit.register_tool_function(self.check_performance)
        
        print(f"\n{'='*60}")
        print(f"[SQLValidationAgent] 使用 register_tool_function 注册工具")
        print(f"{'='*60}")
        print(f"[SQLValidationAgent] 已注册工具: ['get_database_schema', 'validate_syntax', 'check_performance']")
        print(f"{'='*60}\n")
        
        return toolkit
    
    def _create_model(self, stream: bool = False):
        """创建模型实例"""
        if self.model_type == "openai":
            return OpenAIChatModel(
                model_name=self.model_name,
                api_key=self.api_key,
                base_url=self.base_url,
                stream=stream,
            )
        else:
            return DashScopeChatModel(
                model_name=self.model_name,
                api_key=self.api_key,
                stream=stream,
            )
    
    # ============ 工具函数 ============
    
    def get_database_schema(self) -> ToolResponse:
        """
        获取数据库结构信息
        
        Returns:
            ToolResponse: 数据库结构描述
        """
        try:
            schema_text = db_service.get_schema_for_llm()
            return ToolResponse(
                content=[TextBlock(type="text", text=schema_text)],
                is_last=False
            )
        except Exception as e:
            return ToolResponse(
                content=[TextBlock(type="text", text=f"获取数据库结构失败: {str(e)}")],
                is_last=False
            )
    
    def validate_syntax(self, sql: str) -> ToolResponse:
        """
        验证SQL语法（使用EXPLAIN）
        
        Args:
            sql: 要验证的SQL语句
            
        Returns:
            ToolResponse: 验证结果
        """
        try:
            # 清理SQL
            sql = sql.strip()
            if sql.endswith(';'):
                sql = sql[:-1]
            
            # 使用EXPLAIN验证
            explain_sql = f"EXPLAIN {sql}"
            result = db_service.execute_sql(explain_sql)
            
            if result['success']:
                return ToolResponse(
                    content=[TextBlock(type="text", text="SQL语法验证通过！")],
                    is_last=False
                )
            else:
                error_msg = result.get('error', '未知错误')
                return ToolResponse(
                    content=[TextBlock(type="text", text=f"SQL语法错误: {error_msg}")],
                    is_last=False
                )
        except Exception as e:
            return ToolResponse(
                content=[TextBlock(type="text", text=f"验证失败: {str(e)}")],
                is_last=False
            )
    
    def check_performance(self, sql: str) -> ToolResponse:
        """
        检查SQL性能问题
        
        Args:
            sql: 要检查的SQL语句
            
        Returns:
            ToolResponse: 性能检查结果
        """
        warnings = []
        sql_upper = sql.upper()
        
        # 检查 SELECT *
        if re.search(r'SELECT\s+\*', sql_upper):
            warnings.append("使用了 SELECT *，建议指定具体字段")
        
        # 检查缺少 WHERE
        if 'WHERE' not in sql_upper and ('SELECT' in sql_upper or 'DELETE' in sql_upper or 'UPDATE' in sql_upper):
            if 'JOIN' not in sql_upper:  # 简单查询
                warnings.append("缺少 WHERE 条件，可能导致全表扫描")
        
        # 检查笛卡尔积
        if re.search(r'FROM\s+\w+\s*,\s*\w+', sql_upper) and 'WHERE' not in sql_upper:
            warnings.append("多表查询缺少 JOIN 条件，可能产生笛卡尔积")
        
        # 检查 JOIN 没有 ON
        join_count = len(re.findall(r'\bJOIN\b', sql_upper))
        on_count = len(re.findall(r'\bON\b', sql_upper))
        if join_count > on_count:
            warnings.append("JOIN 语句缺少 ON 条件")
        
        # 检查没有 LIMIT 的大查询
        if 'SELECT' in sql_upper and 'LIMIT' not in sql_upper:
            if 'ORDER BY' in sql_upper:
                warnings.append("有 ORDER BY 但没有 LIMIT，大数据量时可能很慢")
        
        if warnings:
            return ToolResponse(
                content=[TextBlock(type="text", text="性能警告:\n" + "\n".join(f"- {w}" for w in warnings))],
                is_last=False
            )
        else:
            return ToolResponse(
                content=[TextBlock(type="text", text="未发现明显性能问题")],
                is_last=False
            )
    
    # ============ 主要功能方法 ============
    
    async def validate_sql(self, sql: str) -> Dict[str, Any]:
        """
        验证SQL语句（非流式）
        
        Args:
            sql: 要验证的SQL语句
            
        Returns:
            Dict: 验证结果
        """
        # 先做基础检查
        errors = []
        warnings = []
        is_valid = True
        sql_type = "normal"
        fixed_sql = None
        
        sql_clean = sql.strip()
        sql_upper = sql_clean.upper()
        
        # 检查常见语法错误
        syntax_errors = [
            (r'\bSELCET\b', 'SELCET 应为 SELECT'),
            (r'\bFORM\b', 'FORM 应为 FROM'),
            (r'\bWEHRE\b', 'WEHRE 应为 WHERE'),
            (r'\bGRUOP\s+BY\b', 'GRUOP BY 应为 GROUP BY'),
            (r'\bORDER\s+BT\b', 'ORDER BT 应为 ORDER BY'),
            (r'\bINNER\s+JION\b', 'INNER JION 应为 INNER JOIN'),
            (r'\bLEFT\s+JION\b', 'LEFT JION 应为 LEFT JOIN'),
        ]
        
        for pattern, msg in syntax_errors:
            if re.search(pattern, sql_upper):
                errors.append({
                    "position": "关键字",
                    "message": msg,
                    "suggestion": f"修正拼写错误"
                })
                is_valid = False
                sql_type = "syntax_error"
        
        # 检查括号匹配
        if sql_clean.count('(') != sql_clean.count(')'):
            errors.append({
                "position": "括号",
                "message": "括号不匹配",
                "suggestion": "检查并补齐括号"
            })
            is_valid = False
            sql_type = "syntax_error"
        
        # 使用数据库验证
        syntax_result = self.validate_syntax(sql_clean)
        syntax_text = extract_text_from_content(syntax_result.content[0]) if syntax_result.content else ""
        
        if "语法错误" in syntax_text:
            if is_valid:  # 只有之前没发现错误才更新
                errors.append({
                    "position": "SQL语句",
                    "message": syntax_text,
                    "suggestion": "请检查SQL语法"
                })
                is_valid = False
                sql_type = "syntax_error"
        
        # 检查性能问题
        perf_result = self.check_performance(sql_clean)
        perf_text = extract_text_from_content(perf_result.content[0]) if perf_result.content else ""
        
        if "性能警告" in perf_text:
            for line in perf_text.split('\n'):
                if line.startswith('- '):
                    warning_msg = line[2:]
                    warnings.append({
                        "type": "performance",
                        "message": warning_msg,
                        "suggestion": "建议优化"
                    })
            if sql_type == "normal":
                sql_type = "performance_issue"
        
        # 如果有语法错误，尝试修复
        if sql_type == "syntax_error":
            fixed_sql = sql_clean
            for pattern, msg in syntax_errors:
                if 'SELCET' in pattern:
                    fixed_sql = re.sub(r'\bSELCET\b', 'SELECT', fixed_sql, flags=re.IGNORECASE)
                elif 'FORM' in pattern:
                    fixed_sql = re.sub(r'\bFORM\b', 'FROM', fixed_sql, flags=re.IGNORECASE)
                elif 'WEHRE' in pattern:
                    fixed_sql = re.sub(r'\bWEHRE\b', 'WHERE', fixed_sql, flags=re.IGNORECASE)
                elif 'GRUOP' in pattern:
                    fixed_sql = re.sub(r'\bGRUOP\s+BY\b', 'GROUP BY', fixed_sql, flags=re.IGNORECASE)
        
        return {
            "is_valid": is_valid,
            "sql_type": sql_type,
            "original_sql": sql,
            "errors": errors,
            "warnings": warnings,
            "fixed_sql": fixed_sql
        }
    
    async def validate_sql_stream(self, sql: str):
        """
        验证SQL语句（流式输出）
        
        Args:
            sql: 要验证的SQL语句
            
        Yields:
            Dict: 事件数据
        """
        yield {
            "type": "step",
            "step": 1,
            "title": "初始化Agent",
            "status": "done",
            "message": f"使用模型: {self.model_name}"
        }
        
        yield {
            "type": "step",
            "step": 2,
            "title": "基础语法检查",
            "status": "running",
            "message": "正在检查SQL基础语法..."
        }
        
        sql_clean = sql.strip()
        sql_upper = sql_clean.upper()
        errors = []
        warnings = []
        is_valid = True
        sql_type = "normal"
        fixed_sql = None
        
        # 常见语法错误
        syntax_checks = [
            (r'\bSELCET\b', 'SELCET', 'SELECT', '关键字拼写错误: SELCET 应为 SELECT'),
            (r'\bFORM\b', 'FORM', 'FROM', '关键字拼写错误: FORM 应为 FROM'),
            (r'\bWEHRE\b', 'WEHRE', 'WHERE', '关键字拼写错误: WEHRE 应为 WHERE'),
            (r'\bGRUOP\s+BY\b', 'GRUOP BY', 'GROUP BY', '关键字拼写错误: GRUOP BY 应为 GROUP BY'),
            (r'\bORDER\s+BT\b', 'ORDER BT', 'ORDER BY', '关键字拼写错误: ORDER BT 应为 ORDER BY'),
        ]
        
        fixed_sql = sql_clean
        for pattern, wrong, correct, msg in syntax_checks:
            if re.search(pattern, sql_upper):
                errors.append({
                    "position": "关键字",
                    "message": msg,
                    "suggestion": f"将 {wrong} 改为 {correct}"
                })
                is_valid = False
                sql_type = "syntax_error"
                fixed_sql = re.sub(pattern, correct, fixed_sql, flags=re.IGNORECASE)
        
        # 括号检查
        if sql_clean.count('(') != sql_clean.count(')'):
            errors.append({
                "position": "括号",
                "message": "括号不匹配",
                "suggestion": "检查左右括号数量是否相等"
            })
            is_valid = False
            sql_type = "syntax_error"
        
        yield {
            "type": "step",
            "step": 2,
            "title": "基础语法检查",
            "status": "done",
            "message": f"发现 {len(errors)} 个语法错误"
        }
        
        # 数据库验证
        yield {
            "type": "step",
            "step": 3,
            "title": "数据库语法验证",
            "status": "running",
            "message": "使用EXPLAIN验证SQL..."
        }
        
        syntax_result = self.validate_syntax(sql_clean)
        syntax_text = extract_text_from_content(syntax_result.content[0]) if syntax_result.content else ""
        
        db_syntax_ok = "验证通过" in syntax_text
        
        yield {
            "type": "tool_result",
            "tool": "validate_syntax",
            "result": syntax_text
        }
        
        if not db_syntax_ok and "语法错误" in syntax_text:
            if is_valid:
                errors.append({
                    "position": "SQL语句",
                    "message": syntax_text.replace("SQL语法错误: ", ""),
                    "suggestion": "请检查SQL语法"
                })
                is_valid = False
                sql_type = "syntax_error"
        
        yield {
            "type": "step",
            "step": 3,
            "title": "数据库语法验证",
            "status": "done",
            "message": "验证通过" if db_syntax_ok else "发现语法错误"
        }
        
        # 性能检查
        yield {
            "type": "step",
            "step": 4,
            "title": "性能问题检查",
            "status": "running",
            "message": "检查潜在性能问题..."
        }
        
        perf_result = self.check_performance(sql_clean)
        perf_text = extract_text_from_content(perf_result.content[0]) if perf_result.content else ""
        
        yield {
            "type": "tool_result",
            "tool": "check_performance",
            "result": perf_text
        }
        
        if "性能警告" in perf_text:
            for line in perf_text.split('\n'):
                if line.startswith('- '):
                    warnings.append({
                        "type": "performance",
                        "message": line[2:],
                        "suggestion": "建议优化"
                    })
            if sql_type == "normal":
                sql_type = "performance_issue"
        
        yield {
            "type": "step",
            "step": 4,
            "title": "性能问题检查",
            "status": "done",
            "message": f"发现 {len(warnings)} 个性能警告"
        }
        
        # 生成结果
        yield {
            "type": "step",
            "step": 5,
            "title": "生成校验报告",
            "status": "done",
            "message": "校验完成"
        }
        
        result = {
            "is_valid": is_valid,
            "sql_type": sql_type,
            "original_sql": sql,
            "errors": errors,
            "warnings": warnings,
            "fixed_sql": fixed_sql if fixed_sql != sql_clean else None
        }
        
        yield {
            "type": "result",
            "data": result
        }
    
    async def generate_test_sql(self, sql_type: str = None) -> Dict[str, Any]:
        """
        生成测试用SQL
        
        Args:
            sql_type: SQL类型，可选 'normal', 'syntax_error', 'performance_issue'
                     如果不指定，按概率随机选择：60%正常, 20%语法错误, 20%性能问题
            
        Returns:
            Dict: 包含生成的SQL和类型
        """
        # 确定SQL类型
        if sql_type is None:
            rand = random.random()
            if rand < 0.6:
                sql_type = "normal"
            elif rand < 0.8:
                sql_type = "syntax_error"
            else:
                sql_type = "performance_issue"
        
        # 获取数据库结构
        tables = db_service.get_all_tables()
        if not tables:
            return {"sql": "SELECT 1", "type": sql_type, "error": "无可用表"}
        
        # 随机选择表
        table = random.choice(tables)
        table_name = table["table_name"]
        schema = db_service.get_table_schema(table_name)
        columns = [col["name"] for col in schema.get("columns", [])]
        
        if not columns:
            return {"sql": f"SELECT * FROM {table_name}", "type": sql_type}
        
        # 根据类型生成SQL
        if sql_type == "normal":
            # 正常SQL
            selected_cols = random.sample(columns, min(3, len(columns)))
            sql = f"SELECT {', '.join(selected_cols)} FROM {table_name}"
            
            # 随机添加条件
            if random.random() > 0.5 and len(columns) > 0:
                col = random.choice(columns)
                sql += f" WHERE {col} IS NOT NULL"
            
            sql += " LIMIT 10"
            
        elif sql_type == "syntax_error":
            # 语法错误SQL
            error_type = random.choice(["keyword", "comma", "bracket"])
            
            if error_type == "keyword":
                # 关键字拼写错误
                errors = [
                    ("SELECT", "SELCET"),
                    ("FROM", "FORM"),
                    ("WHERE", "WEHRE"),
                ]
                keyword, wrong = random.choice(errors)
                col = columns[0] if columns else "*"
                
                if keyword == "SELECT":
                    sql = f"SELCET {col} FROM {table_name} LIMIT 5"
                elif keyword == "FROM":
                    sql = f"SELECT {col} FORM {table_name} LIMIT 5"
                else:
                    sql = f"SELECT {col} FROM {table_name} WEHRE {col} IS NOT NULL LIMIT 5"
                    
            elif error_type == "comma":
                # 逗号错误
                if len(columns) >= 2:
                    sql = f"SELECT {columns[0]} {columns[1]} FROM {table_name}"  # 缺少逗号
                else:
                    sql = f"SELECT {columns[0]}, FROM {table_name}"  # 多余逗号
                    
            else:  # bracket
                # 括号不匹配
                col = columns[0] if columns else "*"
                sql = f"SELECT ({col} FROM {table_name}"
                
        else:  # performance_issue
            # 性能问题SQL
            issue_type = random.choice(["select_star", "no_where", "no_limit"])
            
            if issue_type == "select_star":
                sql = f"SELECT * FROM {table_name}"
                
            elif issue_type == "no_where":
                col = columns[0] if columns else "*"
                sql = f"SELECT {col} FROM {table_name} ORDER BY {col}"
                
            else:  # no_limit
                all_cols = ", ".join(columns[:5])
                sql = f"SELECT {all_cols} FROM {table_name}"
        
        return {
            "sql": sql,
            "type": sql_type,
            "table": table_name,
            "description": {
                "normal": "语法正确、逻辑合理的SQL",
                "syntax_error": "包含语法错误的SQL",
                "performance_issue": "有性能隐患的SQL"
            }.get(sql_type, "")
        }


# 便捷函数
def create_sql_validation_agent(
    api_key: str = None,
    model_name: str = "qwen3-max",
    model_type: str = "dashscope"
) -> SQLValidationAgent:
    """
    创建SQL纠错校验智能体实例
    """
    return SQLValidationAgent(
        api_key=api_key,
        model_name=model_name,
        model_type=model_type
    )
