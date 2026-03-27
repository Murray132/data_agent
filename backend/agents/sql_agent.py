# -*- coding: utf-8 -*-
"""
SQL生成Agent
基于AgentScope框架开发的智能体，用于根据自然语言需求生成SQL语句

功能说明：
- 理解用户的自然语言查询需求
- 自动识别相关的数据表
- 基于表结构生成准确的SQL语句
- 验证SQL语法正确性
- 返回可执行的SQL及说明

注意：本Agent使用纯粹的Agent Skill方式实现数据库分析能力，
与MetadataAgent使用register_tool_function的方式形成对比。
"""

import os
import sys
import inspect
import traceback
import json
import re
from datetime import datetime
from typing import Optional, List, Dict, Any, Callable
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from agentscope.agent import ReActAgent
from agentscope.model import OpenAIChatModel
from agentscope.formatter import OpenAIChatFormatter
from agentscope.memory import InMemoryMemory
from agentscope.tool import (
    Toolkit, 
    ToolResponse,
    execute_shell_command,
    view_text_file,
)
from agentscope.message import Msg, TextBlock

# 导入数据库服务（仅用于Agent内部工具）
sys.path.insert(0, str(Path(__file__).parent.parent))
from database import db_service

# 导入配置
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
import config


def extract_text_from_content(content_item):
    """
    从content块中安全提取文本内容
    支持TextBlock对象和dict两种格式
    """
    if content_item is None:
        return ""
    if hasattr(content_item, 'text'):
        return content_item.text
    if isinstance(content_item, dict):
        return content_item.get('text', '')
    return str(content_item)


class SQLGenerationAgent:
    """
    SQL生成智能体
    
    该智能体能够：
    1. 理解用户的自然语言查询需求
    2. 搜索和识别相关的数据表
    3. 分析表结构和关联关系
    4. 生成正确的SQL语句
    5. 验证SQL语法并提供执行建议
    
    本Agent使用纯粹的Agent Skill方式实现：
    - 通过register_agent_skill注册Database Schema Analysis Skill
    - 配备view_text_file和execute_shell_command通用工具
    - LLM通过读取SKILL.md了解如何使用技能
    - 通过执行skill目录中的脚本来分析数据库
    
    使用方式：
    ```python
    agent = SQLGenerationAgent(api_key="your_api_key")
    result = await agent.generate_sql("查询所有VIP客户的账户余额")
    ```
    """
    
    # 系统提示词，指导大模型如何生成SQL
    SYSTEM_PROMPT = """你是一个专业的SQL专家，精通数据库查询语句的编写。你的任务是根据用户的自然语言描述，生成准确、高效的SQL查询语句。

你需要遵循以下原则：
1. 先了解数据库中有哪些表，找到与用户需求相关的表
2. 仔细分析表结构，理解字段含义和表之间的关联关系
3. 生成的SQL应该语法正确、逻辑清晰
4. 对于复杂查询，添加适当的注释说明
5. 考虑SQL性能，避免不必要的全表扫描
6. 使用标准SQL语法，确保兼容性

# 重要：使用Agent Skill（渐进式披露）

你已配备了 "Database Schema Analysis" 技能（Agent Skill）。
请按“渐进式披露”方式使用，避免一次性加载过多上下文：
1. 优先直接使用 execute_shell_command 调用 skill 工具命令获取结构化信息
2. 仅当你对 skill 用法不确定时，再使用 view_text_file 查看 SKILL.md
3. 查看 SKILL.md 时优先按范围读取（ranges），先读开头摘要，再按需读取具体章节
4. 不要默认整篇读取 SKILL.md，除非确有必要

# SQL相关工具

除了上述skill，你还可以使用以下SQL专用工具：
- validate_sql: 验证SQL语法是否正确
- execute_sql_query: 执行SQL查询（仅支持SELECT语句）

生成SQL时请注意：
1. 本数据库使用SQLite语法
2. 字符串使用单引号
3. 日期格式为 'YYYY-MM-DD'
4. 布尔值使用 0/1

最终返回的SQL应该包含：
1. SQL语句本身
2. SQL的功能说明
3. 涉及的表和字段说明
"""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None,
        base_url: Optional[str] = None,
        schema_text: Optional[str] = None,
        sql_dialect: str = "SQLite",
        sql_executor: Optional[Callable[[str], Dict[str, Any]]] = None,
        enable_schema_skill: bool = True,
        enable_thinking: Optional[bool] = None,
        temperature: Optional[float] = None,
    ):
        """
        初始化SQL生成智能体（OpenAI-compatible）

        Args:
            api_key: API密钥，默认从配置读取
            model_name: 模型名称，默认从配置读取
            base_url: API基础URL，默认从配置读取
        """
        from config import ModelConfig

        self.api_key = api_key or ModelConfig.get_api_key()
        self.model_name = model_name or ModelConfig.get_model_name()
        self.base_url = base_url or ModelConfig.get_base_url()
        self.schema_text = schema_text
        self.sql_dialect = sql_dialect
        self.sql_executor = sql_executor
        self.enable_schema_skill = enable_schema_skill and not bool(schema_text)
        self.enable_thinking = enable_thinking
        self.temperature = temperature
        
        # 创建工具集
        self.toolkit = self._create_toolkit()
        
        # 创建智能体（延迟初始化）
        self._agent = None
    
    def _create_toolkit(self) -> Toolkit:
        """
        创建工具集，使用纯粹的Agent Skill方式
        
        本方法展示了Agent Skill的正确使用方式：
        1. 使用register_agent_skill注册技能目录
        2. 配备view_text_file工具让LLM能读取SKILL.md
        3. 配备execute_shell_command工具让LLM能执行skill脚本
        4. LLM按需阅读SKILL.md了解如何使用技能（渐进式披露）
        
        这与MetadataAgent使用register_tool_function直接注册工具函数的方式形成对比。
        
        Returns:
            Toolkit: 工具集对象
        """
        toolkit = Toolkit()
        self.skill_dir = str(Path(__file__).parent.parent.parent / "skills" / "database-schema-analysis")
        print(f"\n{'='*60}")
        if self.enable_schema_skill:
            toolkit.register_agent_skill(self.skill_dir)
            print(f"[SQLAgent] 使用纯粹的 Agent Skill 方式")
            print(f"{'='*60}")
            for skill_name, skill_info in toolkit.skills.items():
                print(f"[SQLAgent] Skill注册成功:")
                print(f"  - 名称 (name): {skill_info['name']}")
                print(f"  - 描述 (description): {skill_info['description']}")
                print(f"  - 目录 (dir): {skill_info['dir']}")
            skill_prompt = toolkit.get_agent_skill_prompt()
            if skill_prompt:
                print(f"[SQLAgent] Skill提示词:")
                for line in skill_prompt.split('\n')[:10]:
                    print(f"  {line}")
                if skill_prompt.count('\n') > 10:
                    print(f"  ... (共 {skill_prompt.count(chr(10))+1} 行)")
            print(f"{'='*60}")
            toolkit.register_tool_function(view_text_file)
            toolkit.register_tool_function(execute_shell_command)
            print(f"[SQLAgent] 已注册通用工具: ['view_text_file', 'execute_shell_command']")
        else:
            print(f"[SQLAgent] 使用内联Schema模式，跳过本地技能工具")
            print(f"{'='*60}")
        
        # ============ 注册SQL专用工具（Agent内部工具） ============
        # 这些工具不属于Skill，是Agent自身的能力
        toolkit.register_tool_function(self.validate_sql)
        toolkit.register_tool_function(self.execute_sql_query)
        
        print(f"[SQLAgent] 已注册SQL专用工具: ['validate_sql', 'execute_sql_query']")
        print(f"{'='*60}\n")
        
        return toolkit

    def _get_system_prompt(self) -> str:
        prompt = self.SYSTEM_PROMPT.replace("本数据库使用SQLite语法", f"当前数据库使用{self.sql_dialect}语法")
        if self.schema_text:
            prompt += f"""

# 当前数据源结构
你当前面对的不是默认本地库，而是服务端已经为你准备好的当前数据源结构。
请严格基于下面这份结构生成SQL，不要假设额外表名或字段名：

{self.schema_text}

你可以使用 validate_sql 和 execute_sql_query 工具验证语法与样例执行结果。
不要再尝试读取本地 skill 文件或调用本地 schema 分析脚本。
"""
        return prompt

    def _execute_sql(self, sql: str) -> Dict[str, Any]:
        if self.sql_executor:
            return self.sql_executor(sql)
        return db_service.execute_sql(sql)

    def _log_exception(self, where: str, err: Exception, extra: Optional[Dict[str, Any]] = None) -> None:
        """统一异常日志打印，便于后端定位问题。"""
        print(f"\n[SQLAgent][ERROR] {where}: {err}")
        if extra:
            print(f"[SQLAgent][ERROR] context: {extra}")
        print(f"[SQLAgent][ERROR] traceback:\n{traceback.format_exc()}")
    
    def _create_agent(self) -> ReActAgent:
        """
        创建ReAct智能体
        
        Returns:
            ReActAgent: 智能体对象
        """
        # 使用OpenAI-compatible模型
        model = OpenAIChatModel(
            model_name=self.model_name,
            api_key=self.api_key,
            client_kwargs={"base_url": self.base_url},
            stream=True,
            generate_kwargs=config.ModelConfig.get_generate_kwargs(
                base_url=self.base_url,
                model_name=self.model_name,
                stream=True,
                enable_thinking=self.enable_thinking,
                temperature=self.temperature,
            ),
        )
        formatter = OpenAIChatFormatter()
        print(f"\n{'='*60}")
        print(f"[SQLAgent] 模型配置信息:")
        print(f"  - 模型名称: {self.model_name}")
        print(f"  - API URL: {self.base_url}")
        print(f"  - API Key: {f'{self.api_key[:8]}...{self.api_key[-4:]}' if self.api_key else 'None'}")
        print(f"{'='*60}\n")
        
        # 创建智能体
        agent = ReActAgent(
            name="SQLExpert",
            sys_prompt=self._get_system_prompt(),
            model=model,
            memory=InMemoryMemory(),
            formatter=formatter,
            toolkit=self.toolkit,
            max_iters=10,
        )
        
        return agent
    
    @property
    def agent(self) -> ReActAgent:
        """延迟初始化智能体"""
        if self._agent is None:
            self._agent = self._create_agent()
        return self._agent
    
    # ============ Agent专用工具函数（非Skill） ============
    
    def validate_sql(self, sql: str) -> ToolResponse:
        """
        验证SQL语法是否正确（通过EXPLAIN）
        
        Args:
            sql: 要验证的SQL语句
            
        Returns:
            ToolResponse: 验证结果
        """
        try:
            # 使用EXPLAIN来验证SQL语法
            explain_sql = f"EXPLAIN {sql}"
            result = self._execute_sql(explain_sql)
            
            if result['success']:
                return ToolResponse(
                    content=[TextBlock(type="text", text="SQL语法验证通过！")],
                    is_last=True
                )
            else:
                return ToolResponse(
                    content=[TextBlock(type="text", text=f"SQL语法错误: {result.get('error', '未知错误')}")],
                    is_last=True
                )
        except Exception as e:
            self._log_exception("validate_sql", e, {"sql": sql})
            return ToolResponse(
                content=[TextBlock(type="text", text=f"验证失败: {str(e)}")],
                is_last=True
            )
    
    def execute_sql_query(self, sql: str, limit: int = 10) -> ToolResponse:
        """
        执行SQL查询并返回结果（仅支持SELECT语句）
        
        Args:
            sql: SQL查询语句
            limit: 结果行数限制
            
        Returns:
            ToolResponse: 查询结果
        """
        try:
            # 安全检查：只允许SELECT语句
            sql_upper = sql.strip().upper()
            if not sql_upper.startswith("SELECT"):
                return ToolResponse(
                    content=[TextBlock(type="text", text="安全限制：只允许执行SELECT查询语句")],
                    is_last=True
                )
            
            # 添加LIMIT限制
            if "LIMIT" not in sql_upper:
                sql = f"{sql} LIMIT {limit}"
            
            result = self._execute_sql(sql)
            
            if result['success']:
                data = result.get('data', [])
                cols = result.get('columns', [])
                
                if not data:
                    return ToolResponse(
                        content=[TextBlock(type="text", text="查询成功，但没有返回数据")],
                        is_last=True
                    )
                
                lines = [f"查询结果 ({len(data)}行):"]
                
                # 格式化输出
                for i, row in enumerate(data[:5], 1):  # 最多显示5行
                    lines.append(f"\n记录 {i}:")
                    for col in cols[:8]:  # 最多显示8个字段
                        val = row.get(col, "")
                        lines.append(f"  {col}: {val}")
                
                if len(data) > 5:
                    lines.append(f"\n... 还有 {len(data) - 5} 条记录")
                
                return ToolResponse(
                    content=[TextBlock(type="text", text="\n".join(lines))],
                    is_last=True
                )
            else:
                return ToolResponse(
                    content=[TextBlock(type="text", text=f"执行失败: {result.get('error', '未知错误')}")],
                    is_last=True
                )
        except Exception as e:
            self._log_exception("execute_sql_query", e, {"sql": sql, "limit": limit})
            return ToolResponse(
                content=[TextBlock(type="text", text=f"执行异常: {str(e)}")],
                is_last=True
            )
    
    # ============ 主要功能方法 ============
    
    async def generate_sql(self, requirement: str, context: Optional[str] = None) -> Dict[str, Any]:
        """
        根据自然语言需求生成SQL语句
        
        Args:
            requirement: 用户的自然语言需求描述
            context: 额外的上下文信息（可选）
            
        Returns:
            Dict: 生成结果，包含：
                - sql: 生成的SQL语句
                - explanation: SQL说明
                - tables_used: 涉及的表
                - raw_response: 原始响应
        """
        # 构建提示消息
        prompt = f"""请根据以下需求生成SQL查询语句：

需求描述：{requirement}
"""
        if context:
            prompt += f"\n补充信息：{context}\n"
        if self.schema_text:
            prompt += f"""
当前数据源类型：{self.sql_dialect}

以下是当前数据源的数据库结构，请严格基于它生成SQL：
{self.schema_text}

请按以下步骤操作：
1. 先理解需求与上面的表结构
2. 找到相关的表和字段
3. 必要时使用 validate_sql 验证语法
4. 可选：使用 execute_sql_query 测试查询结果

最后，请按以下JSON格式返回结果：
```json
{{
    "sql": "SELECT ...",
    "explanation": "这个SQL的功能说明",
    "tables_used": ["table1", "table2"],
    "key_points": ["要点1", "要点2"]
}}
```
"""
        else:
            prompt += """
请按以下步骤操作：
1. 首先使用 list_all_tables 了解数据库中有哪些表
2. 根据需求找到相关的表，使用 get_table_schema 获取表结构
3. 如果涉及多表查询，使用 get_related_tables 了解表之间的关联关系
4. 如果需要，可以使用 get_sample_data 查看数据样例
5. 生成SQL语句，并使用 validate_sql 验证语法
6. 可选：使用 execute_sql_query 测试SQL是否能正确执行

最后，请按以下JSON格式返回结果：
```json
{{
    "sql": "SELECT ...",
    "explanation": "这个SQL的功能说明",
    "tables_used": ["table1", "table2"],
    "key_points": ["要点1", "要点2"]
}}
```
"""
        
        # 调用智能体
        msg = Msg("user", prompt, "user")
        response = await self.agent.reply(msg)
        
        # 解析返回结果
        raw_text = response.get_text_content()
        response_text = raw_text if isinstance(raw_text, str) else str(raw_text or "")
        
        # 尝试从返回文本中提取JSON
        try:
            # 尝试提取JSON块
            json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
                result = json.loads(json_str)
            else:
                # 尝试直接提取SQL
                sql_match = re.search(r'```sql\s*(.*?)\s*```', response_text, re.DOTALL)
                if sql_match:
                    result = {
                        "sql": sql_match.group(1).strip(),
                        "explanation": response_text,
                        "tables_used": [],
                        "key_points": []
                    }
                else:
                    result = {
                        "sql": "",
                        "explanation": response_text,
                        "tables_used": [],
                        "key_points": []
                    }
            
            result["raw_response"] = response_text
            result["requirement"] = requirement
            return result
            
        except json.JSONDecodeError:
            return {
                "sql": "",
                "explanation": "",
                "tables_used": [],
                "key_points": [],
                "raw_response": response_text,
                "requirement": requirement,
                "parse_error": True
            }
    
    async def generate_sql_stream(self, requirement: str, context: Optional[str] = None):
        """
        根据自然语言需求生成SQL语句（流式输出）
        
        该方法会通过yield返回Agent的思考过程和执行步骤，
        手动实现ReAct循环以捕获所有中间工具调用
        
        Args:
            requirement: 用户的自然语言需求描述
            context: 额外的上下文信息（可选）
            
        Yields:
            Dict: 事件数据，包含type和相关信息
        """
        # 确保agent已初始化，这会触发模型配置信息的打印
        _ = self.agent
        
        yield {
            "type": "step",
            "step": 1,
            "title": "初始化Agent",
            "status": "done",
            "message": f"使用模型: {self.model_name}"
        }
        
        # ============ 第一阶段：任务理解和规划 ============
        yield {
            "type": "step",
            "step": 2,
            "title": "任务理解与规划",
            "status": "running",
            "message": "大模型正在理解任务并制定执行计划..."
        }
        
        # 构建包含规划要求的提示
        planning_prompt = f"""请根据以下需求生成SQL查询语句：

需求描述：{requirement}
"""
        if context:
            planning_prompt += f"\n补充信息：{context}\n"
        
        if self.schema_text:
            planning_prompt += f"""
当前数据源类型：{self.sql_dialect}

当前数据源结构如下，请基于这份结构推理并生成SQL：
{self.schema_text}

在开始执行之前，请先：
1. 分析这个查询需求的核心目标是什么
2. 思考需要查询哪些数据、涉及哪些表
3. 规划你打算如何基于现有结构生成SQL

# SQL相关工具
- validate_sql: 验证SQL语法是否正确
- execute_sql_query: 执行SQL查询（仅SELECT）

请先输出你的思考和计划，格式如下：
【任务理解】
（描述你对这个SQL需求的理解）

【执行计划】
1. 第一步：...
2. 第二步：...
...

最终请按以下JSON格式返回结果：
```json
{{
    "sql": "SELECT ...",
    "explanation": "这个SQL的功能说明",
    "tables_used": ["table1", "table2"],
    "key_points": ["要点1", "要点2"]
}}
```
"""
        else:
            # 获取skill目录路径（用于提示词）
            skill_md_path = f"{self.skill_dir}/SKILL.md"
            db_tools_path = f"{self.skill_dir}/tools/db_tools.py"
            
            planning_prompt += f"""
在开始执行之前，请先：
1. 分析这个查询需求的核心目标是什么
2. 思考需要查询哪些数据、涉及哪些表
3. 规划你打算调用哪些工具来获取必要信息

# 使用 Database Schema Analysis Skill（渐进式披露）

你已配备了数据库分析技能。推荐流程：
1. 先直接调用数据库分析命令获取信息（优先）：
   - 列出所有表: execute_shell_command(command="python {db_tools_path} --action list_all_tables")
   - 获取表结构: execute_shell_command(command="python {db_tools_path} --action get_table_schema --table_name <表名>")
   - 获取关联关系: execute_shell_command(command="python {db_tools_path} --action get_related_tables --table_name <表名>")
   - 获取样本数据: execute_shell_command(command="python {db_tools_path} --action get_sample_data --table_name <表名>")
   - 获取字段样本值: execute_shell_command(command="python {db_tools_path} --action get_sample_values --table_name <表名> --column_name <字段名>")
2. 仅当你对skill用法不确定时，再读取SKILL.md：
   - 先读开头摘要: view_text_file(file_path="{skill_md_path}", ranges=[1, 120])
   - 再根据需要读取对应行段，不要默认整篇读取

# SQL相关工具
- validate_sql: 验证SQL语法是否正确
- execute_sql_query: 执行SQL查询（仅SELECT）

请先输出你的思考和计划，格式如下：
【任务理解】
（描述你对这个SQL需求的理解）

【执行计划】
1. 第一步：...
2. 第二步：...
...

然后开始执行计划，调用相应的工具获取信息。

最终请按以下JSON格式返回结果：
```json
{{
    "sql": "SELECT ...",
    "explanation": "这个SQL的功能说明",
    "tables_used": ["table1", "table2"],
    "key_points": ["要点1", "要点2"]
}}
```
"""
        
        # 将AgentScope自动生成的Skill提示注入system prompt
        # 这样可利用register_agent_skill带来的技能元信息（名称/描述/目录）
        skill_prompt = self.toolkit.get_agent_skill_prompt()
        system_prompt = self._get_system_prompt()
        if skill_prompt:
            system_prompt = f"{system_prompt}\n\n{skill_prompt}"

        # 初始化对话消息列表
        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": planning_prompt}
        ]
        
        # 工具定义 - 获取toolkit的JSON schema
        tools_json = self.toolkit.get_json_schemas()
        
        # 创建一个非流式模型用于ReAct循环（OpenAI-compatible）
        non_stream_model = OpenAIChatModel(
            model_name=self.model_name,
            api_key=self.api_key,
            client_kwargs={"base_url": self.base_url},
            stream=False,  # 非流式
            generate_kwargs=config.ModelConfig.get_generate_kwargs(
                base_url=self.base_url,
                model_name=self.model_name,
                stream=False,
                enable_thinking=self.enable_thinking,
                temperature=self.temperature,
            )
        )
        
        # 迭代计数器
        iteration = 0
        max_iterations = 10
        step_num = 2
        total_input_tokens = 0
        total_output_tokens = 0
        total_elapsed_time = 0.0
        
        try:
            while iteration < max_iterations:
                iteration += 1
                
                # 调用模型（非流式，直接返回ChatResponse）
                print(f"\n[SQLAgent] 迭代 {iteration}: 调用大模型...")
                
                # 发送迭代开始事件
                yield {
                    "type": "iteration_start",
                    "iteration": iteration,
                    "message": f"开始第 {iteration} 轮推理"
                }
                
                request_kwargs = {
                    "model": non_stream_model.model_name,
                    "messages": messages,
                    "stream": False,
                    **non_stream_model.generate_kwargs,
                }
                if tools_json:
                    request_kwargs["tools"] = non_stream_model._format_tools_json_schemas(tools_json)

                yield {
                    "type": "model_request",
                    "iteration": iteration,
                    "payload": request_kwargs,
                }

                provider_start = datetime.now()
                provider_response = await non_stream_model.client.chat.completions.create(**request_kwargs)
                response = non_stream_model._parse_openai_completion_response(
                    provider_start,
                    provider_response,
                )

                # 统计token用量（若模型返回usage）
                usage = getattr(response, "usage", None)
                if usage is not None:
                    input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
                    output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
                    elapsed_time = float(getattr(usage, "time", 0.0) or 0.0)

                    total_input_tokens += input_tokens
                    total_output_tokens += output_tokens
                    total_elapsed_time += elapsed_time

                    yield {
                        "type": "usage",
                        "iteration": iteration,
                        "usage": {
                            "input_tokens": input_tokens,
                            "output_tokens": output_tokens,
                            "total_tokens": input_tokens + output_tokens,
                            "time": elapsed_time,
                        },
                        "usage_total": {
                            "input_tokens": total_input_tokens,
                            "output_tokens": total_output_tokens,
                            "total_tokens": total_input_tokens + total_output_tokens,
                            "time": round(total_elapsed_time, 3),
                        }
                    }
                
                # ChatResponse.content 可能是未标注类型，先做显式收窄，避免类型检查误报
                raw_content = getattr(response, "content", None)
                content_blocks = raw_content if isinstance(raw_content, list) else []
                raw_response_payload = provider_response.model_dump() if hasattr(provider_response, "model_dump") else str(provider_response)
                try:
                    serialized_blocks = []
                    for block in content_blocks:
                        if isinstance(block, dict):
                            serialized_blocks.append(block)
                        else:
                            block_type = getattr(block, "type", "")
                            item = {"type": block_type}
                            for attr in ("thinking", "text", "id", "name", "input"):
                                val = getattr(block, attr, None)
                                if val is not None:
                                    item[attr] = val
                            serialized_blocks.append(item)
                except Exception:
                    serialized_blocks = []
                
                print(f"[SQLAgent] 收到响应，content_blocks数量: {len(content_blocks)}")
                yield {
                    "type": "raw_model_response",
                    "iteration": iteration,
                    "payload": raw_response_payload,
                }
                yield {
                    "type": "parsed_model_blocks",
                    "iteration": iteration,
                    "blocks": serialized_blocks,
                }
                
                # 分离工具调用和文本内容
                tool_calls = []
                text_content = []

                def extract_block_text(block: Any) -> str:
                    """兼容 text/thinking 等多种内容块格式，尽可能提取文本。"""
                    # dict风格
                    if isinstance(block, dict):
                        for key in ("text", "thinking", "content", "reasoning", "reasoning_content"):
                            val = block.get(key)
                            if isinstance(val, str) and val.strip():
                                return val
                        return ""
                    # 对象风格
                    for attr in ("text", "thinking", "content", "reasoning", "reasoning_content"):
                        val = getattr(block, attr, None)
                        if isinstance(val, str) and val.strip():
                            return val
                    return ""
                
                for block in content_blocks:
                    block_type = block.get('type', '') if isinstance(block, dict) else getattr(block, 'type', '')
                    print(f"[SQLAgent] 处理block类型: {block_type}")
                    
                    # 检查是否是 ToolUseBlock
                    if block_type == 'tool_use':
                        tool_calls.append(block)
                    elif block_type in ('text', 'thinking', 'reasoning'):
                        text = extract_block_text(block)
                        if text:
                            text_content.append(text)
                    # 兼容旧格式
                    elif isinstance(block, dict):
                        if 'name' in block and 'input' in block:
                            tool_calls.append(block)
                        else:
                            text = extract_block_text(block)
                            if text:
                                text_content.append(text)
                    else:
                        text = extract_block_text(block)
                        if text:
                            text_content.append(text)
                
                # 如果有文本内容，发送思考事件
                if text_content:
                    thinking_text = '\n'.join(text_content)
                    yield {
                        "type": "thinking",
                        "iteration": iteration,
                        "message": thinking_text
                    }
                    
                    # 第一次迭代时，标记规划完成
                    if iteration == 1:
                        yield {
                            "type": "step",
                            "step": step_num,
                            "title": "任务理解与规划",
                            "status": "done",
                            "message": "已完成任务分析和执行计划"
                        }
                else:
                    yield {
                        "type": "thinking",
                        "iteration": iteration,
                        "message": "本轮未返回可展示的文字思考，模型可能直接发起了工具调用，或仅返回了结构化内容。"
                    }
                
                if tool_calls:
                    # 有工具调用
                    print(f"[SQLAgent] 发现 {len(tool_calls)} 个工具调用")
                    
                    # 构建assistant消息
                    assistant_content = '\n'.join(text_content) if text_content else ''
                    
                    # 辅助函数：安全获取属性
                    def safe_get(obj, key, default=None):
                        if isinstance(obj, dict):
                            return obj.get(key, default)
                        return getattr(obj, key, default)

                    # 先将tool_calls标准化为确定结构，避免类型检查器连锁报红
                    normalized_tool_calls: List[Dict[str, Any]] = []
                    for i, tc in enumerate(tool_calls):
                        tc_name_raw = safe_get(tc, "name", "")
                        tc_input_raw = safe_get(tc, "input", {})
                        tc_id_raw = safe_get(tc, "id", f"call_{iteration}_{i}")

                        tc_name = str(tc_name_raw) if tc_name_raw is not None else ""
                        tc_input = tc_input_raw if isinstance(tc_input_raw, dict) else {}
                        tc_id = str(tc_id_raw) if tc_id_raw is not None else f"call_{iteration}_{i}"

                        normalized_tool_calls.append({
                            "id": tc_id,
                            "name": tc_name,
                            "input": tc_input,
                        })
                    
                    # 添加assistant消息到历史（包含工具调用）
                    messages.append({
                        "role": "assistant",
                        "content": assistant_content,
                        "tool_calls": [
                            {
                                "id": tc["id"],
                                "type": "function",
                                "function": {
                                    "name": tc["name"],
                                    "arguments": json.dumps(tc["input"], ensure_ascii=False)
                                }
                            }
                            for tc in normalized_tool_calls
                        ]
                    })
                    
                    # 执行每个工具调用
                    for tc in normalized_tool_calls:
                        tool_name = tc["name"]
                        tool_args = tc["input"]
                        tool_id = tc["id"]

                        # 渐进式披露护栏：
                        # 若模型尝试整篇读取 SKILL.md，则自动降级为按范围读取，
                        # 避免无意义的大段上下文占用。
                        if tool_name == "view_text_file":
                            file_path = str(tool_args.get("file_path", ""))
                            has_ranges = isinstance(tool_args.get("ranges"), list)
                            if file_path.endswith("SKILL.md") and not has_ranges:
                                tool_args = {**tool_args, "ranges": [1, 120]}
                                yield {
                                    "type": "thinking",
                                    "iteration": iteration,
                                    "message": "检测到 SKILL.md 全文读取请求，已自动改为分段读取 ranges=[1,120]（渐进式披露）"
                                }
                        
                        print(f"[SQLAgent] 执行工具: {tool_name}, 参数: {tool_args}")
                        
                        # 检查是否是通过skill执行的命令（execute_shell_command调用db_tools.py）
                        is_skill_execution = (
                            tool_name == 'execute_shell_command' and 
                            'db_tools.py' in str(tool_args.get('command', ''))
                        )
                        
                        step_num += 1
                        # 避免f-string中包含花括号导致格式化错误
                        tool_args_str = json.dumps(tool_args, ensure_ascii=False)[:100]
                        yield {
                            "type": "step",
                            "step": step_num,
                            "title": "调用工具: {}".format(tool_name),
                            "status": "running",
                            "message": "参数: " + tool_args_str + "..."
                        }
                        
                        try:
                            # 通过Toolkit官方调用入口执行工具（兼容同步/异步/流式）
                            if tool_name in self.toolkit.tools:
                                tool_call_block = {
                                    "type": "tool_use",
                                    "id": tool_id,
                                    "name": tool_name,
                                    "input": tool_args or {},
                                }

                                tool_response_stream = await self.toolkit.call_tool_function(tool_call_block)
                                latest_chunk = None
                                async for chunk in tool_response_stream:
                                    latest_chunk = chunk

                                if latest_chunk and latest_chunk.content:
                                    tool_result = extract_text_from_content(latest_chunk.content[0])
                                else:
                                    tool_result = "无结果"
                            else:
                                # 兜底：兼容未注册到toolkit、但在类上存在的方法
                                tool_func = getattr(self, tool_name, None)
                                if tool_func:
                                    tool_response = tool_func(**tool_args)
                                    if inspect.isawaitable(tool_response):
                                        tool_response = await tool_response
                                    tool_result = extract_text_from_content(tool_response.content[0]) if tool_response.content else "无结果"
                                else:
                                    tool_result = f"工具 {tool_name} 不存在"
                            
                            yield {
                                "type": "tool_result",
                                "iteration": iteration,
                                "tool": tool_name,
                                "input": tool_args,
                                "result": tool_result,
                                "is_skill_execution": is_skill_execution
                            }
                            
                            yield {
                                "type": "step",
                                "step": step_num,
                                "title": "调用工具: {}".format(tool_name),
                                "status": "done",
                                "message": "执行完成"
                            }
                            
                            # 添加工具结果到消息历史
                            messages.append({
                                "role": "tool",
                                "tool_call_id": tool_id,
                                "content": tool_result
                            })
                            
                            print(f"[SQLAgent] 工具 {tool_name} 执行完成")
                            
                        except Exception as e:
                            self._log_exception(
                                "generate_sql_stream.tool_call",
                                e,
                                {
                                    "iteration": iteration,
                                    "tool_name": tool_name,
                                    "tool_args": tool_args,
                                },
                            )
                            error_msg = f"工具执行错误: {str(e)}"
                            yield {
                                "type": "error",
                                "message": error_msg,
                                "iteration": iteration,
                                "tool": tool_name,
                                "tool_input": tool_args,
                                "detail": traceback.format_exc(),
                            }
                            messages.append({
                                "role": "tool",
                                "tool_call_id": tool_id,
                                "content": error_msg
                            })
                    
                else:
                    # 没有工具调用，这是最终响应
                    response_text = '\n'.join(text_content) if text_content else ''
                    if not response_text.strip():
                        # 兜底：某些模型把可见内容放在response.text或get_text_content中
                        try:
                            fallback_text = response.get_text_content()
                            if isinstance(fallback_text, str) and fallback_text.strip():
                                response_text = fallback_text
                        except Exception:
                            pass
                    
                    step_num += 1
                    yield {
                        "type": "step",
                        "step": step_num,
                        "title": "生成SQL语句",
                        "status": "done",
                        "message": "SQL生成完成"
                    }
                    
                    yield {
                        "type": "llm_response",
                        "content": response_text
                    }
                    
                    if not response_text.strip():
                        response_text = "模型已完成推理，但未返回可解析的文本内容。请检查模型输出格式或切换模型重试。"
                    print(f"[SQLAgent] 大模型响应完成，响应长度: {len(response_text)} 字符\n")
                    
                    # 解析结果
                    try:
                        json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
                        if json_match:
                            json_str = json_match.group(1)
                            result = json.loads(json_str)
                        else:
                            sql_match = re.search(r'```sql\s*(.*?)\s*```', response_text, re.DOTALL)
                            if sql_match:
                                result = {
                                    "sql": sql_match.group(1).strip(),
                                    "explanation": response_text,
                                    "tables_used": [],
                                    "key_points": []
                                }
                            else:
                                result = {
                                    "sql": "",
                                    "explanation": response_text,
                                    "tables_used": [],
                                    "key_points": []
                                }
                        
                        result["raw_response"] = response_text
                        result["requirement"] = requirement
                        result["usage"] = {
                            "input_tokens": total_input_tokens,
                            "output_tokens": total_output_tokens,
                            "total_tokens": total_input_tokens + total_output_tokens,
                            "time": round(total_elapsed_time, 3),
                        }
                        
                        yield {
                            "type": "result",
                            "data": result
                        }
                        
                    except json.JSONDecodeError:
                        yield {
                            "type": "result",
                            "data": {
                                "sql": "",
                                "explanation": "",
                                "tables_used": [],
                                "key_points": [],
                                "raw_response": response_text,
                                "requirement": requirement,
                                "parse_error": True,
                                "usage": {
                                    "input_tokens": total_input_tokens,
                                    "output_tokens": total_output_tokens,
                                    "total_tokens": total_input_tokens + total_output_tokens,
                                    "time": round(total_elapsed_time, 3),
                                }
                            }
                        }
                    
                    # 退出循环
                    break
                    
        except Exception as e:
            self._log_exception(
                "generate_sql_stream.main_loop",
                e,
                {"requirement": requirement, "context": context, "iteration": iteration},
            )
            yield {
                "type": "error",
                "message": f"执行错误: {str(e)}",
                "iteration": iteration,
                "detail": traceback.format_exc(),
                "context": {
                    "requirement": requirement,
                    "context": context,
                }
            }
    
    async def optimize_sql(self, sql: str) -> Dict[str, Any]:
        """
        优化已有的SQL语句
        
        Args:
            sql: 要优化的SQL语句
            
        Returns:
            Dict: 优化结果
        """
        prompt = f"""请分析并优化以下SQL语句：

```sql
{sql}
```

请检查：
1. 是否有性能问题（如全表扫描、缺少索引利用）
2. 是否可以简化查询逻辑
3. 是否有语法可以改进
4. JOIN顺序是否合理

请返回优化后的SQL和优化说明。
"""
        
        msg = Msg("user", prompt, "user")
        response = await self.agent.reply(msg)
        
        return {
            "original_sql": sql,
            "analysis": response.get_text_content()
        }
    
    async def explain_sql(self, sql: str) -> Dict[str, Any]:
        """
        解释SQL语句的功能
        
        Args:
            sql: 要解释的SQL语句
            
        Returns:
            Dict: 解释结果
        """
        prompt = f"""请解释以下SQL语句的功能：

```sql
{sql}
```

请说明：
1. 这个SQL查询的业务含义是什么
2. 涉及哪些表和字段
3. 各个子句（WHERE、JOIN、GROUP BY等）的作用
4. 预期的返回结果
"""
        
        msg = Msg("user", prompt, "user")
        response = await self.agent.reply(msg)
        
        return {
            "sql": sql,
            "explanation": response.get_text_content()
        }


# 便捷函数：创建SQL生成智能体实例
def create_sql_agent(
    api_key: Optional[str] = None,
    model_name: Optional[str] = None,
    base_url: Optional[str] = None,
    schema_text: Optional[str] = None,
    sql_dialect: str = "SQLite",
    sql_executor: Optional[Callable[[str], Dict[str, Any]]] = None,
    enable_schema_skill: bool = True,
    enable_thinking: Optional[bool] = None,
    temperature: Optional[float] = None,
) -> SQLGenerationAgent:
    """
    创建SQL生成智能体实例（OpenAI-compatible）

    Args:
        api_key: API密钥，默认从配置读取
        model_name: 模型名称，默认从配置读取
        base_url: API基础URL，默认从配置读取

    Returns:
        SQLGenerationAgent: 智能体实例
    """
    from config import ModelConfig

    return SQLGenerationAgent(
        api_key=api_key or ModelConfig.get_api_key(),
        model_name=model_name or ModelConfig.get_model_name(),
        base_url=base_url or ModelConfig.get_base_url(),
        schema_text=schema_text,
        sql_dialect=sql_dialect,
        sql_executor=sql_executor,
        enable_schema_skill=enable_schema_skill,
        enable_thinking=enable_thinking,
        temperature=temperature,
    )
