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
from typing import Optional, List, Dict, Any
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from agentscope.agent import ReActAgent
from agentscope.model import OpenAIChatModel, DashScopeChatModel
from agentscope.formatter import OpenAIChatFormatter, DashScopeChatFormatter
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

# 重要：使用Agent Skill

你已配备了 "Database Schema Analysis" 技能（Agent Skill）。
使用该技能时，请：
1. 先使用 view_text_file 工具读取skill的SKILL.md文件了解使用方法
2. 根据SKILL.md中的指引，使用 execute_shell_command 执行相应的命令

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
        api_key: str = None,
        model_name: str = "qwen3-max",
        model_type: str = "dashscope",
        base_url: str = None,
    ):
        """
        初始化SQL生成智能体
        
        Args:
            api_key: API密钥，默认从环境变量获取
            model_name: 模型名称，默认使用qwen-plus
            model_type: 模型类型，支持 "dashscope" 或 "openai"
            base_url: API基础URL（仅openai类型需要）
        """
        self.api_key = api_key or os.environ.get("DASHSCOPE_API_KEY") or os.environ.get("OPENAI_API_KEY")
        self.model_name = model_name
        self.model_type = model_type
        self.base_url = base_url
        
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
        4. LLM通过阅读SKILL.md了解如何使用技能
        
        这与MetadataAgent使用register_tool_function直接注册工具函数的方式形成对比。
        
        Returns:
            Toolkit: 工具集对象
        """
        toolkit = Toolkit()
        
        # ============ 使用 register_agent_skill 注册Skill ============
        # 获取skill目录路径
        self.skill_dir = str(Path(__file__).parent.parent.parent / "skills" / "database-schema-analysis")
        
        # 注册Agent Skill
        toolkit.register_agent_skill(self.skill_dir)
        
        # 打印注册的Skill信息，验证注册成功
        print(f"\n{'='*60}")
        print(f"[SQLAgent] 使用纯粹的 Agent Skill 方式")
        print(f"{'='*60}")
        
        # 获取并打印skill详细信息
        for skill_name, skill_info in toolkit.skills.items():
            print(f"[SQLAgent] Skill注册成功:")
            print(f"  - 名称 (name): {skill_info['name']}")
            print(f"  - 描述 (description): {skill_info['description']}")
            print(f"  - 目录 (dir): {skill_info['dir']}")
        
        # 获取skill提示词
        skill_prompt = toolkit.get_agent_skill_prompt()
        if skill_prompt:
            print(f"[SQLAgent] Skill提示词:")
            # 打印完整提示词（因为这是关键信息）
            for line in skill_prompt.split('\n')[:10]:
                print(f"  {line}")
            if skill_prompt.count('\n') > 10:
                print(f"  ... (共 {skill_prompt.count(chr(10))+1} 行)")
        
        print(f"{'='*60}")
        
        # ============ 注册通用工具（用于执行Skill） ============
        # Agent Skill的核心思想：LLM通过读取SKILL.md了解如何使用技能，
        # 然后通过通用工具（view_text_file, execute_shell_command）来执行
        
        # 注册文件查看工具 - 让LLM能读取SKILL.md
        toolkit.register_tool_function(view_text_file)
        
        # 注册shell命令执行工具 - 让LLM能执行skill脚本
        toolkit.register_tool_function(execute_shell_command)
        
        print(f"[SQLAgent] 已注册通用工具: ['view_text_file', 'execute_shell_command']")
        
        # ============ 注册SQL专用工具（Agent内部工具） ============
        # 这些工具不属于Skill，是Agent自身的能力
        toolkit.register_tool_function(self.validate_sql)
        toolkit.register_tool_function(self.execute_sql_query)
        
        print(f"[SQLAgent] 已注册SQL专用工具: ['validate_sql', 'execute_sql_query']")
        print(f"{'='*60}\n")
        
        return toolkit
    
    def _create_agent(self) -> ReActAgent:
        """
        创建ReAct智能体
        
        Returns:
            ReActAgent: 智能体对象
        """
        # 根据模型类型选择相应的模型和格式化器
        if self.model_type == "openai":
            base_url = self.base_url or "https://api.openai.com/v1"
            model = OpenAIChatModel(
                model_name=self.model_name,
                api_key=self.api_key,
                base_url=self.base_url,
                stream=True,
            )
            formatter = OpenAIChatFormatter()
            print(f"\n{'='*60}")
            print(f"[SQLAgent] 模型配置信息:")
            print(f"  - 模型类型: OpenAI")
            print(f"  - 模型名称: {self.model_name}")
            print(f"  - API URL: {base_url}")
            # print(f"  - API Key: {self.api_key[:8]}...{self.api_key[-4:] if self.api_key else 'None'}")
            print(f"{'='*60}\n")
        else:  # dashscope
            base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"
            model = DashScopeChatModel(
                model_name=self.model_name,
                api_key=self.api_key,
                stream=True,
            )
            formatter = DashScopeChatFormatter()
            print(f"\n{'='*60}")
            print(f"[SQLAgent] 模型配置信息:")
            print(f"  - 模型类型: DashScope (阿里云通义)")
            print(f"  - 模型名称: {self.model_name}")
            print(f"  - API URL: {base_url}")
            print(f"  - API Key: {self.api_key[:8]}...{self.api_key[-4:] if self.api_key else 'None'}")
            print(f"{'='*60}\n")
        
        # 创建智能体
        agent = ReActAgent(
            name="SQLExpert",
            sys_prompt=self.SYSTEM_PROMPT,
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
            result = db_service.execute_sql(explain_sql)
            
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
            
            result = db_service.execute_sql(sql)
            
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
            return ToolResponse(
                content=[TextBlock(type="text", text=f"执行异常: {str(e)}")],
                is_last=True
            )
    
    # ============ 主要功能方法 ============
    
    async def generate_sql(self, requirement: str, context: str = None) -> Dict[str, Any]:
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
        response_text = response.get_text_content()
        
        # 尝试从返回文本中提取JSON
        try:
            import json
            import re
            
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
    
    async def generate_sql_stream(self, requirement: str, context: str = None):
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
        import json
        import re
        
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
        
        # 获取skill目录路径（用于提示词）
        skill_md_path = f"{self.skill_dir}/SKILL.md"
        db_tools_path = f"{self.skill_dir}/tools/db_tools.py"
        
        planning_prompt += f"""
在开始执行之前，请先：
1. 分析这个查询需求的核心目标是什么
2. 思考需要查询哪些数据、涉及哪些表
3. 规划你打算调用哪些工具来获取必要信息

# 使用 Database Schema Analysis Skill

你已配备了数据库分析技能，使用方式：

1. 首先使用 view_text_file 读取技能说明：
   view_text_file(file_path="{skill_md_path}")

2. 根据SKILL.md中的指引，使用 execute_shell_command 执行数据库分析命令，例如：
   - 列出所有表: execute_shell_command(command="python {db_tools_path} --action list_all_tables")
   - 获取表结构: execute_shell_command(command="python {db_tools_path} --action get_table_schema --table_name <表名>")
   - 获取关联关系: execute_shell_command(command="python {db_tools_path} --action get_related_tables --table_name <表名>")
   - 获取样本数据: execute_shell_command(command="python {db_tools_path} --action get_sample_data --table_name <表名>")
   - 获取字段样本值: execute_shell_command(command="python {db_tools_path} --action get_sample_values --table_name <表名> --column_name <字段名>")

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
        
        # 初始化对话消息列表
        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": planning_prompt}
        ]
        
        # 工具定义 - 获取toolkit的JSON schema
        tools_json = self.toolkit.get_json_schemas()
        
        # 创建一个非流式模型用于ReAct循环（原模型是stream=True会返回AsyncGenerator）
        if self.model_type == "openai":
            non_stream_model = OpenAIChatModel(
                model_name=self.model_name,
                api_key=self.api_key,
                base_url=self.base_url,
                stream=False,  # 非流式
            )
        else:
            non_stream_model = DashScopeChatModel(
                model_name=self.model_name,
                api_key=self.api_key,
                stream=False,  # 非流式
            )
        
        # 迭代计数器
        iteration = 0
        max_iterations = 10
        step_num = 2
        
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
                
                response = await non_stream_model(
                    messages=messages,
                    tools=tools_json if tools_json else None
                )
                
                # ChatResponse.content 是一个列表，包含 TextBlock, ToolUseBlock 等
                content_blocks = response.content if hasattr(response, 'content') else []
                
                print(f"[SQLAgent] 收到响应，content_blocks数量: {len(content_blocks)}")
                
                # 分离工具调用和文本内容
                tool_calls = []
                text_content = []
                
                for block in content_blocks:
                    block_type = block.get('type', '') if isinstance(block, dict) else getattr(block, 'type', '')
                    print(f"[SQLAgent] 处理block类型: {block_type}")
                    
                    # 检查是否是 ToolUseBlock
                    if block_type == 'tool_use':
                        tool_calls.append(block)
                    elif block_type == 'text':
                        text = block.get('text', '') if isinstance(block, dict) else getattr(block, 'text', '')
                        if text:
                            text_content.append(text)
                    # 兼容旧格式
                    elif isinstance(block, dict):
                        if 'name' in block and 'input' in block:
                            tool_calls.append(block)
                        elif 'text' in block:
                            text_content.append(block['text'])
                
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
                    
                    # 添加assistant消息到历史（包含工具调用）
                    messages.append({
                        "role": "assistant",
                        "content": assistant_content,
                        "tool_calls": [
                            {
                                "id": safe_get(tc, 'id', f'call_{iteration}_{i}'),
                                "type": "function",
                                "function": {
                                    "name": safe_get(tc, 'name'),
                                    "arguments": json.dumps(safe_get(tc, 'input', {}), ensure_ascii=False)
                                }
                            }
                            for i, tc in enumerate(tool_calls)
                        ]
                    })
                    
                    # 执行每个工具调用
                    for i, tc in enumerate(tool_calls):
                        tool_name = safe_get(tc, 'name')
                        tool_args = safe_get(tc, 'input', {})
                        tool_id = safe_get(tc, 'id', f'call_{iteration}_{i}')
                        
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
                            # 通过toolkit执行工具
                            # view_text_file, execute_shell_command 已注册到toolkit
                            # validate_sql, execute_sql_query 是Agent内部工具
                            tool_func = self.toolkit.tools.get(tool_name)
                            if tool_func is None:
                                # 尝试从self获取（Agent内部工具）
                                tool_func = getattr(self, tool_name, None)
                            
                            if tool_func:
                                tool_response = tool_func(**tool_args)
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
                            error_msg = f"工具执行错误: {str(e)}"
                            yield {"type": "error", "message": error_msg}
                            messages.append({
                                "role": "tool",
                                "tool_call_id": tool_id,
                                "content": error_msg
                            })
                    
                else:
                    # 没有工具调用，这是最终响应
                    response_text = '\n'.join(text_content) if text_content else ''
                    
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
                                "parse_error": True
                            }
                        }
                    
                    # 退出循环
                    break
                    
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            print(f"[SQLAgent] 错误: {error_detail}")
            yield {"type": "error", "message": f"执行错误: {str(e)}"}
    
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
    api_key: str = None,
    model_name: str = "qwen3-max",
    model_type: str = "dashscope"
) -> SQLGenerationAgent:
    """
    创建SQL生成智能体实例
    
    Args:
        api_key: API密钥
        model_name: 模型名称
        model_type: 模型类型
        
    Returns:
        SQLGenerationAgent: 智能体实例
    """
    return SQLGenerationAgent(
        api_key=api_key,
        model_name=model_name,
        model_type=model_type
    )
