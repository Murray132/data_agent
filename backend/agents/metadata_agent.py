# -*- coding: utf-8 -*-
"""
元数据补全Agent
基于AgentScope框架开发的智能体，用于自动生成表和字段的元数据描述

功能说明：
- 分析表名、字段名的命名规则
- 查看表的数据样本
- 分析表与其他表的关联关系
- 基于以上信息，生成表和字段的元数据描述文本
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
from agentscope.tool import Toolkit, ToolResponse
from agentscope.message import Msg, TextBlock

# 导入数据库服务
sys.path.insert(0, str(Path(__file__).parent.parent))
from database import db_service

# 导入数据库工具函数
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from skills import db_tools


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


class MetadataCompletionAgent:
    """
    元数据补全智能体
    
    该智能体能够：
    1. 获取数据库中所有表的结构信息
    2. 分析表和字段的命名规则
    3. 查看表的样本数据
    4. 分析表之间的关联关系
    5. 基于以上信息生成元数据描述
    
    使用方式：
    ```python
    agent = MetadataCompletionAgent(api_key="your_api_key")
    result = await agent.generate_metadata("customers")
    ```
    """
    
    # 系统提示词，指导大模型如何生成元数据
    SYSTEM_PROMPT = """你是一个专业的数据库元数据专家。你的任务是根据给定的数据库表结构、样本数据和关联关系信息，生成准确、专业的元数据描述。

你需要遵循以下原则：
1. 表描述应该简洁明了，说明表的业务用途和存储的数据类型
2. 字段描述应该准确说明字段的含义、取值范围或枚举值
3. 对于外键字段，要说明其关联的表和业务含义
4. 使用专业的金融/数据库术语
5. 描述要符合中文表达习惯

你可以使用以下工具来获取信息：
- get_table_schema: 获取表结构信息
- get_sample_data: 获取表的样本数据
- get_related_tables: 获取表的关联关系
- get_sample_values: 获取某字段的样本值

当你准备好生成元数据描述时，请调用 generate_metadata_response 工具返回结果。
"""
    
    def __init__(
        self,
        api_key: str = None,
        model_name: str = "qwen3-max",
        model_type: str = "dashscope",
        base_url: str = None,
    ):
        """
        初始化元数据补全智能体
        
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
        创建工具集，使用register_tool_function注册数据库分析工具
        
        这与SQLAgent使用register_agent_skill的方式形成对比，
        展示框架支持的两种不同注册模式：
        - MetadataAgent: 直接注册工具函数 (register_tool_function)
        - SQLAgent: 注册Skill目录 (register_agent_skill)
        
        Returns:
            Toolkit: 工具集对象
        """
        toolkit = Toolkit()
        
        # ============ 使用 register_tool_function 注册工具 ============
        # 注册数据库分析工具函数
        toolkit.register_tool_function(db_tools.get_table_schema)
        toolkit.register_tool_function(db_tools.get_sample_data)
        toolkit.register_tool_function(db_tools.get_related_tables)
        toolkit.register_tool_function(db_tools.get_sample_values)
        
        # 打印已注册的工具信息
        registered_tools = list(self.DB_TOOLS)
        print(f"\n{'='*60}")
        print(f"[MetadataAgent] 使用 register_tool_function 注册工具")
        print(f"{'='*60}")
        print(f"[MetadataAgent] 已注册数据库分析工具:")
        for tool_name in registered_tools:
            print(f"  - {tool_name}")
        print(f"[MetadataAgent] 共注册 {len(registered_tools)} 个工具")
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
            print(f"[MetadataAgent] 模型配置信息:")
            print(f"  - 模型类型: OpenAI")
            print(f"  - 模型名称: {self.model_name}")
            print(f"  - API URL: {base_url}")
            print(f"  - API Key: {self.api_key[:8]}...{self.api_key[-4:] if self.api_key else 'None'}")
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
            print(f"[MetadataAgent] 模型配置信息:")
            print(f"  - 模型类型: DashScope (阿里云通义)")
            print(f"  - 模型名称: {self.model_name}")
            print(f"  - API URL: {base_url}")
            print(f"  - API Key: {self.api_key[:8]}...{self.api_key[-4:] if self.api_key else 'None'}")
            print(f"{'='*60}\n")
        
        # 创建智能体
        agent = ReActAgent(
            name="MetadataExpert",
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
    
    # ============ 主要功能方法 ============
    
    async def generate_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        为指定表生成元数据描述
        
        该方法会调用智能体分析表结构和数据，然后生成元数据描述
        
        Args:
            table_name: 要生成元数据的表名
            
        Returns:
            Dict: 生成的元数据，包含：
                - table_name: 表名
                - table_description: 表描述
                - column_descriptions: 字段描述字典
        """
        # 构建提示消息
        prompt = f"""请为数据库表 "{table_name}" 生成元数据描述。

请按以下步骤操作：
1. 首先使用 get_table_schema 获取表的结构信息
2. 使用 get_sample_data 查看表的样本数据
3. 使用 get_related_tables 了解表的关联关系
4. 如果某些字段的含义不清楚，可以使用 get_sample_values 查看该字段的样本值

分析完成后，请生成：
1. 一个简洁准确的表描述（说明表的业务用途）
2. 每个字段的描述（说明字段含义、可能的取值范围）

请用JSON格式返回结果，格式如下：
```json
{{
    "table_description": "表的描述文字",
    "column_descriptions": {{
        "column1": "字段1的描述",
        "column2": "字段2的描述"
    }}
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
            else:
                # 尝试直接解析整个响应
                json_str = response_text
            
            result = json.loads(json_str)
            result["table_name"] = table_name
            result["raw_response"] = response_text
            return result
            
        except json.JSONDecodeError:
            # 如果无法解析JSON，返回原始响应
            return {
                "table_name": table_name,
                "table_description": "",
                "column_descriptions": {},
                "raw_response": response_text,
                "parse_error": True
            }
    
    def _extract_metadata_from_text(self, text: str, table_name: str) -> Dict[str, Any]:
        """
        从非JSON格式的文本中尝试提取元数据
        
        Args:
            text: 大模型返回的文本
            table_name: 表名
            
        Returns:
            Dict: 提取的元数据
        """
        import re
        
        result = {
            "table_name": table_name,
            "table_description": "",
            "column_descriptions": {}
        }
        
        # 尝试提取表描述
        table_desc_patterns = [
            r'[*]*表描述[*]*[：:]\s*(.+?)(?:\n|$)',
            r'table_description[：:]\s*["\']?(.+?)["\']?(?:,|\n|$)',
            r'该表(?:用于|存储|记录)(.+?)(?:。|\n|$)',
        ]
        
        for pattern in table_desc_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result["table_description"] = match.group(1).strip()
                break
        
        # 尝试提取字段描述 - 支持多种格式
        # 格式1: - **frozen_amount**: 描述内容
        # 格式2: - frozen_amount: 描述内容
        # 格式3: "frozen_amount": "描述内容"
        column_patterns = [
            # 匹配 **字段名**: 描述 或 **字段名**：描述 (Markdown加粗格式)
            r'[-*]\s*\*\*([a-zA-Z_][a-zA-Z0-9_]*)\*\*\s*[：:]\s*(.+?)(?:\n|$)',
            # 匹配 - 字段名: 描述 或 字段名：描述
            r'[-*]\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*[：:]\s*(.+?)(?:\n|$)',
            # 匹配 "字段名": "描述"
            r'"([a-zA-Z_][a-zA-Z0-9_]*)"\s*[：:]\s*"(.+?)"',
            # 匹配 字段名：描述（无前缀）
            r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*[：:]\s*(.+?)(?:\n|$)',
        ]
        
        # 按优先级尝试各种模式
        for pattern in column_patterns:
            matches = re.findall(pattern, text, re.MULTILINE)
            for col_name, col_desc in matches:
                # 过滤掉一些非字段名的匹配
                skip_names = {'table_description', 'column_descriptions', 'json', 
                             'table_name', 'http', 'https', 'api', 'url'}
                if col_name.lower() not in skip_names:
                    # 清理描述文本
                    clean_desc = col_desc.strip().rstrip('。').rstrip('.')
                    if clean_desc and len(clean_desc) > 2:  # 描述至少3个字符
                        result["column_descriptions"][col_name] = clean_desc
        
        return result
    
    # 数据库分析工具列表
    DB_TOOLS = {'get_table_schema', 'get_sample_data', 'get_related_tables', 'get_sample_values'}
    
    async def generate_metadata_stream(self, table_name: str):
        """
        为指定表生成元数据描述（流式输出）
        
        该方法会通过yield返回Agent的思考过程和执行步骤，
        手动实现ReAct循环以捕获所有中间步骤
        
        Args:
            table_name: 要生成元数据的表名
            
        Yields:
            Dict: 事件数据，包含type和相关信息
        """
        import json
        import re
        
        # 确保agent已初始化
        _ = self.agent
        
        yield {
            "type": "step",
            "step": 1,
            "title": "初始化Agent",
            "status": "done",
            "message": f"使用模型: {self.model_name}"
        }
        
        # ============ 预检查：分析哪些字段缺失元数据 ============
        yield {
            "type": "step",
            "step": 2,
            "title": "检查缺失元数据",
            "status": "running",
            "message": f"正在分析表 {table_name} 的元数据缺失情况..."
        }
        
        # 获取表的schema信息，检查哪些字段缺失描述
        schema = db_service.get_table_schema(table_name)
        missing_table_desc = not schema.get("description")
        missing_columns = []
        existing_columns = []
        
        for col in schema.get("columns", []):
            if not col.get("description"):
                missing_columns.append(col.get("name"))
            else:
                existing_columns.append({
                    "name": col.get("name"),
                    "description": col.get("description")
                })
        
        # 构建缺失信息摘要
        missing_summary = []
        if missing_table_desc:
            missing_summary.append("表描述缺失")
        if missing_columns:
            missing_summary.append(f"{len(missing_columns)}个字段缺失描述: {', '.join(missing_columns[:5])}{'...' if len(missing_columns) > 5 else ''}")
        
        if not missing_summary:
            # 无缺失，无需生成
            yield {
                "type": "step",
                "step": 2,
                "title": "检查缺失元数据",
                "status": "done",
                "message": "该表元数据已完整，无需补全"
            }
            yield {
                "type": "result",
                "data": {
                    "table_name": table_name,
                    "table_description": schema.get("description", ""),
                    "column_descriptions": {col.get("name"): col.get("description") for col in schema.get("columns", [])},
                    "complete": True,
                    "message": "元数据已完整"
                }
            }
            return
        
        # 发送缺失分析事件
        yield {
            "type": "missing_analysis",
            "table_name": table_name,
            "missing_table_description": missing_table_desc,
            "missing_columns": missing_columns,
            "existing_columns": existing_columns,
            "summary": "; ".join(missing_summary)
        }
        
        yield {
            "type": "step",
            "step": 2,
            "title": "检查缺失元数据",
            "status": "done",
            "message": "; ".join(missing_summary)
        }
        
        # ============ 第一阶段：任务理解和规划 ============
        yield {
            "type": "step",
            "step": 3,
            "title": "任务理解与规划",
            "status": "running",
            "message": "大模型正在理解任务并制定执行计划..."
        }
        
        # 构建针对性的提示词
        existing_desc_text = ""
        if existing_columns:
            existing_desc_text = "\n已有描述的字段（无需重新生成）：\n" + "\n".join([
                f"  - {col['name']}: {col['description']}" for col in existing_columns[:10]
            ])
            if len(existing_columns) > 10:
                existing_desc_text += f"\n  ... 等共 {len(existing_columns)} 个字段"
        
        missing_info_text = f"""
【待补全的元数据】
- 表描述: {"缺失，需要生成" if missing_table_desc else "已存在，无需生成"}
- 缺失描述的字段（共{len(missing_columns)}个）: {', '.join(missing_columns) if missing_columns else "无"}
{existing_desc_text}
"""
        
        planning_prompt = f"""我需要为数据库表 "{table_name}" 生成元数据描述。
{missing_info_text}

注意：你只需要为上面列出的缺失字段生成描述，已有描述的字段不要重新生成。

在开始执行之前，请先：
1. 分析这个任务的目标是什么（只补全缺失的元数据）
2. 思考需要获取哪些信息才能完成任务
3. 规划你打算调用哪些工具，以及调用顺序

你可以使用的数据库分析工具：
- get_table_schema: 获取表结构信息（字段名、类型、约束等）
- get_sample_data: 获取表的样本数据
- get_related_tables: 获取表的外键关联关系
- get_sample_values: 获取某个字段的样本值

请先输出你的思考和计划，格式如下：
【任务理解】
（描述你对这个任务的理解，包括需要补全哪些元数据）

【执行计划】
1. 第一步：...
2. 第二步：...
...

然后开始执行计划，调用相应的工具。

【重要】完成分析后，必须以JSON格式返回结果，格式如下：
```json
{{
    "table_description": "表的描述（如果需要生成）",
    "column_descriptions": {{
        "字段名1": "字段描述1",
        "字段名2": "字段描述2"
    }}
}}
```
只包含需要补全的字段，已有描述的字段不要包含在结果中。"""
        
        # 创建非流式模型用于ReAct循环
        if self.model_type == "openai":
            non_stream_model = OpenAIChatModel(
                model_name=self.model_name,
                api_key=self.api_key,
                base_url=self.base_url,
                stream=False,
            )
        else:
            non_stream_model = DashScopeChatModel(
                model_name=self.model_name,
                api_key=self.api_key,
                stream=False,
            )
        
        # 初始化对话消息
        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": planning_prompt}
        ]
        
        # 工具定义
        tools_json = self.toolkit.get_json_schemas()
        
        # 迭代变量
        iteration = 0
        max_iterations = 10
        step_num = 3  # 从步骤3开始，因为步骤1是初始化，步骤2是检查缺失元数据
        
        try:
            while iteration < max_iterations:
                iteration += 1
                
                print(f"\n[MetadataAgent] 迭代 {iteration}: 调用大模型...")
                
                # 发送迭代开始事件
                yield {
                    "type": "iteration_start",
                    "iteration": iteration,
                    "message": f"开始第 {iteration} 轮推理"
                }
                
                # 调用模型
                response = await non_stream_model(
                    messages=messages,
                    tools=tools_json if tools_json else None
                )
                
                content_blocks = response.content if hasattr(response, 'content') else []
                
                # 分离工具调用和文本内容
                tool_calls = []
                text_content = []
                
                for block in content_blocks:
                    block_type = block.get('type', '') if isinstance(block, dict) else getattr(block, 'type', '')
                    
                    if block_type == 'tool_use':
                        tool_calls.append(block)
                    elif block_type == 'text':
                        text = block.get('text', '') if isinstance(block, dict) else getattr(block, 'text', '')
                        if text:
                            text_content.append(text)
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
                    print(f"[MetadataAgent] 发现 {len(tool_calls)} 个工具调用")
                    
                    # 辅助函数
                    def safe_get(obj, key, default=None):
                        if isinstance(obj, dict):
                            return obj.get(key, default)
                        return getattr(obj, key, default)
                    
                    # 构建assistant消息
                    assistant_content = '\n'.join(text_content) if text_content else ''
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
                        
                        # 检查是否是数据库工具
                        if tool_name in self.DB_TOOLS:
                            print(f"[MetadataAgent] 调用数据库工具: {tool_name}")
                        
                        step_num += 1
                        # 避免f-string中包含花括号导致格式化错误
                        tool_args_str = json.dumps(tool_args, ensure_ascii=False)
                        yield {
                            "type": "step",
                            "step": step_num,
                            "title": f"调用工具: {tool_name}",
                            "status": "running",
                            "message": "参数: " + tool_args_str
                        }
                        
                        try:
                            # 调用数据库工具函数
                            tool_func = getattr(db_tools, tool_name, None)
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
                                "is_db_tool": tool_name in self.DB_TOOLS
                            }
                            
                            yield {
                                "type": "step",
                                "step": step_num,
                                "title": f"调用工具: {tool_name}",
                                "status": "done",
                                "message": "执行完成"
                            }
                            
                            messages.append({
                                "role": "tool",
                                "tool_call_id": tool_id,
                                "content": tool_result
                            })
                            
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
                        "title": "生成元数据描述",
                        "status": "done",
                        "message": "元数据生成完成"
                    }
                    
                    yield {
                        "type": "llm_response",
                        "content": response_text
                    }
                    
                    # 解析结果 - 尝试多种方式提取JSON
                    result = None
                    parse_error = False
                    
                    try:
                        # 方式1: 匹配 ```json ... ``` 代码块
                        json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
                        if json_match:
                            json_str = json_match.group(1)
                            result = json.loads(json_str)
                        else:
                            # 方式2: 匹配 ``` ... ``` 代码块（无json标识）
                            json_match = re.search(r'```\s*(.*?)\s*```', response_text, re.DOTALL)
                            if json_match:
                                json_str = json_match.group(1)
                                result = json.loads(json_str)
                            else:
                                # 方式3: 直接匹配 { ... } JSON对象
                                json_match = re.search(r'\{[\s\S]*"table_description"[\s\S]*\}', response_text)
                                if json_match:
                                    json_str = json_match.group(0)
                                    result = json.loads(json_str)
                                else:
                                    # 方式4: 尝试直接解析整个响应
                                    result = json.loads(response_text)
                                    
                    except json.JSONDecodeError as e:
                        print(f"[MetadataAgent] JSON解析失败: {e}")
                        parse_error = True
                    
                    if result and not parse_error:
                        result["table_name"] = table_name
                        result["raw_response"] = response_text
                        
                        yield {
                            "type": "result",
                            "data": result
                        }
                    else:
                        # 尝试从响应中提取有用信息
                        extracted = self._extract_metadata_from_text(response_text, table_name)
                        extracted["raw_response"] = response_text
                        extracted["parse_error"] = True if not extracted.get("table_description") else False
                        
                        yield {
                            "type": "result",
                            "data": extracted
                        }
                    
                    break
                    
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            print(f"[MetadataAgent] 错误: {error_detail}")
            yield {"type": "error", "message": f"执行错误: {str(e)}"}
    
    async def generate_all_missing_metadata(self) -> List[Dict[str, Any]]:
        """
        为所有缺少元数据的表生成描述
        
        Returns:
            List[Dict]: 生成的元数据列表
        """
        missing = db_service.get_tables_with_missing_metadata()
        results = []
        
        for table_info in missing:
            table_name = table_info["table_name"]
            result = await self.generate_metadata(table_name)
            results.append(result)
        
        return results
    
    async def apply_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        将生成的元数据应用到数据库
        
        Args:
            metadata: 生成的元数据字典
            
        Returns:
            Dict: 应用结果
        """
        table_name = metadata.get("table_name")
        if not table_name:
            return {"success": False, "error": "缺少表名"}
        
        results = {"table_name": table_name, "table_updated": False, "columns_updated": []}
        
        # 更新表描述
        table_desc = metadata.get("table_description")
        if table_desc:
            success = db_service.update_table_description(table_name, table_desc)
            results["table_updated"] = success
        
        # 更新字段描述
        column_descs = metadata.get("column_descriptions", {})
        for col_name, col_desc in column_descs.items():
            if col_desc:
                success = db_service.update_column_description(table_name, col_name, col_desc)
                if success:
                    results["columns_updated"].append(col_name)
        
        results["success"] = True
        return results


# 便捷函数：创建元数据补全智能体实例
def create_metadata_agent(
    api_key: str = None,
    model_name: str = "qwen3-max",
    model_type: str = "dashscope"
) -> MetadataCompletionAgent:
    """
    创建元数据补全智能体实例
    
    Args:
        api_key: API密钥
        model_name: 模型名称
        model_type: 模型类型
        
    Returns:
        MetadataCompletionAgent: 智能体实例
    """
    return MetadataCompletionAgent(
        api_key=api_key,
        model_name=model_name,
        model_type=model_type
    )
