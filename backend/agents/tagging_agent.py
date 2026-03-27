# -*- coding: utf-8 -*-
"""
数据资产打标Agent
基于AgentScope框架开发的智能体，用于自动生成表和字段的语义标签

功能说明：
- 分析表的元数据描述和数据样本
- 使用LLM生成语义标签（2-4个字，反映业务含义）
- 支持表级别和字段级别打标
- 标签用于数据资产的检索和分类
"""

import os
import sys
import json
import re
from typing import Optional, List, Dict, Any
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from agentscope.agent import ReActAgent
from agentscope.model import OpenAIChatModel
from agentscope.formatter import OpenAIChatFormatter
from agentscope.memory import InMemoryMemory
from agentscope.tool import Toolkit, ToolResponse
from agentscope.message import Msg, TextBlock

# 导入数据库服务
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


class DataTaggingAgent:
    """
    数据资产打标智能体
    
    该智能体能够：
    1. 获取表的结构信息和元数据描述
    2. 查看表的样本数据
    3. 使用LLM分析并生成语义标签
    4. 支持表级别和字段级别的标签生成
    
    使用方式：
    ```python
    agent = DataTaggingAgent(api_key="your_api_key")
    result = await agent.generate_tags("deposit_products")
    ```
    """
    
    # 系统提示词
    SYSTEM_PROMPT = """你是一个专业的数据资产标签专家。你的任务是根据数据表的结构、描述和样本数据，生成准确的语义标签。

标签生成原则：
1. 标签应该简洁（2-4个字），反映数据的业务含义
2. 每个表生成3-5个标签
3. 每个字段生成2-3个标签
4. 标签应该有助于数据检索和分类
5. 使用业务术语而非技术术语
6. 标签要具有区分度，避免太泛化

示例：
- 表 deposit_products: ["存款", "利率", "产品", "定期"]
- 字段 max_amount: ["限额", "金额", "上限"]
- 字段 customer_name: ["客户", "姓名", "身份"]
- 字段 interest_rate: ["利率", "收益", "利息"]
- 字段 created_at: ["时间", "创建", "日期"]

你可以使用以下工具来获取信息：
- get_table_info: 获取表的结构信息和现有描述
- get_sample_data: 获取表的样本数据
- save_tags: 保存生成的标签

当你分析完成后，必须调用 save_tags 工具保存结果。
"""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        """
        初始化数据资产打标智能体（OpenAI-compatible）

        Args:
            api_key: API密钥，默认从配置读取
            model_name: 模型名称，默认从配置读取
            base_url: API基础URL，默认从配置读取
        """
        from config import ModelConfig

        self.api_key = api_key or ModelConfig.get_api_key()
        self.model_name = model_name or ModelConfig.get_model_name()
        self.base_url = base_url or ModelConfig.get_base_url()
        
        # 存储生成的标签结果
        self._generated_tags = None
        
        # 创建工具集
        self.toolkit = self._create_toolkit()
        
        # 创建智能体（延迟初始化）
        self._agent = None
    
    def _create_toolkit(self) -> Toolkit:
        """
        创建工具集，使用register_tool_function注册工具
        
        Returns:
            Toolkit: 工具集对象
        """
        toolkit = Toolkit()
        
        # 注册工具函数
        toolkit.register_tool_function(self.get_table_info)
        toolkit.register_tool_function(self.get_sample_data)
        toolkit.register_tool_function(self.save_tags)
        
        print(f"\n{'='*60}")
        print(f"[TaggingAgent] 使用 register_tool_function 注册工具")
        print(f"{'='*60}")
        print(f"[TaggingAgent] 已注册工具: ['get_table_info', 'get_sample_data', 'save_tags']")
        print(f"{'='*60}\n")
        
        return toolkit
    
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
        )
        formatter = OpenAIChatFormatter()

        print(f"\n{'='*60}")
        print(f"[TaggingAgent] 模型配置信息:")
        print(f"  - 模型名称: {self.model_name}")
        print(f"  - API URL: {self.base_url}")
        print(f"{'='*60}\n")
        
        agent = ReActAgent(
            name="TaggingExpert",
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
    
    # ============ 工具函数 ============
    
    def get_table_info(self, table_name: str) -> ToolResponse:
        """
        获取表的结构信息和现有描述
        
        Args:
            table_name: 表名
            
        Returns:
            ToolResponse: 表结构信息
        """
        try:
            schema = db_service.get_table_schema(table_name)
            
            # 格式化输出
            lines = [f"表名: {table_name}"]
            lines.append(f"表描述: {schema.get('description', '暂无描述')}")
            lines.append(f"\n字段列表 ({len(schema.get('columns', []))} 个):")
            
            for col in schema.get('columns', []):
                col_line = f"  - {col['name']} ({col['type']})"
                if col.get('is_primary_key'):
                    col_line += " [主键]"
                if col.get('description'):
                    col_line += f": {col['description']}"
                lines.append(col_line)
            
            # 获取现有标签
            existing_tags = db_service.get_all_tags_for_table(table_name)
            if existing_tags.get('table_tags'):
                lines.append(f"\n现有表标签: {[t['tag'] for t in existing_tags['table_tags']]}")
            
            return ToolResponse(
                content=[TextBlock(type="text", text="\n".join(lines))],
                is_last=False
            )
        except Exception as e:
            return ToolResponse(
                content=[TextBlock(type="text", text=f"获取表信息失败: {str(e)}")],
                is_last=False
            )
    
    def get_sample_data(self, table_name: str, limit: int = 5) -> ToolResponse:
        """
        获取表的样本数据
        
        Args:
            table_name: 表名
            limit: 返回行数限制，默认5
            
        Returns:
            ToolResponse: 样本数据
        """
        try:
            data = db_service.get_table_data(table_name, limit=limit)
            
            lines = [f"表 {table_name} 的样本数据 ({limit} 行):"]
            
            columns = data.get('columns', [])
            rows = data.get('data', [])
            
            for i, row in enumerate(rows, 1):
                lines.append(f"\n记录 {i}:")
                for col in columns[:10]:  # 最多显示10个字段
                    val = row.get(col, "")
                    # 截断过长的值
                    if isinstance(val, str) and len(val) > 50:
                        val = val[:50] + "..."
                    lines.append(f"  {col}: {val}")
            
            return ToolResponse(
                content=[TextBlock(type="text", text="\n".join(lines))],
                is_last=False
            )
        except Exception as e:
            return ToolResponse(
                content=[TextBlock(type="text", text=f"获取样本数据失败: {str(e)}")],
                is_last=False
            )
    
    def save_tags(
        self, 
        table_name: str, 
        table_tags: List[str], 
        column_tags: Dict[str, List[str]]
    ) -> ToolResponse:
        """
        保存生成的标签（暂存，等待用户确认后再应用）
        
        Args:
            table_name: 表名
            table_tags: 表标签列表
            column_tags: 字段标签字典 {column_name: [tags]}
            
        Returns:
            ToolResponse: 保存结果
        """
        # 存储到实例变量，供后续使用
        self._generated_tags = {
            "table_name": table_name,
            "table_tags": table_tags,
            "column_tags": column_tags
        }
        
        # 格式化输出
        lines = ["标签生成完成！"]
        lines.append(f"\n表 {table_name} 的标签: {table_tags}")
        lines.append("\n字段标签:")
        for col, tags in column_tags.items():
            if tags:
                lines.append(f"  - {col}: {tags}")
        
        return ToolResponse(
            content=[TextBlock(type="text", text="\n".join(lines))],
            is_last=True
        )
    
    # ============ 主要功能方法 ============
    
    async def generate_tags(self, table_name: str, target: str = 'all') -> Dict[str, Any]:
        """
        为指定表生成标签
        
        Args:
            table_name: 要生成标签的表名
            target: 生成目标 ('all', 'table', 'columns')
            
        Returns:
            Dict: 生成的标签结果
        """
        # 重置生成结果
        self._generated_tags = None
        
        # 构建提示消息
        target_desc = {
            'all': '表和所有字段',
            'table': '仅表',
            'columns': '仅字段'
        }.get(target, '表和所有字段')
        
        prompt = f"""请为数据库表 "{table_name}" 生成语义标签。

生成范围：{target_desc}

请按以下步骤操作：
1. 使用 get_table_info 获取表的结构信息和现有描述
2. 使用 get_sample_data 查看表的样本数据
3. 分析表和字段的业务含义
4. 生成合适的语义标签
5. 使用 save_tags 保存结果

标签要求：
- 每个标签2-4个字
- 表标签3-5个
- 字段标签2-3个
- 使用业务术语
"""
        
        msg = Msg("user", prompt, "user")
        response = await self.agent.reply(msg)
        
        # 返回生成的标签
        if self._generated_tags:
            return self._generated_tags
        else:
            return {
                "table_name": table_name,
                "table_tags": [],
                "column_tags": {},
                "raw_response": response.get_text_content(),
                "parse_error": True
            }
    
    async def generate_tags_stream(self, table_name: str, target: str = 'all'):
        """
        为指定表生成标签（流式输出）
        
        该方法通过yield返回Agent的思考过程和执行步骤
        
        Args:
            table_name: 要生成标签的表名
            target: 生成目标
            
        Yields:
            Dict: 事件数据
        """
        # 重置生成结果
        self._generated_tags = None
        
        # 确保agent已初始化
        _ = self.agent
        
        yield {
            "type": "step",
            "step": 1,
            "title": "初始化Agent",
            "status": "done",
            "message": f"使用模型: {self.model_name}"
        }
        
        # 检查现有标签
        yield {
            "type": "step",
            "step": 2,
            "title": "检查现有标签",
            "status": "running",
            "message": f"正在检查表 {table_name} 的现有标签..."
        }
        
        existing_tags = db_service.get_all_tags_for_table(table_name)
        existing_count = len(existing_tags.get('table_tags', []))
        column_count = sum(len(tags) for tags in existing_tags.get('column_tags', {}).values())
        
        yield {
            "type": "existing_tags",
            "table_name": table_name,
            "table_tags": existing_tags.get('table_tags', []),
            "column_tags": existing_tags.get('column_tags', {}),
            "summary": f"现有 {existing_count} 个表标签，{column_count} 个字段标签"
        }
        
        yield {
            "type": "step",
            "step": 2,
            "title": "检查现有标签",
            "status": "done",
            "message": f"现有 {existing_count} 个表标签，{column_count} 个字段标签"
        }
        
        # 开始生成
        yield {
            "type": "step",
            "step": 3,
            "title": "任务理解与规划",
            "status": "running",
            "message": "大模型正在理解任务并制定执行计划..."
        }
        
        # 构建提示词
        target_desc = {
            'all': '表和所有字段',
            'table': '仅表',
            'columns': '仅字段'
        }.get(target, '表和所有字段')
        
        planning_prompt = f"""请为数据库表 "{table_name}" 生成语义标签。

生成范围：{target_desc}

在开始之前，请先：
1. 分析这个任务的目标
2. 思考需要获取哪些信息
3. 规划执行步骤

你可以使用的工具：
- get_table_info: 获取表结构和描述
- get_sample_data: 获取样本数据
- save_tags: 保存生成的标签

请先输出你的思考和计划，然后开始执行。

【重要】最后必须调用 save_tags 保存结果，格式如下：
save_tags(
    table_name="{table_name}",
    table_tags=["标签1", "标签2", ...],
    column_tags={{
        "字段名1": ["标签1", "标签2"],
        "字段名2": ["标签1", "标签2"],
        ...
    }}
)
"""
        
        # 创建非流式模型（OpenAI-compatible）
        non_stream_model = OpenAIChatModel(
            model_name=self.model_name,
            api_key=self.api_key,
            client_kwargs={"base_url": self.base_url},
            stream=False,
            generate_kwargs=config.ModelConfig.get_generate_kwargs(
                base_url=self.base_url,
                model_name=self.model_name,
                stream=False
            )
        )
        
        # 初始化消息
        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": planning_prompt}
        ]
        
        tools_json = self.toolkit.get_json_schemas()
        
        iteration = 0
        max_iterations = 10
        step_num = 3
        total_input_tokens = 0
        total_output_tokens = 0
        total_elapsed_time = 0.0
        
        try:
            while iteration < max_iterations:
                iteration += 1
                
                print(f"\n[TaggingAgent] 迭代 {iteration}: 调用大模型...")
                
                yield {
                    "type": "iteration_start",
                    "iteration": iteration,
                    "message": f"开始第 {iteration} 轮推理"
                }
                
                response = await non_stream_model(
                    messages=messages,
                    tools=tools_json if tools_json else None
                )

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
                
                content_blocks = response.content if hasattr(response, 'content') else []
                
                # 分离工具调用和文本
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
                
                if text_content:
                    thinking_text = '\n'.join(text_content)
                    yield {
                        "type": "thinking",
                        "iteration": iteration,
                        "message": thinking_text
                    }
                    
                    if iteration == 1:
                        yield {
                            "type": "step",
                            "step": step_num,
                            "title": "任务理解与规划",
                            "status": "done",
                            "message": "已完成任务分析和执行计划"
                        }
                
                if tool_calls:
                    print(f"[TaggingAgent] 发现 {len(tool_calls)} 个工具调用")
                    
                    def safe_get(obj, key, default=None):
                        if isinstance(obj, dict):
                            return obj.get(key, default)
                        return getattr(obj, key, default)
                    
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
                    
                    for i, tc in enumerate(tool_calls):
                        tool_name = safe_get(tc, 'name')
                        tool_args = safe_get(tc, 'input', {})
                        tool_id = safe_get(tc, 'id', f'call_{iteration}_{i}')
                        
                        step_num += 1
                        tool_args_str = json.dumps(tool_args, ensure_ascii=False)[:100]
                        yield {
                            "type": "step",
                            "step": step_num,
                            "title": f"调用工具: {tool_name}",
                            "status": "running",
                            "message": "参数: " + tool_args_str
                        }
                        
                        try:
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
                                "result": tool_result
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
                    # 没有工具调用，最终响应
                    response_text = '\n'.join(text_content) if text_content else ''
                    
                    step_num += 1
                    yield {
                        "type": "step",
                        "step": step_num,
                        "title": "生成标签完成",
                        "status": "done",
                        "message": "标签生成完成"
                    }
                    
                    yield {
                        "type": "llm_response",
                        "content": response_text
                    }
                    
                    # 返回结果
                    if self._generated_tags:
                        self._generated_tags["usage"] = {
                            "input_tokens": total_input_tokens,
                            "output_tokens": total_output_tokens,
                            "total_tokens": total_input_tokens + total_output_tokens,
                            "time": round(total_elapsed_time, 3),
                        }
                        yield {
                            "type": "result",
                            "data": self._generated_tags
                        }
                    else:
                        # 尝试从响应中解析
                        yield {
                            "type": "result",
                            "data": {
                                "table_name": table_name,
                                "table_tags": [],
                                "column_tags": {},
                                "raw_response": response_text,
                                "parse_error": True,
                                "usage": {
                                    "input_tokens": total_input_tokens,
                                    "output_tokens": total_output_tokens,
                                    "total_tokens": total_input_tokens + total_output_tokens,
                                    "time": round(total_elapsed_time, 3),
                                }
                            }
                        }
                    
                    break
                    
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            print(f"[TaggingAgent] 错误: {error_detail}")
            yield {"type": "error", "message": f"执行错误: {str(e)}"}
    
    async def apply_tags(self, tags_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        将生成的标签应用到数据库
        
        Args:
            tags_data: 标签数据，包含 table_name, table_tags, column_tags
            
        Returns:
            Dict: 应用结果
        """
        table_name = tags_data.get("table_name")
        table_tags = tags_data.get("table_tags", [])
        column_tags = tags_data.get("column_tags", {})
        
        if not table_name:
            return {"success": False, "error": "缺少表名"}
        
        success = db_service.add_all_tags_for_table(
            table_name=table_name,
            table_tags=table_tags,
            column_tags=column_tags,
            created_by='llm'
        )
        
        if success:
            return {
                "success": True,
                "message": f"成功添加 {len(table_tags)} 个表标签和 {sum(len(v) for v in column_tags.values())} 个字段标签"
            }
        else:
            return {"success": False, "error": "保存标签失败"}


# 便捷函数
def create_tagging_agent(
    api_key: Optional[str] = None,
    model_name: str = None,
    base_url: Optional[str] = None
) -> DataTaggingAgent:
    """
    创建数据资产打标智能体实例（OpenAI-compatible）

    Args:
        api_key: API密钥，默认从配置读取
        model_name: 模型名称，默认从配置读取
        base_url: API基础URL，默认从配置读取

    Returns:
        DataTaggingAgent: 智能体实例
    """
    from config import ModelConfig

    return DataTaggingAgent(
        api_key=api_key or ModelConfig.get_api_key(),
        model_name=model_name or ModelConfig.get_model_name(),
        base_url=base_url or ModelConfig.get_base_url(),
    )
