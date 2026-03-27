# -*- coding: utf-8 -*-
"""
后端API服务
基于FastAPI框架，提供数据库管理和智能体服务的RESTful API

主要功能：
1. 数据库表管理API（列表、详情、数据预览）
2. 元数据管理API（查看、编辑）
3. 元数据补全Agent API
4. SQL生成Agent API
5. 数据资产打标Agent API
6. SQL纠错校验Agent API
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Body, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from pydantic import BaseModel, Field

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from database import db_service
from agents import (
    create_metadata_agent, 
    create_sql_agent,
    create_tagging_agent,
    create_sql_validation_agent
)
import config


# ============ Pydantic模型定义 ============

class TableListResponse(BaseModel):
    """表列表响应模型"""
    tables: List[Dict[str, Any]]
    total: int


class TableSchemaResponse(BaseModel):
    """表结构响应模型"""
    table_name: str
    description: str
    columns: List[Dict[str, Any]]
    foreign_keys: List[Dict[str, Any]]
    indexes: List[Dict[str, Any]]


class TableDataResponse(BaseModel):
    """表数据响应模型"""
    columns: List[str]
    data: List[Dict[str, Any]]
    total: int
    limit: int
    offset: int


class UpdateDescriptionRequest(BaseModel):
    """更新描述请求模型"""
    description: str = Field(..., description="新的描述文字")


class SQLExecuteRequest(BaseModel):
    """SQL执行请求模型"""
    sql: str = Field(..., description="要执行的SQL语句")


class MetadataGenerateRequest(BaseModel):
    """元数据生成请求模型"""
    table_name: str = Field(..., description="要生成元数据的表名")


class MetadataApplyRequest(BaseModel):
    """元数据应用请求模型"""
    table_name: str
    table_description: Optional[str] = None
    column_descriptions: Optional[Dict[str, str]] = None


class SQLGenerateRequest(BaseModel):
    """SQL生成请求模型"""
    requirement: str = Field(..., description="自然语言需求描述")
    context: Optional[str] = Field(None, description="额外上下文信息")


class SQLExplainRequest(BaseModel):
    """SQL解释请求模型"""
    sql: str = Field(..., description="要解释的SQL语句")


class TagGenerateRequest(BaseModel):
    """标签生成请求模型"""
    table_name: str = Field(..., description="要生成标签的表名")
    target: str = Field('all', description="生成目标: 'all', 'table', 'columns'")


class TagUpdateRequest(BaseModel):
    """标签更新请求模型"""
    tag: str = Field(..., description="标签内容")
    action: str = Field(..., description="操作: 'add' 或 'delete'")
    column_name: Optional[str] = Field(None, description="字段名（None表示表标签）")


class TagApplyRequest(BaseModel):
    """标签应用请求模型"""
    table_name: str
    table_tags: List[str] = []
    column_tags: Dict[str, List[str]] = {}


class SQLValidateRequest(BaseModel):
    """SQL校验请求模型"""
    sql: str = Field(..., description="要校验的SQL语句")


class SQLGenerateTypeRequest(BaseModel):
    """SQL生成类型请求模型"""
    sql_type: Optional[str] = Field(None, description="SQL类型: 'normal', 'syntax_error', 'performance_issue'，不指定则随机")


# ============ 全局变量 ============

# 智能体实例（延迟初始化）
metadata_agent = None
sql_agent = None
tagging_agent = None
sql_validation_agent = None


def get_metadata_agent():
    """获取元数据补全智能体实例"""
    global metadata_agent
    if metadata_agent is None:
        metadata_agent = create_metadata_agent(api_key=config.DASHSCOPE_API_KEY)
    return metadata_agent


def get_sql_agent():
    """获取SQL生成智能体实例"""
    global sql_agent
    if sql_agent is None:
        sql_agent = create_sql_agent(api_key=config.DASHSCOPE_API_KEY)
    return sql_agent


def get_tagging_agent():
    """获取数据资产打标智能体实例"""
    global tagging_agent
    if tagging_agent is None:
        tagging_agent = create_tagging_agent(api_key=config.DASHSCOPE_API_KEY)
    return tagging_agent


def get_sql_validation_agent():
    """获取SQL纠错校验智能体实例"""
    global sql_validation_agent
    if sql_validation_agent is None:
        sql_validation_agent = create_sql_validation_agent(api_key=config.DASHSCOPE_API_KEY)
    return sql_validation_agent


# ============ FastAPI应用配置 ============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    print("DATA AGENT 服务启动中...")
    yield
    # 关闭时
    print("DATA AGENT 服务关闭")


app = FastAPI(
    title="DATA AGENT API",
    description="数据治理智能体服务API，提供元数据补全和SQL生成功能",
    version="1.0.0",
    lifespan=lifespan
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ 数据库管理API ============

@app.get("/api/tables", response_model=TableListResponse, tags=["数据库管理"])
async def list_tables():
    """
    获取所有数据表列表
    
    返回数据库中所有表的基本信息，包括表名、描述、字段数量和数据行数
    """
    tables = db_service.get_all_tables()
    return TableListResponse(tables=tables, total=len(tables))


@app.get("/api/tables/{table_name}/schema", response_model=TableSchemaResponse, tags=["数据库管理"])
async def get_table_schema(table_name: str):
    """
    获取指定表的结构信息
    
    Args:
        table_name: 表名
        
    Returns:
        表的详细结构信息，包括字段、外键、索引等
    """
    try:
        schema = db_service.get_table_schema(table_name)
        return TableSchemaResponse(**schema)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"表 {table_name} 不存在: {str(e)}")


@app.get("/api/tables/{table_name}/data", response_model=TableDataResponse, tags=["数据库管理"])
async def get_table_data(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000, description="返回行数限制"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    """
    获取表数据预览
    
    Args:
        table_name: 表名
        limit: 返回行数限制，默认100，最大1000
        offset: 偏移量，默认0
        
    Returns:
        表的数据预览
    """
    try:
        data = db_service.get_table_data(table_name, limit=limit, offset=offset)
        return TableDataResponse(**data)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"获取数据失败: {str(e)}")


@app.get("/api/tables/{table_name}/related", tags=["数据库管理"])
async def get_related_tables(table_name: str):
    """
    获取与指定表相关联的表
    
    Args:
        table_name: 表名
        
    Returns:
        关联表信息，包括引用的表和被引用的表
    """
    try:
        related = db_service.get_related_tables(table_name)
        return related
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"获取关联信息失败: {str(e)}")


@app.get("/api/database/summary", tags=["数据库管理"])
async def get_database_summary():
    """
    获取数据库概览信息
    
    Returns:
        数据库概览，包括表数量、总记录数等
    """
    return db_service.get_database_summary()


@app.get("/api/database/er-diagram", tags=["数据库管理"])
async def get_er_diagram_data():
    """
    获取ER图数据，包含所有表和它们之间的关联关系
    
    Returns:
        ER图数据，包含节点（表）和边（关联关系）
    """
    try:
        tables = db_service.get_all_tables()
        
        # 构建节点列表
        nodes = []
        for table in tables:
            schema = db_service.get_table_schema(table["table_name"])
            columns = [
                {
                    "name": col["name"],
                    "type": col["type"],
                    "is_pk": col["is_primary_key"],
                    "is_fk": False  # 后面会更新
                }
                for col in schema["columns"]
            ]
            nodes.append({
                "id": table["table_name"],
                "label": table["table_name"],
                "description": table.get("description", ""),
                "row_count": table["row_count"],
                "columns": columns
            })
        
        # 构建边列表（外键关系）
        edges = []
        for table in tables:
            related = db_service.get_related_tables(table["table_name"])
            for ref in related.get("references", []):
                edges.append({
                    "from": table["table_name"],
                    "to": ref["referenced_table"],
                    "from_column": ref["column"],
                    "to_column": ref["referenced_column"],
                    "label": f"{ref['column']} → {ref['referenced_column']}"
                })
                # 标记外键字段
                for node in nodes:
                    if node["id"] == table["table_name"]:
                        for col in node["columns"]:
                            if col["name"] == ref["column"]:
                                col["is_fk"] = True
        
        return {
            "nodes": nodes,
            "edges": edges
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取ER图数据失败: {str(e)}")


@app.post("/api/sql/execute", tags=["数据库管理"])
async def execute_sql(request: SQLExecuteRequest):
    """
    执行SQL查询（仅支持SELECT语句）
    
    Args:
        request: SQL执行请求
        
    Returns:
        查询结果
    """
    sql = request.sql.strip()
    
    # 安全检查：只允许SELECT语句
    if not sql.upper().startswith("SELECT"):
        raise HTTPException(status_code=400, detail="安全限制：只允许执行SELECT查询语句")
    
    result = db_service.execute_sql(sql)
    
    if not result['success']:
        raise HTTPException(status_code=400, detail=result.get('error', '执行失败'))
    
    return result


# ============ 元数据管理API ============

@app.put("/api/tables/{table_name}/description", tags=["元数据管理"])
async def update_table_description(table_name: str, request: UpdateDescriptionRequest):
    """
    更新表描述
    
    Args:
        table_name: 表名
        request: 更新请求，包含新的描述文字
        
    Returns:
        更新结果
    """
    success = db_service.update_table_description(table_name, request.description)
    if success:
        return {"success": True, "message": "表描述更新成功"}
    else:
        raise HTTPException(status_code=500, detail="更新失败")


@app.put("/api/tables/{table_name}/columns/{column_name}/description", tags=["元数据管理"])
async def update_column_description(
    table_name: str,
    column_name: str,
    request: UpdateDescriptionRequest
):
    """
    更新字段描述
    
    Args:
        table_name: 表名
        column_name: 字段名
        request: 更新请求，包含新的描述文字
        
    Returns:
        更新结果
    """
    success = db_service.update_column_description(table_name, column_name, request.description)
    if success:
        return {"success": True, "message": "字段描述更新成功"}
    else:
        raise HTTPException(status_code=500, detail="更新失败")


@app.get("/api/metadata/missing", tags=["元数据管理"])
async def get_missing_metadata():
    """
    获取缺少元数据描述的表和字段列表
    
    Returns:
        缺少元数据的表和字段信息
    """
    return db_service.get_tables_with_missing_metadata()


# ============ 元数据补全Agent API ============

@app.post("/api/agent/metadata/generate", tags=["元数据Agent"])
async def generate_metadata(request: MetadataGenerateRequest):
    """
    使用AI智能体为指定表生成元数据描述
    
    该接口会调用元数据补全Agent，分析表结构和数据后生成元数据描述建议
    
    Args:
        request: 生成请求，包含表名
        
    Returns:
        生成的元数据描述
    """
    try:
        agent = get_metadata_agent()
        result = await agent.generate_metadata(request.table_name)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成元数据失败: {str(e)}")


@app.post("/api/agent/metadata/generate/stream", tags=["元数据Agent"])
async def generate_metadata_stream(request: MetadataGenerateRequest):
    """
    使用AI智能体为指定表生成元数据描述（流式输出）
    
    该接口通过SSE流式返回Agent的思考过程和执行步骤
    
    Args:
        request: 生成请求，包含表名
        
    Returns:
        SSE流式响应，包含思考过程和最终结果
    """
    import json
    
    async def event_generator():
        try:
            agent = get_metadata_agent()
            
            # 发送开始事件
            start_message = f'开始分析表 {request.table_name}...'
            start_event = json.dumps({'type': 'start', 'message': start_message}, ensure_ascii=False)
            yield "data: " + start_event + "\n\n"
            
            # 使用流式生成方法
            async for event in agent.generate_metadata_stream(request.table_name):
                event_json = json.dumps(event, ensure_ascii=False)
                yield "data: " + event_json + "\n\n"
            
            # 发送完成事件
            end_event = json.dumps({'type': 'end', 'message': '分析完成'}, ensure_ascii=False)
            yield "data: " + end_event + "\n\n"
            
        except Exception as e:
            error_event = json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)
            yield "data: " + error_event + "\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.post("/api/agent/metadata/apply", tags=["元数据Agent"])
async def apply_metadata(request: MetadataApplyRequest):
    """
    将生成的元数据应用到数据库
    
    Args:
        request: 应用请求，包含表名和元数据描述
        
    Returns:
        应用结果
    """
    try:
        agent = get_metadata_agent()
        result = await agent.apply_metadata({
            "table_name": request.table_name,
            "table_description": request.table_description,
            "column_descriptions": request.column_descriptions or {}
        })
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"应用元数据失败: {str(e)}")


# ============ SQL生成Agent API ============

@app.post("/api/agent/sql/generate", tags=["SQL Agent"])
async def generate_sql(request: SQLGenerateRequest):
    """
    使用AI智能体根据自然语言需求生成SQL
    
    该接口会调用SQL生成Agent，理解用户需求并生成相应的SQL语句
    
    Args:
        request: 生成请求，包含自然语言需求描述
        
    Returns:
        生成的SQL语句和说明
    """
    try:
        agent = get_sql_agent()
        result = await agent.generate_sql(request.requirement, request.context)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成SQL失败: {str(e)}")


@app.post("/api/agent/sql/generate/stream", tags=["SQL Agent"])
async def generate_sql_stream(request: SQLGenerateRequest):
    """
    使用AI智能体根据自然语言需求生成SQL（流式输出）
    
    该接口通过SSE流式返回Agent的思考过程和执行步骤
    
    Args:
        request: 生成请求，包含自然语言需求描述
        
    Returns:
        SSE流式响应，包含思考过程和最终结果
    """
    import json
    
    async def event_generator():
        try:
            agent = get_sql_agent()
            
            # 发送开始事件
            start_event = json.dumps({'type': 'start', 'message': '开始分析需求...'}, ensure_ascii=False)
            yield "data: " + start_event + "\n\n"
            
            # 使用流式生成方法
            async for event in agent.generate_sql_stream(request.requirement, request.context):
                event_json = json.dumps(event, ensure_ascii=False)
                yield "data: " + event_json + "\n\n"
            
            # 发送完成事件
            end_event = json.dumps({'type': 'end', 'message': '生成完成'}, ensure_ascii=False)
            yield "data: " + end_event + "\n\n"
            
        except Exception as e:
            error_event = json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)
            yield "data: " + error_event + "\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.post("/api/agent/sql/explain", tags=["SQL Agent"])
async def explain_sql(request: SQLExplainRequest):
    """
    使用AI智能体解释SQL语句的功能
    
    Args:
        request: 解释请求，包含SQL语句
        
    Returns:
        SQL语句的功能解释
    """
    try:
        agent = get_sql_agent()
        result = await agent.explain_sql(request.sql)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"解释SQL失败: {str(e)}")


@app.post("/api/agent/sql/optimize", tags=["SQL Agent"])
async def optimize_sql(request: SQLExplainRequest):
    """
    使用AI智能体优化SQL语句
    
    Args:
        request: 优化请求，包含SQL语句
        
    Returns:
        优化建议和优化后的SQL
    """
    try:
        agent = get_sql_agent()
        result = await agent.optimize_sql(request.sql)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"优化SQL失败: {str(e)}")


# ============ 标签管理API ============

@app.get("/api/tables/{table_name}/tags", tags=["标签管理"])
async def get_table_all_tags(table_name: str):
    """
    获取表和字段的所有标签
    
    Args:
        table_name: 表名
        
    Returns:
        表标签和字段标签
    """
    try:
        return db_service.get_all_tags_for_table(table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取标签失败: {str(e)}")


@app.post("/api/tables/{table_name}/tags", tags=["标签管理"])
async def update_table_tag(table_name: str, request: TagUpdateRequest):
    """
    手动添加或删除标签
    
    Args:
        table_name: 表名
        request: 标签更新请求
        
    Returns:
        操作结果
    """
    try:
        if request.action == 'add':
            if request.column_name:
                success = db_service.add_column_tags(
                    table_name, request.column_name, [request.tag], created_by='user'
                )
            else:
                success = db_service.add_table_tags(table_name, [request.tag], created_by='user')
        elif request.action == 'delete':
            if request.column_name:
                success = db_service.delete_column_tag(
                    table_name, request.column_name, request.tag
                )
            else:
                success = db_service.delete_table_tag(table_name, request.tag)
        else:
            raise HTTPException(status_code=400, detail="action 必须是 'add' 或 'delete'")
        
        if success:
            return {"success": True, "message": "标签更新成功"}
        else:
            raise HTTPException(status_code=500, detail="标签更新失败")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"标签更新失败: {str(e)}")


# ============ 数据资产打标Agent API ============

@app.post("/api/agent/tagging/generate/stream", tags=["打标Agent"])
async def generate_tags_stream(request: TagGenerateRequest):
    """
    使用AI智能体为表生成语义标签（流式输出）
    
    Args:
        request: 生成请求，包含表名
        
    Returns:
        SSE流式响应
    """
    import json
    
    async def event_generator():
        try:
            agent = get_tagging_agent()
            
            # 发送开始事件
            start_event = json.dumps({
                'type': 'start', 
                'message': f'开始为表 {request.table_name} 生成标签...'
            }, ensure_ascii=False)
            yield "data: " + start_event + "\n\n"
            
            # 使用流式生成方法
            async for event in agent.generate_tags_stream(request.table_name, request.target):
                event_json = json.dumps(event, ensure_ascii=False)
                yield "data: " + event_json + "\n\n"
            
            # 发送完成事件
            end_event = json.dumps({'type': 'end', 'message': '标签生成完成'}, ensure_ascii=False)
            yield "data: " + end_event + "\n\n"
            
        except Exception as e:
            error_event = json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)
            yield "data: " + error_event + "\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.post("/api/agent/tagging/apply", tags=["打标Agent"])
async def apply_tags(request: TagApplyRequest):
    """
    将生成的标签应用到数据库
    
    Args:
        request: 标签应用请求
        
    Returns:
        应用结果
    """
    try:
        agent = get_tagging_agent()
        result = await agent.apply_tags({
            "table_name": request.table_name,
            "table_tags": request.table_tags,
            "column_tags": request.column_tags
        })
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"应用标签失败: {str(e)}")


# ============ SQL纠错校验Agent API ============

@app.post("/api/agent/sql-validation/validate/stream", tags=["SQL校验Agent"])
async def validate_sql_stream(request: SQLValidateRequest):
    """
    使用AI智能体校验SQL语句（流式输出）
    
    Args:
        request: 校验请求，包含SQL语句
        
    Returns:
        SSE流式响应
    """
    import json
    
    async def event_generator():
        try:
            agent = get_sql_validation_agent()
            
            # 发送开始事件
            start_event = json.dumps({
                'type': 'start', 
                'message': '开始校验SQL...'
            }, ensure_ascii=False)
            yield "data: " + start_event + "\n\n"
            
            # 使用流式校验方法
            async for event in agent.validate_sql_stream(request.sql):
                event_json = json.dumps(event, ensure_ascii=False)
                yield "data: " + event_json + "\n\n"
            
            # 发送完成事件
            end_event = json.dumps({'type': 'end', 'message': '校验完成'}, ensure_ascii=False)
            yield "data: " + end_event + "\n\n"
            
        except Exception as e:
            error_event = json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)
            yield "data: " + error_event + "\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.post("/api/agent/sql-validation/validate", tags=["SQL校验Agent"])
async def validate_sql(request: SQLValidateRequest):
    """
    使用AI智能体校验SQL语句（非流式）
    
    Args:
        request: 校验请求，包含SQL语句
        
    Returns:
        校验结果
    """
    try:
        agent = get_sql_validation_agent()
        result = await agent.validate_sql(request.sql)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"校验SQL失败: {str(e)}")


@app.post("/api/agent/sql-validation/generate", tags=["SQL校验Agent"])
async def generate_test_sql(request: SQLGenerateTypeRequest = None):
    """
    生成测试用SQL
    
    按概率随机生成三种类型的SQL：
    - 60% 正常SQL
    - 20% 语法错误SQL
    - 20% 性能问题SQL
    
    Args:
        request: 可选，指定生成的SQL类型
        
    Returns:
        生成的SQL和类型信息
    """
    try:
        agent = get_sql_validation_agent()
        sql_type = request.sql_type if request else None
        result = await agent.generate_test_sql(sql_type)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成测试SQL失败: {str(e)}")


# ============ 静态文件服务 ============

# 获取前端目录路径
# server.py 在 backend/api/ 目录下，前端在 data_agent/frontend/
frontend_dir = Path(__file__).parent.parent.parent / "frontend"

# 挂载静态文件目录
if (frontend_dir / "static").exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir / "static")), name="static")


@app.get("/", response_class=HTMLResponse, tags=["前端页面"])
async def index():
    """返回前端首页"""
    index_file = frontend_dir / "templates" / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return HTMLResponse(content="<h1>DATA AGENT</h1><p>前端页面未找到</p>")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """返回favicon图标（避免404错误）"""
    # 返回空响应，避免浏览器报错
    return Response(status_code=204)


# ============ 健康检查 ============

@app.get("/health", tags=["系统"])
async def health_check():
    """健康检查接口"""
    return {
        "status": "healthy",
        "service": "DATA AGENT API",
        "version": "1.0.0"
    }


# ============ 启动入口 ============

def start_server(host: str = "0.0.0.0", port: int = 8000):
    """启动服务器"""
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    start_server()
