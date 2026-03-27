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
import json
import secrets
import hashlib
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Body, Response, Request, Depends
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
    enable_thinking: Optional[bool] = Field(None, description="是否开启 thinking")
    temperature: Optional[float] = Field(None, description="温度参数")


class SQLExplainRequest(BaseModel):
    """SQL解释请求模型"""
    sql: str = Field(..., description="要解释的SQL语句")
    enable_thinking: Optional[bool] = Field(None, description="是否开启 thinking")
    temperature: Optional[float] = Field(None, description="温度参数")


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
    enable_thinking: Optional[bool] = Field(None, description="是否开启 thinking")
    temperature: Optional[float] = Field(None, description="温度参数")


class SQLGenerateTypeRequest(BaseModel):
    """SQL生成类型请求模型"""
    sql_type: Optional[str] = Field(None, description="SQL类型: 'normal', 'syntax_error', 'performance_issue'，不指定则随机")


class ModelConfigRequest(BaseModel):
    """模型配置请求模型"""
    base_url: str = Field(..., description="API基础URL")
    api_key: str = Field(..., description="API密钥")
    model_name: str = Field(..., description="模型名称")
    temperature: float = Field(0.7, description="温度参数")


class LoginRequest(BaseModel):
    """登录请求模型"""
    username: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")


class DataSourceCreateRequest(BaseModel):
    """数据源创建请求模型（当前支持 mysql）"""
    name: str = Field(..., description="数据源名称")
    type: str = Field(..., description="数据源类型，如 mysql")
    host: str = Field(..., description="主机地址")
    port: int = Field(3306, description="端口")
    username: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    database: str = Field(..., description="数据库名")
    charset: str = Field("utf8mb4", description="字符集")


class DataSourceTestRequest(BaseModel):
    """数据源测试请求模型"""
    type: str = Field(..., description="数据源类型，如 mysql")
    host: str = Field(..., description="主机地址")
    port: int = Field(3306, description="端口")
    username: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    database: str = Field(..., description="数据库名")
    charset: str = Field("utf8mb4", description="字符集")


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
        metadata_agent = create_metadata_agent()
    return metadata_agent


def get_sql_agent():
    """获取SQL生成智能体实例"""
    global sql_agent
    if sql_agent is None:
        sql_agent = create_sql_agent()
    return sql_agent


def get_tagging_agent():
    """获取数据资产打标智能体实例"""
    global tagging_agent
    if tagging_agent is None:
        tagging_agent = create_tagging_agent()
    return tagging_agent


def get_sql_validation_agent():
    """获取SQL纠错校验智能体实例"""
    global sql_validation_agent
    if sql_validation_agent is None:
        sql_validation_agent = create_sql_validation_agent()
    return sql_validation_agent


def reset_agents():
    """重置所有智能体实例（在配置更改后调用）"""
    global metadata_agent, sql_agent, tagging_agent, sql_validation_agent
    metadata_agent = None
    sql_agent = None
    tagging_agent = None
    sql_validation_agent = None


# ============ 认证与权限配置 ============

SESSION_COOKIE_NAME = "data_agent_session"

# 可按需扩展权限点
ROLE_PERMISSIONS: Dict[str, List[str]] = {
    "admin": [
        "tables.read",
        "tables.write",
        "datasource.manage",
        "agent.metadata",
        "agent.sql",
        "agent.tagging",
        "agent.validation",
        "sql.execute",
        "model_config.manage",
    ],
    "user": [
        "tables.read",
        "tables.write",
        "agent.metadata",
        "agent.sql",
        "agent.tagging",
        "agent.validation",
        "sql.execute",
    ],
}

# 演示账号（密码为SHA256）
USER_STORE: Dict[str, Dict[str, str]] = {
    "admin": {
        "password_hash": "b68343b1f7f151c70abaaefdd7a48bc7a0b3f72237d61411c1ff06c2e7c4cbdc",
        "role": "admin",
        "display_name": "系统管理员",
    },
}

# 数据源配置文件（仅管理用途，当前查询链路仍使用本地 finance.db）
DATASOURCE_CONFIG_FILE = Path(__file__).parent.parent.parent / "data" / "datasources.json"
SESSION_DB_FILE = Path(__file__).parent.parent.parent / "data" / "app_sessions.db"
LOCAL_DATASOURCE_ID = "local_finance"


def _ensure_datasource_config_dir():
    DATASOURCE_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)


def _ensure_session_db_dir():
    SESSION_DB_FILE.parent.mkdir(parents=True, exist_ok=True)


def _get_session_connection() -> sqlite3.Connection:
    _ensure_session_db_dir()
    conn = sqlite3.connect(SESSION_DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def _init_session_store() -> None:
    conn = _get_session_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS app_sessions (
                session_token TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                display_name TEXT NOT NULL,
                role TEXT NOT NULL,
                permissions_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_app_sessions_username ON app_sessions(username)"
        )
        conn.commit()
    finally:
        conn.close()


def _delete_expired_sessions() -> None:
    conn = _get_session_connection()
    try:
        conn.execute("DELETE FROM app_sessions WHERE expires_at <= ?", (int(time.time()),))
        conn.commit()
    finally:
        conn.close()


def _create_session(profile: Dict[str, Any], max_age_seconds: int) -> str:
    session_token = secrets.token_urlsafe(32)
    now_ts = int(time.time())
    expires_at = now_ts + max_age_seconds
    conn = _get_session_connection()
    try:
        conn.execute(
            """
            INSERT INTO app_sessions (
                session_token, username, display_name, role, permissions_json, created_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_token,
                profile["username"],
                profile["display_name"],
                profile["role"],
                json.dumps(profile.get("permissions", []), ensure_ascii=False),
                now_ts,
                expires_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return session_token


def _get_session_profile(session_token: str) -> Optional[Dict[str, Any]]:
    conn = _get_session_connection()
    try:
        row = conn.execute(
            """
            SELECT username, display_name, role, permissions_json, expires_at
            FROM app_sessions
            WHERE session_token = ?
            """,
            (session_token,),
        ).fetchone()
        if not row:
            return None
        if int(row["expires_at"]) <= int(time.time()):
            conn.execute("DELETE FROM app_sessions WHERE session_token = ?", (session_token,))
            conn.commit()
            return None
        return {
            "username": row["username"],
            "display_name": row["display_name"],
            "role": row["role"],
            "permissions": json.loads(row["permissions_json"]),
        }
    finally:
        conn.close()


def _delete_session(session_token: str) -> None:
    conn = _get_session_connection()
    try:
        conn.execute("DELETE FROM app_sessions WHERE session_token = ?", (session_token,))
        conn.commit()
    finally:
        conn.close()


def _load_external_datasources() -> List[Dict[str, Any]]:
    _ensure_datasource_config_dir()
    if not DATASOURCE_CONFIG_FILE.exists():
        return []
    try:
        with open(DATASOURCE_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        sources = data.get("sources", [])
        return sources if isinstance(sources, list) else []
    except Exception:
        return []


def _save_external_datasources(sources: List[Dict[str, Any]]) -> None:
    _ensure_datasource_config_dir()
    with open(DATASOURCE_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump({"sources": sources}, f, ensure_ascii=False, indent=2)


def _mask_datasource(source: Dict[str, Any]) -> Dict[str, Any]:
    masked = dict(source)
    if masked.get("password"):
        pwd = str(masked["password"])
        masked["password"] = (pwd[:2] + "*" * max(0, len(pwd) - 4) + pwd[-2:]) if len(pwd) > 4 else "****"
    return masked


def _get_local_datasource() -> Dict[str, Any]:
    return {
        "id": LOCAL_DATASOURCE_ID,
        "name": "本地数据源",
        "type": "sqlite",
        "engine": "sqlite",
        "database": "data/finance.db",
        "is_builtin": True,
        "status": "online",
        "description": "系统内置本地数据源",
    }


def _resolve_datasource(datasource_id: Optional[str]) -> Dict[str, Any]:
    resolved_id = (datasource_id or LOCAL_DATASOURCE_ID).strip()
    if not resolved_id or resolved_id == LOCAL_DATASOURCE_ID:
        return _get_local_datasource()

    external = _load_external_datasources()
    source = next((x for x in external if x.get("id") == resolved_id), None)
    if not source:
        raise HTTPException(status_code=404, detail="数据源不存在")
    return source


def _get_mysql_connection(source: Dict[str, Any]):
    try:
        import pymysql
    except Exception:
        raise HTTPException(status_code=400, detail="缺少 pymysql 依赖，请先安装后再使用 MySQL 数据源")

    try:
        return pymysql.connect(
            host=source.get("host"),
            port=int(source.get("port", 3306)),
            user=source.get("username"),
            password=source.get("password"),
            database=source.get("database"),
            charset=source.get("charset") or "utf8mb4",
            connect_timeout=5,
            read_timeout=10,
            write_timeout=10,
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"MySQL 连接失败: {str(e)}")


def _quote_mysql_identifier(identifier: str) -> str:
    return "`" + str(identifier).replace("`", "``") + "`"


def _fetch_mysql_tables(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    conn = _get_mysql_connection(source)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    t.TABLE_NAME AS table_name,
                    COALESCE(t.TABLE_COMMENT, '') AS description,
                    COALESCE(c.column_count, 0) AS column_count,
                    COALESCE(s.table_rows, 0) AS row_count
                FROM information_schema.TABLES t
                LEFT JOIN (
                    SELECT TABLE_SCHEMA, TABLE_NAME, COUNT(*) AS column_count
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                    GROUP BY TABLE_SCHEMA, TABLE_NAME
                ) c
                    ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
                LEFT JOIN (
                    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS AS table_rows
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = %s
                ) s
                    ON s.TABLE_SCHEMA = t.TABLE_SCHEMA AND s.TABLE_NAME = t.TABLE_NAME
                WHERE t.TABLE_SCHEMA = %s AND t.TABLE_TYPE = 'BASE TABLE'
                ORDER BY t.TABLE_NAME
                """,
                (source.get("database"), source.get("database"), source.get("database")),
            )
            return [
                {
                    "table_name": row["table_name"],
                    "description": row.get("description") or "",
                    "column_count": int(row.get("column_count") or 0),
                    "row_count": int(row.get("row_count") or 0),
                }
                for row in cursor.fetchall()
            ]
    finally:
        conn.close()


def _fetch_mysql_table_schema(source: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    conn = _get_mysql_connection(source)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT TABLE_NAME, COALESCE(TABLE_COMMENT, '') AS table_comment
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (source.get("database"), table_name),
            )
            table_row = cursor.fetchone()
            if not table_row:
                raise HTTPException(status_code=404, detail=f"表 {table_name} 不存在")

            cursor.execute(
                """
                SELECT
                    COLUMN_NAME,
                    COLUMN_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    COLUMN_KEY,
                    COALESCE(COLUMN_COMMENT, '') AS column_comment
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (source.get("database"), table_name),
            )
            columns = [
                {
                    "name": row["COLUMN_NAME"],
                    "type": row["COLUMN_TYPE"],
                    "not_null": row["IS_NULLABLE"] == "NO",
                    "default": row["COLUMN_DEFAULT"],
                    "is_primary_key": row["COLUMN_KEY"] == "PRI",
                    "description": row.get("column_comment") or "",
                }
                for row in cursor.fetchall()
            ]

            cursor.execute(
                """
                SELECT
                    COLUMN_NAME,
                    REFERENCED_TABLE_NAME,
                    REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                  AND REFERENCED_TABLE_NAME IS NOT NULL
                ORDER BY ORDINAL_POSITION
                """,
                (source.get("database"), table_name),
            )
            foreign_keys = [
                {
                    "column": row["COLUMN_NAME"],
                    "references_table": row["REFERENCED_TABLE_NAME"],
                    "references_column": row["REFERENCED_COLUMN_NAME"],
                }
                for row in cursor.fetchall()
            ]

            cursor.execute(
                """
                SELECT INDEX_NAME, NON_UNIQUE
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                GROUP BY INDEX_NAME, NON_UNIQUE
                ORDER BY INDEX_NAME
                """,
                (source.get("database"), table_name),
            )
            indexes = [
                {
                    "name": row["INDEX_NAME"],
                    "unique": int(row["NON_UNIQUE"]) == 0,
                }
                for row in cursor.fetchall()
            ]

            return {
                "table_name": table_name,
                "description": table_row.get("table_comment") or "",
                "columns": columns,
                "foreign_keys": foreign_keys,
                "indexes": indexes,
            }
    finally:
        conn.close()


def _fetch_mysql_table_data(source: Dict[str, Any], table_name: str, limit: int, offset: int) -> Dict[str, Any]:
    conn = _get_mysql_connection(source)
    try:
        with conn.cursor() as cursor:
            quoted_table = _quote_mysql_identifier(table_name)
            cursor.execute(f"SELECT COUNT(*) AS total FROM {quoted_table}")
            total_row = cursor.fetchone() or {}
            total = int(total_row.get("total") or 0)

            schema = _fetch_mysql_table_schema(source, table_name)
            columns = [col["name"] for col in schema["columns"]]

            cursor.execute(f"SELECT * FROM {quoted_table} LIMIT %s OFFSET %s", (limit, offset))
            rows = cursor.fetchall()
            return {
                "columns": columns,
                "data": rows,
                "total": total,
                "limit": limit,
                "offset": offset,
            }
    finally:
        conn.close()


def _fetch_mysql_related_tables(source: Dict[str, Any], table_name: str) -> Dict[str, List[Dict[str, str]]]:
    conn = _get_mysql_connection(source)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COLUMN_NAME,
                    REFERENCED_TABLE_NAME,
                    REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                  AND REFERENCED_TABLE_NAME IS NOT NULL
                ORDER BY ORDINAL_POSITION
                """,
                (source.get("database"), table_name),
            )
            references = [
                {
                    "column": row["COLUMN_NAME"],
                    "referenced_table": row["REFERENCED_TABLE_NAME"],
                    "referenced_column": row["REFERENCED_COLUMN_NAME"],
                }
                for row in cursor.fetchall()
            ]

            cursor.execute(
                """
                SELECT
                    TABLE_NAME,
                    COLUMN_NAME,
                    REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s
                  AND REFERENCED_TABLE_NAME = %s
                  AND REFERENCED_TABLE_NAME IS NOT NULL
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """,
                (source.get("database"), table_name),
            )
            referenced_by = [
                {
                    "table": row["TABLE_NAME"],
                    "column": row["COLUMN_NAME"],
                    "referenced_column": row["REFERENCED_COLUMN_NAME"],
                }
                for row in cursor.fetchall()
            ]
            return {"references": references, "referenced_by": referenced_by}
    finally:
        conn.close()


def _get_tables_for_source(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    if source.get("type") == "sqlite":
        return db_service.get_all_tables()
    if source.get("type") == "mysql":
        return _fetch_mysql_tables(source)
    raise HTTPException(status_code=400, detail=f"暂不支持的数据源类型: {source.get('type')}")


def _get_table_schema_for_source(source: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    if source.get("type") == "sqlite":
        return db_service.get_table_schema(table_name)
    if source.get("type") == "mysql":
        return _fetch_mysql_table_schema(source, table_name)
    raise HTTPException(status_code=400, detail=f"暂不支持的数据源类型: {source.get('type')}")


def _get_table_data_for_source(source: Dict[str, Any], table_name: str, limit: int, offset: int) -> Dict[str, Any]:
    if source.get("type") == "sqlite":
        return db_service.get_table_data(table_name, limit=limit, offset=offset)
    if source.get("type") == "mysql":
        return _fetch_mysql_table_data(source, table_name, limit=limit, offset=offset)
    raise HTTPException(status_code=400, detail=f"暂不支持的数据源类型: {source.get('type')}")


def _get_related_tables_for_source(source: Dict[str, Any], table_name: str) -> Dict[str, List[Dict[str, str]]]:
    if source.get("type") == "sqlite":
        return db_service.get_related_tables(table_name)
    if source.get("type") == "mysql":
        return _fetch_mysql_related_tables(source, table_name)
    raise HTTPException(status_code=400, detail=f"暂不支持的数据源类型: {source.get('type')}")


def _get_database_summary_for_source(source: Dict[str, Any]) -> Dict[str, Any]:
    tables = _get_tables_for_source(source)
    return {
        "table_count": len(tables),
        "total_rows": sum(int(t.get("row_count") or 0) for t in tables),
        "total_columns": sum(int(t.get("column_count") or 0) for t in tables),
        "tables": tables,
        "datasource": {
            "id": source.get("id"),
            "name": source.get("name"),
            "type": source.get("type"),
            "readonly": source.get("type") != "sqlite",
        },
    }


def _get_er_diagram_for_source(source: Dict[str, Any]) -> Dict[str, Any]:
    tables = _get_tables_for_source(source)
    nodes = []
    for table in tables:
        schema = _get_table_schema_for_source(source, table["table_name"])
        columns = [
            {
                "name": col["name"],
                "type": col["type"],
                "is_pk": col["is_primary_key"],
                "is_fk": False,
            }
            for col in schema["columns"]
        ]
        nodes.append(
            {
                "id": table["table_name"],
                "label": table["table_name"],
                "description": table.get("description", ""),
                "row_count": int(table.get("row_count") or 0),
                "columns": columns,
            }
        )

    edges = []
    for table in tables:
        related = _get_related_tables_for_source(source, table["table_name"])
        for ref in related.get("references", []):
            edges.append(
                {
                    "from": table["table_name"],
                    "to": ref["referenced_table"],
                    "from_column": ref["column"],
                    "to_column": ref["referenced_column"],
                    "label": f"{ref['column']} → {ref['referenced_column']}",
                }
            )
            for node in nodes:
                if node["id"] == table["table_name"]:
                    for col in node["columns"]:
                        if col["name"] == ref["column"]:
                            col["is_fk"] = True

    return {"nodes": nodes, "edges": edges}


def _get_schema_text_for_source(source: Dict[str, Any]) -> str:
    tables = _get_tables_for_source(source)
    if not tables:
        return "当前数据源中没有可用数据表。"

    schema_text = []
    for table in tables:
        table_name = table["table_name"]
        schema = _get_table_schema_for_source(source, table_name)
        table_desc = f"表名: {schema['table_name']}"
        if schema.get("description"):
            table_desc += f"\n描述: {schema['description']}"
        schema_text.append(table_desc)
        schema_text.append("字段:")
        for col in schema["columns"]:
            col_line = f"  - {col['name']} ({col['type']})"
            if col.get("is_primary_key"):
                col_line += " [主键]"
            if col.get("not_null"):
                col_line += " [非空]"
            if col.get("description"):
                col_line += f": {col['description']}"
            schema_text.append(col_line)
        if schema.get("foreign_keys"):
            schema_text.append("外键关系:")
            for fk in schema["foreign_keys"]:
                schema_text.append(
                    f"  - {fk['column']} -> {fk['references_table']}.{fk['references_column']}"
                )
        schema_text.append("")
    return "\n".join(schema_text)


def _execute_sql_for_source(source: Dict[str, Any], sql: str) -> Dict[str, Any]:
    sql_clean = sql.strip().rstrip(";")
    if source.get("type") == "sqlite":
        return db_service.execute_sql(sql_clean)
    if source.get("type") != "mysql":
        return {"success": False, "error": f"暂不支持的数据源类型: {source.get('type')}"}

    conn = _get_mysql_connection(source)
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_clean)
            sql_upper = sql_clean.strip().upper()
            if sql_upper.startswith("SELECT") or sql_upper.startswith("EXPLAIN"):
                rows = cursor.fetchall()
                columns = list(rows[0].keys()) if rows else [desc[0] for desc in (cursor.description or [])]
                return {
                    "success": True,
                    "columns": columns,
                    "data": rows,
                    "row_count": len(rows),
                }
            return {"success": True, "affected_rows": cursor.rowcount}
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def _get_sql_validation_agent_for_source(source: Dict[str, Any], enable_thinking: Optional[bool] = None, temperature: Optional[float] = None):
    if source.get("type") == "sqlite" and source.get("id") == LOCAL_DATASOURCE_ID:
        if enable_thinking is None and temperature is None:
            return get_sql_validation_agent()
        return create_sql_validation_agent(enable_thinking=enable_thinking, temperature=temperature)
    return create_sql_validation_agent(
        schema_text=_get_schema_text_for_source(source),
        sql_dialect="MySQL" if source.get("type") == "mysql" else "SQLite",
        sql_executor=lambda sql: _execute_sql_for_source(source, sql),
        tables_provider=lambda: _get_tables_for_source(source),
        table_schema_provider=lambda table_name: _get_table_schema_for_source(source, table_name),
        enable_thinking=enable_thinking,
        temperature=temperature,
    )


def _get_sql_agent_for_source(source: Dict[str, Any], enable_thinking: Optional[bool] = None, temperature: Optional[float] = None):
    if source.get("type") == "sqlite" and source.get("id") == LOCAL_DATASOURCE_ID:
        if enable_thinking is None and temperature is None:
            return get_sql_agent()
        return create_sql_agent(enable_thinking=enable_thinking, temperature=temperature)
    return create_sql_agent(
        schema_text=_get_schema_text_for_source(source),
        sql_dialect="MySQL" if source.get("type") == "mysql" else "SQLite",
        sql_executor=lambda sql: _execute_sql_for_source(source, sql),
        enable_schema_skill=False,
        enable_thinking=enable_thinking,
        temperature=temperature,
    )


def hash_password(password: str) -> str:
    """密码哈希"""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def build_user_profile(username: str) -> Dict[str, Any]:
    """构建用户信息（含权限）"""
    user = USER_STORE[username]
    role = user["role"]
    return {
        "username": username,
        "display_name": user.get("display_name", username),
        "role": role,
        "permissions": ROLE_PERMISSIONS.get(role, []),
    }


def get_current_user(request: Request) -> Dict[str, Any]:
    """从会话中获取当前用户"""
    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_token:
        raise HTTPException(status_code=401, detail="未登录或登录已失效")
    user = _get_session_profile(session_token)
    if not user:
        raise HTTPException(status_code=401, detail="未登录或登录已失效")
    return user


def require_permission(permission: str):
    """权限校验依赖"""

    def _checker(request: Request) -> Dict[str, Any]:
        user = get_current_user(request)
        if permission not in user.get("permissions", []):
            raise HTTPException(status_code=403, detail=f"权限不足: 需要 {permission}")
        return user

    return _checker


# ============ FastAPI应用配置 ============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    print("DATA AGENT 服务启动中...")
    _init_session_store()
    _delete_expired_sessions()
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


# ============ 认证API ============

@app.post("/api/auth/login", tags=["认证"])
async def login(request: LoginRequest, response: Response):
    """登录并建立会话"""
    username = request.username.strip()
    password = request.password

    user = USER_STORE.get(username)
    if not user or user.get("password_hash") != hash_password(password):
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    profile = build_user_profile(username)
    max_age_seconds = 60 * 60 * 12
    session_token = _create_session(profile, max_age_seconds)

    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_token,
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=max_age_seconds,
    )

    return {
        "success": True,
        "message": "登录成功",
        "user": profile,
    }


@app.post("/api/auth/logout", tags=["认证"])
async def logout(request: Request, response: Response):
    """退出登录"""
    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    if session_token:
        _delete_session(session_token)

    response.delete_cookie(SESSION_COOKIE_NAME)
    return {"success": True, "message": "已退出登录"}


@app.get("/api/auth/me", tags=["认证"])
async def auth_me(request: Request):
    """获取当前登录用户"""
    user = get_current_user(request)
    return {"authenticated": True, "user": user}


# ============ 数据库管理API ============

@app.get("/api/tables", response_model=TableListResponse, tags=["数据库管理"], dependencies=[Depends(require_permission("tables.read"))])
async def list_tables(datasource_id: Optional[str] = Query(None, description="数据源ID")):
    """
    获取所有数据表列表
    
    返回数据库中所有表的基本信息，包括表名、描述、字段数量和数据行数
    """
    source = _resolve_datasource(datasource_id)
    tables = _get_tables_for_source(source)
    return TableListResponse(tables=tables, total=len(tables))


@app.get("/api/tables/{table_name}/schema", response_model=TableSchemaResponse, tags=["数据库管理"], dependencies=[Depends(require_permission("tables.read"))])
async def get_table_schema(table_name: str, datasource_id: Optional[str] = Query(None, description="数据源ID")):
    """
    获取指定表的结构信息
    
    Args:
        table_name: 表名
        
    Returns:
        表的详细结构信息，包括字段、外键、索引等
    """
    try:
        source = _resolve_datasource(datasource_id)
        schema = _get_table_schema_for_source(source, table_name)
        return TableSchemaResponse(**schema)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"表 {table_name} 不存在: {str(e)}")


@app.get("/api/tables/{table_name}/data", response_model=TableDataResponse, tags=["数据库管理"], dependencies=[Depends(require_permission("tables.read"))])
async def get_table_data(
    table_name: str,
    datasource_id: Optional[str] = Query(None, description="数据源ID"),
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
        source = _resolve_datasource(datasource_id)
        data = _get_table_data_for_source(source, table_name, limit=limit, offset=offset)
        return TableDataResponse(**data)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"获取数据失败: {str(e)}")


@app.get("/api/tables/{table_name}/related", tags=["数据库管理"], dependencies=[Depends(require_permission("tables.read"))])
async def get_related_tables(table_name: str, datasource_id: Optional[str] = Query(None, description="数据源ID")):
    """
    获取与指定表相关联的表
    
    Args:
        table_name: 表名
        
    Returns:
        关联表信息，包括引用的表和被引用的表
    """
    try:
        source = _resolve_datasource(datasource_id)
        related = _get_related_tables_for_source(source, table_name)
        return related
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"获取关联信息失败: {str(e)}")


@app.get("/api/database/summary", tags=["数据库管理"], dependencies=[Depends(require_permission("tables.read"))])
async def get_database_summary(datasource_id: Optional[str] = Query(None, description="数据源ID")):
    """
    获取数据库概览信息
    
    Returns:
        数据库概览，包括表数量、总记录数等
    """
    source = _resolve_datasource(datasource_id)
    return _get_database_summary_for_source(source)


@app.get("/api/database/er-diagram", tags=["数据库管理"], dependencies=[Depends(require_permission("tables.read"))])
async def get_er_diagram_data(datasource_id: Optional[str] = Query(None, description="数据源ID")):
    """
    获取ER图数据，包含所有表和它们之间的关联关系
    
    Returns:
        ER图数据，包含节点（表）和边（关联关系）
    """
    try:
        source = _resolve_datasource(datasource_id)
        return _get_er_diagram_for_source(source)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取ER图数据失败: {str(e)}")


@app.post("/api/sql/execute", tags=["数据库管理"], dependencies=[Depends(require_permission("sql.execute"))])
async def execute_sql(request: SQLExecuteRequest, datasource_id: Optional[str] = Query(None, description="数据源ID")):
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
    
    source = _resolve_datasource(datasource_id)
    result = _execute_sql_for_source(source, sql)
    
    if not result['success']:
        raise HTTPException(status_code=400, detail=result.get('error', '执行失败'))
    
    return result


# ============ 元数据管理API ============

@app.put("/api/tables/{table_name}/description", tags=["元数据管理"], dependencies=[Depends(require_permission("tables.write"))])
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


@app.put("/api/tables/{table_name}/columns/{column_name}/description", tags=["元数据管理"], dependencies=[Depends(require_permission("tables.write"))])
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


@app.get("/api/metadata/missing", tags=["元数据管理"], dependencies=[Depends(require_permission("agent.metadata"))])
async def get_missing_metadata():
    """
    获取缺少元数据描述的表和字段列表
    
    Returns:
        缺少元数据的表和字段信息
    """
    return db_service.get_tables_with_missing_metadata()


# ============ 元数据补全Agent API ============

@app.post("/api/agent/metadata/generate", tags=["元数据Agent"], dependencies=[Depends(require_permission("agent.metadata"))])
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


@app.post("/api/agent/metadata/generate/stream", tags=["元数据Agent"], dependencies=[Depends(require_permission("agent.metadata"))])
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


@app.post("/api/agent/metadata/apply", tags=["元数据Agent"], dependencies=[Depends(require_permission("tables.write"))])
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

@app.post("/api/agent/sql/generate", tags=["SQL Agent"], dependencies=[Depends(require_permission("agent.sql"))])
async def generate_sql(request: SQLGenerateRequest, datasource_id: Optional[str] = Query(None, description="数据源ID")):
    """
    使用AI智能体根据自然语言需求生成SQL
    
    该接口会调用SQL生成Agent，理解用户需求并生成相应的SQL语句
    
    Args:
        request: 生成请求，包含自然语言需求描述
        
    Returns:
        生成的SQL语句和说明
    """
    try:
        source = _resolve_datasource(datasource_id)
        agent = _get_sql_agent_for_source(source, request.enable_thinking, request.temperature)
        result = await agent.generate_sql(request.requirement, request.context)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成SQL失败: {str(e)}")


@app.post("/api/agent/sql/generate/stream", tags=["SQL Agent"], dependencies=[Depends(require_permission("agent.sql"))])
async def generate_sql_stream(request: SQLGenerateRequest, datasource_id: Optional[str] = Query(None, description="数据源ID")):
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
            source = _resolve_datasource(datasource_id)
            agent = _get_sql_agent_for_source(source, request.enable_thinking, request.temperature)
            
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


@app.post("/api/agent/sql/explain", tags=["SQL Agent"], dependencies=[Depends(require_permission("agent.sql"))])
async def explain_sql(request: SQLExplainRequest):
    """
    使用AI智能体解释SQL语句的功能
    
    Args:
        request: 解释请求，包含SQL语句
        
    Returns:
        SQL语句的功能解释
    """
    try:
        agent = create_sql_agent(enable_thinking=request.enable_thinking, temperature=request.temperature) if (request.enable_thinking is not None or request.temperature is not None) else get_sql_agent()
        result = await agent.explain_sql(request.sql)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"解释SQL失败: {str(e)}")


@app.post("/api/agent/sql/explain/stream", tags=["SQL Agent"], dependencies=[Depends(require_permission("agent.sql"))])
async def explain_sql_stream(request: SQLExplainRequest):
    """
    流式解释SQL语句的功能（SSE）

    Args:
        request: 解释请求，包含SQL语句

    Returns:
        SSE流式响应，包含开始、处理中、完成事件
    """
    import json

    async def event_generator():
        try:
            agent = create_sql_agent(enable_thinking=request.enable_thinking, temperature=request.temperature) if (request.enable_thinking is not None or request.temperature is not None) else get_sql_agent()
            task = asyncio.create_task(agent.explain_sql(request.sql))

            start_event = json.dumps(
                {"type": "start", "message": "开始解释SQL..."},
                ensure_ascii=False,
            )
            yield "data: " + start_event + "\n\n"

            elapsed = 0
            while not task.done():
                await asyncio.sleep(1)
                elapsed += 1
                progress_event = json.dumps(
                    {
                        "type": "thinking",
                        "message": f"AI正在分析SQL结构与语义（{elapsed}s）..."
                    },
                    ensure_ascii=False,
                )
                yield "data: " + progress_event + "\n\n"

            result = await task
            result_event = json.dumps(
                {"type": "result", "data": result},
                ensure_ascii=False,
            )
            yield "data: " + result_event + "\n\n"

            end_event = json.dumps(
                {"type": "end", "message": "解释完成"},
                ensure_ascii=False,
            )
            yield "data: " + end_event + "\n\n"

        except Exception as e:
            error_event = json.dumps(
                {"type": "error", "message": f"解释SQL失败: {str(e)}"},
                ensure_ascii=False,
            )
            yield "data: " + error_event + "\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/agent/sql/optimize", tags=["SQL Agent"], dependencies=[Depends(require_permission("agent.sql"))])
async def optimize_sql(request: SQLExplainRequest):
    """
    使用AI智能体优化SQL语句
    
    Args:
        request: 优化请求，包含SQL语句
        
    Returns:
        优化建议和优化后的SQL
    """
    try:
        agent = create_sql_agent(enable_thinking=request.enable_thinking, temperature=request.temperature) if (request.enable_thinking is not None or request.temperature is not None) else get_sql_agent()
        result = await agent.optimize_sql(request.sql)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"优化SQL失败: {str(e)}")


# ============ 标签管理API ============

@app.get("/api/tables/{table_name}/tags", tags=["标签管理"], dependencies=[Depends(require_permission("tables.read"))])
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


@app.post("/api/tables/{table_name}/tags", tags=["标签管理"], dependencies=[Depends(require_permission("agent.tagging"))])
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

@app.post("/api/agent/tagging/generate/stream", tags=["打标Agent"], dependencies=[Depends(require_permission("agent.tagging"))])
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


@app.post("/api/agent/tagging/apply", tags=["打标Agent"], dependencies=[Depends(require_permission("agent.tagging"))])
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

@app.post("/api/agent/sql-validation/validate/stream", tags=["SQL校验Agent"], dependencies=[Depends(require_permission("agent.validation"))])
async def validate_sql_stream(request: SQLValidateRequest, datasource_id: Optional[str] = Query(None, description="数据源ID")):
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
            source = _resolve_datasource(datasource_id)
            agent = _get_sql_validation_agent_for_source(source, request.enable_thinking, request.temperature)
            
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


@app.post("/api/agent/sql-validation/validate", tags=["SQL校验Agent"], dependencies=[Depends(require_permission("agent.validation"))])
async def validate_sql(request: SQLValidateRequest, datasource_id: Optional[str] = Query(None, description="数据源ID")):
    """
    使用AI智能体校验SQL语句（非流式）
    
    Args:
        request: 校验请求，包含SQL语句
        
    Returns:
        校验结果
    """
    try:
        source = _resolve_datasource(datasource_id)
        agent = _get_sql_validation_agent_for_source(source, request.enable_thinking, request.temperature)
        result = await agent.validate_sql(request.sql)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"校验SQL失败: {str(e)}")


@app.post("/api/agent/sql-validation/generate", tags=["SQL校验Agent"], dependencies=[Depends(require_permission("agent.validation"))])
async def generate_test_sql(request: SQLGenerateTypeRequest = None, datasource_id: Optional[str] = Query(None, description="数据源ID")):
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
        source = _resolve_datasource(datasource_id)
        agent = _get_sql_validation_agent_for_source(source)
        sql_type = request.sql_type if request else None
        result = await agent.generate_test_sql(sql_type)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成测试SQL失败: {str(e)}")


# ============ 模型配置API ============

@app.get("/api/model/config", tags=["模型配置"], dependencies=[Depends(require_permission("model_config.manage"))])
async def get_model_config():
    """
    获取当前模型配置

    Returns:
        当前的模型配置，包含 base_url, api_key(部分隐藏), model_name
    """
    from config import ModelConfig

    config = ModelConfig.load()
    # 隐藏API密钥的大部分内容
    masked_key = config["api_key"][:4] + "..." + config["api_key"][-4:] if len(config["api_key"]) > 8 else "****"
    return {
        "base_url": config["base_url"],
        "api_key": masked_key,
        "model_name": config["model_name"],
        "full_key": config["api_key"],  # 完整key，前端保存到内存中
        "temperature": float(config.get("temperature", 0.7)),
    }


@app.post("/api/model/config", tags=["模型配置"], dependencies=[Depends(require_permission("model_config.manage"))])
async def save_model_config(request: ModelConfigRequest):
    """
    保存模型配置

    Args:
        request: 模型配置请求

    Returns:
        保存结果
    """
    from config import ModelConfig

    success = ModelConfig.save({
        "base_url": request.base_url,
        "api_key": request.api_key,
        "model_name": request.model_name,
        "temperature": request.temperature,
    })

    if success:
        # 重置所有智能体实例
        reset_agents()
        # 清除配置缓存
        ModelConfig.clear_cache()
        return {
            "success": True,
            "message": "模型配置已保存",
            "config": {
                "base_url": request.base_url,
                "api_key": request.api_key[:4] + "..." + request.api_key[-4:],
                "model_name": request.model_name,
                "temperature": request.temperature,
            }
        }
    else:
        raise HTTPException(status_code=500, detail="保存模型配置失败")


@app.post("/api/model/test", tags=["模型配置"], dependencies=[Depends(require_permission("model_config.manage"))])
async def test_model_config(request: ModelConfigRequest):
    """
    测试模型配置是否可用

    Args:
        request: 模型配置请求

    Returns:
        测试结果
    """
    try:
        # 简单测试：创建一个模型实例并发送一条消息
        from agentscope.model import OpenAIChatModel
        from config import ModelConfig

        generate_kwargs = ModelConfig.get_generate_kwargs(
            base_url=request.base_url,
            model_name=request.model_name,
            stream=False,
            temperature=request.temperature,
        )
        generate_kwargs["max_tokens"] = 5

        model = OpenAIChatModel(
            model_name=request.model_name,
            api_key=request.api_key,
            client_kwargs={"base_url": request.base_url},
            stream=False,
            generate_kwargs=generate_kwargs
        )

        # 发送一个简单的测试消息
        response = await model(
            messages=[{"role": "user", "content": "你好，请回复'测试成功'"}]
        )

        return {
            "success": True,
            "message": "测试成功",
            "response": str(response)[:200]  # 只返回前200个字符
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"测试失败: {str(e)}")


# ============ 数据源管理API ============

@app.get("/api/datasources", tags=["数据源管理"], dependencies=[Depends(require_permission("datasource.manage"))])
async def list_datasources():
    """获取数据源列表（内置本地 + 外部接入）"""
    local_source = {
        "id": "local_finance",
        "name": "本地数据源",
        "type": "sqlite",
        "engine": "sqlite",
        "database": "data/finance.db",
        "is_builtin": True,
        "status": "online",
        "description": "系统内置本地数据源（当前业务查询默认使用）",
    }
    external = _load_external_datasources()
    return {
        "sources": [local_source] + [_mask_datasource(x) for x in external],
        "total": 1 + len(external),
    }


@app.post("/api/datasources", tags=["数据源管理"], dependencies=[Depends(require_permission("datasource.manage"))])
async def create_datasource(request: DataSourceCreateRequest):
    """新增外部数据源（当前支持 mysql）"""
    ds_type = request.type.strip().lower()
    if ds_type != "mysql":
        raise HTTPException(status_code=400, detail="当前仅支持 mysql 类型数据源")

    external = _load_external_datasources()
    source = {
        "id": f"mysql_{uuid.uuid4().hex[:10]}",
        "name": request.name.strip(),
        "type": "mysql",
        "engine": "mysql",
        "host": request.host.strip(),
        "port": request.port,
        "username": request.username.strip(),
        "password": request.password,
        "database": request.database.strip(),
        "charset": request.charset.strip() or "utf8mb4",
        "is_builtin": False,
        "status": "unknown",
    }
    external.append(source)
    _save_external_datasources(external)
    return {"success": True, "message": "数据源已添加", "source": _mask_datasource(source)}


@app.delete("/api/datasources/{source_id}", tags=["数据源管理"], dependencies=[Depends(require_permission("datasource.manage"))])
async def delete_datasource(source_id: str):
    """删除外部数据源"""
    external = _load_external_datasources()
    before = len(external)
    external = [x for x in external if x.get("id") != source_id]
    if len(external) == before:
        raise HTTPException(status_code=404, detail="数据源不存在")
    _save_external_datasources(external)
    return {"success": True, "message": "数据源已删除"}


@app.post("/api/datasources/test", tags=["数据源管理"], dependencies=[Depends(require_permission("datasource.manage"))])
async def test_datasource(request: DataSourceTestRequest):
    """测试外部数据源连通性（当前支持 mysql）"""
    ds_type = request.type.strip().lower()
    if ds_type != "mysql":
        raise HTTPException(status_code=400, detail="当前仅支持 mysql 类型测试")

    try:
        import pymysql
    except Exception:
        raise HTTPException(status_code=400, detail="缺少 pymysql 依赖，请先安装后再测试 MySQL 连接")

    try:
        conn = pymysql.connect(
            host=request.host,
            port=request.port,
            user=request.username,
            password=request.password,
            database=request.database,
            charset=request.charset or "utf8mb4",
            connect_timeout=5,
            read_timeout=5,
            write_timeout=5,
            autocommit=True,
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            _ = cursor.fetchone()
        conn.close()
        return {"success": True, "message": "MySQL 连接测试成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"MySQL 连接测试失败: {str(e)}")


@app.post("/api/datasources/{source_id}/test", tags=["数据源管理"], dependencies=[Depends(require_permission("datasource.manage"))])
async def test_datasource_by_id(source_id: str):
    """根据已保存配置测试数据源连接"""
    external = _load_external_datasources()
    source = next((x for x in external if x.get("id") == source_id), None)
    if not source:
        raise HTTPException(status_code=404, detail="数据源不存在")

    ds_type = str(source.get("type", "")).lower()
    if ds_type != "mysql":
        raise HTTPException(status_code=400, detail="当前仅支持 mysql 类型测试")

    try:
        import pymysql
    except Exception:
        raise HTTPException(status_code=400, detail="缺少 pymysql 依赖，请先安装后再测试 MySQL 连接")

    try:
        conn = pymysql.connect(
            host=source.get("host"),
            port=int(source.get("port", 3306)),
            user=source.get("username"),
            password=source.get("password"),
            database=source.get("database"),
            charset=source.get("charset") or "utf8mb4",
            connect_timeout=5,
            read_timeout=5,
            write_timeout=5,
            autocommit=True,
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            _ = cursor.fetchone()
        conn.close()
        return {"success": True, "message": "MySQL 连接测试成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"MySQL 连接测试失败: {str(e)}")


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
        return FileResponse(
            str(index_file),
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
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
