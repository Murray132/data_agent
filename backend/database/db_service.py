# -*- coding: utf-8 -*-
"""
数据库服务模块
提供数据库连接管理、元数据查询、数据预览等功能
"""

import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import json


class DatabaseService:
    """
    数据库服务类
    封装所有数据库操作，包括：
    - 连接管理
    - 元数据查询（表信息、字段信息）
    - 数据预览
    - SQL执行
    - 元数据更新
    """
    
    def __init__(self, db_path: str = None):
        """
        初始化数据库服务
        
        Args:
            db_path: 数据库文件路径，默认使用data目录下的finance.db
        """
        if db_path is None:
            # 从当前文件向上找到data_agent目录
            current_dir = Path(__file__).resolve().parent
            # backend/database -> backend -> data_agent
            data_agent_dir = current_dir.parent.parent
            db_path = data_agent_dir / "data" / "finance.db"
        self.db_path = str(db_path)
    
    def get_connection(self) -> sqlite3.Connection:
        """
        获取数据库连接
        
        Returns:
            sqlite3.Connection: 数据库连接对象
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # 使查询结果可以通过列名访问
        return conn
    
    def get_all_tables(self) -> List[Dict[str, Any]]:
        """
        获取所有数据表信息
        
        Returns:
            List[Dict]: 表信息列表，每个字典包含：
                - table_name: 表名
                - description: 表描述
                - column_count: 字段数量
                - row_count: 数据行数
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # 获取所有用户表（排除元数据表和sqlite内部表）
            # 注意：SQLite中下划线_是LIKE的通配符，需要用ESCAPE转义
            cursor.execute(r"""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name NOT LIKE 'sqlite_%'
                AND name NOT LIKE '\_%' ESCAPE '\'
                ORDER BY name
            """)
            tables = cursor.fetchall()
            
            result = []
            for table in tables:
                table_name = table[0]
                
                # 获取表描述
                cursor.execute(
                    "SELECT description FROM _table_metadata WHERE table_name = ?",
                    (table_name,)
                )
                desc_row = cursor.fetchone()
                description = desc_row[0] if desc_row else ""
                
                # 获取字段数量
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                column_count = len(columns)
                
                # 获取数据行数
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                
                result.append({
                    "table_name": table_name,
                    "description": description,
                    "column_count": column_count,
                    "row_count": row_count
                })
            
            return result
        finally:
            conn.close()
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        获取指定表的详细结构信息
        
        Args:
            table_name: 表名
            
        Returns:
            Dict: 表结构信息，包含：
                - table_name: 表名
                - description: 表描述
                - columns: 字段列表
                - foreign_keys: 外键关系
                - indexes: 索引信息
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # 获取表描述
            cursor.execute(
                "SELECT description FROM _table_metadata WHERE table_name = ?",
                (table_name,)
            )
            desc_row = cursor.fetchone()
            table_description = desc_row[0] if desc_row else ""
            
            # 获取字段信息
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()
            
            columns = []
            for col in columns_info:
                col_name = col[1]
                col_type = col[2]
                not_null = bool(col[3])
                default_value = col[4]
                is_pk = bool(col[5])
                
                # 获取字段描述
                cursor.execute(
                    "SELECT description FROM _column_metadata WHERE table_name = ? AND column_name = ?",
                    (table_name, col_name)
                )
                col_desc_row = cursor.fetchone()
                col_description = col_desc_row[0] if col_desc_row else ""
                
                columns.append({
                    "name": col_name,
                    "type": col_type,
                    "not_null": not_null,
                    "default": default_value,
                    "is_primary_key": is_pk,
                    "description": col_description
                })
            
            # 获取外键信息
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            fk_info = cursor.fetchall()
            foreign_keys = []
            for fk in fk_info:
                foreign_keys.append({
                    "column": fk[3],
                    "references_table": fk[2],
                    "references_column": fk[4]
                })
            
            # 获取索引信息
            cursor.execute(f"PRAGMA index_list({table_name})")
            idx_info = cursor.fetchall()
            indexes = []
            for idx in idx_info:
                indexes.append({
                    "name": idx[1],
                    "unique": bool(idx[2])
                })
            
            return {
                "table_name": table_name,
                "description": table_description,
                "columns": columns,
                "foreign_keys": foreign_keys,
                "indexes": indexes
            }
        finally:
            conn.close()
    
    def get_table_data(self, table_name: str, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        获取表数据预览
        
        Args:
            table_name: 表名
            limit: 返回行数限制，默认100
            offset: 偏移量，默认0
            
        Returns:
            Dict: 包含：
                - columns: 列名列表
                - data: 数据行列表
                - total: 总行数
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # 获取总行数
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total = cursor.fetchone()[0]
            
            # 获取列名
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]
            
            # 获取数据
            cursor.execute(f"SELECT * FROM {table_name} LIMIT ? OFFSET ?", (limit, offset))
            rows = cursor.fetchall()
            
            # 转换为字典列表
            data = []
            for row in rows:
                data.append(dict(zip(columns, row)))
            
            return {
                "columns": columns,
                "data": data,
                "total": total,
                "limit": limit,
                "offset": offset
            }
        finally:
            conn.close()
    
    def execute_sql(self, sql: str, params: tuple = None) -> Dict[str, Any]:
        """
        执行SQL查询
        
        Args:
            sql: SQL语句
            params: 参数元组
            
        Returns:
            Dict: 查询结果，包含：
                - success: 是否成功
                - columns: 列名列表（查询语句）
                - data: 数据行列表（查询语句）
                - affected_rows: 受影响行数（修改语句）
                - error: 错误信息（如果失败）
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            # 判断是查询还是修改
            sql_upper = sql.strip().upper()
            if sql_upper.startswith("SELECT"):
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = cursor.fetchall()
                data = [dict(zip(columns, row)) for row in rows]
                return {
                    "success": True,
                    "columns": columns,
                    "data": data,
                    "row_count": len(data)
                }
            else:
                conn.commit()
                return {
                    "success": True,
                    "affected_rows": cursor.rowcount
                }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            conn.close()
    
    def update_table_description(self, table_name: str, description: str) -> bool:
        """
        更新表描述
        
        Args:
            table_name: 表名
            description: 新描述
            
        Returns:
            bool: 是否成功
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO _table_metadata (table_name, description) VALUES (?, ?)",
                (table_name, description)
            )
            conn.commit()
            return True
        except Exception as e:
            print(f"更新表描述失败: {e}")
            return False
        finally:
            conn.close()
    
    def update_column_description(self, table_name: str, column_name: str, description: str) -> bool:
        """
        更新字段描述
        
        Args:
            table_name: 表名
            column_name: 字段名
            description: 新描述
            
        Returns:
            bool: 是否成功
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO _column_metadata (table_name, column_name, description) VALUES (?, ?, ?)",
                (table_name, column_name, description)
            )
            conn.commit()
            return True
        except Exception as e:
            print(f"更新字段描述失败: {e}")
            return False
        finally:
            conn.close()
    
    def get_related_tables(self, table_name: str) -> Dict[str, List[Dict[str, str]]]:
        """
        获取与指定表相关的其他表（通过外键关系）
        
        Args:
            table_name: 表名
            
        Returns:
            Dict: 包含引用的表和被引用的表
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # 获取该表引用的其他表
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            references = []
            for fk in cursor.fetchall():
                references.append({
                    "column": fk[3],
                    "referenced_table": fk[2],
                    "referenced_column": fk[4]
                })
            
            # 获取引用该表的其他表
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name NOT LIKE 'sqlite_%'
                AND name NOT LIKE '_%'
            """)
            all_tables = [row[0] for row in cursor.fetchall()]
            
            referenced_by = []
            for other_table in all_tables:
                if other_table == table_name:
                    continue
                cursor.execute(f"PRAGMA foreign_key_list({other_table})")
                for fk in cursor.fetchall():
                    if fk[2] == table_name:
                        referenced_by.append({
                            "table": other_table,
                            "column": fk[3],
                            "referenced_column": fk[4]
                        })
            
            return {
                "references": references,  # 该表引用的表
                "referenced_by": referenced_by  # 引用该表的表
            }
        finally:
            conn.close()
    
    def get_sample_values(self, table_name: str, column_name: str, limit: int = 10) -> List[Any]:
        """
        获取字段的样本值
        
        Args:
            table_name: 表名
            column_name: 字段名
            limit: 返回数量限制
            
        Returns:
            List: 样本值列表
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT DISTINCT {column_name} 
                FROM {table_name} 
                WHERE {column_name} IS NOT NULL 
                LIMIT ?
            """, (limit,))
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"获取样本值失败: {e}")
            return []
        finally:
            conn.close()
    
    def get_database_summary(self) -> Dict[str, Any]:
        """
        获取数据库概览信息
        
        Returns:
            Dict: 数据库概览，包含表数量、总记录数等
        """
        tables = self.get_all_tables()
        total_rows = sum(t["row_count"] for t in tables)
        total_columns = sum(t["column_count"] for t in tables)
        
        return {
            "table_count": len(tables),
            "total_rows": total_rows,
            "total_columns": total_columns,
            "tables": tables
        }
    
    def get_schema_for_llm(self, table_names: List[str] = None) -> str:
        """
        生成供大模型使用的数据库结构描述文本
        
        Args:
            table_names: 指定表名列表，为None时获取所有表
            
        Returns:
            str: 数据库结构的文本描述
        """
        if table_names is None:
            tables_info = self.get_all_tables()
            table_names = [t["table_name"] for t in tables_info]
        
        schema_text = []
        for table_name in table_names:
            schema = self.get_table_schema(table_name)
            
            # 表头
            table_desc = f"表名: {schema['table_name']}"
            if schema['description']:
                table_desc += f"\n描述: {schema['description']}"
            schema_text.append(table_desc)
            
            # 字段信息
            schema_text.append("字段:")
            for col in schema['columns']:
                col_line = f"  - {col['name']} ({col['type']})"
                if col['is_primary_key']:
                    col_line += " [主键]"
                if col['not_null']:
                    col_line += " [非空]"
                if col['description']:
                    col_line += f": {col['description']}"
                schema_text.append(col_line)
            
            # 外键关系
            if schema['foreign_keys']:
                schema_text.append("外键关系:")
                for fk in schema['foreign_keys']:
                    schema_text.append(
                        f"  - {fk['column']} -> {fk['references_table']}.{fk['references_column']}"
                    )
            
            schema_text.append("")  # 空行分隔
        
        return "\n".join(schema_text)
    
    def get_tables_with_missing_metadata(self) -> List[Dict[str, Any]]:
        """
        获取缺少元数据描述的表和字段
        
        Returns:
            List[Dict]: 缺少元数据的表和字段信息
        """
        tables = self.get_all_tables()
        result = []
        
        for table_info in tables:
            table_name = table_info["table_name"]
            schema = self.get_table_schema(table_name)
            
            missing_info = {
                "table_name": table_name,
                "missing_table_description": not schema["description"],
                "missing_column_descriptions": []
            }
            
            for col in schema["columns"]:
                if not col["description"]:
                    missing_info["missing_column_descriptions"].append(col["name"])
            
            if missing_info["missing_table_description"] or missing_info["missing_column_descriptions"]:
                result.append(missing_info)
        
        return result
    
    # ============ 标签管理功能 ============
    
    def init_tag_tables(self) -> bool:
        """
        初始化标签表（如果不存在则创建）
        
        Returns:
            bool: 是否成功
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # 创建表标签表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS _table_tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by TEXT DEFAULT 'llm',
                    UNIQUE(table_name, tag)
                )
            """)
            
            # 创建字段标签表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS _column_tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by TEXT DEFAULT 'llm',
                    UNIQUE(table_name, column_name, tag)
                )
            """)
            
            conn.commit()
            return True
        except Exception as e:
            print(f"初始化标签表失败: {e}")
            return False
        finally:
            conn.close()
    
    def get_table_tags(self, table_name: str) -> List[Dict[str, Any]]:
        """
        获取表的标签列表
        
        Args:
            table_name: 表名
            
        Returns:
            List[Dict]: 标签列表，每个包含tag, created_at, created_by
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT tag, created_at, created_by FROM _table_tags WHERE table_name = ? ORDER BY created_at",
                (table_name,)
            )
            return [
                {"tag": row[0], "created_at": row[1], "created_by": row[2]}
                for row in cursor.fetchall()
            ]
        except Exception as e:
            print(f"获取表标签失败: {e}")
            return []
        finally:
            conn.close()
    
    def add_table_tags(self, table_name: str, tags: List[str], created_by: str = 'llm') -> bool:
        """
        为表添加标签
        
        Args:
            table_name: 表名
            tags: 标签列表
            created_by: 创建者（'llm' 或 'user'）
            
        Returns:
            bool: 是否成功
        """
        if not tags:
            return True
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            for tag in tags:
                cursor.execute(
                    "INSERT OR IGNORE INTO _table_tags (table_name, tag, created_by) VALUES (?, ?, ?)",
                    (table_name, tag.strip(), created_by)
                )
            conn.commit()
            return True
        except Exception as e:
            print(f"添加表标签失败: {e}")
            return False
        finally:
            conn.close()
    
    def delete_table_tag(self, table_name: str, tag: str) -> bool:
        """
        删除表的指定标签
        
        Args:
            table_name: 表名
            tag: 要删除的标签
            
        Returns:
            bool: 是否成功
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM _table_tags WHERE table_name = ? AND tag = ?",
                (table_name, tag)
            )
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"删除表标签失败: {e}")
            return False
        finally:
            conn.close()
    
    def clear_table_tags(self, table_name: str) -> bool:
        """
        清空表的所有标签
        
        Args:
            table_name: 表名
            
        Returns:
            bool: 是否成功
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM _table_tags WHERE table_name = ?", (table_name,))
            conn.commit()
            return True
        except Exception as e:
            print(f"清空表标签失败: {e}")
            return False
        finally:
            conn.close()
    
    def get_column_tags(self, table_name: str, column_name: str) -> List[Dict[str, Any]]:
        """
        获取字段的标签列表
        
        Args:
            table_name: 表名
            column_name: 字段名
            
        Returns:
            List[Dict]: 标签列表
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT tag, created_at, created_by FROM _column_tags WHERE table_name = ? AND column_name = ? ORDER BY created_at",
                (table_name, column_name)
            )
            return [
                {"tag": row[0], "created_at": row[1], "created_by": row[2]}
                for row in cursor.fetchall()
            ]
        except Exception as e:
            print(f"获取字段标签失败: {e}")
            return []
        finally:
            conn.close()
    
    def add_column_tags(self, table_name: str, column_name: str, tags: List[str], created_by: str = 'llm') -> bool:
        """
        为字段添加标签
        
        Args:
            table_name: 表名
            column_name: 字段名
            tags: 标签列表
            created_by: 创建者
            
        Returns:
            bool: 是否成功
        """
        if not tags:
            return True
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            for tag in tags:
                cursor.execute(
                    "INSERT OR IGNORE INTO _column_tags (table_name, column_name, tag, created_by) VALUES (?, ?, ?, ?)",
                    (table_name, column_name, tag.strip(), created_by)
                )
            conn.commit()
            return True
        except Exception as e:
            print(f"添加字段标签失败: {e}")
            return False
        finally:
            conn.close()
    
    def delete_column_tag(self, table_name: str, column_name: str, tag: str) -> bool:
        """
        删除字段的指定标签
        
        Args:
            table_name: 表名
            column_name: 字段名
            tag: 要删除的标签
            
        Returns:
            bool: 是否成功
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM _column_tags WHERE table_name = ? AND column_name = ? AND tag = ?",
                (table_name, column_name, tag)
            )
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"删除字段标签失败: {e}")
            return False
        finally:
            conn.close()
    
    def clear_column_tags(self, table_name: str, column_name: str) -> bool:
        """
        清空字段的所有标签
        
        Args:
            table_name: 表名
            column_name: 字段名
            
        Returns:
            bool: 是否成功
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM _column_tags WHERE table_name = ? AND column_name = ?",
                (table_name, column_name)
            )
            conn.commit()
            return True
        except Exception as e:
            print(f"清空字段标签失败: {e}")
            return False
        finally:
            conn.close()
    
    def get_all_tags_for_table(self, table_name: str) -> Dict[str, Any]:
        """
        获取表和其所有字段的标签
        
        Args:
            table_name: 表名
            
        Returns:
            Dict: 包含 table_name, table_tags, column_tags
        """
        # 获取表标签
        table_tags = self.get_table_tags(table_name)
        
        # 获取所有字段
        schema = self.get_table_schema(table_name)
        column_names = [col["name"] for col in schema.get("columns", [])]
        
        # 获取每个字段的标签
        column_tags = {}
        for col_name in column_names:
            tags = self.get_column_tags(table_name, col_name)
            column_tags[col_name] = tags
        
        return {
            "table_name": table_name,
            "table_tags": table_tags,
            "column_tags": column_tags
        }
    
    def add_all_tags_for_table(
        self, 
        table_name: str, 
        table_tags: List[str], 
        column_tags: Dict[str, List[str]],
        created_by: str = 'llm'
    ) -> bool:
        """
        批量添加表和字段的标签
        
        Args:
            table_name: 表名
            table_tags: 表标签列表
            column_tags: 字段标签字典 {column_name: [tags]}
            created_by: 创建者
            
        Returns:
            bool: 是否成功
        """
        # 添加表标签
        if table_tags:
            if not self.add_table_tags(table_name, table_tags, created_by):
                return False
        
        # 添加字段标签
        for col_name, tags in column_tags.items():
            if tags:
                if not self.add_column_tags(table_name, col_name, tags, created_by):
                    return False
        
        return True


# 创建全局数据库服务实例
db_service = DatabaseService()

# 初始化标签表
db_service.init_tag_tables()
