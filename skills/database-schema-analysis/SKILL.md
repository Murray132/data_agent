---
name: Database Schema Analysis
description: 分析数据库表结构、字段信息和表间关系，支持元数据补全和SQL生成
---

<!-- "progressive disclosure"（渐进式披露）-->

# Database Schema Analysis Skill

数据库结构分析技能，提供统一的数据库元数据访问和分析能力。

## 概述

该技能封装了数据库表结构查询、样本数据获取、关联关系分析等核心功能，
为元数据补全Agent和SQL生成Agent提供通用的数据库分析基础能力。

## 使用方式

本技能通过Python脚本提供数据库分析能力。使用时需要执行skill目录下的`tools/db_tools.py`脚本。

### 执行命令格式

```bash
python {skill_dir}/tools/db_tools.py --action <action_name> [--args ...]
```

## 可用工具

### 1. list_all_tables
列出数据库中所有表及其基本信息。

**命令：**
```bash
python {skill_dir}/tools/db_tools.py --action list_all_tables
```

**返回内容：**
- 表名
- 表描述
- 数据量
- 字段数量

**使用场景：**
- 初次了解数据库结构
- 搜索相关业务表

### 2. get_table_schema
获取指定表的详细结构信息。

**命令：**
```bash
python {skill_dir}/tools/db_tools.py --action get_table_schema --table_name <表名>
```

**参数：**
- `--table_name`: 表名

**返回内容：**
- 字段名、类型、约束
- 主键信息
- 外键关系
- 默认值和注释

**使用场景：**
- 理解表结构
- 生成SQL时确定字段

### 3. get_sample_data
获取表的样本数据。

**命令：**
```bash
python {skill_dir}/tools/db_tools.py --action get_sample_data --table_name <表名> [--limit <条数>]
```

**参数：**
- `--table_name`: 表名
- `--limit`: 返回条数（默认5条）

**返回内容：**
- 前N条数据记录
- 各字段的实际值

**使用场景：**
- 理解数据格式
- 验证字段含义

### 4. get_related_tables
获取与指定表有关联关系的表。

**命令：**
```bash
python {skill_dir}/tools/db_tools.py --action get_related_tables --table_name <表名>
```

**参数：**
- `--table_name`: 表名

**返回内容：**
- 引用的表（外键指向）
- 被引用的表（被其他表外键引用）
- 关联字段信息

**使用场景：**
- 理解表间关系
- 设计JOIN查询

### 5. get_sample_values
获取指定字段的样本值。

**命令：**
```bash
python {skill_dir}/tools/db_tools.py --action get_sample_values --table_name <表名> --column_name <字段名> [--limit <条数>]
```

**参数：**
- `--table_name`: 表名
- `--column_name`: 字段名
- `--limit`: 返回条数（默认10条）

**返回内容：**
- 字段的样本值列表

**使用场景：**
- 理解字段取值范围
- 识别枚举类型字段

## 典型使用流程

### 元数据补全场景
```
1. 调用 get_table_schema 获取表和字段结构
2. 调用 get_sample_data 查看数据样例
3. 如需进一步了解字段含义，调用 get_sample_values
4. 基于结构和样例，生成中文描述
```

### SQL生成场景
```
1. 调用 list_all_tables 浏览可用表
2. 调用 get_table_schema 获取相关表结构
3. 调用 get_related_tables 了解表间关系
4. 基于以上信息，构建准确的SQL查询
```

## 注意事项

- 所有工具返回的是格式化的文本，便于大模型理解
- 样本数据默认限制条数，避免返回过多数据
- 表关系查询基于外键约束，无外键的表可能无法获取关联信息
